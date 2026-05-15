#!/usr/bin/env python3
"""
dispatch_emails.py — Fase 2 do pipeline prepare→dispatch

Lê emails 'pending' da fila (preparados pelo prepare_daily/weekly.py)
e dispara via Resend.

- Dispara em paralelo (10 workers default)
- Idempotente: marca 'sending' antes do envio (evita duplo envio se
  rodar 2 vezes simultâneas, ex: GitHub + cron-job.org backup)
- Retry: se falhar (rede, Resend down), incrementa attempts. Após 5 tentativas, marca 'failed'

Uso:
    python dispatch_emails.py --kind daily     # default
    python dispatch_emails.py --kind weekly
    python dispatch_emails.py --kind any       # dispara qualquer pending
    python dispatch_emails.py --date 2026-05-15
"""

import os
import sys
import argparse
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import resend
from supabase import create_client

from feedback_token import unsub_url as gen_unsub_url

# ============ CONFIG ============
BRT = timezone(timedelta(hours=-3))

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
RESEND_KEY = os.environ["RESEND_API_KEY"]
FROM_EMAIL = os.environ.get("RESEND_FROM", "Recorte News <hoje@recorte.news>")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
resend.api_key = RESEND_KEY

MAX_ATTEMPTS = 5
DISPATCH_WORKERS = int(os.environ.get("DISPATCH_WORKERS", "10"))


def log(msg, **kwargs):
    ts = datetime.now(BRT).strftime("%H:%M:%S")
    extras = " ".join(f"{k}={v}" for k, v in kwargs.items())
    print(f"[{ts}] {msg} {extras}".strip())


def claim_email(queue_id, current_attempts):
    """
    Tenta marcar status='sending' (CAS — Compare And Swap).
    Retorna True se conseguiu (ninguém mais pegou esse).
    Garante atomicidade: dois workers nunca enviam o mesmo email.
    """
    res = supabase.table("email_queue").update({
        "status": "sending",
        "attempts": current_attempts + 1,
    }).eq("id", queue_id).eq("status", "pending").execute()
    return len(res.data or []) > 0


def mark_sent(queue_id, resend_id):
    supabase.table("email_queue").update({
        "status": "sent",
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "resend_id": resend_id,
    }).eq("id", queue_id).execute()


def mark_failed(queue_id, attempts, error_msg):
    """Após MAX_ATTEMPTS, marca como 'failed'. Antes disso, volta pra 'pending'."""
    if attempts >= MAX_ATTEMPTS:
        new_status = "failed"
    else:
        new_status = "pending"  # vai tentar de novo na próxima execução
    supabase.table("email_queue").update({
        "status": new_status,
        "error": error_msg[:500],  # trunca erros gigantes
    }).eq("id", queue_id).execute()


def get_user_email(user_id):
    """Busca email do user a partir do user_id da fila."""
    res = supabase.table("users").select("email,name,active").eq("id", user_id).single().execute()
    return res.data


def dispatch_one(queue_row):
    """Envia 1 email. Idempotente: usa claim_email atomic."""
    qid = queue_row["id"]
    user_id = queue_row["user_id"]
    current_attempts = queue_row.get("attempts", 0)

    # 1. Claim (atomic)
    if not claim_email(qid, current_attempts):
        return ("skipped", qid, "outro worker pegou")

    try:
        # 2. Busca email do usuário (não armazenado na queue pra respeitar privacidade)
        user = get_user_email(user_id)
        if not user:
            mark_failed(qid, MAX_ATTEMPTS, "user não encontrado")
            return ("failed", qid, "user não encontrado")
        if not user.get("active"):
            supabase.table("email_queue").update({"status": "skipped", "error": "user inativo"}).eq("id", qid).execute()
            return ("skipped", qid, "user inativo")

        # 3. Envia (com headers RFC 8058 List-Unsubscribe pra deliverability + opt-out 1-click)
        unsub_url = gen_unsub_url(SUPABASE_URL, user_id)
        result = resend.Emails.send({
            "from": FROM_EMAIL,
            "to": user["email"],
            "subject": queue_row["subject"],
            "html": queue_row["html"],
            "headers": {
                # RFC 2369: link de unsubscribe (Gmail/Apple Mail mostram botão "Cancelar inscrição" no header)
                "List-Unsubscribe": f"<{unsub_url}>, <mailto:unsubscribe@recorte.news?subject=Unsubscribe>",
                # RFC 8058: indica que o link suporta opt-out one-click (sem confirmação intermediária)
                "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
            },
        })
        resend_id = result.get("id") if isinstance(result, dict) else None

        # 4. Marca sent
        mark_sent(qid, resend_id)

        # 5. Atualiza last_sent_at do user (pra o dedup do prepare funcionar)
        supabase.table("users").update({
            "last_sent_at": datetime.now(timezone.utc).isoformat(),
            "welcome_sent": True,
        }).eq("id", user_id).execute()

        return ("sent", qid, user["email"])

    except Exception as e:
        mark_failed(qid, current_attempts + 1, str(e))
        return ("error", qid, str(e))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kind", choices=["daily", "weekly", "any"], default="any",
                        help="Tipo de email pra disparar")
    parser.add_argument("--date", help="Data específica (YYYY-MM-DD). Default: hoje BRT")
    args = parser.parse_args()

    now_brt = datetime.now(BRT)
    target_date = args.date or now_brt.date().isoformat()

    log(f"=== dispatch run ===", kind=args.kind, date=target_date)

    # Busca pending
    q = supabase.table("email_queue").select("*") \
        .eq("status", "pending").eq("scheduled_for", target_date)
    if args.kind != "any":
        q = q.eq("kind", args.kind)
    res = q.execute()
    pending = res.data or []

    log(f"pending na fila", count=len(pending))

    if not pending:
        log("=== fim (nada pra disparar) ===")
        return

    workers = max(1, min(DISPATCH_WORKERS, len(pending)))
    log(f"disparando em paralelo", workers=workers)

    counts = {"sent": 0, "failed": 0, "skipped": 0, "error": 0}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(dispatch_one, row) for row in pending]
        for fut in as_completed(futures):
            status, qid, info = fut.result()
            counts[status] = counts.get(status, 0) + 1
            if status == "sent":
                log(f"  ✓ enviado", to=info)
            elif status == "skipped":
                log(f"  ⏭ skip qid={qid}: {info}")
            else:
                log(f"  ✗ {status} qid={qid}: {info}")

    log(f"=== fim ===", **counts)


if __name__ == "__main__":
    main()
