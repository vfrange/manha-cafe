#!/usr/bin/env python3
"""
dispatch_emails.py — Fase 2 do pipeline prepare→dispatch

Lê emails 'pending' da fila (preparados pelo prepare_daily/weekly.py)
e dispara via Resend.

- Dispara em paralelo (10 workers default)
- Idempotente: marca 'sending' antes do envio (evita duplo envio se
  rodar 2 vezes simultâneas, ex: GitHub + cron-job.org backup)
- Retry: se falhar (rede, Resend down), incrementa attempts. Após 5 tentativas, marca 'failed'
- AUTO-RECOVERY: no início, libera rows presas em 'sending' há mais de
  STUCK_THRESHOLD_MIN minutos (acontece se run anterior crashou após claim
  mas antes do envio — sem isso, a edição fica "perdida")

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

# Reusa o _supabase_retry do daily_digest pra ter retry consistente em todo
# o pipeline (backoff exponencial em RemoteProtocolError / disconnect HTTP/2).
from daily_digest import _supabase_retry

# ============ CONFIG ============
BRT = timezone(timedelta(hours=-3))

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
RESEND_KEY = os.environ["RESEND_API_KEY"]
FROM_EMAIL = os.environ.get("FROM_EMAIL", os.environ.get("RESEND_FROM", "Recorte News <hoje@recorte.news>"))

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
resend.api_key = RESEND_KEY

MAX_ATTEMPTS = 5
DISPATCH_WORKERS = int(os.environ.get("DISPATCH_WORKERS", "10"))
# Rows em status='sending' por mais de N minutos = stuck (run anterior crashou).
# Antes de buscar pending, fazemos reset desses pra 'pending' (recuperação automática).
STUCK_THRESHOLD_MIN = int(os.environ.get("STUCK_THRESHOLD_MIN", "10"))


def log(msg, **kwargs):
    ts = datetime.now(BRT).strftime("%H:%M:%S")
    extras = " ".join(f"{k}={v}" for k, v in kwargs.items())
    print(f"[{ts}] {msg} {extras}".strip())


def reset_stuck_sending(target_date, kind):
    """
    Recovery: libera rows 'sending' há mais de STUCK_THRESHOLD_MIN minutos.
    Acontece quando run anterior crashou após claim_email mas antes de
    mark_sent/mark_failed — sem isso, a edição ficaria invisível pra
    runs seguintes (que filtram status='pending').

    Usa o campo updated_at do Postgres (auto-atualizado em cada UPDATE
    via trigger ou DEFAULT). Se a tabela não tiver, cai pro fallback
    via created_at (mais conservador).
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=STUCK_THRESHOLD_MIN)).isoformat()
    # Tenta com updated_at primeiro
    try:
        q = supabase.table("email_queue").update({"status": "pending"}) \
            .eq("status", "sending").eq("scheduled_for", target_date) \
            .lt("updated_at", cutoff)
        if kind != "any":
            q = q.eq("kind", kind)
        res = _supabase_retry(lambda: q.execute(), label="reset_stuck_sending(updated_at)")
        rows = res.data or []
        if rows:
            log(f"  🔄 recovery: {len(rows)} row(s) presa(s) em 'sending' > {STUCK_THRESHOLD_MIN}min liberada(s)")
        return len(rows)
    except Exception as e:
        # Fallback: tenta com created_at (menos preciso mas funciona se updated_at não existir)
        log(f"  ⚠ updated_at indisponível ({type(e).__name__}), tentando created_at...")
        try:
            q = supabase.table("email_queue").update({"status": "pending"}) \
                .eq("status", "sending").eq("scheduled_for", target_date) \
                .lt("created_at", cutoff)
            if kind != "any":
                q = q.eq("kind", kind)
            res = _supabase_retry(lambda: q.execute(), label="reset_stuck_sending(created_at)")
            rows = res.data or []
            if rows:
                log(f"  🔄 recovery: {len(rows)} row(s) presa(s) liberadas via created_at")
            return len(rows)
        except Exception as e2:
            log(f"  ⚠ recovery falhou (não bloqueia): {e2}")
            return 0


def claim_email(queue_id, current_attempts):
    """
    Tenta marcar status='sending' (CAS — Compare And Swap).
    Retorna True se conseguiu (ninguém mais pegou esse).
    Garante atomicidade: dois workers nunca enviam o mesmo email.
    """
    res = _supabase_retry(
        lambda: supabase.table("email_queue").update({
            "status": "sending",
            "attempts": current_attempts + 1,
        }).eq("id", queue_id).eq("status", "pending").execute(),
        label="email_queue.claim",
    )
    return len(res.data or []) > 0


def mark_sent(queue_id, resend_id):
    _supabase_retry(
        lambda: supabase.table("email_queue").update({
            "status": "sent",
            "sent_at": datetime.now(timezone.utc).isoformat(),
            "resend_id": resend_id,
        }).eq("id", queue_id).execute(),
        label="email_queue.mark_sent",
    )


def mark_failed(queue_id, attempts, error_msg):
    """Após MAX_ATTEMPTS, marca como 'failed'. Antes disso, volta pra 'pending'."""
    if attempts >= MAX_ATTEMPTS:
        new_status = "failed"
    else:
        new_status = "pending"  # vai tentar de novo na próxima execução
    try:
        _supabase_retry(
            lambda: supabase.table("email_queue").update({
                "status": new_status,
                "error": error_msg[:500],  # trunca erros gigantes
            }).eq("id", queue_id).execute(),
            label=f"email_queue.mark_failed({new_status})",
        )
    except Exception as e:
        # CRÍTICO: se mark_failed também falhar, a row fica presa em 'sending'.
        # O auto-recovery (reset_stuck_sending) no próximo run vai liberá-la.
        log(f"  ✗ mark_failed falhou após retry — row qid={queue_id} ficará em 'sending' "
            f"até recovery automático (>{STUCK_THRESHOLD_MIN}min): {e}")


def get_user_email(user_id):
    """Busca email do user a partir do user_id da fila."""
    res = _supabase_retry(
        lambda: supabase.table("users").select("email,name,active").eq("id", user_id).single().execute(),
        label="users.select(by_id)",
    )
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
            _supabase_retry(
                lambda: supabase.table("email_queue").update({"status": "skipped", "error": "user inativo"}).eq("id", qid).execute(),
                label="email_queue.skip(inactive)",
            )
            return ("skipped", qid, "user inativo")

        # 3. Envia (com headers RFC 8058 List-Unsubscribe pra deliverability + opt-out 1-click)
        unsub_url = gen_unsub_url(SUPABASE_URL, user_id)
        edition_id = queue_row.get("edition_id")
        kind = queue_row.get("kind", "daily")

        # Tags Resend pra que o webhook consiga mapear o evento → user/edition
        tags = [
            {"name": "kind", "value": str(kind)},
            {"name": "user_id", "value": str(user_id)},
        ]
        if edition_id:
            tags.append({"name": "edition_id", "value": str(edition_id)})

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
            "tags": tags,
        })
        resend_id = result.get("id") if isinstance(result, dict) else None

        # 4. Marca sent
        mark_sent(qid, resend_id)

        # 4b. Marca edition como enviada (sent_at + resend_id)
        if edition_id:
            try:
                from tracking import finalize_edition
                finalize_edition(supabase, edition_id, resend_id)
            except Exception as e:
                log(f"  ⚠ finalize_edition falhou: {e}")

        # 5. Atualiza last_sent_at do user (pra o dedup do prepare funcionar)
        now_iso = datetime.now(timezone.utc).isoformat()
        user_updates = {
            "last_sent_at": now_iso,
            "welcome_sent": True,
        }
        # Se era o primeiro envio (welcome_sent ainda false), grava timestamp.
        # Necessário pra prepare_weekly pular o weekly do mesmo dia.
        if not user.get("welcome_sent"):
            user_updates["welcome_sent_at"] = now_iso
        try:
            _supabase_retry(
                lambda: supabase.table("users").update(user_updates).eq("id", user_id).execute(),
                label="users.update(last_sent)",
            )
        except Exception as e:
            # Email já foi enviado, só perdemos o tracking de last_sent_at.
            # Não propaga — não faz sentido marcar dispatch como failed se o email saiu.
            log(f"  ⚠ users.update last_sent falhou (email já enviado): {e}")

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

    # AUTO-RECOVERY: libera rows presas em 'sending' antes de buscar pending.
    # Sem isso, edições de runs que crasharam após claim ficam invisíveis.
    reset_stuck_sending(target_date, args.kind)

    # Busca pending
    q = supabase.table("email_queue").select("*") \
        .eq("status", "pending").eq("scheduled_for", target_date)
    if args.kind != "any":
        q = q.eq("kind", args.kind)
    res = _supabase_retry(lambda: q.execute(), label="email_queue.select(pending)")
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
