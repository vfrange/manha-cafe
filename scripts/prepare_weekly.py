#!/usr/bin/env python3
"""
prepare_weekly.py — Fase 1 do pipeline prepare→dispatch (versão weekly)

Igual ao prepare_daily.py mas com weekly=True. Roda no sábado 6h BRT
e enfileira pra envio às 8h BRT.
"""

import os
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from supabase import create_client

import prepare_daily as pd_daily

# ============ CONFIG ============
BRT = timezone(timedelta(hours=-3))

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def log(msg, **kwargs):
    ts = datetime.now(BRT).strftime("%H:%M:%S")
    extras = " ".join(f"{k}={v}" for k, v in kwargs.items())
    print(f"[{ts}] {msg} {extras}".strip())


def main():
    now_brt = datetime.now(BRT)
    log(f"=== prepare_weekly run ===", hour=now_brt.hour)

    scheduled_for = now_brt.date().isoformat()

    res = supabase.table("users").select("*").eq("active", True).execute()
    all_users = res.data or []
    if not all_users:
        log("=== fim (nenhum user ativo) ===")
        return

    log(f"users ativos", count=len(all_users), scheduled_for=scheduled_for)

    workers = int(os.environ.get("PARALLEL_WORKERS", "5"))
    workers = max(1, min(workers, len(all_users)))
    log(f"processando em paralelo (weekly)", workers=workers)

    def _safe_prepare(u):
        try:
            status = pd_daily.prepare_user(u, now_brt, scheduled_for, weekly=True)
            return ("ok", u.get("email", "?"), status)
        except Exception as e:
            import traceback
            return ("err", u.get("email", "?"), f"{e}\n{traceback.format_exc()}")

    counts = {"enqueued": 0, "skipped": 0, "empty": 0, "err": 0}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_safe_prepare, u) for u in all_users]
        for fut in as_completed(futures):
            kind, email, info = fut.result()
            if kind == "ok":
                counts[info] = counts.get(info, 0) + 1
            else:
                counts["err"] += 1
                log(f"  ✗ ERRO {email}: {info}")

    log(f"=== fim ===", **counts)


if __name__ == "__main__":
    main()
