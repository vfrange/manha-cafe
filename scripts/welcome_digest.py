#!/usr/bin/env python3
"""
welcome_digest.py — processa UM usuário específico (welcome email instantâneo).
Lê env USER_ID e dispara process_user() do daily_digest.py para esse user só.
"""
import os
import sys
from datetime import datetime

from daily_digest import (
    BRT, supabase, log, process_user,
)

USER_ID = os.environ.get("USER_ID", "").strip()


def main():
    if not USER_ID:
        log("✗ USER_ID env ausente")
        sys.exit(1)

    now_brt = datetime.now(BRT)
    log(f"=== Welcome Recorte === user_id={USER_ID}")

    res = supabase.table("users").select("*").eq("id", USER_ID).maybeSingle().execute()
    user = res.data

    if not user:
        log(f"✗ user não encontrado: {USER_ID}")
        sys.exit(1)

    if not user.get("active", True):
        log(f"✗ user inativo, pulando")
        sys.exit(0)

    if user.get("welcome_sent"):
        log(f"⚠ welcome já enviado anteriormente, ignorando")
        sys.exit(0)

    log(f"processando welcome email={user['email']}")
    ok = process_user(user, now_brt)
    if ok:
        log(f"✓ welcome enviado e marcado")
    else:
        log(f"✗ falha no envio")
        sys.exit(1)


if __name__ == "__main__":
    main()
