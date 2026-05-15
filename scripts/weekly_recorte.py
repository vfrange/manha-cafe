#!/usr/bin/env python3
"""
weekly_recorte.py — Recorte da Semana (sábado 10h BRT).
Pra cada user ativo, envia uma edição especial: 5 temas top + análise retrospectiva.
Reutiliza process_user() do daily_digest com flag weekly=True.
"""
import os
import sys
from datetime import datetime

from daily_digest import BRT, supabase, log, process_user


def main():
    now_brt = datetime.now(BRT)
    log(f"=== Recorte da Semana run === {now_brt.isoformat()}")

    res = supabase.table("users").select("*").eq("active", True).execute()
    users = res.data or []
    log(f"users ativos: {len(users)}")

    sent = 0
    for user in users:
        # Não envia 2x na mesma semana
        last = user.get("last_weekly_sent_at")
        if last:
            try:
                last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
                # se enviou nas últimas 6 dias, pula
                if (now_brt - last_dt.astimezone(BRT)).days < 6:
                    log(f"  ⏭ {user['email']}: weekly enviado há <6 dias, skip")
                    continue
            except Exception:
                pass

        try:
            ok = process_user(user, now_brt, weekly=True)
            if ok:
                sent += 1
                supabase.table("users").update({
                    "last_weekly_sent_at": now_brt.isoformat(),
                }).eq("id", user["id"]).execute()
        except Exception as e:
            log(f"  ✗ erro em {user.get('email','?')}: {e}")

    log(f"=== Recorte da Semana finalizado: {sent}/{len(users)} enviados ===")


if __name__ == "__main__":
    main()
