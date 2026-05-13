#!/usr/bin/env python3
"""
Manhã ☕ — Consolidação semanal do perfil aprendido
Roda sextas à noite. Lê feedback_events não processados do user,
junta com perfil atual e pede pro Claude reescrever em texto coerente.
"""

import os
import re
import json
from datetime import datetime, timezone

from supabase import create_client
from anthropic import Anthropic

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
MODEL = os.environ.get("PROFILE_MODEL", "claude-haiku-4-5-20251001")
MAX_PROFILE_CHARS = 800

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
claude = Anthropic(api_key=ANTHROPIC_API_KEY)


def log(msg, **kv):
    extra = " ".join(f"{k}={v}" for k, v in kv.items())
    print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] {msg} {extra}".strip(), flush=True)


def consolidate_profile(user_name, current_profile, events):
    """Pede pro Claude consolidar perfil + eventos novos em um texto coeso."""
    likes  = [e["item_summary"] for e in events if e["signal"] == 1]
    dislikes = [e["item_summary"] for e in events if e["signal"] == -1]

    prompt = f"""Você está mantendo o "perfil de interesses" de {user_name} para uma newsletter de notícias personalizada.

**Perfil atual** (pode estar vazio se for a primeira vez):
{current_profile or '(vazio)'}

**Sinais novos da última semana**:
GOSTOU MAIS de:
{json.dumps(likes, ensure_ascii=False, indent=2) if likes else '(nada)'}

GOSTOU MENOS de:
{json.dumps(dislikes, ensure_ascii=False, indent=2) if dislikes else '(nada)'}

**Sua tarefa**: reescreva o perfil em PT-BR, conciso (máx {MAX_PROFILE_CHARS} caracteres), em parágrafos curtos. Estrutura sugerida:
- "Gosta de: ..." (interesses, temas, fontes, ângulos)
- "Evita: ..." (o que descartar ou despriorizar)

Regras:
- Combine sinais individuais em padrões gerais ("gosta de M&A no setor de food service" em vez de listar cada notícia)
- Mantenha o que já estava no perfil se ainda for relevante
- Se sinais novos contradizem o perfil antigo, atualize
- Linguagem direta, sem floreio
- Não invente preferências que não estão nos sinais

**Responda APENAS o texto do perfil**, sem markdown, sem prefixos, sem comentários."""

    resp = claude.messages.create(model=MODEL, max_tokens=1000,
                                   messages=[{"role": "user", "content": prompt}])
    text = resp.content[0].text.strip()
    text = re.sub(r"^```\w*\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return text[:MAX_PROFILE_CHARS]


def cleanup_expired_pauses(paused_topics, now):
    """Remove pausas expiradas."""
    out = []
    for p in paused_topics or []:
        until_str = p.get("until")
        if not until_str:
            continue
        try:
            until = datetime.fromisoformat(until_str.replace("Z","+00:00"))
            if until > now:
                out.append(p)
        except Exception:
            pass
    return out


def main():
    log("=== consolidação semanal ===")
    now = datetime.now(timezone.utc)

    # pega users com feedback não processado
    users_res = supabase.table("users").select("*").eq("active", True).execute()
    users = users_res.data or []

    for user in users:
        uid = user["id"]
        try:
            # eventos não processados
            ev_res = (supabase.table("feedback_events")
                      .select("*")
                      .eq("user_id", uid)
                      .eq("processed", False)
                      .order("created_at")
                      .execute())
            events = ev_res.data or []

            # perfil atual
            prof_res = supabase.table("user_profile").select("*").eq("user_id", uid).execute()
            prof = prof_res.data[0] if prof_res.data else {"learned_text":"", "paused_topics":[]}

            # limpa pausas expiradas
            cleaned_pauses = cleanup_expired_pauses(prof.get("paused_topics",[]), now)

            # se não tem eventos novos, só limpa pausas
            if not events:
                if cleaned_pauses != prof.get("paused_topics",[]):
                    supabase.table("user_profile").upsert({
                        "user_id": uid,
                        "learned_text": prof.get("learned_text",""),
                        "paused_topics": cleaned_pauses,
                        "updated_at": now.isoformat(),
                    }).execute()
                continue

            log(f"user {user['email']}", eventos=len(events))

            new_profile = consolidate_profile(
                user["name"], prof.get("learned_text",""), events
            )

            # upsert perfil
            supabase.table("user_profile").upsert({
                "user_id": uid,
                "learned_text": new_profile,
                "paused_topics": cleaned_pauses,
                "updated_at": now.isoformat(),
            }).execute()

            # marca eventos como processados
            event_ids = [e["id"] for e in events]
            supabase.table("feedback_events").update({"processed": True}).in_("id", event_ids).execute()

            log(f"  ✓ perfil atualizado ({len(new_profile)} chars)")

        except Exception as e:
            log(f"  ✗ ERRO {user.get('email','?')}: {e}")
            import traceback; traceback.print_exc()

    # opcional: limpa email_items antigos (>30 dias) pra não inchar
    cutoff = (now.replace(hour=0,minute=0,second=0,microsecond=0)
              .replace(day=now.day if now.day>30 else 1)).isoformat()
    # simpler: 30 dias atrás
    from datetime import timedelta
    cutoff_30d = (now - timedelta(days=30)).isoformat()
    supabase.table("email_items").delete().lt("created_at", cutoff_30d).execute()

    log("=== fim ===")


if __name__ == "__main__":
    main()
