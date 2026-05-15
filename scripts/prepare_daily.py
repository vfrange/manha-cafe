#!/usr/bin/env python3
"""
prepare_daily.py — Fase 1 do pipeline prepare→dispatch

Faz coleta + curadoria + render do email, mas em vez de ENVIAR,
salva o HTML pronto na fila `email_queue` com status='pending'.

O dispatcher (dispatch_emails.py) lê essa fila no horário exato
e dispara via Resend.

Vantagens:
- Email chega às 6h00 exatas (não 6h15 ou 8h)
- Tempo de coleta+IA não afeta horário de envio (buffer de 2h)
- Se falhar, dispatcher pode tentar de novo
- Cron externo pode disparar (não depende só do GitHub Actions)

Roda em paralelo (5 workers). Idempotente: se já existe email enfileirado
pra (user, hoje, daily), não cria de novo.
"""

import os
import re
import json
import time
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from supabase import create_client
from anthropic import Anthropic

# Importa tudo do daily_digest pra reaproveitar lógica
import daily_digest as dd
from feedback_token import manage_url as gen_manage_url
from email_template import render_email

# ============ CONFIG ============
BRT = timezone(timedelta(hours=-3))

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
MANAGE_URL = os.environ["MANAGE_URL"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def log(msg, **kwargs):
    ts = datetime.now(BRT).strftime("%H:%M:%S")
    extras = " ".join(f"{k}={v}" for k, v in kwargs.items())
    print(f"[{ts}] {msg} {extras}".strip())


def already_queued(user_id, scheduled_for, kind="daily"):
    """Verifica se já existe email enfileirado pra (user, data, tipo)."""
    res = supabase.table("email_queue").select("id,status").eq("user_id", user_id) \
        .eq("scheduled_for", scheduled_for).eq("kind", kind).execute()
    rows = res.data or []
    if not rows:
        return None
    # se já tem qualquer registro (pending, sent, sending, failed), retorna ele
    return rows[0]


def enqueue_email(user_id, kind, scheduled_for, subject, html):
    """Insere na fila com status='pending'. Idempotente via unique index."""
    try:
        supabase.table("email_queue").insert({
            "user_id": user_id,
            "kind": kind,
            "scheduled_for": scheduled_for,
            "subject": subject,
            "html": html,
            "status": "pending",
            "attempts": 0,
        }).execute()
        return True
    except Exception as e:
        # se for duplicate key, é ok (algum outro worker enfileirou primeiro)
        if "duplicate" in str(e).lower() or "23505" in str(e):
            log(f"  ⚠ já enfileirado (race condition), pulando user_id={user_id}")
            return False
        raise


def prepare_user(user, now_brt, scheduled_for, weekly=False):
    """
    Versão modificada do process_user que ENFILEIRA em vez de enviar.
    Reaproveita 100% da lógica de coleta + curadoria + render do daily_digest.
    """
    uid = user["id"]
    email = user["email"]
    kind = "weekly" if weekly else "daily"

    # Checa idempotência
    existing = already_queued(uid, scheduled_for, kind)
    if existing:
        log(f"  ⏭ {email}: já existe na fila (status={existing.get('status')})")
        return "skipped"

    log(f"preparando", email=email, kind=kind, scheduled_for=scheduled_for)

    # === Coleta + curadoria (mesma lógica do daily_digest.process_user) ===
    default_country = user.get("default_country") or "BR"

    profile = dd.load_profile(uid)
    learned = profile.get("learned_text", "") or ""
    paused = profile.get("paused_topics", []) or []

    # Trending
    trending = []
    trending_label = ""
    if user.get("trending_enabled", True):
        raw_scope = user.get("trending_scope") or "br"
        scopes = [s.strip() for s in raw_scope.split(",") if s.strip()] or ["br"]
        labels = []
        for scope in scopes:
            if scope == "global":
                tcountry, tlabel = "GLOBAL", "🌍 Mundo"
            elif scope == "br":
                tcountry, tlabel = "BR", "🇧🇷 Brasil"
            elif scope.startswith("country:"):
                tcountry = scope.split(":", 1)[1] or "BR"
                tlabel = f"🎯 {dd.COUNTRY_NAMES.get(tcountry, tcountry)}"
            else:
                tcountry = user.get("trending_country") or "BR"
                tlabel = dd.COUNTRY_NAMES.get(tcountry, tcountry)
            labels.append(tlabel)
            raw_trends = dd.fetch_trending(tcountry)
            log(f"  trends brutos", count=len(raw_trends), scope=tcountry)
            if raw_trends:
                curated = dd.curate_trends(user["name"], tlabel, raw_trends, learned)
                for item in curated:
                    item.setdefault("scope_origin", tlabel)
                trending.extend(curated)
        trending_label = " + ".join(labels) if labels else ""

    # Notícias por tema
    topics_res = supabase.table("topics").select("*").eq("user_id", uid).execute()
    topics = topics_res.data or []
    fallback_country = "GLOBAL" if default_country == "INTL" else default_country

    by_label = {}
    for t in topics:
        if dd.is_topic_paused(t["label"], paused, datetime.now(timezone.utc)):
            continue
        country = t.get("country") or fallback_country
        category = t.get("category")
        news = dd.fetch_all_sources(
            t["query"], country, category=category,
            label=t["label"], source_type=t.get("source", "curated")
        )
        if not news:
            continue
        for n in news:
            n.setdefault("scope_origin", country)
        if t["label"] not in by_label:
            by_label[t["label"]] = {
                "label": t["label"], "country": country,
                "scopes": [], "topic_id": t["id"], "news": []
            }
        by_label[t["label"]]["scopes"].append(country)
        by_label[t["label"]]["news"].extend(news)

    # Dedup por URL
    topics_with_news = []
    for group in by_label.values():
        seen = set()
        deduped = []
        for n in group["news"]:
            url = n.get("link") or n.get("url") or ""
            if url and url in seen:
                continue
            seen.add(url)
            deduped.append(n)
        group["news"] = deduped
        topics_with_news.append(group)

    sections = []
    if topics_with_news:
        raw_sections = dd.curate_news(user["name"], topics_with_news, learned)
        label_meta = {t["label"]: {"topic_id": t["topic_id"], "scopes": t["scopes"]} for t in topics_with_news}
        link_to_lang = {}
        for t in topics_with_news:
            for n in t.get("news", []):
                ln = n.get("link", "")
                lg = (n.get("lang") or "").lower()
                if ln and lg and lg != "pt":
                    link_to_lang[ln] = lg
        for s in raw_sections:
            if s.get("noticias"):
                clean = [n for n in s["noticias"] if n.get("manchete") and n.get("resumo")]
                if not clean:
                    continue
                s["noticias"] = clean
                tema = s.get("tema", "")
                meta = label_meta.get(tema, {})
                scopes = meta.get("scopes", [])
                if scopes:
                    flag_parts = [dd.COUNTRY_NAMES.get(sc, sc).split()[0] for sc in scopes]
                    country_label = " + ".join(flag_parts)
                else:
                    country_label = s.get("pais", "")
                for noticia in s["noticias"]:
                    lk = noticia.get("link", "")
                    if lk in link_to_lang:
                        noticia["lang"] = link_to_lang[lk]
                sections.append({
                    "topic": tema,
                    "topic_id": meta.get("topic_id"),
                    "country_label": country_label,
                    "noticias": s["noticias"],
                })

    if not trending and not sections:
        log(f"  ⏭ {email}: nada pra mandar, pulando enfileiramento")
        return "empty"

    # Dedup cruzado trends ↔ sections (mesma lógica do daily_digest)
    if trending and sections:
        dd._dedupe_sections_against_trends(sections, trending)
        sections = [s for s in sections if s.get("noticias")]

    # Feedback links
    sections = dd.add_feedback_links(uid, sections)

    # Recap
    recap_data = dd.generate_daily_recap(user["name"], sections, trending, learned)
    daily_recap = recap_data.get("recap", "") if isinstance(recap_data, dict) else ""
    daily_quote = recap_data.get("quote", "") if isinstance(recap_data, dict) else ""
    daily_quote_author = recap_data.get("quote_author", "") if isinstance(recap_data, dict) else ""

    # Render
    signed_manage = gen_manage_url(MANAGE_URL, uid, ttl_days=30)
    email_mode = (user.get("email_mode") or "coado").lower()

    html = render_email(
        user_name=user["name"], date_obj=now_brt,
        trending=trending, trending_label=trending_label,
        sections=sections, manage_url=signed_manage,
        user_id=uid,
        daily_recap=daily_recap, daily_quote=daily_quote, daily_quote_author=daily_quote_author,
        email_mode=email_mode, weekly_mode=weekly,
    )

    # Subject
    first = user["name"].split()[0] if user.get("name") else "Você"
    if weekly:
        subject = f"🗞 Seu Recorte da Semana, {first} — {now_brt.strftime('%d/%m')}"
    else:
        subject = f"☕ Seu Recorte de hoje, {first} — {now_brt.strftime('%d/%m')}"

    # Enfileira
    ok = enqueue_email(uid, kind, scheduled_for, subject, html)
    if ok:
        log(f"  ✓ {email}: enfileirado ({len(html)} chars HTML)")
        return "enqueued"
    return "skipped"


def main():
    now_brt = datetime.now(BRT)
    log(f"=== prepare_daily run ===", hour=now_brt.hour)

    # scheduled_for = HOJE em BRT (será disparado às 6h dessa mesma data BRT)
    scheduled_for = now_brt.date().isoformat()

    # Pega usuários ativos
    res = supabase.table("users").select("*").eq("active", True).execute()
    all_users = res.data or []
    if not all_users:
        log("=== fim (nenhum user ativo) ===")
        return

    log(f"users ativos", count=len(all_users), scheduled_for=scheduled_for)

    # Paraleliza preparação
    workers = int(os.environ.get("PARALLEL_WORKERS", "5"))
    workers = max(1, min(workers, len(all_users)))
    log(f"processando em paralelo", workers=workers)

    def _safe_prepare(u):
        try:
            status = prepare_user(u, now_brt, scheduled_for, weekly=False)
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
