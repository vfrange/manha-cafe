#!/usr/bin/env python3
"""
prepare_daily.py — Fase 1 do pipeline prepare→dispatch

Faz coleta + curadoria + render do email, mas em vez de ENVIAR,
salva o HTML pronto na fila `email_queue` com status='pending'.

O dispatcher (dispatch_emails.py) lê essa fila no horário exato
e dispara via Resend.
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
from feedback_token import manage_url as gen_manage_url, unsub_url as gen_unsub_url
from email_template import render_email
from hallucination_guard import (
    validate_and_clean_sections,
    validate_and_clean_trending,
)

# ============ CONFIG ============
BRT = timezone(timedelta(hours=-3))

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
MANAGE_URL = os.environ["MANAGE_URL"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
# Reusa o claude do dd (mesmo client) mas também tem o nosso pra validação
claude = Anthropic(api_key=ANTHROPIC_API_KEY)
MODEL = "claude-haiku-4-5-20251001"


def log(msg, **kwargs):
    ts = datetime.now(BRT).strftime("%H:%M:%S")
    extras = " ".join(f"{k}={v}" for k, v in kwargs.items())
    print(f"[{ts}] {msg} {extras}".strip())


def already_queued(user_id, scheduled_for, kind="daily"):
    """Verifica se já existe email enfileirado pra (user, data, tipo)."""
    res = dd._supabase_retry(
        lambda: supabase.table("email_queue").select("id,status").eq("user_id", user_id)
            .eq("scheduled_for", scheduled_for).eq("kind", kind).execute(),
        label="email_queue.select(check)",
    )
    rows = res.data or []
    if not rows:
        return None
    return rows[0]


def enqueue_email(user_id, kind, scheduled_for, subject, html, edition_id=None):
    """Insere na fila com status='pending'. Idempotente via unique index."""
    try:
        row = {
            "user_id": user_id,
            "kind": kind,
            "scheduled_for": scheduled_for,
            "subject": subject,
            "html": html,
            "status": "pending",
            "attempts": 0,
        }
        if edition_id:
            row["edition_id"] = edition_id
        dd._supabase_retry(
            lambda: supabase.table("email_queue").insert(row).execute(),
            label="email_queue.insert",
        )
        return True
    except Exception as e:
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

    if weekly:
        welcome_at = user.get("welcome_sent_at")
        if welcome_at:
            try:
                w_dt = datetime.fromisoformat(welcome_at.replace("Z", "+00:00"))
                w_brt_date = w_dt.astimezone(now_brt.tzinfo).date().isoformat()
                if w_brt_date == scheduled_for:
                    log(f"  ⏭ {email}: welcome foi hoje ({w_brt_date}), pulando weekly")
                    return "skipped"
            except Exception as e:
                log(f"  ⚠ {email}: erro ao parsear welcome_sent_at='{welcome_at}': {e}")

    existing = already_queued(uid, scheduled_for, kind)
    if existing:
        log(f"  ⏭ {email}: já existe na fila (status={existing.get('status')})")
        return "skipped"

    log(f"preparando", email=email, kind=kind, scheduled_for=scheduled_for)

    # PATCH ANTI-ALUCINAÇÃO: gera edition_id cedo pra usar no log de validação
    from tracking import gen_edition_id, save_edition, wrap_links_in_html
    edition_id = gen_edition_id()

    default_country = user.get("default_country") or "BR"

    profile = dd.load_profile(uid)
    learned = profile.get("learned_text", "") or ""
    paused = profile.get("paused_topics", []) or []
    filtered_items = profile.get("filtered_items", []) or []
    if filtered_items:
        log(f"  filtros do user: {len(filtered_items)} itens")

    _topics_pre = dd._supabase_retry(
        lambda: supabase.table("topics").select("label").eq("user_id", uid).execute(),
        label="topics.select(pre)",
    )
    user_topic_labels = [t["label"] for t in (_topics_pre.data or [])]
    unique_topic_count = len({lbl for lbl in user_topic_labels}) if user_topic_labels else 0

    stale_window_h = dd.get_stale_window_hours(weekly, now_brt)
    log(f"  📅 janela frescor: {stale_window_h}h ({'weekly' if weekly else ('segunda' if now_brt.weekday() == 0 else 'daily')})")

    # Trending
    trending = []
    trending_label = ""
    raw_trends_combined = []
    if user.get("trending_enabled", True):
        raw_scope = user.get("trending_scope") or "br"
        scopes = [s.strip() for s in raw_scope.split(",") if s.strip()] or ["br"]
        labels = []
        if weekly:
            TOTAL_TRENDING_BUDGET = dd.weekly_trending_budget(unique_topic_count)
        else:
            TOTAL_TRENDING_BUDGET = dd.daily_trending_budget(unique_topic_count)
        budget_per_scope = max(2, TOTAL_TRENDING_BUDGET // len(scopes) + 1)
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
            # PATCH ANTI-ALUCINAÇÃO: passa max_age_hours pro fetch_trending
            raw_trends = dd.fetch_trending(tcountry, weekly=weekly, max_age_hours=stale_window_h)
            log(f"  trends brutos", count=len(raw_trends), scope=tcountry, weekly=weekly)
            raw_trends_combined.extend(raw_trends)
            if raw_trends:
                curated = dd.curate_trends(
                    user["name"], tlabel, raw_trends, learned,
                    user_topics_labels=user_topic_labels,
                    filtered_items=filtered_items,
                    max_out=budget_per_scope,
                    weekly=weekly,
                )
                for item in curated:
                    item.setdefault("scope_origin", tlabel)
                trending.extend(curated)
        trending_label = " + ".join(labels) if labels else ""

        if len(trending) > TOTAL_TRENDING_BUDGET:
            by_scope_origin = {}
            for item in trending:
                origin = item.get("scope_origin", "")
                by_scope_origin.setdefault(origin, []).append(item)
            balanced = []
            scope_lists = list(by_scope_origin.values())
            while len(balanced) < TOTAL_TRENDING_BUDGET and any(scope_lists):
                for lst in scope_lists:
                    if lst and len(balanced) < TOTAL_TRENDING_BUDGET:
                        balanced.append(lst.pop(0))
            trending = balanced

        # PATCH ANTI-ALUCINAÇÃO: valida trending
        if trending and raw_trends_combined:
            try:
                trend_stats = validate_and_clean_trending(
                    trending, raw_trends_combined, supabase, uid, edition_id,
                    claude, MODEL,
                )
                if trend_stats.get("discarded_critical") or trend_stats.get("discarded_moderate") or trend_stats.get("rewritten"):
                    log(f"  🛡 anti-aluc trending: ok={trend_stats['ok']} reescritos={trend_stats['rewritten']} "
                        f"descartados={trend_stats['discarded_critical']+trend_stats['discarded_moderate']} "
                        f"(crit={trend_stats['discarded_critical']}/mod={trend_stats['discarded_moderate']})")
                if len(trending) > TOTAL_TRENDING_BUDGET:
                    trending = trending[:TOTAL_TRENDING_BUDGET]
            except Exception as e:
                log(f"  ⚠ anti-aluc trending falhou (não bloqueia): {e}")

    # Notícias por tema
    topics_res = dd._supabase_retry(
        lambda: supabase.table("topics").select("*").eq("user_id", uid).execute(),
        label="topics.select(full)",
    )
    topics = topics_res.data or []
    fallback_country = "GLOBAL" if default_country == "INTL" else default_country

    unique_labels = {t["label"] for t in topics}
    topic_count_for_scaling = len(unique_labels)
    if weekly:
        news_per_topic = dd.weekly_news_per_topic(topic_count_for_scaling)
    else:
        news_per_topic = dd.daily_news_per_topic(topic_count_for_scaling)

    is_welcome = not user.get("welcome_sent")

    by_label = {}
    for t in topics:
        if dd.is_topic_paused(t["label"], paused, datetime.now(timezone.utc)):
            continue
        country = t.get("country") or fallback_country
        category = t.get("category")
        news = dd.fetch_all_sources(
            t["query"], country, category=category,
            label=t["label"], source_type=t.get("source", "curated"),
            weekly=weekly,
            max_age_hours=stale_window_h,
        )
        if not news:
            continue
        for n in news:
            n.setdefault("scope_origin", country)
        if t["label"] not in by_label:
            by_label[t["label"]] = {
                "label": t["label"], "country": country,
                "scopes": [], "topic_id": t["id"],
                "source": t.get("source", "curated"),
                "news": []
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

    # URL VALIDATION
    try:
        from sources.utils import filter_valid_urls
        for group in topics_with_news:
            original_count = len(group["news"])
            gnews = [n for n in group["news"] if "news.google.com" in (n.get("link") or "")]
            others = [n for n in group["news"] if "news.google.com" not in (n.get("link") or "")]
            validated_others = filter_valid_urls(others, url_key="link") if others else []
            group["news"] = gnews + validated_others
            removed = original_count - len(group["news"])
            if removed:
                log(f"  🔗 {group['label']}: removidas {removed}/{original_count} URLs inválidas (não-GNews)")
    except Exception as e:
        log(f"  ⚠ URL validation falhou (não bloqueia): {e}")

    # RESOLVE GNEWS PRE-CURATE
    try:
        gnews_targets = []
        for group in topics_with_news:
            for n in group["news"]:
                if "news.google.com" in (n.get("link") or ""):
                    gnews_targets.append(n)
        if gnews_targets:
            log(f"  🔓 pré-decodificando {len(gnews_targets)} URL(s) Google News antes do Claude curar...")
            def _resolve_pre(item):
                url = item.get("link", "")
                new_url = dd._try_decode_gnews_url(url)
                if new_url != url:
                    item["link"] = new_url
                    return True
                return False
            resolved_pre = 0
            with ThreadPoolExecutor(max_workers=6) as ex:
                results = list(ex.map(_resolve_pre, gnews_targets))
                resolved_pre = sum(1 for r in results if r)
            log(f"  ✓ {resolved_pre}/{len(gnews_targets)} URLs Google News decodificadas pré-curate")
            removed_pre = 0
            for group in topics_with_news:
                before = len(group["news"])
                group["news"] = [n for n in group["news"]
                                 if "news.google.com" not in (n.get("link") or "")]
                removed_pre += before - len(group["news"])
            if removed_pre > 0:
                log(f"  🚫 removidas {removed_pre} notícia(s) com wrapper Gnews não decodificado (antes do Claude)")
    except Exception as e:
        log(f"  ⚠ resolve gnews pré-curate falhou (não bloqueia): {e}")

    # DEDUP 5D
    try:
        sent_links, sent_title_sigs = dd._load_recently_sent_signatures(uid, days=5)
        if sent_links or sent_title_sigs:
            removed_5d = dd._filter_already_sent(topics_with_news, sent_links, sent_title_sigs)
            if removed_5d > 0:
                log(f"  🔁 dedup 5d: removidas {removed_5d} notícia(s) já enviada(s) recentemente "
                    f"(banco: {len(sent_links)} URLs + {len(sent_title_sigs)} signatures)")
            topics_with_news = [g for g in topics_with_news if g.get("news")]
    except Exception as e:
        log(f"  ⚠ dedup 5d cross-edição falhou (não bloqueia): {e}")

    topics_with_news.sort(key=lambda g: 0 if g.get("source") == "custom" else 1)

    sections = []
    if topics_with_news:
        raw_sections = dd.curate_news(
            user["name"], topics_with_news, learned,
            filtered_items=filtered_items,
            weekly=weekly,
            news_per_topic=news_per_topic,
            is_welcome=is_welcome,
        )
        # PATCH ANTI-ALUCINAÇÃO: valida cada notícia curada contra fonte bruta
        try:
            val_stats = validate_and_clean_sections(
                raw_sections, topics_with_news, supabase, uid, edition_id,
                claude, MODEL,
            )
            if val_stats.get("discarded_critical") or val_stats.get("discarded_moderate") or val_stats.get("rewritten"):
                log(f"  🛡 anti-aluc sections: ok={val_stats['ok']} reescritos={val_stats['rewritten']} "
                    f"descartados={val_stats['discarded_critical']+val_stats['discarded_moderate']} "
                    f"(crit={val_stats['discarded_critical']}/mod={val_stats['discarded_moderate']}) "
                    f"sem_fonte={val_stats['no_source']}")
            # Trunca cada seção pra news_per_topic (depois do buffer +2 do Claude)
            for s in raw_sections:
                if s.get("noticias"):
                    s["noticias"] = s["noticias"][:news_per_topic]
        except Exception as e:
            log(f"  ⚠ anti-aluc sections falhou (não bloqueia): {e}")

        label_meta = {t["label"]: {"topic_id": t["topic_id"], "scopes": t["scopes"]} for t in topics_with_news}
        link_to_lang = {}
        link_to_img = {}
        for t in topics_with_news:
            for n in t.get("news", []):
                ln = n.get("link", "")
                lg = (n.get("lang") or "").lower()
                if ln and lg and lg != "pt":
                    link_to_lang[ln] = lg
                img = n.get("img_url") or ""
                if ln and img:
                    link_to_img[ln] = img
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
                    if lk in link_to_img:
                        noticia["img_url"] = link_to_img[lk]
                sections.append({
                    "topic": tema,
                    "topic_id": meta.get("topic_id"),
                    "country_label": country_label,
                    "noticias": s["noticias"],
                })

    if not trending and not sections:
        log(f"  ⏭ {email}: nada pra mandar, pulando enfileiramento")
        return "empty"

    if trending and sections:
        dd._dedupe_sections_against_trends(sections, trending)
        sections = [s for s in sections if s.get("noticias")]

    # ============ SAIBA ANTES DE TODOS (undercovered) ============
    # Sinal fraco / não coberto pela imprensa BR mainstream.
    # Aplica TODAS as regras dos outros capítulos: temas do user, filtered_items,
    # learned_text, paused_topics, dedup 5d, frescor, anti-aluc, og:image.
    # Opt-in: default ATIVADO, user pode desativar via preferência undercovered_enabled.
    undercovered = []
    if not user.get("undercovered_enabled", True):
        log(f"  ⏭ saiba_antes desativado pelo user")
    else:
        try:
            scope_for_uc = "BR" if user.get("default_country", "BR") in ("BR", None) else "GLOBAL"
            uc_raw = dd.fetch_undercovered(
                country=scope_for_uc,
                weekly=weekly,
                max_age_hours=stale_window_h,
            )
            log(f"  📡 saiba_antes brutos count={len(uc_raw)}")

            # FILTRO: paused_topics (se algum candidato bate com tema pausado, descarta)
            if paused and uc_raw:
                before_paused = len(uc_raw)
                uc_raw = [
                    it for it in uc_raw
                    if not dd.is_topic_paused(it.get("title", "")[:80], paused, datetime.now(timezone.utc))
                ]
                removed_paused = before_paused - len(uc_raw)
                if removed_paused > 0:
                    log(f"  ⏸ saiba_antes: removidos {removed_paused} de temas pausados")

            # FILTRO: resolve URLs do Google News (CVM usa GNews wrapper)
            try:
                gnews_targets = [it for it in uc_raw if "news.google.com" in (it.get("link") or "")]
                if gnews_targets:
                    log(f"  🔓 saiba_antes: pré-decodificando {len(gnews_targets)} URL(s) Google News...")
                    def _resolve_uc(item):
                        url = item.get("link", "")
                        new_url = dd._try_decode_gnews_url(url)
                        if new_url != url:
                            item["link"] = new_url
                            return True
                        return False
                    with ThreadPoolExecutor(max_workers=6) as ex:
                        list(ex.map(_resolve_uc, gnews_targets))
                    before_gn = len(uc_raw)
                    uc_raw = [it for it in uc_raw if "news.google.com" not in (it.get("link") or "")]
                    removed_gn = before_gn - len(uc_raw)
                    if removed_gn > 0:
                        log(f"  🚫 saiba_antes: removidos {removed_gn} GNews wrappers não decodificados")
            except Exception as e:
                log(f"  ⚠ resolve gnews undercovered falhou (não bloqueia): {e}")

            # FILTRO: dedup 5d cross-edição (não enviar repetido em 5 dias)
            try:
                if uc_raw:
                    _sent_links, _sent_sigs = dd._load_recently_sent_signatures(uid, days=5)
                    if _sent_links or _sent_sigs:
                        before_5d = len(uc_raw)
                        _uc_group = [{"news": uc_raw, "label": "_uc"}]
                        dd._filter_already_sent(_uc_group, _sent_links, _sent_sigs)
                        uc_raw = _uc_group[0].get("news", [])
                        removed_5d = before_5d - len(uc_raw)
                        if removed_5d > 0:
                            log(f"  🔁 saiba_antes dedup 5d: removidos {removed_5d} já enviado(s)")
            except Exception as e:
                log(f"  ⚠ dedup 5d undercovered falhou (não bloqueia): {e}")

            if uc_raw:
                # FILTRO: dedup vs sections + trending
                exclude_links = set()
                exclude_titles = set()
                for s in sections:
                    for n in s.get("noticias", []):
                        if n.get("link"):
                            exclude_links.add(n["link"])
                        title = n.get("manchete", "")
                        title_key = re.sub(r"\W+", "", title).lower()[:80]
                        if title_key:
                            exclude_titles.add(title_key)
                for t in trending:
                    if t.get("link"):
                        exclude_links.add(t["link"])
                    title = t.get("manchete", "")
                    title_key = re.sub(r"\W+", "", title).lower()[:80]
                    if title_key:
                        exclude_titles.add(title_key)

                # CURADORIA: Claude escolhe melhores considerando TEMAS + filtros + learned
                undercovered = dd.curate_undercovered(
                    user["name"], uc_raw, learned,
                    user_topic_labels=user_topic_labels,
                    filtered_items=filtered_items,
                    exclude_links=exclude_links,
                    exclude_titles=exclude_titles,
                    max_out=8,
                    weekly=weekly,
                )
                log(f"  📡 saiba_antes curados count={len(undercovered)}")

                # ANTI-ALUCINAÇÃO
                if undercovered:
                    try:
                        uc_stats = validate_and_clean_trending(
                            undercovered, uc_raw, supabase, uid, edition_id,
                            claude, MODEL,
                        )
                        if uc_stats.get("discarded_critical") or uc_stats.get("discarded_moderate") or uc_stats.get("rewritten"):
                            log(f"  🛡 anti-aluc saiba_antes: ok={uc_stats['ok']} reescritos={uc_stats['rewritten']} "
                                f"descartados={uc_stats['discarded_critical']+uc_stats['discarded_moderate']}")
                    except Exception as e:
                        log(f"  ⚠ anti-aluc saiba_antes falhou (não bloqueia): {e}")
        except Exception as e:
            log(f"  ⚠ saiba_antes falhou (não bloqueia): {e}")

    label_to_source = {t["label"]: t.get("source", "curated") for t in topics_with_news}
    sections.sort(key=lambda s: 0 if label_to_source.get(s.get("topic"), "curated") == "custom" else 1)

    dd.resolve_gnews_urls(sections, trending)

    before_count = len(sections)
    sections = [s for s in sections if s.get("noticias")]
    dropped_empty = before_count - len(sections)
    if dropped_empty > 0:
        log(f"  🧹 removidos {dropped_empty} tema(s) sem notícias")

    sections = dd.add_feedback_links(uid, sections)

    recap_data = dd.generate_daily_recap(user["name"], sections, trending, learned)
    daily_recap = recap_data.get("recap", "") if isinstance(recap_data, dict) else ""
    daily_quote = recap_data.get("quote", "") if isinstance(recap_data, dict) else ""
    daily_quote_author = recap_data.get("quote_author", "") if isinstance(recap_data, dict) else ""

    # IMAGE EXTRACTION
    try:
        from sources.utils import extract_images, validate_images
        urls_para_scrape = []
        for sec in sections:
            for n in sec.get("noticias", []):
                if not n.get("img_url") and n.get("link"):
                    urls_para_scrape.append(n["link"])
        for t in trending:
            if not t.get("img_url") and t.get("link"):
                urls_para_scrape.append(t["link"])
        for u in undercovered:
            if not u.get("img_url") and u.get("link"):
                urls_para_scrape.append(u["link"])
        if urls_para_scrape:
            log(f"  🌐 scraping og:image de {len(urls_para_scrape)} URLs (paralelo)...")
            img_map = extract_images(urls_para_scrape)
            non_empty = [v for v in img_map.values() if v]
            valid_imgs = validate_images(non_empty) if non_empty else {}
            for sec in sections:
                for n in sec.get("noticias", []):
                    if n.get("img_url"):
                        continue
                    link = n.get("link", "")
                    if link in img_map:
                        img = img_map[link]
                        if img and valid_imgs.get(img):
                            n["img_url"] = img
            for t in trending:
                if t.get("img_url"):
                    continue
                link = t.get("link", "")
                if link in img_map:
                    img = img_map[link]
                    if img and valid_imgs.get(img):
                        t["img_url"] = img
            for u in undercovered:
                if u.get("img_url"):
                    continue
                link = u.get("link", "")
                if link in img_map:
                    img = img_map[link]
                    if img and valid_imgs.get(img):
                        u["img_url"] = img
        if trending:
            n_with_img = sum(1 for t in trending if t.get("img_url"))
            log(f"  📷 trending: {n_with_img}/{len(trending)} com imagem")
        if undercovered:
            n_with_img = sum(1 for u in undercovered if u.get("img_url"))
            log(f"  📷 saiba_antes: {n_with_img}/{len(undercovered)} com imagem")
        if sections:
            total_n = sum(len(s.get("noticias", [])) for s in sections)
            total_img = sum(1 for s in sections for n in s.get("noticias", []) if n.get("img_url"))
            log(f"  📷 notícias: {total_img}/{total_n} com imagem (híbrido A+B)")
    except Exception as e:
        log(f"  ⚠ Image hybrid extraction falhou (não bloqueia): {e}")

    # Render
    signed_manage = gen_manage_url(MANAGE_URL, uid, ttl_days=30)
    signed_unsub = gen_unsub_url(SUPABASE_URL, uid)
    email_mode = (user.get("email_mode") or "coado").lower()

    user_tz = user.get("timezone") or "America/Sao_Paulo"
    if is_welcome:
        saudacao_mode = "auto"
    elif weekly:
        saudacao_mode = "sabado"
    else:
        saudacao_mode = "manha"

    click_base = os.environ.get("CLICK_BASE_URL", "https://recorte.news/c")
    share_base = os.environ.get("SHARE_BASE_URL", f"{SUPABASE_URL}/functions/v1/edition")

    html = render_email(
        user_name=user["name"], date_obj=now_brt,
        trending=trending, trending_label=trending_label,
        undercovered=undercovered,
        sections=sections, manage_url=signed_manage,
        user_id=uid,
        daily_recap=daily_recap, daily_quote=daily_quote, daily_quote_author=daily_quote_author,
        email_mode=email_mode, weekly_mode=weekly,
        user_tz=user_tz, saudacao_mode=saudacao_mode,
        filtered_items_count=len(filtered_items),
        is_welcome=is_welcome,
        unsub_url=signed_unsub,
        edition_id=edition_id,
        share_base_url=share_base,
    )

    first = user["name"].split()[0] if user.get("name") else "Você"
    if weekly:
        subject = f"Bom domingo, {first}. Sua semana, recortada ✂"
    else:
        subject = f"Bom dia, {first}. Hoje tem ✂ ({now_brt.strftime('%d/%m')})"

    try:
        save_edition(
            supabase, user_id=uid, kind=kind, subject=subject,
            html=html, scheduled_for=scheduled_for,
            edition_id=edition_id,
        )
    except Exception as e:
        log(f"  ⚠ save_edition falhou (não bloqueia): {e}")

    try:
        html = wrap_links_in_html(
            html, user_id=uid, edition_id=edition_id,
            supabase_client=supabase,
            click_base_url=click_base,
        )
        try:
            dd._supabase_retry(
                lambda: supabase.table("editions").update({"html": html}).eq("id", edition_id).execute(),
                label="editions.update(html)",
            )
        except Exception as e:
            log(f"  ⚠ Update edition html falhou (não bloqueia): {e}")
    except Exception as e:
        log(f"  ⚠ Click tracking wrap falhou (não bloqueia): {e}")

    ok = enqueue_email(uid, kind, scheduled_for, subject, html, edition_id=edition_id)
    if ok:
        log(f"  ✓ {email}: enfileirado ({len(html)} chars HTML, edition={edition_id[:8]})")
        return "enqueued"
    return "skipped"


def main():
    now_brt = datetime.now(BRT)
    log(f"=== prepare_daily run ===", hour=now_brt.hour)

    scheduled_for = now_brt.date().isoformat()

    res = dd._supabase_retry(
        lambda: supabase.table("users").select("*").eq("active", True).execute(),
        label="users.select(active)",
    )
    all_users = res.data or []
    if not all_users:
        log("=== fim (nenhum user ativo) ===")
        return

    log(f"users ativos", count=len(all_users), scheduled_for=scheduled_for)

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
