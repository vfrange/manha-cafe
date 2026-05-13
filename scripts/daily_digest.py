#!/usr/bin/env python3
"""
Manhã ☕ — Digest diário V3
- Fontes: Google News + Trends + Hacker News + Reddit + RSS BR
- Lê perfil aprendido do user e injeta no prompt do Claude
- Gera links de feedback (+/-) por notícia e (pausar) por tema
- Respeita temas pausados nos últimos 7 dias
"""

import os
import re
import json
import time
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import resend
from supabase import create_client
from anthropic import Anthropic

from email_template import render_email
from feedback_token import short_id, feedback_url
from sources import google_news, hacker_news, reddit, br_rss

# ============ CONFIG ============
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
RESEND_API_KEY = os.environ["RESEND_API_KEY"]
FEEDBACK_BASE_URL = os.environ["FEEDBACK_BASE_URL"]  # ex: https://xxx.functions.supabase.co/feedback
FROM_EMAIL = os.environ.get("FROM_EMAIL", "Manhã <digest@onresend.dev>")
MANAGE_URL = os.environ.get("MANAGE_URL", "https://seudominio.netlify.app/cadastro.html")

TARGET_HOUR_BRT = int(os.environ.get("TARGET_HOUR_BRT", "-1"))
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"

MODEL = "claude-haiku-4-5-20251001"
MAX_NEWS_OUT_PER_TOPIC = 3
MAX_TRENDING_OUT = 5

COUNTRY_NAMES = {
    "BR": "🇧🇷 Brasil", "US": "🇺🇸 EUA", "GB": "🇬🇧 Reino Unido",
    "FR": "🇫🇷 França", "DE": "🇩🇪 Alemanha", "ES": "🇪🇸 Espanha",
    "PT": "🇵🇹 Portugal", "IT": "🇮🇹 Itália", "NL": "🇳🇱 Holanda",
    "CH": "🇨🇭 Suíça", "SE": "🇸🇪 Suécia", "JP": "🇯🇵 Japão",
    "KR": "🇰🇷 Coreia", "CN": "🇨🇳 China", "IN": "🇮🇳 Índia",
    "AE": "🇦🇪 Emirados", "IL": "🇮🇱 Israel", "MX": "🇲🇽 México",
    "AR": "🇦🇷 Argentina", "CL": "🇨🇱 Chile", "CO": "🇨🇴 Colômbia",
    "CA": "🇨🇦 Canadá", "AU": "🇦🇺 Austrália", "ZA": "🇿🇦 África do Sul",
    "GLOBAL": "🌍 Mundo",
}

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
claude = Anthropic(api_key=ANTHROPIC_API_KEY)
resend.api_key = RESEND_API_KEY
BRT = timezone(timedelta(hours=-3))


def log(msg, **kv):
    extra = " ".join(f"{k}={v}" for k, v in kv.items())
    print(f"[{datetime.now(BRT).strftime('%H:%M:%S')}] {msg} {extra}".strip(), flush=True)


# ============ MULTI-SOURCE FETCH ============
def fetch_all_sources(query, country, category=None):
    """
    Roda todas as fontes em paralelo. Retorna lista combinada de notícias brutas.
    Para temas BR, prioriza RSS BR + Google News BR + Reddit.
    Para temas Global/Tech, prioriza Google News Global + HN + Reddit.
    """
    is_br = country == "BR"
    is_tech = category == "tecnologia" or any(
        kw in query.lower() for kw in ["tech","ia ","ai","intelig","gpt","openai","software"]
    )

    fetchers = []
    fetchers.append(("google_news", lambda: google_news.fetch(query, country, max_items=8)))
    if is_tech:
        fetchers.append(("hacker_news", lambda: hacker_news.fetch(query, max_items=5)))
    fetchers.append(("reddit", lambda: reddit.fetch(query, category=category, max_items=4)))
    if is_br:
        fetchers.append(("br_rss", lambda: br_rss.fetch(query, category=category, max_items=8)))

    results = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(fn): name for name, fn in fetchers}
        for fut in as_completed(futures, timeout=20):
            name = futures[fut]
            try:
                items = fut.result()
                results.extend(items)
            except Exception as e:
                log(f"  ⚠ erro fonte {name}: {e}")

    # dedupe por título (cross-source)
    seen = set()
    deduped = []
    for r in results:
        key = re.sub(r"\W+", "", r.get("title","")).lower()[:80]
        if key and key not in seen:
            seen.add(key)
            deduped.append(r)
    return deduped


def fetch_trending(country):
    """Combina trends do Google Trends com top do Reddit (proxy de viralidade social)."""
    trends = google_news.fetch_trends(country)
    # Bonus: top de r/popular ajuda a capturar viralização internet-wide
    if country in ("GLOBAL", "US"):
        try:
            trends.extend(reddit.fetch_trending_general(max_items=8))
        except Exception as e:
            log(f"  ⚠ reddit trending: {e}")
    return trends


# ============ CLAUDE CURATION ============
MAX_NEWS_INPUT_PER_TOPIC = 6   # quantas notícias brutas mandar pro Claude por tema
MAX_TOPICS_PER_BATCH = 6       # quantos temas processar numa chamada (evita JSON gigante)


def _robust_json_parse(text):
    """
    Tenta parsear JSON com várias estratégias de fallback.
    Retorna dict vazio se tudo falhar (não quebra o fluxo).
    """
    # 1) Limpa markdown
    text = re.sub(r"^```(?:json)?\s*", "", text.strip())
    text = re.sub(r"\s*```$", "", text)

    # 2) Tentativa direta
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # 3) Extrai bloco {...} mais externo
    i, j = text.find("{"), text.rfind("}")
    if i >= 0 and j > i:
        try:
            return json.loads(text[i:j+1])
        except json.JSONDecodeError:
            pass

    # 4) Tenta reparar: trunca no último objeto válido fechado
    # Encontra a última posição onde temos um JSON parcial válido tipo {"secoes":[{...},{...},
    if '"secoes"' in text:
        # tenta cortar antes do último objeto incompleto
        partial = text[i:] if i >= 0 else text
        # remove possíveis lixos no final
        for cutoff in range(len(partial), 100, -100):
            candidate = partial[:cutoff]
            # tenta fechar manualmente: ]}
            for ending in [']}', '"}]}', '"}]}']:
                try:
                    test = candidate.rstrip(',\n\r ') + ending
                    parsed = json.loads(test)
                    if "secoes" in parsed:
                        log(f"  ⚠ JSON reparado parcialmente em cutoff={cutoff}")
                        return parsed
                except json.JSONDecodeError:
                    continue

    # 5) Desistiu — retorna vazio (o email vai sair sem essas seções, em vez de quebrar tudo)
    log(f"  ✗ JSON do Claude irrecuperável, pulando esse batch")
    return {}


def _curate_news_batch(user_name, topics_with_news, learned_profile=""):
    """Processa UM batch de temas (até MAX_TOPICS_PER_BATCH)."""
    payload = []
    for t in topics_with_news:
        # Trunca: passa só as primeiras N notícias por tema pro Claude
        news_truncated = t["news"][:MAX_NEWS_INPUT_PER_TOPIC]
        payload.append({
            "tema": t["label"],
            "pais": COUNTRY_NAMES.get(t["country"], t["country"]),
            "noticias_brutas": [
                {"titulo": n["title"], "fonte": n["source"], "preview": n["summary"],
                 "link": n["link"], "origem": n.get("origin","")}
                for n in news_truncated
            ]
        })

    profile_section = ""
    if learned_profile.strip():
        profile_section = f"""
**PERFIL APRENDIDO DESTE USUÁRIO** (use isso pra priorizar e filtrar):
{learned_profile}

Priorize notícias que casem com o que ele gosta. Evite (ou despriorize) o que ele não gosta. Não comente sobre o perfil na resposta.
"""

    prompt = f"""Você é editor de uma newsletter premium em PT-BR ao estilo Morning Brew, escrevendo para {user_name}.
{profile_section}
Para cada tema abaixo, selecione as **até {MAX_NEWS_OUT_PER_TOPIC} notícias mais relevantes** do dia (priorize: impacto real, novidade, alinhamento com perfil; evite duplicatas e clickbait).

🇧🇷 **REGRA CRÍTICA DE IDIOMA**: TODO o conteúdo gerado (manchete, resumo, why_matters) DEVE estar em **português brasileiro natural**, MESMO QUE a matéria original esteja em inglês, espanhol ou qualquer outro idioma. Traduza com fluência.

⚠️ **REGRA CRÍTICA DE FORMATO**: Retorne APENAS JSON VÁLIDO, sem markdown, sem ```. Escape TODAS as aspas duplas dentro de strings com \\". Não use quebras de linha dentro de strings (use espaço).

Para cada notícia selecionada, retorne:
- **manchete**: título em PT-BR direto, máx 90 caracteres, sem clickbait
- **resumo**: 2 frases curtas (40-60 palavras) em PT-BR
- **why_matters** (opcional): 1 frase em PT-BR ligando ao interesse do usuário (omita se forçado)
- **link**: copie o link original
- **fonte**: nome do veículo

Se um tema tiver pouca notícia relevante, retorne menos itens. Se nada for relevante, omita o tema.

Dados:
{json.dumps(payload, ensure_ascii=False, indent=2)}

**RESPONDA APENAS JSON VÁLIDO**:
{{"secoes":[{{"tema":"<nome>","noticias":[{{"manchete":"...","resumo":"...","link":"...","fonte":"..."}}]}}]}}"""

    try:
        resp = claude.messages.create(model=MODEL, max_tokens=8000,
                                       messages=[{"role": "user", "content": prompt}])
        text = resp.content[0].text.strip()
        parsed = _robust_json_parse(text)
        return parsed.get("secoes", [])
    except Exception as e:
        log(f"  ✗ Claude API erro no batch: {e}")
        return []


def curate_news(user_name, topics_with_news, learned_profile=""):
    """
    Curadoria com batching: divide temas em grupos de até MAX_TOPICS_PER_BATCH
    pra evitar JSON gigante que pode quebrar.
    """
    if not topics_with_news:
        return []

    all_sections = []
    batches = [
        topics_with_news[i:i+MAX_TOPICS_PER_BATCH]
        for i in range(0, len(topics_with_news), MAX_TOPICS_PER_BATCH)
    ]
    log(f"  curando em {len(batches)} batch(es) de até {MAX_TOPICS_PER_BATCH} temas")

    for idx, batch in enumerate(batches, 1):
        log(f"  batch {idx}/{len(batches)}: {len(batch)} temas")
        sections = _curate_news_batch(user_name, batch, learned_profile)
        all_sections.extend(sections)

    return all_sections


def curate_trends(user_name, scope_label, trends, learned_profile=""):
    if not trends:
        return []
    profile_section = ""
    if learned_profile.strip():
        profile_section = f"\n**PERFIL DO USUÁRIO**: {learned_profile}\nUse pra priorizar trends que casem com interesses dele.\n"

    prompt = f"""Você é editor de "🔥 Trending Topics" em PT-BR pra {user_name}, escopo: **{scope_label}**.
{profile_section}
Selecione os **{MAX_TRENDING_OUT} mais relevantes** evitando fofoca rasa, conteúdo regional sem contexto, ou jargão obscuro. Priorize: eventos significativos, lançamentos, esporte de impacto, fenômenos culturais reais.

🇧🇷 **REGRA CRÍTICA DE IDIOMA**: TODO o conteúdo (termo e contexto) DEVE estar em **português brasileiro fluente**, MESMO QUE o termo original esteja em inglês ou outro idioma. Traduza nomes próprios quando faz sentido (Champions League pode ficar, mas "earnings call" deve virar "balanço trimestral" ou similar). Mantenha nomes de pessoas e empresas no original.

Para cada um, em PT-BR:
- **termo**: o termo (pode reescrever pra clareza, traduzir se ajudar)
- **buscas**: volume aproximado (copie do input se houver)
- **contexto**: 1 frase em PT-BR de até 25 palavras explicando POR QUE está em alta hoje
- **link**: URL relacionada (se houver)
- **fonte**: veículo

Trends:
{json.dumps(trends, ensure_ascii=False, indent=2)}

**APENAS JSON VÁLIDO**:
{{"trending":[{{"termo":"...","buscas":"...","contexto":"...","link":"...","fonte":"..."}}]}}"""

    resp = claude.messages.create(model=MODEL, max_tokens=2000,
                                   messages=[{"role": "user", "content": prompt}])
    text = resp.content[0].text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text).get("trending", [])
    except json.JSONDecodeError:
        i, j = text.find("{"), text.rfind("}")
        return json.loads(text[i:j+1]).get("trending", [])


# ============ EMAIL ITEMS + FEEDBACK LINKS ============
def create_email_item(user_id, kind, payload):
    """Insere snapshot em email_items, retorna o id curto."""
    iid = short_id()
    supabase.table("email_items").insert({
        "id": iid,
        "user_id": user_id,
        "kind": kind,
        "payload": payload,
    }).execute()
    return iid


def add_feedback_links(user_id, sections):
    """Pra cada notícia e cada seção, gera email_items + URLs assinadas."""
    for sec in sections:
        # tema → link de pausar
        if sec.get("topic_id") or sec.get("topic"):
            tid = create_email_item(user_id, "topic", {
                "topic_id": sec.get("topic_id"),
                "topic_label": sec.get("topic",""),
            })
            sec["fb_pause_url"] = feedback_url(FEEDBACK_BASE_URL, tid, -1)
        # notícias → +/-
        for n in sec.get("noticias", []):
            nid = create_email_item(user_id, "news", {
                "title": n.get("manchete",""),
                "source": n.get("fonte",""),
                "link": n.get("link",""),
                "topic_label": sec.get("topic",""),
            })
            n["fb_more_url"] = feedback_url(FEEDBACK_BASE_URL, nid, +1)
            n["fb_less_url"] = feedback_url(FEEDBACK_BASE_URL, nid, -1)
    return sections


# ============ PROFILE / PAUSED TOPICS ============
def load_profile(user_id):
    res = supabase.table("user_profile").select("*").eq("user_id", user_id).execute()
    if res.data:
        return res.data[0]
    return {"learned_text": "", "paused_topics": []}


def is_topic_paused(topic_label, paused_topics, now):
    for p in paused_topics or []:
        until_str = p.get("until")
        if not until_str:
            continue
        try:
            until = datetime.fromisoformat(until_str.replace("Z","+00:00"))
        except Exception:
            continue
        if until > now and p.get("label") == topic_label:
            return True
    return False


# ============ SENDER ============
def send_email(to_email, to_name, html, date_obj):
    first = to_name.split()[0]
    subject = f"☕ Sua manhã, {first} — {date_obj.strftime('%d/%m')}"
    if DRY_RUN:
        log("DRY_RUN", to=to_email, subject=subject)
        fname = f"preview_{to_email.replace('@','_at_')}.html"
        with open(fname, "w", encoding="utf-8") as f:
            f.write(html)
        return {"id": "dry-run"}
    return resend.Emails.send({
        "from": FROM_EMAIL, "to": to_email,
        "subject": subject, "html": html,
    })


# ============ MAIN ============
def process_user(user, now_brt):
    uid = user["id"]
    default_country = user.get("default_country") or "BR"
    log(f"processando", email=user["email"], pais=default_country)

    # carrega perfil aprendido
    profile = load_profile(uid)
    learned = profile.get("learned_text", "") or ""
    paused = profile.get("paused_topics", []) or []
    if learned:
        log(f"  perfil: {learned[:80]}...")
    if paused:
        log(f"  temas pausados: {len(paused)}")

    # 1) TRENDING — suporta CSV multi-scope ex: "br,global,country:IL"
    trending = []
    trending_label = ""
    if user.get("trending_enabled", True):
        raw_scope = user.get("trending_scope") or "br"
        scopes = [s.strip() for s in raw_scope.split(",") if s.strip()]
        if not scopes:
            scopes = ["br"]

        labels = []
        for scope in scopes:
            if scope == "global":
                tcountry, tlabel = "GLOBAL", "🌍 Mundo"
            elif scope == "br":
                tcountry, tlabel = "BR", "🇧🇷 Brasil"
            elif scope.startswith("country:"):
                tcountry = scope.split(":", 1)[1] or "BR"
                tlabel = f"🎯 {COUNTRY_NAMES.get(tcountry, tcountry)}"
            else:
                # fallback legado: scope == 'country' usa trending_country da v3
                tcountry = user.get("trending_country") or "BR"
                tlabel = COUNTRY_NAMES.get(tcountry, tcountry)

            labels.append(tlabel)
            raw_trends = fetch_trending(tcountry)
            log(f"  trends brutos", count=len(raw_trends), scope=tcountry)
            if raw_trends:
                curated = curate_trends(user["name"], tlabel, raw_trends, learned)
                # marca o escopo de origem em cada item pra debug/futuro uso
                for item in curated:
                    item.setdefault("scope_origin", tlabel)
                trending.extend(curated)

        trending_label = " + ".join(labels) if labels else ""

    # 2) NOTÍCIAS POR TEMA
    topics_res = supabase.table("topics").select("*").eq("user_id", uid).execute()
    topics = topics_res.data or []
    # Fallback geográfico: se "Onde você está" = INTL, usa GLOBAL como default
    fallback_country = "GLOBAL" if default_country == "INTL" else default_country

    # Cada tema pode aparecer múltiplas vezes (1 por escopo escolhido). Fetch por registro,
    # depois agrupa pelo label pra virar 1 seção no email.
    by_label = {}
    for t in topics:
        if is_topic_paused(t["label"], paused, datetime.now(timezone.utc)):
            log(f"  ⏸ pulando {t['label']} (pausado)")
            continue
        country = t.get("country") or fallback_country
        category = t.get("category")
        news = fetch_all_sources(t["query"], country, category=category)
        log(f"  {t['label']} ({country}): {len(news)} brutas")
        if not news:
            continue
        # marca cada notícia com o escopo de origem (pra mostrar bandeirinha no email se quisermos)
        for n in news:
            n.setdefault("scope_origin", country)
        if t["label"] not in by_label:
            by_label[t["label"]] = {
                "label": t["label"],
                "country": country,           # primeiro country do grupo (legado)
                "scopes": [],                  # lista de todos os escopos cobertos
                "topic_id": t["id"],
                "news": []
            }
        by_label[t["label"]]["scopes"].append(country)
        by_label[t["label"]]["news"].extend(news)

    # Deduplica notícias por URL dentro de cada grupo
    topics_with_news = []
    for group in by_label.values():
        seen_urls = set()
        deduped = []
        for n in group["news"]:
            url = n.get("link") or n.get("url") or ""
            if url and url in seen_urls:
                continue
            seen_urls.add(url)
            deduped.append(n)
        group["news"] = deduped
        topics_with_news.append(group)

    sections = []
    if topics_with_news:
        raw_sections = curate_news(user["name"], topics_with_news, learned)
        # casa metadados de volta pelo label (topic_id + scopes reais)
        label_meta = {t["label"]: {"topic_id": t["topic_id"], "scopes": t["scopes"]} for t in topics_with_news}
        for s in raw_sections:
            if s.get("noticias"):
                tema = s.get("tema", "")
                meta = label_meta.get(tema, {})
                scopes = meta.get("scopes", [])
                # Monta country_label a partir dos escopos reais (não do Claude)
                if scopes:
                    flag_parts = [COUNTRY_NAMES.get(sc, sc).split()[0] for sc in scopes]
                    country_label = " + ".join(flag_parts)
                else:
                    country_label = s.get("pais", "")
                sections.append({
                    "topic": tema,
                    "topic_id": meta.get("topic_id"),
                    "country_label": country_label,
                    "noticias": s["noticias"],
                })

    if not trending and not sections:
        log("  nada pra mandar, pulando")
        return False

    # 3) GERA email_items + URLs de feedback
    sections = add_feedback_links(uid, sections)

    # 4) RENDER + ENVIO
    html = render_email(
        user_name=user["name"], date_obj=now_brt,
        trending=trending, trending_label=trending_label,
        sections=sections, manage_url=MANAGE_URL,
    )
    result = send_email(user["email"], user["name"], html, now_brt)
    log(f"  ✓ enviado", id=result.get("id"))
    supabase.table("users").update({"last_sent_at": now_brt.isoformat()}).eq("id", uid).execute()
    return True


def main():
    now_brt = datetime.now(BRT)
    target_hour = TARGET_HOUR_BRT if TARGET_HOUR_BRT >= 0 else now_brt.hour
    log(f"=== Manhã ☕ V3 run ===", hora=target_hour, dry=DRY_RUN)

    res = supabase.table("users").select("*").eq("active", True).eq("send_hour", target_hour).execute()
    users = res.data or []
    log(f"usuários elegíveis", count=len(users))

    for user in users:
        try:
            process_user(user, now_brt)
        except Exception as e:
            log(f"  ✗ ERRO {user.get('email','?')}: {e}")
            import traceback; traceback.print_exc()

    log("=== fim ===")


if __name__ == "__main__":
    main()
