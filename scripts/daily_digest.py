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
from feedback_token import short_id, feedback_url, manage_url as gen_manage_url
from sources import google_news, hacker_news, reddit, br_rss, bluesky, youtube_trending, intl_rss
from safety import (
    is_safe_news, is_safe_curated, SAFETY_INSTRUCTIONS,
    POLITICAL_BIAS_INSTRUCTIONS, is_political_topic,
)

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
MAX_NEWS_OUT_PER_TOPIC = 2
MAX_TRENDING_OUT = 10

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
def fetch_all_sources(query, country, category=None, label=None, source_type="curated"):
    """
    Roda todas as fontes em paralelo. Retorna lista combinada de notícias brutas.
    Para temas BR, prioriza RSS BR + Google News BR + Reddit.
    Para temas Global/Tech, prioriza Google News Global + HN + Reddit.
    Para temas CURADOS GLOBAL, adiciona RSS direto de NYT/WaPo/BBC/etc.
    """
    is_br = country == "BR"
    is_global = country == "GLOBAL"
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
    # RSS internacional: só pra temas CURADOS GLOBAIS (não para customs)
    if is_global and source_type == "curated" and label:
        fetchers.append(("intl_rss", lambda: intl_rss.fetch_for_topic(label, max_per_feed=2)))

    results = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(fn): name for name, fn in fetchers}
        for fut in as_completed(futures, timeout=25):
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
    """Combina trends: Google News Top Stories + Reddit + Bluesky + YouTube Trending."""
    trends = []

    # 1) Google News Top Stories (manchetes em alta) — sempre tenta
    try:
        trends.extend(google_news.fetch_trends(country))
    except Exception as e:
        log(f"  ⚠ google news top: {e}")

    # 2) Reddit top (viralidade social geral)
    if country in ("GLOBAL", "US"):
        try:
            trends.extend(reddit.fetch_trending_general(max_items=6))
        except Exception as e:
            log(f"  ⚠ reddit trending: {e}")

    # 3) Bluesky "What's Hot" (buzz social que substituiu o Twitter)
    try:
        trends.extend(bluesky.fetch_trending(max_items=8))
    except Exception as e:
        log(f"  ⚠ bluesky: {e}")

    # 4) YouTube Trending (cultura/política/esportes em vídeo) — só se tiver API key
    try:
        trends.extend(youtube_trending.fetch_trending(country=country, max_items=8))
    except Exception as e:
        log(f"  ⚠ youtube trending: {e}")

    return trends


# ============ CLAUDE CURATION ============
MAX_NEWS_INPUT_PER_TOPIC = 6   # quantas notícias brutas mandar pro Claude por tema
MAX_TOPICS_PER_BATCH = 4       # quantos temas processar numa chamada (evita JSON gigante)


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
    has_political = False
    for t in topics_with_news:
        # PRE-FILTER: descarta matérias com sinais de conteúdo proibido
        clean_news = [n for n in t["news"][:MAX_NEWS_INPUT_PER_TOPIC] if is_safe_news(n)]
        if not clean_news:
            continue  # tema sem matéria limpa após filtro
        topic_is_political = is_political_topic(t["label"])
        if topic_is_political:
            has_political = True
        payload.append({
            "tema": t["label"],
            "pais": COUNTRY_NAMES.get(t["country"], t["country"]),
            "tema_politico": topic_is_political,
            "noticias_brutas": [
                {"titulo": n["title"], "fonte": n["source"], "preview": n["summary"],
                 "link": n["link"], "origem": n.get("origin","")}
                for n in clean_news
            ]
        })

    if not payload:
        return []

    profile_section = ""
    if learned_profile.strip():
        profile_section = f"""
**PERFIL APRENDIDO DESTE USUÁRIO** (use isso pra priorizar e filtrar):
{learned_profile}

Priorize notícias que casem com o que ele gosta. Evite (ou despriorize) o que ele não gosta. Não comente sobre o perfil na resposta.
"""

    bias_section = POLITICAL_BIAS_INSTRUCTIONS if has_political else ""

    prompt = f"""Você é editor de uma newsletter premium em PT-BR ao estilo Morning Brew, escrevendo para {user_name}.
{profile_section}
{SAFETY_INSTRUCTIONS}
{bias_section}
Para cada tema abaixo, selecione as **até {MAX_NEWS_OUT_PER_TOPIC} notícias mais relevantes** do dia (priorize: impacto real, novidade, alinhamento com perfil; evite duplicatas e clickbait).

🇧🇷 **REGRA CRÍTICA DE IDIOMA**: TODO o conteúdo gerado DEVE estar em **português brasileiro natural**, MESMO QUE a matéria original esteja em inglês, espanhol ou outro idioma. Traduza com fluência.

⚠️ **REGRA CRÍTICA DE FORMATO**: Retorne APENAS JSON VÁLIDO, sem markdown, sem ```. Escape TODAS as aspas duplas dentro de strings com \\". Não use quebras de linha dentro de strings.

📰 **OBJETIVO**: O usuário deve conseguir entender a notícia INTEIRA sem precisar abrir o link. Seja rico em fatos, números e contexto. Mas mantenha estilo Morning Brew: direto, esperto, sem encher linguiça.

Para cada notícia selecionada, retorne:
- **manchete**: título em PT-BR direto, máx 90 caracteres, sem clickbait
- **resumo**: 3-4 frases (100-160 palavras) em PT-BR. Explica o que aconteceu, números/fatos centrais, contexto e implicação imediata
- **fatos_chave**: array de 3 a 5 bullets curtos (cada um 6-15 palavras) com os pontos mais importantes — números, datas, players, valores, decisões. Ex: ["Selic caiu de 13,75% para 13,25%", "1ª redução em 12 meses", "Mercado esperava corte de 0,75 ponto"]
- **link**: copie o link original
- **fonte**: nome do veículo
- **lang**: código do idioma original da matéria (ex: "en", "fr", "de"). Omita se for PT-BR.
- **pol_bias** (APENAS para temas marcados `tema_politico: true`): "factual", "centro", "esq" ou "dir". Veja regras acima.

Se um tema tiver pouca notícia relevante, retorne menos itens. Se nada for relevante, omita o tema.

Dados:
{json.dumps(payload, ensure_ascii=False, indent=2)}

**RESPONDA APENAS JSON VÁLIDO**:
{{"secoes":[{{"tema":"<nome>","noticias":[{{"manchete":"...","resumo":"...","fatos_chave":["...","..."],"link":"...","fonte":"...","lang":"...","pol_bias":"..."}}]}}]}}"""

    try:
        resp = claude.messages.create(model=MODEL, max_tokens=12000,
                                       messages=[{"role": "user", "content": prompt}])
        text = resp.content[0].text.strip()
        parsed = _robust_json_parse(text)
        secoes = parsed.get("secoes", [])
        # POST-FILTER: re-valida cada item curado
        for sec in secoes:
            sec["noticias"] = [n for n in sec.get("noticias", []) if is_safe_curated(n)]
        return secoes
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


def _norm_for_dedup(text: str) -> str:
    """Normaliza string pra detectar duplicatas: minúsculo, sem pontuação/espaços."""
    if not text:
        return ""
    return re.sub(r'[^a-z0-9]', '', text.lower())


def _dedupe_trends(items):
    """Remove trends duplicados: mesmo link OU manchetes muito similares."""
    seen_links = set()
    seen_signatures = set()
    out = []
    for it in items:
        link = (it.get("link") or "").strip().lower()
        if link and link in seen_links:
            continue
        # Assinatura = primeiros 50 caracteres normalizados da manchete
        sig = _norm_for_dedup(it.get("manchete", ""))[:50]
        if sig and sig in seen_signatures:
            continue
        if link:
            seen_links.add(link)
        if sig:
            seen_signatures.add(sig)
        out.append(it)
    return out


def curate_trends(user_name, scope_label, trends, learned_profile=""):
    """Em Alta: agora formato completo (manchete + resumo + fatos_chave) igual seções."""
    if not trends:
        return []

    MAX_TRENDS_INPUT = 18
    # Pre-filter
    trends_clean = [t for t in trends if is_safe_news(t)]
    trends_truncated = trends_clean[:MAX_TRENDS_INPUT]

    profile_section = ""
    if learned_profile.strip():
        profile_section = f"\n**PERFIL DO USUÁRIO**: {learned_profile}\nUse pra priorizar trends que casem com interesses dele.\n"

    prompt = f"""Você é editor da seção "🔥 Em Alta" em PT-BR pra {user_name}, escopo: **{scope_label}**.
{profile_section}
{SAFETY_INSTRUCTIONS}

Selecione os **{MAX_TRENDING_OUT} eventos mais relevantes** que estão em alta hoje (top stories + redes sociais + viralizações). Priorize: eventos significativos, lançamentos, esporte/cultura de impacto. Evite fofoca rasa, conteúdo regional sem contexto, ou jargão obscuro.

🚫 **REGRA CRÍTICA DE DEDUPLICAÇÃO**: NUNCA inclua duas manchetes sobre o MESMO evento, mesmo que venham de fontes diferentes ou com palavras ligeiramente diferentes. Se ver vários trends brutos sobre o mesmo acontecimento (ex: "Lula faz pronunciamento" e "Pronunciamento de Lula" e "Discurso presidencial"), escolha APENAS UM (o de fonte mais relevante) e descarte os outros. Cada item do output deve representar UM evento ÚNICO. Em caso de dúvida sobre se 2 trends são o mesmo evento, considere que SÃO e una.

🇧🇷 **REGRA CRÍTICA DE IDIOMA**: TODO conteúdo (manchete, resumo, fatos) DEVE estar em **português brasileiro fluente**, MESMO QUE original esteja em inglês ou outro idioma. Mantenha nomes próprios e marcas no original.

⚠️ **REGRA CRÍTICA DE FORMATO**: APENAS JSON VÁLIDO, sem markdown, sem ```. Escape aspas duplas com \\". Sem quebras de linha dentro de strings.

📰 **OBJETIVO**: o usuário entende cada evento sem precisar abrir o link. Seja rico em fatos e contexto.

Pra cada item:
- **manchete**: título PT-BR direto, máx 90 chars, sem clickbait
- **resumo**: 3-4 frases (100-160 palavras) explicando o que rolou, números/contexto, e POR QUE está em alta hoje
- **fatos_chave**: array de 3-5 bullets curtos (6-15 palavras cada) com números, datas, players, valores
- **buscas** (opcional): se vier do input, copie
- **link**: URL relacionada
- **fonte**: veículo

Trends brutos:
{json.dumps(trends_truncated, ensure_ascii=False, indent=2)}

**APENAS JSON VÁLIDO**:
{{"trending":[{{"manchete":"...","resumo":"...","fatos_chave":["..."],"link":"...","fonte":"..."}}]}}"""

    try:
        resp = claude.messages.create(model=MODEL, max_tokens=6000,
                                       messages=[{"role": "user", "content": prompt}])
        text = resp.content[0].text.strip()
        parsed = _robust_json_parse(text)
        items = parsed.get("trending", [])
        # Post-filter: safety
        items = [it for it in items if is_safe_curated(it)]
        # Post-filter: dedup
        before = len(items)
        items = _dedupe_trends(items)
        if len(items) < before:
            log(f"  ⚠ dedup removeu {before - len(items)} duplicatas do Em Alta")
        return items
    except Exception as e:
        log(f"  ✗ erro curate_trends, retornando vazio: {e}")
        return []


def generate_daily_recap(user_name, sections, trending, learned_profile=""):
    """
    Gera o 'Seu dia em 60 segundos' + uma quote de destaque pro topo.
    Retorna dict {recap, quote, quote_author} ou string vazia se nada.
    """
    if not sections and not trending:
        return {"recap": "", "quote": "", "quote_author": ""}

    # Compila uma lista enxuta do que tem no email pra passar pro Claude
    summary_input = []
    if trending:
        for t in trending[:5]:
            title = t.get("manchete") or t.get("termo", "")
            if title:
                summary_input.append({"area": "🔥 Em Alta", "manchete": title})
    for sec in sections[:10]:
        topic = sec.get("topic", "")
        for n in sec.get("noticias", [])[:2]:
            summary_input.append({"area": topic, "manchete": n.get("manchete", "")})

    if not summary_input:
        return {"recap": "", "quote": "", "quote_author": ""}

    profile_section = ""
    if learned_profile.strip():
        profile_section = f"\nPerfil de {user_name}: {learned_profile}\nUse pra dar destaque ao que casa com o perfil.\n"

    prompt = f"""Você escreve "Seu dia em 60 segundos" — o briefing executivo no topo do email do {user_name}, estilo Morning Brew.
{profile_section}
**TAREFA 1 — RECAP**: Cria UM PARÁGRAFO único de **140-180 palavras** em PT-BR que faça o usuário entender, em 1 minuto, o que rolou hoje no mundo dele.

Regras do recap:
- Tom direto, esperto, sem clichê. Lê como se fosse um amigo bem informado contando.
- Cobre 4-6 fatos do dia, escolhendo os de maior impacto/novidade.
- Conecta áreas quando faz sentido ("...na mesma semana em que..." / "...enquanto isso...").
- Termina com 1 frase de fechamento.
- NÃO use bullets. NÃO use markdown. NÃO repita "hoje". UM parágrafo corrido.

**TAREFA 2 — QUOTE**: Pinça UMA frase forte ou citação marcante de alguma das manchetes/notícias do dia. Pode ser declaração de pessoa pública, dado impressionante, ou conclusão de matéria. **Máximo 18 palavras**. Use aspas curvas “”. Pode ser uma observação sua sobre o dia inteiro também, no estilo de uma editorial. Deve ter caráter, personalidade.

Manchetes de hoje:
{json.dumps(summary_input, ensure_ascii=False, indent=2)}

Responda APENAS JSON VÁLIDO neste formato exato:
{{"recap": "<parágrafo>", "quote": "<frase com aspas curvas>", "quote_author": "<autor ou contexto curto, ex: 'André Lara Resende, FSP' ou 'da redação do Recorte'>"}}"""

    try:
        resp = claude.messages.create(model=MODEL, max_tokens=900,
                                       messages=[{"role": "user", "content": prompt}])
        text = resp.content[0].text.strip()
        parsed = _robust_json_parse(text)
        return {
            "recap": (parsed.get("recap") or "").strip(),
            "quote": (parsed.get("quote") or "").strip(),
            "quote_author": (parsed.get("quote_author") or "").strip(),
        }
    except Exception as e:
        log(f"  ⚠ erro generate_daily_recap: {e}")
        return {"recap": "", "quote": "", "quote_author": ""}


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
        news = fetch_all_sources(
            t["query"], country,
            category=category,
            label=t["label"],
            source_type=t.get("source", "curated")
        )
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
        # mapa link → lang pra reanexar idioma na resposta do Claude
        link_to_lang = {}
        for t in topics_with_news:
            for n in t.get("news", []):
                ln = n.get("link", "")
                lg = (n.get("lang") or "").lower()
                if ln and lg and lg != "pt":
                    link_to_lang[ln] = lg

        for s in raw_sections:
            if s.get("noticias"):
                # Defensivo: filtra notícias sem campos mínimos antes de prosseguir
                clean = [n for n in s["noticias"] if n.get("manchete") and n.get("resumo")]
                if len(clean) < len(s["noticias"]):
                    log(f"  ⚠ tema {s.get('tema','?')}: descartadas {len(s['noticias']) - len(clean)} notícias incompletas")
                if not clean:
                    continue
                s["noticias"] = clean

                tema = s.get("tema", "")
                meta = label_meta.get(tema, {})
                scopes = meta.get("scopes", [])
                if scopes:
                    flag_parts = [COUNTRY_NAMES.get(sc, sc).split()[0] for sc in scopes]
                    country_label = " + ".join(flag_parts)
                else:
                    country_label = s.get("pais", "")
                # anexa lang em cada notícia (com base no link original)
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
        log("  nada pra mandar, pulando")
        return False

    # 3) GERA email_items + URLs de feedback
    sections = add_feedback_links(uid, sections)

    # 4) RESUMO EXECUTIVO "Seu dia em 60 segundos"
    log(f"  gerando recap executivo...")
    recap_data = generate_daily_recap(user["name"], sections, trending, learned)
    daily_recap = recap_data.get("recap", "") if isinstance(recap_data, dict) else (recap_data or "")
    daily_quote = recap_data.get("quote", "") if isinstance(recap_data, dict) else ""
    daily_quote_author = recap_data.get("quote_author", "") if isinstance(recap_data, dict) else ""
    if daily_recap:
        log(f"  ✓ recap gerado ({len(daily_recap)} chars)")
    if daily_quote:
        log(f"  ✓ quote do dia: {daily_quote[:60]}")

    # 5) RENDER + ENVIO
    # Link assinado HMAC pra página /manage (válido 30 dias)
    signed_manage = gen_manage_url(MANAGE_URL, uid, ttl_days=30)
    email_mode = (user.get("email_mode") or "coado").lower()

    html = render_email(
        user_name=user["name"], date_obj=now_brt,
        trending=trending, trending_label=trending_label,
        sections=sections, manage_url=signed_manage,
        user_id=uid,
        daily_recap=daily_recap,
        daily_quote=daily_quote,
        daily_quote_author=daily_quote_author,
        email_mode=email_mode,
    )
    result = send_email(user["email"], user["name"], html, now_brt)
    log(f"  ✓ enviado", id=result.get("id"))
    supabase.table("users").update({
        "last_sent_at": now_brt.isoformat(),
        "welcome_sent": True,
    }).eq("id", uid).execute()
    return True


def main():
    now_brt = datetime.now(BRT)
    target_hour = TARGET_HOUR_BRT if TARGET_HOUR_BRT >= 0 else now_brt.hour
    log(f"=== Manhã ☕ V3 run ===", hora=target_hour, dry=DRY_RUN)

    # CATCH-UP: pega TODOS os usuários ativos que ainda não receberam hoje.
    # Não filtra mais por hora exata — quem cadastrou pra 6h e perdeu, recebe na próxima execução.
    today_start_brt = now_brt.replace(hour=0, minute=0, second=0, microsecond=0)

    res = supabase.table("users").select("*").eq("active", True).execute()
    all_users = res.data or []

    users = []
    for u in all_users:
        last = u.get("last_sent_at")
        if not last:
            # nunca recebeu — manda
            users.append(u)
            continue
        # parse ISO timestamp (Supabase devolve com timezone UTC)
        try:
            last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=BRT)
            last_brt = last_dt.astimezone(BRT)
            if last_brt < today_start_brt:
                users.append(u)
            # senão: já recebeu hoje, pula
        except Exception as e:
            # se não conseguir parsear, assume que precisa enviar
            log(f"  ⚠ não consegui parsear last_sent_at de {u.get('email')}: {e}")
            users.append(u)

    log(f"usuários elegíveis (catch-up)", count=len(users), total_ativos=len(all_users))

    for user in users:
        try:
            process_user(user, now_brt)
        except Exception as e:
            log(f"  ✗ ERRO {user.get('email','?')}: {e}")
            import traceback; traceback.print_exc()

    log("=== fim ===")


if __name__ == "__main__":
    main()
