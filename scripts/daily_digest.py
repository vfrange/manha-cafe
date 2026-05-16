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
from feedback_token import short_id, feedback_url, manage_url as gen_manage_url, unsub_url as gen_unsub_url
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
MAX_NEWS_OUT_PER_TOPIC = 2  # legado — agora cálculo dinâmico via daily_news_per_topic()
MAX_TRENDING_OUT = 10

# ============================================================================
# CONFIG DE VOLUME — ESCADINHAS POR NÚMERO DE TEMAS
# ============================================================================
# A escadinha controla quantas notícias o user recebe baseado em quantos temas escolheu.
# Mais temas → menos por tema (pra não saturar). E o "Em Alta" é ELÁSTICO:
# preenche o que falta até atingir o cap total da edição.

# --- DAILY (~5 min Espresso / ~9 min Coado em users medianos) ---
DAILY_TOTAL_CAP = 28              # cap absoluto de notícias na edição daily
DAILY_TRENDING_MIN = 3            # Em Alta nunca cai abaixo disso
DAILY_TRENDING_MAX = 6            # Em Alta nunca passa disso (preserva foco nos temas)

def daily_news_per_topic(topic_count: int) -> int:
    """Escadinha do daily: quantas notícias por tema baseado em qtd de temas."""
    if topic_count <= 3:
        return 5
    elif topic_count <= 6:
        return 4
    elif topic_count <= 10:
        return 3
    else:  # 11+
        return 2

def daily_trending_budget(topic_count: int) -> int:
    """Em Alta elástico no daily: preenche o que sobra do cap."""
    per_topic = daily_news_per_topic(topic_count)
    used = per_topic * topic_count
    remaining = DAILY_TOTAL_CAP - used
    return max(DAILY_TRENDING_MIN, min(DAILY_TRENDING_MAX, remaining))


# --- WEEKLY (~12 min Coado em users medianos, retrospectiva semanal) ---
WEEKLY_TOTAL_CAP = 35             # cap absoluto de notícias na edição weekly
WEEKLY_TRENDING_MIN = 5           # Em Alta nunca cai abaixo disso
WEEKLY_TRENDING_MAX = 10          # Em Alta nunca passa disso

def weekly_news_per_topic(topic_count: int) -> int:
    """Escadinha do weekly: quantas notícias por tema."""
    if topic_count <= 4:
        return 5
    elif topic_count <= 7:
        return 4
    elif topic_count <= 11:
        return 3
    else:  # 12-15
        return 2

def weekly_trending_budget(topic_count: int) -> int:
    """Em Alta elástico no weekly: preenche o que sobra do cap."""
    per_topic = weekly_news_per_topic(topic_count)
    used = per_topic * topic_count
    remaining = WEEKLY_TOTAL_CAP - used
    return max(WEEKLY_TRENDING_MIN, min(WEEKLY_TRENDING_MAX, remaining))


# Legados pra compatibilidade (alguns lugares ainda usam)
WEEKLY_TRENDING_BUDGET = 10
WEEKLY_NEWS_PER_TOPIC_FEW = 5
WEEKLY_NEWS_PER_TOPIC_MANY = 3
WEEKLY_THRESHOLD_FEW_TOPICS = 4

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
def fetch_all_sources(query, country, category=None, label=None, source_type="curated", weekly=False):
    """
    Roda todas as fontes em paralelo. Retorna lista combinada de notícias brutas.
    Para temas BR, prioriza RSS BR + Google News BR + Reddit.
    Para temas Global/Tech, prioriza Google News Global + HN + Reddit.
    Para temas CURADOS GLOBAL, adiciona RSS direto de NYT/WaPo/BBC/etc.

    Args:
        weekly: se True, busca notícias dos últimos 7 dias (Recorte da Semana).
                Senão, busca padrão (últimas 24-48h).
    """
    is_br = country == "BR"
    is_global = country == "GLOBAL"
    is_tech = category == "tecnologia" or any(
        kw in query.lower() for kw in ["tech","ia ","ai","intelig","gpt","openai","software"]
    )

    # Filtros temporais quando weekly=True
    gnews_when = "7d" if weekly else None
    reddit_time = "week" if weekly else "day"
    # No weekly aumentamos limites das fontes pra ter mais material pra curar
    gnews_max = 15 if weekly else 8
    reddit_max = 8 if weekly else 4
    hn_max = 10 if weekly else 5
    br_max = 15 if weekly else 8

    fetchers = []
    fetchers.append(("google_news", lambda: google_news.fetch(query, country, max_items=gnews_max, when=gnews_when)))
    if is_tech:
        fetchers.append(("hacker_news", lambda: hacker_news.fetch(query, max_items=hn_max)))
    fetchers.append(("reddit", lambda: reddit.fetch(query, category=category, max_items=reddit_max, time_filter=reddit_time)))
    if is_br:
        fetchers.append(("br_rss", lambda: br_rss.fetch(query, category=category, max_items=br_max)))
    # RSS internacional: só pra temas CURADOS GLOBAIS (não para customs)
    if is_global and source_type == "curated" and label:
        fetchers.append(("intl_rss", lambda: intl_rss.fetch_for_topic(label, max_per_feed=4 if weekly else 2)))

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


def fetch_trending(country, weekly=False):
    """Combina trends: Google News Top Stories + Reddit + Bluesky + YouTube Trending.

    Args:
        weekly: se True, prioriza top da semana (top stories continua igual,
                Reddit muda pra t=week).
    """
    trends = []

    # 1) Google News Top Stories (manchetes em alta) — sempre tenta
    # Obs: Top Stories do Google News é sempre "agora", não tem filtro semanal nativo.
    # Pra weekly, complementamos com Reddit top da semana e maior volume.
    try:
        trends.extend(google_news.fetch_trends(country))
    except Exception as e:
        log(f"  ⚠ google news top: {e}")

    # 2) Reddit top (viralidade social geral)
    reddit_time = "week" if weekly else "day"
    reddit_max = 12 if weekly else 6
    if country in ("GLOBAL", "US"):
        try:
            trends.extend(reddit.fetch_trending_general(max_items=reddit_max, time_filter=reddit_time))
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


def _call_claude_json(prompt, max_tokens=4000, retries=2, log_prefix=""):
    """
    Chama Claude e parseia JSON. Se falhar, tenta de novo (até `retries` vezes adicionais).
    Reforça o prompt em cada retry pra forçar JSON limpo.
    Retorna dict (vazio se tudo falhar).
    """
    last_err = None
    current_prompt = prompt
    for attempt in range(1, retries + 2):  # 1ª tentativa + retries
        try:
            resp = claude.messages.create(
                model=MODEL, max_tokens=max_tokens,
                messages=[{"role": "user", "content": current_prompt}]
            )
            text = resp.content[0].text.strip()
            parsed = _robust_json_parse(text)
            if parsed:  # retorno não-vazio = sucesso
                if attempt > 1:
                    log(f"  ✓ Claude OK na tentativa {attempt}{log_prefix}")
                return parsed
            # JSON inválido — log e retry
            if attempt <= retries:
                log(f"  ⚠ JSON inválido tentativa {attempt}/{retries+1}{log_prefix}, retentando...")
                current_prompt = prompt + "\n\n⚠️ ATENÇÃO: A resposta anterior teve JSON inválido. Responda APENAS com JSON válido, sem markdown, sem ```, sem nenhum texto antes ou depois. Apenas o objeto JSON."
        except Exception as e:
            last_err = e
            if attempt <= retries:
                log(f"  ⚠ Claude API erro tentativa {attempt}{log_prefix}: {e}, retentando...")
                time.sleep(1.5)  # backoff curto
    log(f"  ✗ JSON do Claude irrecuperável após {retries+1} tentativas{log_prefix}" + (f" (último erro: {last_err})" if last_err else ""))
    return {}


def _curate_news_batch(user_name, topics_with_news, learned_profile="", filtered_items=None,
                       weekly=False, news_per_topic=None):
    """Processa UM batch de temas (até MAX_TOPICS_PER_BATCH). Aplica filtros do user se passados.

    Args:
        weekly: se True, prompts focam em retrospectiva semanal e sintese contextualizada.
        news_per_topic: limite de notícias por tema (sobrescreve MAX_NEWS_OUT_PER_TOPIC).
                        Se None, usa default daily (2). Weekly tipicamente passa 3 ou 4.
    """
    out_per_topic = news_per_topic if news_per_topic is not None else MAX_NEWS_OUT_PER_TOPIC

    payload = []
    has_political = False
    for t in topics_with_news:
        # PRE-FILTER: descarta matérias com sinais de conteúdo proibido
        # No weekly, aceitamos mais matérias por tema (até 12 brutas)
        max_input = 12 if weekly else MAX_NEWS_INPUT_PER_TOPIC
        clean_news = [n for n in t["news"][:max_input] if is_safe_news(n)]
        # Pre-filter também por filtros do user (case-insensitive)
        if filtered_items:
            norm_filters = [f.strip().lower() for f in filtered_items if isinstance(f, str) and f.strip()]
            if norm_filters:
                clean_news = [
                    n for n in clean_news
                    if not any(
                        f in (n.get("source") or "").lower() or
                        f in (n.get("title") or "").lower() or
                        f in (n.get("summary") or "").lower()
                        for f in norm_filters
                    )
                ]
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

    # Instrução de filtros (lista de "não receber")
    filter_instruction = ""
    if filtered_items:
        filter_list = ", ".join(f'"{f}"' for f in filtered_items[:20])
        filter_instruction = f"""

🚫 **FILTROS DURO DO USUÁRIO** — proibido incluir notícias que envolvam estes temas/veículos:
{filter_list}

Se o "fonte" de uma matéria estiver nessa lista, DESCARTE-A integralmente. Se a manchete trata de um tema dessa lista, DESCARTE-A. Em dúvida, descarte.
"""

    # Contexto temporal e instruções específicas pra weekly
    if weekly:
        time_context = "Esta é a **edição SEMANAL** do Recorte (Recorte da Semana), enviada aos sábados. As notícias abaixo cobrem os **últimos 7 dias**."
        editorial_brief = f"Para cada tema, selecione as **até {out_per_topic} notícias mais marcantes da SEMANA** (priorize: eventos com desdobramento ao longo dos dias, marcos relevantes, análise de tendência; ignore atualizações intra-dia repetitivas)."
        resumo_instr = "**resumo**: 4-5 frases (140-220 palavras) em PT-BR. Foque em **síntese semanal**: o que aconteceu, como evoluiu nos dias, contexto, e implicação. Para temas com vários acontecimentos na semana, costure-os numa narrativa coesa em vez de listar isoladamente."
        fatos_instr = "**fatos_chave**: array de 4 a 6 bullets curtos (cada um 6-18 palavras) com pontos-chave da semana — números, datas dos acontecimentos, players, valores, decisões."
    else:
        time_context = "As notícias abaixo são dos últimos 1-2 dias (edição diária)."
        editorial_brief = f"Para cada tema abaixo, selecione as **até {out_per_topic} notícias mais relevantes** do dia (priorize: impacto real, novidade, alinhamento com perfil; evite duplicatas e clickbait)."
        resumo_instr = "**resumo**: 3-4 frases (100-160 palavras) em PT-BR. Explica o que aconteceu, números/fatos centrais, contexto e implicação imediata"
        fatos_instr = "**fatos_chave**: array de 3 a 5 bullets curtos (cada um 6-15 palavras) com os pontos mais importantes — números, datas, players, valores, decisões. Ex: [\"Selic caiu de 13,75% para 13,25%\", \"1ª redução em 12 meses\", \"Mercado esperava corte de 0,75 ponto\"]"

    prompt = f"""Você é editor de uma newsletter premium em PT-BR ao estilo Morning Brew, escrevendo para {user_name}.
{profile_section}
{filter_instruction}
{SAFETY_INSTRUCTIONS}
{bias_section}
{time_context}

{editorial_brief}

🇧🇷 **REGRA CRÍTICA DE IDIOMA**: TODO o conteúdo gerado DEVE estar em **português brasileiro natural**, MESMO QUE a matéria original esteja em inglês, espanhol ou outro idioma. Traduza com fluência.

⚠️ **REGRA CRÍTICA DE FORMATO**: Retorne APENAS JSON VÁLIDO, sem markdown, sem ```. Escape TODAS as aspas duplas dentro de strings com \\". Não use quebras de linha dentro de strings.

📰 **OBJETIVO**: O usuário deve conseguir entender a notícia INTEIRA sem precisar abrir o link. Seja rico em fatos, números e contexto. Mas mantenha estilo Morning Brew: direto, esperto, sem encher linguiça.

Para cada notícia selecionada, retorne:
- **manchete**: título em PT-BR direto, máx 90 caracteres, sem clickbait
- {resumo_instr}
- {fatos_instr}
- **link**: copie o link original
- **fonte**: nome do veículo
- **lang**: código do idioma original da matéria (ex: "en", "fr", "de"). Omita se for PT-BR.
- **pol_bias** (APENAS para temas marcados `tema_politico: true`): "factual", "centro", "esq" ou "dir". Veja regras acima.

Se um tema tiver pouca notícia relevante, retorne menos itens. Se nada for relevante, omita o tema.

Dados:
{json.dumps(payload, ensure_ascii=False, indent=2)}

**RESPONDA APENAS JSON VÁLIDO**:
{{"secoes":[{{"tema":"<nome>","noticias":[{{"manchete":"...","resumo":"...","fatos_chave":["...","..."],"link":"...","fonte":"...","lang":"...","pol_bias":"..."}}]}}]}}"""

    parsed = _call_claude_json(prompt, max_tokens=12000, retries=2, log_prefix=" (curate_news)")
    secoes = parsed.get("secoes", []) if parsed else []
    # POST-FILTER: re-valida cada item curado
    for sec in secoes:
        sec["noticias"] = [n for n in sec.get("noticias", []) if is_safe_curated(n)]
    # POST-FILTER: aplica filtros do user (caso Claude tenha escapado)
    if filtered_items:
        for sec in secoes:
            sec["noticias"] = _apply_user_filters(sec.get("noticias", []), filtered_items)
    return secoes


def curate_news(user_name, topics_with_news, learned_profile="", filtered_items=None,
                weekly=False, news_per_topic=None):
    """
    Curadoria com batching: divide temas em grupos de até MAX_TOPICS_PER_BATCH
    pra evitar JSON gigante que pode quebrar.

    Args:
        weekly: edição semanal (prompt e budget diferentes)
        news_per_topic: limite por tema (default 2 daily; weekly tipicamente passa 3 ou 4).
    """
    if not topics_with_news:
        return []

    all_sections = []
    batches = [
        topics_with_news[i:i+MAX_TOPICS_PER_BATCH]
        for i in range(0, len(topics_with_news), MAX_TOPICS_PER_BATCH)
    ]
    log(f"  curando em {len(batches)} batch(es) de até {MAX_TOPICS_PER_BATCH} temas{' (modo weekly)' if weekly else ''}")

    for idx, batch in enumerate(batches, 1):
        log(f"  batch {idx}/{len(batches)}: {len(batch)} temas")
        sections = _curate_news_batch(
            user_name, batch, learned_profile, filtered_items,
            weekly=weekly, news_per_topic=news_per_topic,
        )
        all_sections.extend(sections)

    return all_sections


def _norm_for_dedup(text: str) -> str:
    """Normaliza string pra detectar duplicatas: minúsculo, sem pontuação/espaços."""
    if not text:
        return ""
    return re.sub(r'[^a-z0-9]', '', text.lower())


def _apply_user_filters(items, filtered_items):
    """
    Aplica filtros do usuário (lista de 'temas/veículos a NÃO receber').
    Match case-insensitive em substring contra:
    - campo 'fonte' (pega bloqueios de veículos: "Carta Capital", "UOL")
    - campo 'manchete' e 'resumo' (pega bloqueios de tema: "celebridades")

    Retorna lista filtrada (sem os matches).
    """
    if not filtered_items or not items:
        return items

    # Normaliza filtros: lowercase, strip
    norm_filters = [f.strip().lower() for f in filtered_items if isinstance(f, str) and f.strip()]
    if not norm_filters:
        return items

    out = []
    for it in items:
        fonte = (it.get("fonte") or "").lower()
        manchete = (it.get("manchete") or "").lower()
        resumo = (it.get("resumo") or "").lower()
        # Bloqueia se algum filtro aparecer como substring em fonte/manchete/resumo
        blocked = any(f in fonte or f in manchete or f in resumo for f in norm_filters)
        if blocked:
            continue
        out.append(it)

    removed = len(items) - len(out)
    if removed:
        log(f"  ⚠ filtros do user: removeu {removed} item(ns) por bloqueios explícitos")
    return out


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


def _dedupe_sections_against_trends(sections, trending):
    """
    Remove notícias de cada seção que já apareceram em trending (Em Alta).
    Evita repetir a mesma manchete entre "Em Alta hoje" e "Notícias por tema".
    Critério: mesmo link OU manchetes muito similares (assinatura de 50 chars).
    Modifica sections in-place e retorna o total de itens removidos.
    """
    if not trending or not sections:
        return 0

    trend_links = set()
    trend_sigs = set()
    for t in trending:
        link = (t.get("link") or "").strip().lower()
        if link:
            trend_links.add(link)
        sig = _norm_for_dedup(t.get("manchete", ""))[:50]
        if sig:
            trend_sigs.add(sig)

    removed = 0
    for sec in sections:
        original = sec.get("noticias", [])
        kept = []
        for n in original:
            link = (n.get("link") or "").strip().lower()
            sig = _norm_for_dedup(n.get("manchete", ""))[:50]
            if (link and link in trend_links) or (sig and sig in trend_sigs):
                removed += 1
                continue
            kept.append(n)
        sec["noticias"] = kept

    if removed:
        log(f"  ⚠ dedup cruzado: removeu {removed} notícia(s) repetida(s) entre Em Alta e temas")
    return removed


def curate_trends(user_name, scope_label, trends, learned_profile="",
                  user_topics_labels=None, filtered_items=None, max_out=None, weekly=False):
    """
    Em Alta: até 5 trends (default daily) ou 10 (weekly).
    Se user_topics_labels forem passados, monta híbrido:
      - Daily: 3 gerais + 2 relacionados (total 5)
      - Weekly: 5 gerais + 5 relacionados (total 10)
    Se max_out fornecido, sobrescreve o limite total.
    Se filtered_items presente, instrui Claude a evitar veículos/temas filtrados E aplica pós-filtro.
    Se weekly=True, prompt muda pra "retrospectiva da semana".
    """
    if not trends:
        return []

    MAX_TRENDS_INPUT = 30 if weekly else 18
    # Pre-filter
    trends_clean = [t for t in trends if is_safe_news(t)]
    trends_truncated = trends_clean[:MAX_TRENDS_INPUT]

    profile_section = ""
    if learned_profile.strip():
        profile_section = f"\n**PERFIL DO USUÁRIO**: {learned_profile}\nUse pra priorizar trends que casem com interesses dele.\n"

    # Define totais conforme modo (daily vs weekly)
    if weekly:
        total_target = 10
        gen_count = 5
        rel_count = 5
        context_intro = "Estas são as principais manchetes e trends dos **últimos 7 dias**. Esta é a edição **semanal** do Recorte (recebido aos sábados)."
        instruction_verb = "Selecione os eventos mais marcantes DA SEMANA"
    else:
        total_target = 5
        gen_count = 3
        rel_count = 2
        context_intro = "Estas são as manchetes em alta de hoje."
        instruction_verb = "Selecione os eventos mais relevantes do dia"

    # Modo híbrido se temos os temas do user
    has_topics = bool(user_topics_labels)
    if has_topics:
        topics_str = ", ".join(user_topics_labels[:15])
        mix_instruction = f"""

🎯 **SELEÇÃO HÍBRIDA — total de {total_target} itens**:
- **{gen_count} eventos GERAIS**: top stories {'da semana' if weekly else 'do dia'}, alta circulação, qualquer assunto relevante (sem filtro por interesse)
- **{rel_count} eventos RELACIONADOS aos temas do usuário**: itens que se conectem aos temas dele, mesmo que não sejam os top virais gerais.

**Temas do usuário pra cruzar nos {rel_count} itens "RELACIONADOS"**: {topics_str}

⚠️ Esses {total_target} itens NÃO podem ser repetidos depois nas notícias por tema. Os {rel_count} "RELACIONADOS" são a versão "trending" desses interesses; as notícias por tema serão *outras* manchetes sobre os mesmos assuntos.
"""
        total_out = total_target
    else:
        mix_instruction = ""
        total_out = total_target if weekly else MAX_TRENDING_OUT

    # Sobrescreve com max_out se fornecido (quando múltiplos scopes)
    if max_out is not None and max_out > 0:
        total_out = min(total_out, max_out)

    # Instrução de filtros (lista negra)
    filter_instruction = ""
    if filtered_items:
        filter_list = ", ".join(f'"{f}"' for f in filtered_items[:20])
        filter_instruction = f"""

🚫 **FILTROS DURO DO USUÁRIO** — proibido incluir trends que envolvam estes temas/veículos:
{filter_list}

Se um trend é de um veículo dessa lista (campo "fonte"), DESCARTE-O integralmente, mesmo que o conteúdo seja relevante.
Se um trend é sobre um tema dessa lista, DESCARTE-O.
Não tente reinterpretar — se em dúvida, descarte.
"""

    prompt = f"""Você é editor da seção "🔥 Em Alta" em PT-BR pra {user_name}, escopo: **{scope_label}**.
{profile_section}
{filter_instruction}
{mix_instruction}
{SAFETY_INSTRUCTIONS}

{context_intro}

{instruction_verb} ({total_out} itens): top stories + redes sociais + viralizações. Priorize: eventos significativos, lançamentos, esporte/cultura de impacto. Evite fofoca rasa, conteúdo regional sem contexto, ou jargão obscuro.

🚫 **REGRA CRÍTICA DE DEDUPLICAÇÃO**: NUNCA inclua duas manchetes sobre o MESMO evento, mesmo que venham de fontes diferentes ou com palavras ligeiramente diferentes. Se ver vários trends brutos sobre o mesmo acontecimento, escolha APENAS UM (o de fonte mais relevante) e descarte os outros. Em caso de dúvida sobre se 2 trends são o mesmo evento, considere que SÃO e una.

🇧🇷 **REGRA CRÍTICA DE IDIOMA**: TODO conteúdo (manchete, resumo, fatos) DEVE estar em **português brasileiro fluente**, MESMO QUE original esteja em inglês ou outro idioma. Mantenha nomes próprios e marcas no original.

⚠️ **REGRA CRÍTICA DE FORMATO**: APENAS JSON VÁLIDO, sem markdown, sem ```. Escape aspas duplas com \\". Sem quebras de linha dentro de strings.

📰 **OBJETIVO**: o usuário entende cada evento sem precisar abrir o link. {'Como é semanal, dê contexto e mencione a evolução ao longo dos dias quando relevante.' if weekly else 'Seja rico em fatos e contexto.'}

Pra cada item:
- **manchete**: título PT-BR direto, máx 90 chars, sem clickbait
- **resumo**: {('4-5 frases (140-200 palavras) com contexto semanal: o que aconteceu, como evoluiu, e por que importa' if weekly else '3-4 frases (100-160 palavras) explicando o que rolou, números/contexto, e POR QUE está em alta hoje')}
- **fatos_chave**: array de {('4-6' if weekly else '3-5')} bullets curtos (6-15 palavras cada) com números, datas, players, valores
- **buscas** (opcional): se vier do input, copie
- **link**: URL relacionada
- **fonte**: veículo

Trends brutos:
{json.dumps(trends_truncated, ensure_ascii=False, indent=2)}

**APENAS JSON VÁLIDO**:
{{"trending":[{{"manchete":"...","resumo":"...","fatos_chave":["..."],"link":"...","fonte":"..."}}]}}"""

    parsed = _call_claude_json(prompt, max_tokens=6000, retries=2, log_prefix=" (curate_trends)")
    items = parsed.get("trending", []) if parsed else []
    # Post-filter: safety
    items = [it for it in items if is_safe_curated(it)]
    # Post-filter: filtros do user (case-insensitive substring no campo fonte ou manchete)
    if filtered_items:
        items = _apply_user_filters(items, filtered_items)
    # Post-filter: dedup interno
    before = len(items)
    items = _dedupe_trends(items)
    if len(items) < before:
        log(f"  ⚠ dedup removeu {before - len(items)} duplicatas do Em Alta")
    return items


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

    parsed = _call_claude_json(prompt, max_tokens=900, retries=2, log_prefix=" (recap)")
    if not parsed:
        return {"recap": "", "quote": "", "quote_author": ""}
    return {
        "recap": (parsed.get("recap") or "").strip(),
        "quote": (parsed.get("quote") or "").strip(),
        "quote_author": (parsed.get("quote_author") or "").strip(),
    }


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


def _try_decode_gnews_url(url, timeout=4):
    """
    Tenta decodificar uma URL wrapper do Google News pra URL real da matéria.
    
    URLs do Google News (CBMi...) muitas vezes não redirecionam corretamente,
    abrindo só a página do feed em vez do artigo. Esta função usa a lib
    googlenewsdecoder pra resolver pro link original do publisher.

    Defensivo: se falhar (rate limit, network, etc), retorna a URL original
    sem regredir.
    """
    if not url or "news.google.com" not in url:
        return url
    try:
        from googlenewsdecoder import gnewsdecoder
        result = gnewsdecoder(url, interval=1)
        if isinstance(result, dict) and result.get("status") and result.get("decoded_url"):
            decoded = result["decoded_url"]
            if decoded.startswith("http"):
                return decoded
    except Exception as e:
        log(f"    [gnews] falhou decode {url[:50]}...: {e}")
    return url


def resolve_gnews_urls(sections, trending, max_workers=6):
    """
    Resolve URLs de Google News pra URLs reais.
    Roda em paralelo pra não atrasar o pipeline.
    Aplicado SÓ nas URLs finais (~30-40 notícias), não nas 200+ brutas.
    """
    from concurrent.futures import ThreadPoolExecutor

    # Coleta todas as URLs gnews
    targets = []  # [(container, key, url), ...]
    for sec in sections:
        for n in sec.get("noticias", []):
            link = n.get("link", "")
            if "news.google.com" in link:
                targets.append((n, "link", link))
    for item in (trending or []):
        link = item.get("link", "")
        if "news.google.com" in link:
            targets.append((item, "link", link))

    if not targets:
        return

    log(f"  resolvendo {len(targets)} URLs do Google News em paralelo...")
    
    def _resolve(target):
        container, key, url = target
        new_url = _try_decode_gnews_url(url)
        if new_url != url:
            container[key] = new_url
            return True
        return False

    resolved_count = 0
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        results = list(ex.map(_resolve, targets))
        resolved_count = sum(1 for r in results if r)

    log(f"  ✓ {resolved_count}/{len(targets)} URLs resolvidas (resto mantém wrapper)")


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
        prof = res.data[0]
        prof.setdefault("filtered_items", [])
        return prof
    return {"learned_text": "", "paused_topics": [], "filtered_items": []}


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
def send_email(to_email, to_name, html, date_obj, weekly=False, user_id=None):
    first = to_name.split()[0]
    if weekly:
        subject = f"🗞 Seu Recorte da Semana, {first} — {date_obj.strftime('%d/%m')}"
    else:
        subject = f"☕ Seu Recorte de hoje, {first} — {date_obj.strftime('%d/%m')}"
    if DRY_RUN:
        log("DRY_RUN", to=to_email, subject=subject)
        fname = f"preview_{to_email.replace('@','_at_')}.html"
        with open(fname, "w", encoding="utf-8") as f:
            f.write(html)
        return {"id": "dry-run"}
    # Headers RFC 2369 / 8058 — Gmail/Apple Mail mostram "Cancelar inscrição" no header e suportam 1-click
    headers = {}
    if user_id:
        unsub = gen_unsub_url(SUPABASE_URL, user_id)
        headers = {
            "List-Unsubscribe": f"<{unsub}>, <mailto:unsubscribe@recorte.news?subject=Unsubscribe>",
            "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
        }
    return resend.Emails.send({
        "from": FROM_EMAIL, "to": to_email,
        "subject": subject, "html": html,
        "headers": headers,
    })


# ============ MAIN ============
def process_user(user, now_brt, weekly=False):
    uid = user["id"]
    default_country = user.get("default_country") or "BR"
    log(f"processando", email=user["email"], pais=default_country)

    # carrega perfil aprendido
    profile = load_profile(uid)
    learned = profile.get("learned_text", "") or ""
    paused = profile.get("paused_topics", []) or []
    filtered_items = profile.get("filtered_items", []) or []
    if learned:
        log(f"  perfil: {learned[:80]}...")
    if paused:
        log(f"  temas pausados: {len(paused)}")
    if filtered_items:
        log(f"  filtros do user: {len(filtered_items)} itens")

    # Pré-busca os labels dos temas do user (pra Em Alta híbrido + escadinha)
    _topics_pre = supabase.table("topics").select("label").eq("user_id", uid).execute()
    user_topic_labels = [t["label"] for t in (_topics_pre.data or [])]
    # Conta temas únicos pra alimentar a escadinha (Em Alta elástico + news_per_topic)
    topic_count_for_scaling = len({lbl for lbl in user_topic_labels}) if user_topic_labels else 0

    # 1) TRENDING — suporta CSV multi-scope ex: "br,global,country:IL"
    trending = []
    trending_label = ""
    if user.get("trending_enabled", True):
        raw_scope = user.get("trending_scope") or "br"
        scopes = [s.strip() for s in raw_scope.split(",") if s.strip()]
        if not scopes:
            scopes = ["br"]

        labels = []
        # Budget ELÁSTICO: usa qtd de temas únicos pra calcular
        # quantos itens em Em Alta cabem (cap 28 daily / 35 weekly).
        if weekly:
            TOTAL_TRENDING_BUDGET = weekly_trending_budget(topic_count_for_scaling)
        else:
            TOTAL_TRENDING_BUDGET = daily_trending_budget(topic_count_for_scaling)
        budget_per_scope = max(2, TOTAL_TRENDING_BUDGET // len(scopes) + 1)
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
            raw_trends = fetch_trending(tcountry, weekly=weekly)
            log(f"  trends brutos", count=len(raw_trends), scope=tcountry, weekly=weekly)
            if raw_trends:
                curated = curate_trends(
                    user["name"], tlabel, raw_trends, learned,
                    user_topics_labels=user_topic_labels,
                    filtered_items=filtered_items,
                    max_out=budget_per_scope,
                    weekly=weekly,
                )
                # marca o escopo de origem em cada item pra debug/futuro uso
                for item in curated:
                    item.setdefault("scope_origin", tlabel)
                trending.extend(curated)

        trending_label = " + ".join(labels) if labels else ""

        # Cap global final: 5 itens no Em Alta (independente de quantos scopes)
        if len(trending) > TOTAL_TRENDING_BUDGET:
            # Round-robin entre scopes pra manter balanço
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

    # 2) NOTÍCIAS POR TEMA
    topics_res = supabase.table("topics").select("*").eq("user_id", uid).execute()
    topics = topics_res.data or []
    # Fallback geográfico: se "Onde você está" = INTL, usa GLOBAL como default
    fallback_country = "GLOBAL" if default_country == "INTL" else default_country

    # news_per_topic dinâmico baseado em qtd de temas (escadinha)
    # topic_count_for_scaling foi calculado no início do process_user (linha ~927)
    if weekly:
        news_per_topic = weekly_news_per_topic(topic_count_for_scaling)
    else:
        news_per_topic = daily_news_per_topic(topic_count_for_scaling)

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
            source_type=t.get("source", "curated"),
            weekly=weekly,
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
                "source": t.get("source", "curated"),  # 'custom' ou 'curated' — usado pra ordenar
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

    # Ordena: temas CUSTOMIZADOS (digitados pelo user) primeiro, depois CURADOS (pré-selecionados)
    # Mantém ordem original dentro de cada grupo (estável)
    topics_with_news.sort(key=lambda g: 0 if g.get("source") == "custom" else 1)

    sections = []
    if topics_with_news:
        raw_sections = curate_news(
            user["name"], topics_with_news, learned,
            filtered_items=filtered_items,
            weekly=weekly,
            news_per_topic=news_per_topic,
        )
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

    # 2.5) DEDUP CRUZADO: remove notícias dos temas que já aparecem em Em Alta
    if trending and sections:
        _dedupe_sections_against_trends(sections, trending)
        # Se alguma seção ficou vazia após o dedup, remove ela
        sections = [s for s in sections if s.get("noticias")]

    # 2.6) REORDENAR sections: customizados primeiro, depois curados (Claude pode ter reordenado)
    # Usa label_meta + topics_with_news pra saber source de cada label
    label_to_source = {t["label"]: t.get("source", "curated") for t in topics_with_news}
    sections.sort(key=lambda s: 0 if label_to_source.get(s.get("topic"), "curated") == "custom" else 1)

    # 2.7) RESOLVE URLs do Google News pra URLs reais dos publishers
    # Aplicado SÓ nas notícias finais (~30) — não nas 200+ brutas — pra economizar tempo.
    resolve_gnews_urls(sections, trending)

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
    signed_unsub = gen_unsub_url(SUPABASE_URL, uid)
    email_mode = (user.get("email_mode") or "coado").lower()

    # Saudação: o daily roda 6h BRT (sempre manhã); weekly roda sábado 8h BRT.
    # Mas este path TAMBÉM serve o welcome (que pode rodar a qualquer hora) — nesse caso usa auto.
    user_tz = user.get("timezone") or "America/Sao_Paulo"
    is_welcome = not user.get("welcome_sent")
    if is_welcome:
        saudacao_mode = "auto"  # respeita fuso do user
    elif weekly:
        saudacao_mode = "sabado"
    else:
        saudacao_mode = "manha"

    html = render_email(
        user_name=user["name"], date_obj=now_brt,
        trending=trending, trending_label=trending_label,
        sections=sections, manage_url=signed_manage,
        user_id=uid,
        daily_recap=daily_recap,
        daily_quote=daily_quote,
        daily_quote_author=daily_quote_author,
        email_mode=email_mode,
        weekly_mode=weekly,
        user_tz=user_tz,
        saudacao_mode=saudacao_mode,
        filtered_items_count=len(filtered_items),
        is_welcome=is_welcome,
        unsub_url=signed_unsub,
    )
    result = send_email(user["email"], user["name"], html, now_brt, weekly=weekly, user_id=uid)
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

    if not users:
        log("=== fim (nada pra processar) ===")
        return

    # Paraleliza processamento de usuários — cada user_process é independente.
    # Anthropic + Supabase clients são thread-safe (HTTP).
    # Pool default = 5; ajustável via env PARALLEL_WORKERS.
    workers = int(os.environ.get("PARALLEL_WORKERS", "5"))
    workers = max(1, min(workers, len(users)))  # nunca > total de users
    log(f"processando em paralelo", workers=workers, users=len(users))

    def _safe_process(u):
        try:
            process_user(u, now_brt)
            return ("ok", u.get("email", "?"), None)
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            return ("err", u.get("email", "?"), f"{e}\n{tb}")

    ok_count = 0
    err_count = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_safe_process, u): u for u in users}
        for fut in as_completed(futures):
            status, email, info = fut.result()
            if status == "ok":
                ok_count += 1
            else:
                err_count += 1
                log(f"  ✗ ERRO {email}: {info}")

    log(f"=== fim ===", ok=ok_count, err=err_count)


if __name__ == "__main__":
    main()
