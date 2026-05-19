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
from voice_prompt import VOICE_PROMPT
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
# Modelo premium pra tasks de alta visibilidade (Em Alta + Welcome curate_news)
MODEL_PREMIUM = "claude-sonnet-4-6"
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
DAILY_TRENDING_MIN = 5            # Em Alta nunca cai abaixo disso (mais relevância em destaque)
DAILY_TRENDING_MAX = 6            # Em Alta nunca passa disso (preserva foco nos temas)
def daily_news_per_topic(topic_count: int) -> int:
    """Escadinha do daily: quantas notícias por tema baseado em qtd de temas.
    
    Sem margem — confiamos no resolve_gnews_pre_curate() que valida URLs
    ANTES do Claude curar, garantindo que o que ele curar vai funcionar.
    """
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
    # Filtros temporais:
    # - weekly: últimos 7 dias (Recorte da Semana)
    # - daily/welcome: últimos 2 dias (evita notícias velhas tipo "escalação de jogo de ontem")
    gnews_when = "7d" if weekly else "2d"
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
        try:
            for fut in as_completed(futures, timeout=25):
                name = futures[fut]
                try:
                    items = fut.result()
                    results.extend(items)
                except Exception as e:
                    log(f"  ⚠ erro fonte {name}: {e}")
        except TimeoutError:
            # Não falha o pipeline inteiro se uma fonte travar — segue com o que coletou
            pendentes = [futures[f] for f in futures if not f.done()]
            log(f"  ⚠ timeout 25s — {len(pendentes)}/{len(futures)} fonte(s) não responderam: {', '.join(pendentes)} — seguindo com {len(results)} itens das fontes que responderam")
            # Cancela as travadas pra não vazar threads
            for f in futures:
                if not f.done():
                    f.cancel()
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
    """Combina trends: Google News Top Stories + Reddit + Bluesky + YouTube Trending."""
    trends = []
    try:
        trends.extend(google_news.fetch_trends(country))
    except Exception as e:
        log(f"  ⚠ google news top: {e}")
    reddit_time = "week" if weekly else "day"
    reddit_max = 12 if weekly else 6
    if country in ("GLOBAL", "US"):
        try:
            trends.extend(reddit.fetch_trending_general(max_items=reddit_max, time_filter=reddit_time))
        except Exception as e:
            log(f"  ⚠ reddit trending: {e}")
    try:
        trends.extend(bluesky.fetch_trending(max_items=8))
    except Exception as e:
        log(f"  ⚠ bluesky: {e}")
    try:
        trends.extend(youtube_trending.fetch_trending(country=country, max_items=8))
    except Exception as e:
        log(f"  ⚠ youtube trending: {e}")
    return trends
# ============ CLAUDE CURATION ============
MAX_NEWS_INPUT_PER_TOPIC = 6   # quantas notícias brutas mandar pro Claude por tema
MAX_TOPICS_PER_BATCH = 4       # quantos temas processar numa chamada (evita JSON gigante)
def _robust_json_parse(text):
    """Tenta parsear JSON com várias estratégias de fallback."""
    text = re.sub(r"^```(?:json)?\s*", "", text.strip())
    text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    i, j = text.find("{"), text.rfind("}")
    if i >= 0 and j > i:
        try:
            return json.loads(text[i:j+1])
        except json.JSONDecodeError:
            pass
    if '"secoes"' in text:
        partial = text[i:] if i >= 0 else text
        for cutoff in range(len(partial), 100, -100):
            candidate = partial[:cutoff]
            for ending in [']}', '"}]}', '"}]}']:
                try:
                    test = candidate.rstrip(',\n\r ') + ending
                    parsed = json.loads(test)
                    if "secoes" in parsed:
                        log(f"  ⚠ JSON reparado parcialmente em cutoff={cutoff}")
                        return parsed
                except json.JSONDecodeError:
                    continue
    log(f"  ✗ JSON do Claude irrecuperável, pulando esse batch")
    return {}
def _call_claude_json(prompt, max_tokens=4000, retries=2, log_prefix="",
                      model=None, system_prompt=None):
    """Chama Claude e parseia JSON com retry e prompt caching."""
    last_err = None
    current_prompt = prompt
    selected_model = model or MODEL
    system_blocks = []
    if system_prompt:
        system_blocks = [{
            "type": "text",
            "text": system_prompt,
            "cache_control": {"type": "ephemeral"},
        }]
    for attempt in range(1, retries + 2):
        try:
            kwargs = {
                "model": selected_model,
                "max_tokens": max_tokens,
                "messages": [{"role": "user", "content": current_prompt}],
            }
            if system_blocks:
                kwargs["system"] = system_blocks
            resp = claude.messages.create(**kwargs)
            text = resp.content[0].text.strip()
            parsed = _robust_json_parse(text)
            if parsed:
                if attempt > 1:
                    log(f"  ✓ Claude OK na tentativa {attempt}{log_prefix}")
                return parsed
            if attempt <= retries:
                log(f"  ⚠ JSON inválido tentativa {attempt}/{retries+1}{log_prefix}, retentando...")
                current_prompt = prompt + "\n\n⚠️ ATENÇÃO: A resposta anterior teve JSON inválido. Responda APENAS com JSON válido, sem markdown, sem ```, sem nenhum texto antes ou depois. Apenas o objeto JSON."
        except Exception as e:
            last_err = e
            if attempt <= retries:
                log(f"  ⚠ Claude API erro tentativa {attempt}{log_prefix}: {e}, retentando...")
                time.sleep(1.5)
    log(f"  ✗ JSON do Claude irrecuperável após {retries+1} tentativas{log_prefix}" + (f" (último erro: {last_err})" if last_err else ""))
    return {}
def _curate_news_batch(user_name, topics_with_news, learned_profile="", filtered_items=None,
                       weekly=False, news_per_topic=None, is_welcome=False):
    """Processa UM batch de temas (até MAX_TOPICS_PER_BATCH). Aplica filtros do user se passados."""
    out_per_topic = news_per_topic if news_per_topic is not None else MAX_NEWS_OUT_PER_TOPIC
    selected_model = MODEL_PREMIUM if is_welcome else None
    payload = []
    has_political = False
    for t in topics_with_news:
        max_input = 12 if weekly else MAX_NEWS_INPUT_PER_TOPIC
        clean_news = [n for n in t["news"][:max_input] if is_safe_news(n)]
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
            continue
        topic_is_political = is_political_topic(t["label"])
        if topic_is_political:
            has_political = True
        payload.append({
            "tema": t["label"],
            "pais": COUNTRY_NAMES.get(t["country"], t["country"]),
            "tema_politico": topic_is_political,
            "noticias_brutas": [
                {"titulo": n["title"], "fonte": n["source"], "preview": n["summary"],
                 "link": n["link"], "origem": n.get("origin",""),
                 "publicado_em": n.get("published_at", "")}
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
    filter_instruction = ""
    if filtered_items:
        filter_list = ", ".join(f'"{f}"' for f in filtered_items[:20])
        filter_instruction = f"""
🚫 **FILTROS DURO DO USUÁRIO** — proibido incluir notícias que envolvam estes temas/veículos:
{filter_list}
Se o "fonte" de uma matéria estiver nessa lista, DESCARTE-A integralmente. Se a manchete trata de um tema dessa lista, DESCARTE-A. Em dúvida, descarte.
"""
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
    system_prompt = f"""{VOICE_PROMPT}
# ============================================
# INSTRUÇÕES ESPECÍFICAS DESTA TAREFA — CURADORIA
# ============================================
Você está fazendo a CURADORIA editorial da edição diária do Recorte ✂ — escolhendo as matérias mais relevantes pra este leitor específico, escrevendo as manchetes, resumos e fatos-chave em PT-BR.
Tudo que você escrever vai direto pra caixa de entrada do leitor — siga o VOICE GUIDE acima rigorosamente.
{SAFETY_INSTRUCTIONS}
{POLITICAL_BIAS_INSTRUCTIONS}
🇧🇷 **REGRA CRÍTICA DE IDIOMA**: TODO o conteúdo gerado DEVE estar em **português brasileiro natural**, MESMO QUE a matéria original esteja em inglês, espanhol ou outro idioma. Traduza com fluência, mantendo nomes próprios e marcas no original.
⚠️ **REGRA CRÍTICA DE FORMATO**: Retorne APENAS JSON VÁLIDO, sem markdown, sem blocos de código, sem ```json```. Escape TODAS as aspas duplas dentro de strings com \\". Não use quebras de linha dentro de strings. Não inclua texto antes ou depois do JSON. A primeira character da resposta DEVE ser `{{` e a última `}}`.
📰 **OBJETIVO EDITORIAL**: O leitor deve conseguir entender cada notícia INTEIRA sem precisar abrir o link. Seja rico em fatos, números, datas e contexto. Mantenha o tom do VOICE GUIDE — direto, brasileiro, próximo.
🚫 **REGRA CRÍTICA DE DEDUPLICAÇÃO**: NUNCA inclua duas notícias sobre o MESMO evento, mesmo que venham de fontes diferentes ou com palavras ligeiramente diferentes. Se ver vários itens brutos sobre o mesmo acontecimento, escolha APENAS UM (preferindo: fonte mais respeitável > matéria mais completa > publicação mais recente). Em caso de dúvida sobre se 2 são o mesmo evento, considere que SÃO e una.
🎯 **REGRA CRÍTICA DE COERÊNCIA TEMA ↔ NOTÍCIA** (descarte agressivo se não casar):
Cada notícia que você incluir DEVE ser **ESPECIFICAMENTE sobre o tema declarado**, não sobre algo tangencialmente relacionado. As fontes podem trazer matérias contaminadas por keywords amplas — **filtre você como editor**.
**Regra geral:** se você precisa explicar pra alguém POR QUE essa notícia está nesse tema, ela não está nesse tema. Descarte.
**Exemplos do que NUNCA fazer:**
- Tema "Wellness (Bem-estar)" → NÃO incluir surtos de epidemia, mortalidade hospitalar, sistema de saúde pública, OMS.
- Tema "Trabalho & carreira" → NÃO incluir matéria sobre economia macro, PIB, inflação.
- Tema "Cultura & entretenimento" → NÃO incluir matéria de celebridade processada por crime/escândalo policial.
- Tema "Negócios & M&A" → NÃO incluir matéria de tech/IA empresarial.
- Tema "Tech & IA" → NÃO incluir economia geral ou política tech.
**EM DÚVIDA, DESCARTE.** É melhor o tema vir com 1 notícia perfeita do que com 3 incluindo 1 deslocada. Não force preenchimento.
🕐 **REGRA CRÍTICA DE FRESCOR TEMPORAL**:
- Para qualquer EVENTO com timeline definida: SE a data do evento JÁ PASSOU, NUNCA escolha uma matéria que cubra a PREVISÃO/EXPECTATIVA/ESCALAÇÃO/PRÉ-JOGO. Sempre prefira a matéria com o RESULTADO/desfecho.
- Quando 2 matérias falam do mesmo evento (uma "antes", outra "depois"): SEMPRE escolha a "depois".
- Se TODAS as matérias forem previsões de eventos já passados, MELHOR DESCARTAR a categoria.
📋 **ESTRUTURA DE RESPOSTA**: O JSON deve seguir o schema exato indicado no user message. Não invente campos. Não omita campos requeridos.
🎯 **CRITÉRIOS DE QUALIDADE**:
- Manchetes seguindo o VOICE GUIDE (máx 9 palavras quando possível, máx 90 chars sempre)
- Resumos com números, contexto, e implicação clara
- Fatos-chave concretos: datas, valores, players nomeados
- Sempre cite a fonte original (campo "fonte")
- Indique idioma original se NÃO for PT (campo "lang")
⚖️ **RIGOR EDITORIAL**:
- Não invente fatos, números, citações ou eventos
- Não extrapole além do que está na matéria original
- Política, religião, identidade: enquadramento factual sempre
Os dados específicos do dia + instruções pontuais virão no próximo turn do user."""
    from datetime import datetime, timezone, timedelta
    now_brt_str = datetime.now(timezone(timedelta(hours=-3))).strftime("%Y-%m-%d %H:%M BRT")
    user_message = f"""**USUÁRIO:** {user_name}
**DATA/HORA ATUAL (referência pro frescor temporal):** {now_brt_str}
{profile_section}
{filter_instruction}
{time_context}
{editorial_brief}
Para cada notícia selecionada, retorne:
- **manchete**: título em PT-BR direto, máx 90 caracteres, sem clickbait
- {resumo_instr}
- {fatos_instr}
- **link**: copie o link original
- **fonte**: nome do veículo
- **lang**: código do idioma original da matéria (ex: "en", "fr", "de"). Omita se for PT-BR.
- **pol_bias** (APENAS para temas marcados `tema_politico: true`): "factual", "centro", "esq" ou "dir".
Se um tema tiver pouca notícia relevante, retorne menos itens. Se nada for relevante, omita o tema.
**Para cada item bruto, o campo `publicado_em` indica quando a matéria foi publicada (use isso pra aplicar a REGRA DE FRESCOR TEMPORAL do system prompt — descartando previsões de eventos já passados).**
Dados:
{json.dumps(payload, ensure_ascii=False, indent=2)}
**RESPONDA APENAS JSON VÁLIDO**:
{{"secoes":[{{"tema":"<nome>","noticias":[{{"manchete":"...","resumo":"...","fatos_chave":["...","..."],"link":"...","fonte":"...","lang":"...","pol_bias":"..."}}]}}]}}"""
    parsed = _call_claude_json(user_message, max_tokens=12000, retries=2,
                                log_prefix=" (curate_news)",
                                model=selected_model,
                                system_prompt=system_prompt)
    secoes = parsed.get("secoes", []) if parsed else []
    for sec in secoes:
        sec["noticias"] = [n for n in sec.get("noticias", []) if is_safe_curated(n)]
    if filtered_items:
        for sec in secoes:
            sec["noticias"] = _apply_user_filters(sec.get("noticias", []), filtered_items)
    return secoes
def curate_news(user_name, topics_with_news, learned_profile="", filtered_items=None,
                weekly=False, news_per_topic=None, is_welcome=False):
    """Curadoria com batching."""
    if not topics_with_news:
        return []
    all_sections = []
    batches = [
        topics_with_news[i:i+MAX_TOPICS_PER_BATCH]
        for i in range(0, len(topics_with_news), MAX_TOPICS_PER_BATCH)
    ]
    log(f"  curando em {len(batches)} batch(es) de até {MAX_TOPICS_PER_BATCH} temas{' (modo weekly)' if weekly else ''}{' [WELCOME=Sonnet]' if is_welcome else ''}")
    for idx, batch in enumerate(batches, 1):
        log(f"  batch {idx}/{len(batches)}: {len(batch)} temas")
        sections = _curate_news_batch(
            user_name, batch, learned_profile, filtered_items,
            weekly=weekly, news_per_topic=news_per_topic,
            is_welcome=is_welcome,
        )
        all_sections.extend(sections)
    return all_sections
def _norm_for_dedup(text: str) -> str:
    """Normaliza string pra detectar duplicatas: minúsculo, sem pontuação/espaços."""
    if not text:
        return ""
    return re.sub(r'[^a-z0-9]', '', text.lower())
def _apply_user_filters(items, filtered_items):
    """Aplica filtros do usuário (lista de 'temas/veículos a NÃO receber')."""
    if not filtered_items or not items:
        return items
    norm_filters = [f.strip().lower() for f in filtered_items if isinstance(f, str) and f.strip()]
    if not norm_filters:
        return items
    out = []
    for it in items:
        fonte = (it.get("fonte") or "").lower()
        manchete = (it.get("manchete") or "").lower()
        resumo = (it.get("resumo") or "").lower()
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
        sig = _norm_for_dedup(it.get("manchete", ""))[:50]
        if sig and sig in seen_signatures:
            continue
        if link:
            seen_links.add(link)
        if sig:
            seen_signatures.add(sig)
        out.append(it)
    return out
# ============================================================================
# DEDUP CROSS-EDIÇÃO 5D — adicionado pra evitar repetição entre edições
# ============================================================================
def _load_recently_sent_signatures(user_id, days=5):
    """Pega URLs + assinaturas de manchetes enviadas pro user nos últimos N dias.
    Lê email_items.kind='news' onde payload tem campo 'link' e 'title' (manchete PT-BR).
    Retorna (set de links, set de title signatures).
    Custo: 1 query SQL (~50ms). Sem dependência do Claude.
    """
    from datetime import datetime, timezone, timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    try:
        res = supabase.table("email_items").select("payload") \
            .eq("user_id", user_id).eq("kind", "news") \
            .gte("created_at", cutoff).execute()
        links = set()
        title_sigs = set()
        for row in (res.data or []):
            payload = row.get("payload") or {}
            link = (payload.get("link") or "").strip().lower()
            title = payload.get("title", "")
            if link:
                links.add(link)
            sig = _norm_for_dedup(title)[:50]
            if sig:
                title_sigs.add(sig)
        return links, title_sigs
    except Exception as e:
        log(f"  ⚠ load_recently_sent_signatures falhou: {e}")
        return set(), set()
def _filter_already_sent(topics_with_news, sent_links, sent_title_sigs):
    """Remove notícias brutas cuja URL ou manchete já foi enviada pro user nos últimos N dias.
    Compara link (string match) E title signature (norm chars[:50]).
    Modifica in-place. Retorna total removido.
    """
    if not sent_links and not sent_title_sigs:
        return 0
    removed = 0
    for group in topics_with_news:
        kept = []
        for n in group.get("news", []):
            link = (n.get("link") or "").strip().lower()
            title = n.get("title", "")
            sig = _norm_for_dedup(title)[:50]
            if (link and link in sent_links) or (sig and sig in sent_title_sigs):
                removed += 1
                continue
            kept.append(n)
        group["news"] = kept
    return removed
# ============================================================================
def _dedupe_sections_against_trends(sections, trending):
    """Remove notícias de cada seção que já apareceram em trending (Em Alta)."""
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
    """Em Alta: até 5 trends (default daily) ou 10 (weekly)."""
    if not trends:
        return []
    MAX_TRENDS_INPUT = 30 if weekly else 18
    trends_clean = [t for t in trends if is_safe_news(t)]
    trends_truncated = trends_clean[:MAX_TRENDS_INPUT]
    profile_section = ""
    if learned_profile.strip():
        profile_section = f"\n**PERFIL DO USUÁRIO**: {learned_profile}\nUse pra priorizar trends que casem com interesses dele.\n"
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
    has_topics = bool(user_topics_labels)
    if has_topics:
        topics_str = ", ".join(user_topics_labels[:15])
        mix_instruction = f"""
🎯 **SELEÇÃO HÍBRIDA — total de {total_target} itens**:
- **{gen_count} eventos GERAIS**: top stories {'da semana' if weekly else 'do dia'}, alta circulação, qualquer assunto relevante (sem filtro por interesse)
- **{rel_count} eventos RELACIONADOS aos temas do usuário**: itens que se conectem aos temas dele, mesmo que não sejam os top virais gerais.
**Temas do usuário pra cruzar nos {rel_count} itens "RELACIONADOS"**: {topics_str}
⚠️ Esses {total_target} itens NÃO podem ser repetidos depois nas notícias por tema.
"""
        total_out = total_target
    else:
        mix_instruction = ""
        total_out = total_target if weekly else MAX_TRENDING_OUT
    if max_out is not None and max_out > 0:
        total_out = min(total_out, max_out)
    filter_instruction = ""
    if filtered_items:
        filter_list = ", ".join(f'"{f}"' for f in filtered_items[:20])
        filter_instruction = f"""
🚫 **FILTROS DURO DO USUÁRIO** — proibido incluir trends que envolvam estes temas/veículos:
{filter_list}
Se um trend é de um veículo dessa lista (campo "fonte"), DESCARTE-O integralmente.
"""
    system_prompt = f"""{VOICE_PROMPT}
# ============================================
# INSTRUÇÕES ESPECÍFICAS DESTA TAREFA — EM ALTA
# ============================================
Você está montando a seção "🔥 Em Alta" da edição diária do Recorte ✂.
{SAFETY_INSTRUCTIONS}
🚫 **REGRA CRÍTICA DE DEDUPLICAÇÃO**: NUNCA inclua duas manchetes sobre o MESMO evento.
🇧🇷 **REGRA CRÍTICA DE IDIOMA**: TODO conteúdo DEVE estar em **português brasileiro fluente**.
⚠️ **REGRA CRÍTICA DE FORMATO**: Retorne APENAS JSON VÁLIDO, sem markdown.
📰 **OBJETIVO EDITORIAL**: O leitor entende cada evento sem precisar abrir o link.
🎯 **CRITÉRIOS DE CURADORIA DE TRENDING**:
- Priorize: eventos significativos, lançamentos importantes, esporte/cultura de impacto
- Evite: fofoca rasa, conteúdo regional sem contexto, jargão obscuro
⚖️ **RIGOR**:
- Não invente fatos, números, citações
- Cite a fonte original do trending (campo "fonte")"""
    user_message = f"""**USUÁRIO:** {user_name}
**ESCOPO:** {scope_label}
{profile_section}
{filter_instruction}
{mix_instruction}
{context_intro}
{instruction_verb} ({total_out} itens): top stories + redes sociais + viralizações.
{'Como é semanal, dê contexto e mencione a evolução ao longo dos dias quando relevante.' if weekly else 'Seja rico em fatos e contexto.'}
Pra cada item:
- **manchete**: título PT-BR direto, máx 90 chars, sem clickbait
- **resumo**: {('4-5 frases (140-200 palavras)' if weekly else '3-4 frases (100-160 palavras)')}
- **fatos_chave**: array de {('4-6' if weekly else '3-5')} bullets curtos
- **buscas** (opcional): se vier do input, copie
- **link**: URL relacionada
- **fonte**: veículo
Trends brutos:
{json.dumps(trends_truncated, ensure_ascii=False, indent=2)}
**APENAS JSON VÁLIDO**:
{{"trending":[{{"manchete":"...","resumo":"...","fatos_chave":["..."],"link":"...","fonte":"..."}}]}}"""
    parsed = _call_claude_json(user_message, max_tokens=6000, retries=2,
                                log_prefix=" (curate_trends)",
                                model=MODEL_PREMIUM,
                                system_prompt=system_prompt)
    items = parsed.get("trending", []) if parsed else []
    items = [it for it in items if is_safe_curated(it)]
    if filtered_items:
        items = _apply_user_filters(items, filtered_items)
    before = len(items)
    items = _dedupe_trends(items)
    if len(items) < before:
        log(f"  ⚠ dedup removeu {before - len(items)} duplicatas do Em Alta")
    return items
def generate_daily_recap(user_name, sections, trending, learned_profile=""):
    """Gera o 'Seu dia em 60 segundos' + uma quote de destaque pro topo."""
    if not sections and not trending:
        return {"recap": "", "quote": "", "quote_author": ""}
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
    prompt = f"""Você escreve "Seu dia em 60 segundos" — o briefing no topo do email do {user_name}.
Siga o VOICE GUIDE do Recorte ✂ (tom brasileiro direto, próximo, "a gente lê o mundo pra você").
{profile_section}
**TAREFA 1 — RECAP**: Cria UM PARÁGRAFO único de **140-180 palavras** em PT-BR que faça o leitor entender, em 1 minuto, o que rolou hoje no mundo dele.
Regras do recap:
- Tom direto, brasileiro, próximo. Lê como amigo bem informado contando ao pé do café.
- Cobre 4-6 fatos do dia, escolhendo os de maior impacto/novidade.
- Conecta áreas quando faz sentido ("...na mesma semana em que..." / "...enquanto isso...").
- Termina com 1 frase de fechamento natural.
- NÃO use bullets. NÃO use markdown. NÃO repita "hoje". UM parágrafo corrido.
- Use segunda pessoa quando for natural ("você reparou que...", "vale prestar atenção em...").
**TAREFA 2 — QUOTE**: Pinça UMA frase forte ou citação marcante de alguma das manchetes/notícias do dia. **Máximo 18 palavras**. Use aspas curvas “”.
**REGRAS DA QUOTE (importante seguir TODAS):**
- VARIE A ÁREA: NÃO escolha sempre tech/IA. Considere TODAS as áreas — cultura, esporte, ciência, política, economia, saúde, comportamento. Se você está em dúvida entre uma tech e uma não-tech, escolha a NÃO-tech.
- PREFIRA citação de pessoa (declaração, posicionamento, frase emblemática) sobre dado numérico. Quote humana > quote de número.
- EVITE quotes "óbvias" tipo "X cresceu Y%" — busque frase com OPINIÃO, ÂNGULO ou IRONIA.
- Deve ter CARÁTER, personalidade. Não pode ser dado seco.
- Se a melhor frase do dia for de tech/IA, tudo bem usar — mas não DEFAULTE pra ela.
Manchetes de hoje:
{json.dumps(summary_input, ensure_ascii=False, indent=2)}
Responda APENAS JSON VÁLIDO neste formato exato:
{{"recap": "<parágrafo>", "quote": "<frase com aspas curvas>", "quote_author": "<autor ou contexto curto>"}}"""
    parsed = _call_claude_json(prompt, max_tokens=900, retries=2, log_prefix=" (recap)",
                                system_prompt=VOICE_PROMPT)
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
    """Tenta decodificar uma URL wrapper do Google News pra URL real da matéria."""
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
    """Resolve URLs de Google News pra URLs reais."""
    from concurrent.futures import ThreadPoolExecutor
    targets = []
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
    log(f"  ✓ {resolved_count}/{len(targets)} URLs resolvidas (resto será descartado)")
    dropped_sections = 0
    for sec in sections:
        before = len(sec.get("noticias", []))
        sec["noticias"] = [n for n in sec.get("noticias", [])
                           if "news.google.com" not in (n.get("link", "") or "")]
        dropped_sections += before - len(sec["noticias"])
    dropped_trending = 0
    if trending:
        before = len(trending)
        trending[:] = [item for item in trending
                       if "news.google.com" not in (item.get("link", "") or "")]
        dropped_trending = before - len(trending)
    total_dropped = dropped_sections + dropped_trending
    if total_dropped > 0:
        log(f"  ⚠ descartadas {total_dropped} notícia(s) com URL Google News não resolvida (link quebraria)")
def add_feedback_links(user_id, sections):
    """Pra cada notícia e cada seção, gera email_items + URLs assinadas."""
    for sec in sections:
        if sec.get("topic_id") or sec.get("topic"):
            tid = create_email_item(user_id, "topic", {
                "topic_id": sec.get("topic_id"),
                "topic_label": sec.get("topic",""),
            })
            sec["fb_pause_url"] = feedback_url(FEEDBACK_BASE_URL, tid, -1)
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
def send_email(to_email, to_name, html, date_obj, weekly=False, user_id=None,
               edition_id=None, kind=None, user_tz="America/Sao_Paulo"):
    first = to_name.split()[0]
    if weekly:
        subject = f"Bom domingo, {first}. Sua semana, recortada ✂"
    else:
        try:
            from zoneinfo import ZoneInfo
            local_dt = date_obj.astimezone(ZoneInfo(user_tz)) if date_obj.tzinfo else date_obj
            hour = local_dt.hour
        except Exception:
            hour = date_obj.hour
        if 5 <= hour < 12:
            saudacao = "Bom dia"
        elif 12 <= hour < 18:
            saudacao = "Boa tarde"
        elif 18 <= hour < 24:
            saudacao = "Boa noite"
        else:
            saudacao = "Olá"
        subject = f"{saudacao}, {first}. Hoje tem ✂ ({date_obj.strftime('%d/%m')})"
    if DRY_RUN:
        log("DRY_RUN", to=to_email, subject=subject)
        fname = f"preview_{to_email.replace('@','_at_')}.html"
        with open(fname, "w", encoding="utf-8") as f:
            f.write(html)
        return {"id": "dry-run"}
    headers = {}
    if user_id:
        unsub = gen_unsub_url(SUPABASE_URL, user_id)
        headers = {
            "List-Unsubscribe": f"<{unsub}>, <mailto:unsubscribe@recorte.news?subject=Unsubscribe>",
            "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
        }
    tags = []
    if kind:
        tags.append({"name": "kind", "value": str(kind)})
    if user_id:
        tags.append({"name": "user_id", "value": str(user_id)})
    if edition_id:
        tags.append({"name": "edition_id", "value": str(edition_id)})
    payload = {
        "from": FROM_EMAIL, "to": to_email,
        "subject": subject, "html": html,
        "headers": headers,
    }
    if tags:
        payload["tags"] = tags
    return resend.Emails.send(payload)
# ============ MAIN ============
def process_user(user, now_brt, weekly=False):
    uid = user["id"]
    default_country = user.get("default_country") or "BR"
    log(f"processando", email=user["email"], pais=default_country)
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
    _topics_pre = supabase.table("topics").select("label").eq("user_id", uid).execute()
    user_topic_labels = [t["label"] for t in (_topics_pre.data or [])]
    topic_count_for_scaling = len({lbl for lbl in user_topic_labels}) if user_topic_labels else 0
    # 1) TRENDING
    trending = []
    trending_label = ""
    if user.get("trending_enabled", True):
        raw_scope = user.get("trending_scope") or "br"
        scopes = [s.strip() for s in raw_scope.split(",") if s.strip()]
        if not scopes:
            scopes = ["br"]
        labels = []
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
    # 2) NOTÍCIAS POR TEMA
    topics_res = supabase.table("topics").select("*").eq("user_id", uid).execute()
    topics = topics_res.data or []
    fallback_country = "GLOBAL" if default_country == "INTL" else default_country
    if weekly:
        news_per_topic = weekly_news_per_topic(topic_count_for_scaling)
    else:
        news_per_topic = daily_news_per_topic(topic_count_for_scaling)
    is_welcome = not user.get("welcome_sent")
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
        for n in news:
            n.setdefault("scope_origin", country)
        if t["label"] not in by_label:
            by_label[t["label"]] = {
                "label": t["label"],
                "country": country,
                "scopes": [],
                "topic_id": t["id"],
                "source": t.get("source", "curated"),
                "news": []
            }
        by_label[t["label"]]["scopes"].append(country)
        by_label[t["label"]]["news"].extend(news)
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
    topics_with_news.sort(key=lambda g: 0 if g.get("source") == "custom" else 1)
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
                new_url = _try_decode_gnews_url(url)
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
    # DEDUP 5D CROSS-EDIÇÃO: remove notícias cujas URLs/manchetes já foram enviadas pro user nos últimos 5 dias.
    # Evita repetição em desdobramentos lentos (política, M&A, saúde pública).
    # Custo: 1 query SQL. Roda DEPOIS do resolve gnews (URLs finais) e ANTES do Claude curar.
    try:
        sent_links, sent_title_sigs = _load_recently_sent_signatures(uid, days=5)
        if sent_links or sent_title_sigs:
            removed_5d = _filter_already_sent(topics_with_news, sent_links, sent_title_sigs)
            if removed_5d > 0:
                log(f"  🔁 dedup 5d: removidas {removed_5d} notícia(s) já enviada(s) recentemente "
                    f"(banco: {len(sent_links)} URLs + {len(sent_title_sigs)} signatures)")
            # Remove grupos vazios após o dedup
            topics_with_news = [g for g in topics_with_news if g.get("news")]
    except Exception as e:
        log(f"  ⚠ dedup 5d cross-edição falhou (não bloqueia): {e}")
    sections = []
    if topics_with_news:
        raw_sections = curate_news(
            user["name"], topics_with_news, learned,
            filtered_items=filtered_items,
            weekly=weekly,
            news_per_topic=news_per_topic,
            is_welcome=is_welcome,
        )
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
        log("  nada pra mandar, pulando")
        return False
    if trending and sections:
        _dedupe_sections_against_trends(sections, trending)
        sections = [s for s in sections if s.get("noticias")]
    label_to_source = {t["label"]: t.get("source", "curated") for t in topics_with_news}
    sections.sort(key=lambda s: 0 if label_to_source.get(s.get("topic"), "curated") == "custom" else 1)
    resolve_gnews_urls(sections, trending)
    before_count = len(sections)
    sections = [s for s in sections if s.get("noticias")]
    dropped_empty = before_count - len(sections)
    if dropped_empty > 0:
        log(f"  🧹 removidos {dropped_empty} tema(s) sem notícias")
    sections = add_feedback_links(uid, sections)
    log(f"  gerando recap executivo...")
    recap_data = generate_daily_recap(user["name"], sections, trending, learned)
    daily_recap = recap_data.get("recap", "") if isinstance(recap_data, dict) else (recap_data or "")
    daily_quote = recap_data.get("quote", "") if isinstance(recap_data, dict) else ""
    daily_quote_author = recap_data.get("quote_author", "") if isinstance(recap_data, dict) else ""
    if daily_recap:
        log(f"  ✓ recap gerado ({len(daily_recap)} chars)")
    if daily_quote:
        log(f"  ✓ quote do dia: {daily_quote[:60]}")
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
        if trending:
            n_with_img = sum(1 for t in trending if t.get("img_url"))
            log(f"  📷 trending: {n_with_img}/{len(trending)} com imagem")
        if sections:
            total_n = sum(len(s.get("noticias", [])) for s in sections)
            total_img = sum(1 for s in sections for n in s.get("noticias", []) if n.get("img_url"))
            log(f"  📷 notícias: {total_img}/{total_n} com imagem (híbrido A+B)")
    except Exception as e:
        log(f"  ⚠ Image hybrid extraction falhou (não bloqueia): {e}")
    from tracking import gen_edition_id, save_edition, wrap_links_in_html, finalize_edition
    edition_id = gen_edition_id()
    click_base = os.environ.get("CLICK_BASE_URL", "https://recorte.news/c")
    # PATCH 2: share_base default agora aponta pra edge function /functions/v1/edition
    # (resolve "Abrir online" que redirecionava pra home, já que recorte.news/r/* não tem rota servindo o HTML).
    # Quando criar Worker /r/* ou migrar DNS pra Cloudflare, basta atualizar SHARE_BASE_URL no GitHub Secrets.
    share_base = os.environ.get("SHARE_BASE_URL", f"{SUPABASE_URL}/functions/v1/edition")
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
        edition_id=edition_id,
        share_base_url=share_base,
    )
    edition_kind = "welcome" if is_welcome else ("weekly" if weekly else "daily")
    scheduled_for_date = now_brt.date().isoformat()
    try:
        save_edition(
            supabase, user_id=uid, kind=edition_kind,
            subject=f"Recorte {edition_kind} {scheduled_for_date}",
            html=html, scheduled_for=scheduled_for_date,
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
            supabase.table("editions").update({"html": html}).eq("id", edition_id).execute()
        except Exception as e:
            log(f"  ⚠ Update edition html falhou (não bloqueia): {e}")
    except Exception as e:
        log(f"  ⚠ Click wrap falhou (não bloqueia): {e}")
    result = send_email(user["email"], user["name"], html, now_brt,
                        weekly=weekly, user_id=uid,
                        edition_id=edition_id, kind=edition_kind,
                        user_tz=user_tz)
    log(f"  ✓ enviado", id=result.get("id"))
    try:
        finalize_edition(supabase, edition_id, result.get("id"))
    except Exception as e:
        log(f"  ⚠ finalize_edition falhou: {e}")
    user_updates = {
        "last_sent_at": now_brt.isoformat(),
        "welcome_sent": True,
    }
    if is_welcome:
        user_updates["welcome_sent_at"] = now_brt.isoformat()
    supabase.table("users").update(user_updates).eq("id", uid).execute()
    return True
def main():
    now_brt = datetime.now(BRT)
    target_hour = TARGET_HOUR_BRT if TARGET_HOUR_BRT >= 0 else now_brt.hour
    log(f"=== Manhã ☕ V3 run ===", hora=target_hour, dry=DRY_RUN)
    today_start_brt = now_brt.replace(hour=0, minute=0, second=0, microsecond=0)
    res = supabase.table("users").select("*").eq("active", True).execute()
    all_users = res.data or []
    users = []
    for u in all_users:
        last = u.get("last_sent_at")
        if not last:
            users.append(u)
            continue
        try:
            last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=BRT)
            last_brt = last_dt.astimezone(BRT)
            if last_brt < today_start_brt:
                users.append(u)
        except Exception as e:
            log(f"  ⚠ não consegui parsear last_sent_at de {u.get('email')}: {e}")
            users.append(u)
    log(f"usuários elegíveis (catch-up)", count=len(users), total_ativos=len(all_users))
    if not users:
        log("=== fim (nada pra processar) ===")
        return
    workers = int(os.environ.get("PARALLEL_WORKERS", "5"))
    workers = max(1, min(workers, len(users)))
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
