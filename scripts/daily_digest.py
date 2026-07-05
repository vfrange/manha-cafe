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
from email.utils import parsedate_to_datetime  # parseia data RFC 822 do RSS (Google News)
from concurrent.futures import ThreadPoolExecutor, as_completed
import resend
from supabase import create_client
from anthropic import Anthropic
from voice_prompt import VOICE_PROMPT
from email_template import render_email
from feedback_token import short_id, feedback_url, manage_url as gen_manage_url, unsub_url as gen_unsub_url
from sources import google_news, hacker_news, reddit, br_rss, bluesky, youtube_trending, intl_rss
# Sources novos pra seção "Antes de todos" (sinal fraco / fora do radar BR)
from sources import substack, cvm, google_trends
from safety import (
    is_safe_news, is_safe_curated, SAFETY_INSTRUCTIONS,
    POLITICAL_BIAS_INSTRUCTIONS, is_political_topic,
)
from hallucination_guard import (
    validate_and_clean_sections,
    validate_and_clean_trending,
    get_current_date_context,
    ANTI_HALLUCINATION_RULE,
)
# ============ CONFIG ============
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
RESEND_API_KEY = os.environ["RESEND_API_KEY"]
FEEDBACK_BASE_URL = os.environ["FEEDBACK_BASE_URL"]
FROM_EMAIL = os.environ.get("FROM_EMAIL", "Manhã <digest@onresend.dev>")
MANAGE_URL = os.environ.get("MANAGE_URL", "https://seudominio.netlify.app/cadastro.html")
TARGET_HOUR_BRT = int(os.environ.get("TARGET_HOUR_BRT", "-1"))
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"
MODEL = "claude-haiku-4-5-20251001"
MODEL_PREMIUM = "claude-sonnet-4-6"

# ============================================================================
# FEATURE FLAG — "Saiba antes de todos" (undercovered / sinal fraco)
# Desligado a pedido: some do email (email_template já pula seção vazia),
# do cadastro e das preferências (frontend removeu o toggle).
# Com False, fetch_undercovered retorna [] imediatamente — ZERO tokens,
# zero latência, sem curadoria. Pra REATIVAR: trocar pra True e restaurar
# as etapas nos HTMLs (versões _com_toggle guardadas no zip do deploy).
# A coluna undercovered_enabled no Supabase fica quieta (sem efeito).
# ============================================================================
UNDERCOVERED_ENABLED = False

MAX_NEWS_OUT_PER_TOPIC = 2
MAX_TRENDING_OUT = 10
# ============================================================================
# CONFIG DE VOLUME — ESCADINHAS POR NÚMERO DE TEMAS
# ============================================================================
DAILY_TOTAL_CAP = 28
DAILY_TRENDING_MIN = 5
DAILY_TRENDING_MAX = 6
def daily_news_per_topic(topic_count: int) -> int:
    if topic_count <= 3:
        return 5
    elif topic_count <= 6:
        return 4
    elif topic_count <= 10:
        return 3
    else:
        return 2
def daily_trending_budget(topic_count: int) -> int:
    per_topic = daily_news_per_topic(topic_count)
    used = per_topic * topic_count
    remaining = DAILY_TOTAL_CAP - used
    return max(DAILY_TRENDING_MIN, min(DAILY_TRENDING_MAX, remaining))
WEEKLY_TOTAL_CAP = 35
WEEKLY_TRENDING_MIN = 5
WEEKLY_TRENDING_MAX = 10
def weekly_news_per_topic(topic_count: int) -> int:
    if topic_count <= 4:
        return 5
    elif topic_count <= 7:
        return 4
    elif topic_count <= 11:
        return 3
    else:
        return 2
def weekly_trending_budget(topic_count: int) -> int:
    per_topic = weekly_news_per_topic(topic_count)
    used = per_topic * topic_count
    remaining = WEEKLY_TOTAL_CAP - used
    return max(WEEKLY_TRENDING_MIN, min(WEEKLY_TRENDING_MAX, remaining))
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


# ============================================================================
# STRIP HTML TAGS — segurança defense-in-depth
# Garante que tags HTML (<strong>, <em>, <b>, <i>, etc.) vindas do Claude
# curador NÃO apareçam como texto literal escapado no email (`&lt;strong&gt;`).
# Aplicado nas SAÍDAS dos 3 curadores (sections/trending/undercovered) antes
# do escape no template. Camada 1 = prompt instrui Claude a não usar HTML.
# Camada 2 = essa função remove caso Claude desobedeça.
# ============================================================================
import re as _html_strip_re
_HTML_TAG_RE = _html_strip_re.compile(r"<[^>]+>")
_HTML_ENTITY_RE = _html_strip_re.compile(r"&[a-zA-Z]+;|&#\d+;")


def _strip_html_tags(text):
    """Remove tags HTML e entidades de uma string. Idempotente, tolera None.
    Ex: 'Stanford ensina <strong>agentes</strong>' → 'Stanford ensina agentes'
    """
    if not text or not isinstance(text, str):
        return text
    cleaned = _HTML_TAG_RE.sub("", text)
    # Decodifica entidades comuns que podem ter vindo do Claude
    cleaned = (cleaned
        .replace("&nbsp;", " ").replace("&amp;", "&")
        .replace("&lt;", "<").replace("&gt;", ">")
        .replace("&quot;", '"').replace("&#39;", "'").replace("&apos;", "'"))
    # Remove qualquer outra entidade residual
    cleaned = _HTML_ENTITY_RE.sub("", cleaned)
    # Normaliza espaços
    cleaned = _html_strip_re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _strip_html_from_items(items, fields=("manchete", "resumo", "termo")):
    """Aplica _strip_html_tags nos campos textuais de uma lista de items.
    Também limpa fatos_chave (lista de strings)."""
    if not items:
        return items
    for item in items:
        if not isinstance(item, dict):
            continue
        for f in fields:
            if f in item and item[f]:
                item[f] = _strip_html_tags(item[f])
        # fatos_chave é lista de strings
        if "fatos_chave" in item and isinstance(item["fatos_chave"], list):
            item["fatos_chave"] = [_strip_html_tags(x) if isinstance(x, str) else x
                                    for x in item["fatos_chave"]]
    return items


# ============================================================================
# RETRY com backoff exponencial pra escritas no Supabase
# ============================================================================
# Motivação: Supabase HTTP/2 pool às vezes desconecta abruptamente
# (`httpcore.RemoteProtocolError: Server disconnected`), especialmente
# quando vários workers paralelos compartilham conexões. O cliente postgrest-py
# tem retry built-in, mas só pra erros HTTP 5xx — não pra disconnect do socket.
# Esta função pega QUALQUER operação Supabase e tenta de novo com backoff em
# erros transitórios de rede. Não retenta erros lógicos (4xx, validação).
import httpx
try:
    import httpcore
    _HTTPCORE_TRANSIENT = (httpcore.RemoteProtocolError, httpcore.ConnectError,
                           httpcore.ReadError, httpcore.WriteError, httpcore.PoolTimeout)
except ImportError:
    _HTTPCORE_TRANSIENT = ()

_TRANSIENT_ERRORS = (
    httpx.RemoteProtocolError,
    httpx.ConnectError, httpx.ConnectTimeout,
    httpx.ReadTimeout, httpx.ReadError,
    httpx.WriteError, httpx.WriteTimeout,
    httpx.PoolTimeout,
) + _HTTPCORE_TRANSIENT

def _supabase_retry(fn, label="supabase", attempts=4, base_delay=0.5):
    """Envolve uma operação Supabase com retry exponencial em erros transitórios.

    Retenta com backoff: 0.5s, 1s, 2s, 4s (total ~7.5s no pior caso).
    Erros NÃO-transitórios (validação, 4xx) sobem direto sem retry.

    Args:
        fn: callable que executa a operação (ex: lambda: supabase.table(...).insert(...).execute())
        label: nome curto pro log
        attempts: número total de tentativas (incluindo a primeira)
        base_delay: delay inicial em segundos (dobra a cada tentativa)
    """
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except _TRANSIENT_ERRORS as e:
            last_exc = e
            if attempt < attempts:
                delay = base_delay * (2 ** (attempt - 1))
                log(f"  ⚠ {label} transient erro tentativa {attempt}/{attempts}: {type(e).__name__} — aguardando {delay:.1f}s")
                time.sleep(delay)
            else:
                log(f"  ✗ {label} falhou após {attempts} tentativas: {type(e).__name__}: {e}")
                raise
    if last_exc:
        raise last_exc
# ============================================================================
# JANELA DE FRESCOR TEMPORAL — descarte HARD de notícias velhas
# ============================================================================
def get_stale_window_hours(weekly: bool, now_brt) -> int:
    if weekly:
        return 168
    if now_brt.weekday() == 0:
        return 48
    return 30
def _parse_pub_date(pub_str):
    """Parseia data de publicação em múltiplos formatos.
    Retorna datetime tz-aware (UTC) ou None se não conseguir.

    Formatos suportados:
    - ISO 8601: "2026-04-06T15:22:04-03:00" / "2026-04-06T15:22:04Z"
    - RFC 822 (RSS/Google News): "Sun, 06 Apr 2026 15:22:04 GMT"

    BUG que isso corrige: Google News RSS entrega RFC 822, mas o filtro
    antigo só tentava datetime.fromisoformat() → exceção → notícia passava.
    Resultado: TODAS as notícias do Google News furavam o filtro de frescor.
    """
    if not pub_str:
        return None
    s = pub_str.strip()
    # 1) Tenta ISO 8601
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError):
        pass
    # 2) Tenta RFC 822 (formato RSS: "Sun, 06 Apr 2026 15:22:04 GMT")
    try:
        dt = parsedate_to_datetime(s)
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError, IndexError):
        pass
    return None


def _filter_stale_news(news_list, max_age_hours, strict=True):
    """Descarta notícias mais velhas que max_age_hours.

    Parâmetro `strict` controla o que fazer com item SEM data parseável:
    - strict=True (FAIL-CLOSED): sem data = DESCARTA.
      Usado pra notícias por tema (Google News RSS traz data confiável;
      se não tem, é suspeito → descarta pra não vazar matéria velha).
    - strict=False (FAIL-OPEN): sem data = MANTÉM.
      Usado pro TRENDING/top stories, que por design NÃO carregam data
      (fetch_trends monta item sem published_at). Trending é "o que está
      bombando agora" — inerentemente recente. Mas se TIVER data e for
      velha, descarta mesmo assim (preserva o fix do caso SpaceX 22/05).

    Parse suporta ISO 8601 E RFC 822 (Google News RSS manda RFC 822).
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    kept = []
    dropped_stale = 0
    dropped_no_date = 0
    for n in news_list:
        pub_str = n.get("published_at") or n.get("published") or ""
        pub_dt = _parse_pub_date(pub_str)
        if pub_dt is None:
            # Sem data parseável
            if strict:
                dropped_no_date += 1
                continue  # FAIL-CLOSED
            else:
                kept.append(n)  # FAIL-OPEN (trending: sem data = recente por natureza)
                continue
        if pub_dt < cutoff:
            dropped_stale += 1
            continue
        kept.append(n)
    if dropped_no_date:
        log(f"  📅 fail-closed frescor: descartadas {dropped_no_date} notícia(s) sem data parseável")
    return kept, dropped_stale
# ============ MULTI-SOURCE FETCH ============
def fetch_all_sources(query, country, category=None, label=None, source_type="curated",
                      weekly=False, max_age_hours=None):
    is_br = country == "BR"
    is_global = country == "GLOBAL"
    is_tech = category == "tecnologia" or any(
        kw in query.lower() for kw in ["tech","ia ","ai","intelig","gpt","openai","software"]
    )
    gnews_when = "7d" if weekly else "2d"
    reddit_time = "week" if weekly else "day"
    gnews_max = 15 if weekly else 8
    reddit_max = 8 if weekly else 4
    # HN: volume maior pra tech (sinal forte), menor pra outros temas (cobertura editorial geral)
    hn_max = (10 if weekly else 5) if is_tech else (4 if weekly else 2)
    br_max = 15 if weekly else 8
    fetchers = []
    fetchers.append(("google_news", lambda: google_news.fetch(query, country, max_items=gnews_max, when=gnews_when)))
    # HN agora roda em TODOS os temas (não só tech) — editorial signal forte
    fetchers.append(("hacker_news", lambda: hacker_news.fetch(query, max_items=hn_max)))
    fetchers.append(("reddit", lambda: reddit.fetch(query, category=category, max_items=reddit_max, time_filter=reddit_time)))
    if is_br:
        fetchers.append(("br_rss", lambda: br_rss.fetch(query, category=category, max_items=br_max)))
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
            pendentes = [futures[f] for f in futures if not f.done()]
            log(f"  ⚠ timeout 25s — {len(pendentes)}/{len(futures)} fonte(s) não responderam: {', '.join(pendentes)} — seguindo com {len(results)} itens das fontes que responderam")
            for f in futures:
                if not f.done():
                    f.cancel()
    seen = set()
    deduped = []
    for r in results:
        key = re.sub(r"\W+", "", r.get("title","")).lower()[:80]
        if key and key not in seen:
            seen.add(key)
            deduped.append(r)
    if max_age_hours is not None:
        before = len(deduped)
        deduped, stale_count = _filter_stale_news(deduped, max_age_hours=max_age_hours)
        if stale_count > 0:
            log(f"  🕐 frescor ({max_age_hours}h): descartadas {stale_count}/{before} notícia(s) velha(s)")
    return deduped
def fetch_trending(country, weekly=False, max_age_hours=None):
    """Combina trends: Google News Top Stories + Reddit + Bluesky + YouTube Trending.

    PATCH 22/05: aplica filtro de frescor (max_age_hours) também no trending.
    Antes, trending pulava o filtro temporal e podia trazer notícias velhas
    (caso SpaceX/Anthropic 21/05/2026 — notícia de 15 dias atrás).
    """
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
    # Aplica filtro de frescor com strict=False (FAIL-OPEN).
    # Trending/top stories não carregam published_at por design → sem data
    # = mantém (é recente por natureza). Mas item COM data velha ainda é
    # descartado (preserva fix SpaceX 22/05: agregador re-indexando manchete antiga).
    if max_age_hours is not None and trends:
        before = len(trends)
        trends, stale_count = _filter_stale_news(trends, max_age_hours=max_age_hours, strict=False)
        if stale_count > 0:
            log(f"  🕐 trending frescor ({max_age_hours}h): descartadas {stale_count}/{before} velha(s)")
    return trends
# ============ CLAUDE CURATION ============
MAX_NEWS_INPUT_PER_TOPIC = 6
MAX_TOPICS_PER_BATCH = 4

# ============ UNDERCOVERED SOURCES ============
# Combina fontes que captam sinal ANTES da imprensa BR cobrir:
#   - Substack (newsletters anglo high-signal)
#   - CVM/B3 fatos relevantes (movimentos corporativos BR antes da editorialização)
#   - Google Trends RSS (termos com spike de busca, real-time)
#   - HN top stories (não só tech — editorial de "o que importa")

# Domínios BR mainstream — se um candidato JÁ está nesses domínios, descartamos
# (não é mais "undercovered" se Folha/G1/Valor/Exame já cobriram).
BR_MAINSTREAM_DOMAINS = {
    # Grandes diários e portais
    "folha.uol.com.br", "folha.com.br", "g1.globo.com", "globo.com",
    "estadao.com.br", "oglobo.globo.com", "uol.com.br", "noticias.uol.com.br",
    "r7.com", "noticias.r7.com", "terra.com.br", "ig.com.br",
    "metropoles.com", "correiobraziliense.com.br", "gazetadopovo.com.br",
    "gauchazh.clicrbs.com.br", "zh.clicrbs.com.br",  # Zero Hora
    "em.com.br",  # Estado de Minas
    "correio24horas.com.br",
    # Negócios / mercado financeiro
    "valor.globo.com", "valoreconomico.com.br", "exame.com",
    "infomoney.com.br", "neofeed.com.br", "braziljournal.com",
    "investnews.com.br", "money-times.com.br", "istoedinheiro.com.br",
    "forbes.com.br",
    # Revistas semanais
    "veja.abril.com.br", "veja.com", "istoe.com.br", "cartacapital.com.br",
    "epoca.globo.com",
    # Broadcasts
    "cnnbrasil.com.br", "band.uol.com.br", "band.com.br",
    "jovempan.com.br", "noticias.band.uol.com.br",
    "recordtv.r7.com", "sbt.com.br",
    # BBC/Reuters/AP em PT
    "bbc.com/portuguese", "bbc.com",
    "reuters.com", "br.reuters.com",
    # Tech BR mainstream
    "tecmundo.com.br", "canaltech.com.br", "olhardigital.com.br",
    "tilt.uol.com.br", "tecnoblog.net",
    # Independente mas com grande alcance
    "nexojornal.com.br", "poder360.com.br", "piaui.folha.uol.com.br",
    "revistapiaui.estadao.com.br", "brasil247.com",
    "redebrasilatual.com.br", "icl.com.br",
}

def _is_br_mainstream(url):
    """True se a URL pertence a um veículo mainstream BR (já coberto, não é undercovered)."""
    if not url:
        return False
    try:
        from urllib.parse import urlparse
        host = urlparse(url).hostname or ""
        host = host.lower().lstrip("www.")
        for d in BR_MAINSTREAM_DOMAINS:
            if host == d or host.endswith("." + d):
                return True
    except Exception:
        pass
    return False


def fetch_undercovered(country="BR", weekly=False, max_age_hours=None):
    """
    Busca candidatos pra seção "saiba antes de todos" — histórias que ainda
    não foram cobertas pela imprensa brasileira mainstream.

    Combina substack + cvm + google_trends + HN top, depois filtra:
    1. URLs que JÁ estão em domínios BR mainstream (não é mais "antes")
    2. Filtro de frescor (idade máxima)
    3. Dedup por título
    """
    # FEATURE FLAG: desligado → não busca nada (zero tokens/latência).
    if not UNDERCOVERED_ENABLED:
        return []

    items = []

    # 1. Substack — newsletters anglo (2 posts por feed, 72h)
    try:
        sub_items = substack.fetch(
            max_items_per_feed=2,
            max_age_hours=72 if not weekly else 168,
        )
        for it in sub_items:
            it["_origin"] = "substack"
        items.extend(sub_items)
    except Exception as e:
        log(f"  ⚠ substack: {e}")

    # 2. CVM / B3 fatos relevantes (só BR ou GLOBAL com flavor BR)
    if country in ("BR", "GLOBAL"):
        try:
            cvm_items = cvm.fetch(
                max_items_per_query=3,
                max_age_hours=36 if not weekly else 168,
                weekly=weekly,
            )
            for it in cvm_items:
                it["_origin"] = "cvm"
            items.extend(cvm_items)
        except Exception as e:
            log(f"  ⚠ cvm: {e}")

    # 3. Google Trends RSS (termos com spike de busca)
    try:
        gt_items = google_trends.fetch(
            country=country,
            max_items=12 if not weekly else 20,
            max_age_hours=48 if not weekly else 168,
        )
        for it in gt_items:
            it["_origin"] = "google_trends"
        items.extend(gt_items)
    except Exception as e:
        log(f"  ⚠ google_trends: {e}")

    # 4. HN top stories (sem query — pega o que tá bombando geral)
    try:
        hn_items = hacker_news.fetch(query=None, max_items=15 if not weekly else 25)
        for it in hn_items:
            it["_origin"] = "hacker_news_top"
        items.extend(hn_items)
    except Exception as e:
        log(f"  ⚠ hn top: {e}")

    # FILTRO 1: remove URLs que JÁ estão em mídia BR mainstream
    before_mainstream = len(items)
    items = [it for it in items if not _is_br_mainstream(it.get("link", ""))]
    removed_mainstream = before_mainstream - len(items)
    if removed_mainstream > 0:
        log(f"  🧹 undercovered: removidos {removed_mainstream} já em mídia BR mainstream")

    # FILTRO 2: dedup por título normalizado
    seen = set()
    deduped = []
    for it in items:
        title = it.get("title", "")
        key = re.sub(r"\W+", "", title).lower()[:80]
        if key and key not in seen:
            seen.add(key)
            deduped.append(it)

    # FILTRO 3: frescor
    if max_age_hours is not None and deduped:
        before = len(deduped)
        deduped, stale_count = _filter_stale_news(deduped, max_age_hours=max_age_hours)
        if stale_count > 0:
            log(f"  🕐 undercovered frescor ({max_age_hours}h): descartadas {stale_count}/{before}")

    return deduped


def curate_undercovered(user_name, raw_items, learned_text,
                        user_topic_labels=None, filtered_items=None,
                        exclude_links=None, exclude_titles=None,
                        max_out=10, weekly=False):
    """
    Filtra/cura candidatos da seção "saiba antes de todos" via Claude.

    Aplica TODAS as regras dos outros capítulos:
    - Filtra por temas do user (relevância aos interesses)
    - Aplica filtered_items (remove explicitamente filtrados)
    - Aplica learned_text (preferências aprendidas)
    - Dedup vs sections/trending (exclude_links/titles)
    - Prioriza "fato UAU" verdadeiramente undercovered
    """
    exclude_links = exclude_links or set()
    exclude_titles = exclude_titles or set()
    user_topic_labels = user_topic_labels or []
    filtered_items = filtered_items or []

    # 1. Pré-filtro: remove duplicatas com outras seções
    pre_filtered = []
    for it in raw_items:
        link = it.get("link", "")
        title = it.get("title", "")
        title_key = re.sub(r"\W+", "", title).lower()[:80]
        if link and link in exclude_links:
            continue
        if title_key and title_key in exclude_titles:
            continue
        pre_filtered.append(it)

    if not pre_filtered:
        return []

    # 2. Trunca pra Claude não estourar contexto (max 50 candidatos)
    candidates = pre_filtered[:50]

    # 3. Monta prompt
    items_json = []
    for i, it in enumerate(candidates):
        items_json.append({
            "id": i,
            "title": it.get("title", "")[:200],
            "summary": (it.get("summary") or "")[:300],
            "lang": it.get("lang", "pt"),
        })

    # Contexto do user: temas, filtros, preferências
    user_context_bits = []
    if user_topic_labels:
        user_context_bits.append(f"TEMAS DE INTERESSE DO LEITOR: {', '.join(user_topic_labels)}")
    if filtered_items:
        filtered_str = ", ".join(f'"{f}"' for f in filtered_items[:20])
        user_context_bits.append(f"ITENS FILTRADOS (NÃO trazer nada sobre): {filtered_str}")
    if learned_text and learned_text.strip():
        user_context_bits.append(f"PREFERÊNCIAS APRENDIDAS: {learned_text.strip()[:500]}")
    user_context = "\n".join(user_context_bits) if user_context_bits else "Leitor sem temas/preferências configurados — escolha o mais interessante geral."

    system = (
        "Você é o curador da seção 'SAIBA ANTES DE TODOS' do Recorte News, "
        "uma newsletter brasileira de notícias. Esta seção tem UM objetivo: "
        "trazer histórias verdadeiramente UAU que o leitor brasileiro AINDA NÃO VIU hoje.\n\n"
        "CRITÉRIOS RIGOROSOS pra escolher itens:\n\n"
        "1. RELEVÂNCIA AOS TEMAS DO LEITOR — priorize itens que conversam com os "
        "interesses declarados. Se o leitor segue 'Tech & IA', traga IA undercovered; "
        "se segue 'Geopolítica', traga geopolítica undercovered.\n\n"
        "2. GARANTIA DE 'UNDERCOVERED' — só inclua se você tem ALTA CONFIANÇA que "
        "a história NÃO foi coberta hoje por Folha, G1, Estadão, Valor, Exame, BBC Brasil, "
        "CNN Brasil ou outro veículo brasileiro mainstream. Na dúvida, DESCARTE.\n\n"
        "3. FATOR UAU — preferência absoluta por histórias surpreendentes, contraintuitivas, "
        "ou que revelam algo que vai virar conversa amanhã/semana que vem. EVITE trivialidade, "
        "incremental, ou coisa óbvia.\n\n"
        "4. RESPEITAR FILTROS — JAMAIS traga conteúdo sobre itens explicitamente filtrados "
        "pelo usuário. Esse é um filtro DURO.\n\n"
        "5. EVITAR: celebridade pura, esporte trivial, política partidária, fofoca, "
        "clickbait, conteúdo já viralizado.\n\n"
        "Idioma: aceitar inglês (das newsletters anglo) — você reescreve TUDO em PT-BR fluente."
    )

    user_prompt = (
        f"Leitor: {user_name}.\n\n"
        f"{user_context}\n\n"
        f"Candidatos brutos ({len(candidates)} itens):\n{json.dumps(items_json, ensure_ascii=False)}\n\n"
        f"Escolha até {max_out} MELHORES — pode ser MENOS se não tiver gente realmente "
        f"undercovered/UAU. Qualidade > quantidade. Se só 4 itens passam no filtro rigoroso, "
        f"retorne 4. Pra cada escolhido, retorne JSON:\n"
        "{\n"
        '  "items": [\n'
        '    {\n'
        '      "id": <id original>,\n'
        '      "manchete": "<reescrita em PT-BR fluente, 6-12 palavras, sem clickbait, intrigante>",\n'
        '      "resumo": "<2-3 frases em PT-BR explicando o que é, contexto e por que importa. TEXTO PURO sem nenhuma tag HTML ou markdown.>",\n'
        '      "fatos_chave": ["fato 1 curto", "fato 2 curto", "fato 3 curto"]\n'
        '    }\n'
        '  ]\n'
        "}\n\n"
        "REGRAS DE OUTPUT:\n"
        "- 3 a 4 fatos_chave por item, cada um com 4-10 palavras, factuais (não opinião)\n"
        "- Se não tem info suficiente nos candidatos pra preencher os campos com confiança, "
        "DESCARTE o item (não invente)\n"
        "- 🚫 TEXTO PURO em TUDO (manchete, resumo, fatos_chave): NUNCA use tags HTML "
        "(<strong>, <em>, <b>, <i>, <span>, <br>) nem markdown (**, *, __). "
        "O template do email cuida da tipografia. Se você inserir uma tag, ela aparece "
        "como texto literal escapado no email (bug visual).\n"
        "- Retorne APENAS o JSON, sem comentários antes/depois"
    )

    try:
        resp = claude.messages.create(
            model=MODEL,
            max_tokens=4000,
            system=system,
            messages=[{"role": "user", "content": user_prompt}],
        )
        raw = resp.content[0].text if resp.content else "{}"
        parsed = _robust_json_parse(raw)
    except Exception as e:
        log(f"  ⚠ curate_undercovered falhou: {e}")
        return []

    if not parsed or "items" not in parsed:
        log(f"  ⚠ curate_undercovered: JSON sem 'items'")
        return []

    # 4. Hidrata items escolhidos com link + source originais
    out = []
    for chosen in parsed.get("items", [])[:max_out]:
        idx = chosen.get("id")
        if idx is None or idx < 0 or idx >= len(candidates):
            continue
        original = candidates[idx]
        manchete = (chosen.get("manchete") or "").strip()
        resumo = (chosen.get("resumo") or "").strip()
        if not manchete or not resumo:
            continue  # garante validade mínima
        fatos = chosen.get("fatos_chave") or []
        if not isinstance(fatos, list):
            fatos = []
        out.append({
            "manchete": manchete,
            "resumo": resumo,
            "fatos_chave": fatos[:4],
            "link": original.get("link", ""),
            "fonte": original.get("source", ""),
            "lang": original.get("lang", "pt"),
            "origin": original.get("_origin", ""),  # interno, não exibido
            "img_url": original.get("img_url"),
            "raw_title": original.get("title"),  # pra anti-aluc
            "raw_summary": original.get("summary"),
        })

    # SAFETY: remove qualquer HTML que Claude tenha inserido na manchete/resumo/fatos
    # (ex: <strong>, <em>, &nbsp;) — o template já escapa, então deixar HTML aqui
    # faz aparecer texto literal escapado no email final.
    _strip_html_from_items(out)
    return out


def _robust_json_parse(text):
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
    """Processa UM batch de temas. Aplica buffer +2 pra suportar descartes da validação anti-alucinação."""
    user_target = news_per_topic if news_per_topic is not None else MAX_NEWS_OUT_PER_TOPIC
    # BUFFER ANTI-ALUCINAÇÃO: pede 2 a mais pro Claude. Após validate_and_clean_sections,
    # truncamos pra user_target. Garante volume mesmo com descartes.
    out_per_topic = user_target + 2
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
        resumo_instr = "**resumo**: 4-5 frases (140-220 palavras) em PT-BR. Foque em **síntese semanal**: o que aconteceu, como evoluiu nos dias, contexto, e implicação. Para temas com vários acontecimentos na semana, costure-os numa narrativa coesa em vez de listar isoladamente. **DESTAQUES**: envolva 2-3 termos importantes (números/valores/datas/nomes próprios chave) com `<strong>...</strong>` — serão renderizados em verde-menta com sublinhado pra ajudar a escanear. Ex: \"O Copom cortou a Selic em <strong>0,5 ponto</strong>, levando a taxa a <strong>12,75% ao ano</strong>\". NÃO envolva frases inteiras nem palavras genéricas."
        fatos_instr = "**fatos_chave**: array de 4 a 6 bullets curtos (cada um 6-18 palavras) com pontos-chave da semana — números, datas dos acontecimentos, players, valores, decisões."
    else:
        time_context = "As notícias abaixo são dos últimos 1-2 dias (edição diária)."
        editorial_brief = f"Para cada tema abaixo, selecione as **até {out_per_topic} notícias mais relevantes** do dia (priorize: impacto real, novidade, alinhamento com perfil; evite duplicatas e clickbait)."
        resumo_instr = "**resumo**: 3-4 frases (100-160 palavras) em PT-BR. Explica o que aconteceu, números/fatos centrais, contexto e implicação imediata. **DESTAQUES**: envolva 2-3 termos importantes (números/valores/datas/nomes próprios chave) com `<strong>...</strong>` — serão renderizados em verde-menta com sublinhado pra ajudar o leitor a escanear visualmente. Ex: \"O Copom cortou a Selic em <strong>0,5 ponto</strong>, levando a taxa a <strong>12,75% ao ano</strong>, na primeira redução desde <strong>setembro de 2025</strong>\". NÃO envolva frases inteiras nem palavras genéricas."
        fatos_instr = "**fatos_chave**: array de 3 a 5 bullets curtos (cada um 6-15 palavras) com os pontos mais importantes — números, datas, players, valores, decisões. Ex: [\"Selic caiu de 13,75% para 13,25%\", \"1ª redução em 12 meses\", \"Mercado esperava corte de 0,75 ponto\"]"
    now_brt = datetime.now(BRT)
    date_ctx = get_current_date_context(now_brt)
    system_prompt = f"""{VOICE_PROMPT}
# ============================================
# INSTRUÇÕES ESPECÍFICAS DESTA TAREFA — CURADORIA
# ============================================
📅 **CONTEXTO TEMPORAL**: {date_ctx}
{ANTI_HALLUCINATION_RULE}
Você está fazendo a CURADORIA editorial da edição diária do Recorte ✂ — escolhendo as matérias mais relevantes pra este leitor específico, escrevendo as manchetes, resumos e fatos-chave em PT-BR.
Tudo que você escrever vai direto pra caixa de entrada do leitor — siga o VOICE GUIDE acima rigorosamente.

🚫 **REGRA CRÍTICA DE FORMATO — TEXTO PURO**: TODAS as manchetes, resumos e fatos-chave devem ser **TEXTO PURO**, sem NENHUMA tag HTML ou markdown. NUNCA use `<strong>`, `<em>`, `<b>`, `<i>`, `<span>`, `<br>`, `**negrito**`, `*itálico*`, `__sublinhado__`, ou qualquer outro marcador de formatação. O template do email já cuida da tipografia. Se você adicionar uma tag, ela vai aparecer como texto literal escapado (ex: `&lt;strong&gt;`) no email do leitor — bug visual grave.

{SAFETY_INSTRUCTIONS}
{POLITICAL_BIAS_INSTRUCTIONS}
🇧🇷 **REGRA CRÍTICA DE IDIOMA**: TODO o conteúdo gerado DEVE estar em **português brasileiro natural**, MESMO QUE a matéria original esteja em inglês, espanhol ou outro idioma. Traduza com fluência, mantendo nomes próprios e marcas no original.
⚠️ **REGRA CRÍTICA DE FORMATO**: Retorne APENAS JSON VÁLIDO, sem markdown, sem blocos de código, sem ```json```. Escape TODAS as aspas duplas dentro de strings com \\". Não use quebras de linha dentro de strings. Não inclua texto antes ou depois do JSON. A primeira character da resposta DEVE ser `{{` e a última `}}`.
📰 **OBJETIVO EDITORIAL**: O leitor deve conseguir entender cada notícia INTEIRA sem precisar abrir o link. Seja rico em fatos, números, datas e contexto. Mantenha o tom do VOICE GUIDE — direto, brasileiro, próximo. PORÉM, NÃO INVENTE: tudo que você escrever deve vir EXPLICITAMENTE da fonte (regra anti-alucinação acima).
🚫 **REGRA CRÍTICA DE DEDUPLICAÇÃO**: NUNCA inclua duas notícias sobre o MESMO evento, mesmo que venham de fontes diferentes ou com palavras ligeiramente diferentes. Se ver vários itens brutos sobre o mesmo acontecimento, escolha APENAS UM (preferindo: fonte mais respeitável > matéria mais completa > publicação mais recente). Em caso de dúvida sobre se 2 são o mesmo evento, considere que SÃO e una.
🎯 **REGRA CRÍTICA DE COERÊNCIA TEMA ↔ NOTÍCIA** (descarte agressivo se não casar):
Cada notícia que você incluir DEVE ser **ESPECIFICAMENTE sobre o tema declarado**, não sobre algo tangencialmente relacionado. As fontes podem trazer matérias contaminadas por keywords amplas — **filtre você como editor**.
**Regra geral:** se você precisa explicar pra alguém POR QUE essa notícia está nesse tema, ela não está nesse tema. Descarte.
**Exemplos do que NUNCA fazer:**
- Tema "Wellness (Bem-estar)" — use a framework dos 6 PILARES do Global Wellness Institute (GWI). Wellness é a ECONOMIA DO BEM-ESTAR em pessoas saudáveis, NÃO saúde-doença.

  ✅ OS 6 PILARES (qualquer matéria que se encaixe em UM deles é elegível):
  1. **Saúde e Nutrição Preventiva**: alimentos funcionais, dietas (mediterrânea, plant-based, keto), suplementos (whey, creatina, ômega-3, magnésio, colágeno), chás funcionais, jejum intermitente.
  2. **Atividades Físicas (Fitness)**: academias, yoga, pilates, crossfit, musculação, corrida, athleisure, apps de treino, HIIT, mobilidade.
  3. **Estética e Cuidados Pessoais**: skincare, clean beauty, procedimentos NÃO invasivos (radiofrequência estética, drenagem, massagem), cosméticos naturais.
  4. **Saúde Mental e Mindfulness**: apps de meditação, terapia online, mindfulness, aromaterapia, óleos essenciais, coaching, redução de estresse.
  5. **Mercado do Sono**: colchões/travesseiros tecnológicos, wearables (Oura, Whoop), chás relaxantes, melatonina, higiene do sono.
  6. **Turismo de Bem-Estar e Spas**: retreats, spas urbanos, resorts termais, ofurô, sauna, day spa.

  ❌ NUNCA INCLUIR: doenças, sintomas, dores, vacinação, tratamentos médicos (cirurgia, quimio, transplante), sistema de saúde (hospital, UTI, SUS, OMS), epidemiologia (surto, mortalidade, óbito), diagnósticos médicos. Wellness NÃO é sobre pessoa doente.

  **Teste mental**: encaixa em UM dos 6 pilares? SIM = elegível. NÃO ou descreve doença/sintoma/tratamento? DESCARTE — vai pra Ciência & Saúde, não Wellness.
- Tema "Trabalho & carreira" → NÃO incluir matéria sobre economia macro, PIB, inflação.
- Tema "Cultura & entretenimento" → NÃO incluir matéria de celebridade processada por crime/escândalo policial.
- Tema "Negócios & M&A" → NÃO incluir matéria de tech/IA empresarial.
- Tema "Tech & IA" → NÃO incluir economia geral ou política tech.
**EM DÚVIDA, DESCARTE.** É melhor o tema vir com 1 notícia perfeita do que com 3 incluindo 1 deslocada. Não force preenchimento.
🕐 **REGRA CRÍTICA DE FRESCOR TEMPORAL**:
- **Para edições daily**: NUNCA inclua notícias com `publicado_em` mais antigo que **48 horas** em relação à data atual (que está no user message). Edição diária = notícias de HOJE e do DIA ANTERIOR. Não 3 dias atrás. Não semana passada.
- **CUIDADO COM AGREGADORES** (OneFootball, Flipboard, Google News com re-indexação, Yahoo Sports, etc): eles RE-INDEXAM notícias antigas com data de indexação recente. Se ver matéria sobre evento que SABIDAMENTE aconteceu há vários dias, DESCARTE — está velha mesmo que o `publicado_em` pareça novo. Use seu conhecimento do mundo.
- Para qualquer EVENTO com timeline definida: SE a data do evento JÁ PASSOU, NUNCA escolha uma matéria que cubra a PREVISÃO/EXPECTATIVA/ESCALAÇÃO/PRÉ-JOGO. Sempre prefira a matéria com o RESULTADO/desfecho.
- Quando 2 matérias falam do mesmo evento (uma "antes", outra "depois"): SEMPRE escolha a "depois".
- Se TODAS as matérias forem previsões de eventos já passados, MELHOR DESCARTAR a categoria.
📋 **ESTRUTURA DE RESPOSTA**: O JSON deve seguir o schema exato indicado no user message. Não invente campos. Não omita campos requeridos.
🎯 **CRITÉRIOS DE QUALIDADE**:
- Manchetes seguindo o VOICE GUIDE (máx 9 palavras quando possível, máx 90 chars sempre)
- Resumos com números, contexto, e implicação clara — TUDO DA FONTE (regra anti-alucinação acima)
- Fatos-chave concretos: datas, valores, players nomeados — TUDO DA FONTE
- Sempre cite a fonte original (campo "fonte")
- Indique idioma original se NÃO for PT (campo "lang")
⚖️ **RIGOR EDITORIAL**:
- Não invente fatos, números, citações ou eventos
- Não extrapole além do que está na matéria original
- Política, religião, identidade: enquadramento factual sempre
Os dados específicos do dia + instruções pontuais virão no próximo turn do user."""
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
    # SAFETY: remove HTML que Claude possa ter inserido nas manchetes/resumos/fatos
    for sec in secoes:
        _strip_html_from_items(sec.get("noticias", []))
    return secoes
def curate_news(user_name, topics_with_news, learned_profile="", filtered_items=None,
                weekly=False, news_per_topic=None, is_welcome=False):
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
    if not text:
        return ""
    return re.sub(r'[^a-z0-9]', '', text.lower())

# Stopwords PT/EN pra pular ao extrair primeira palavra "significativa" da manchete.
# Usado no dedup por (fonte, entidade) — captura repetição tipo
# "Trump anuncia X" + "Trump diz Y" do mesmo veículo.
_DEDUP_STOPWORDS = {
    "de","do","da","dos","das","e","a","o","as","os","um","uma","uns","umas",
    "em","no","na","nos","nas","ao","aos","à","às","para","por","pra","com","sem",
    "que","se","ou","mas","mais","menos","muito","muita","muitos","muitas",
    "ele","ela","eles","elas","seu","sua","seus","suas","este","esta","isso","esse","essa",
    "the","of","and","an","to","for","in","on","at","by","is","are","was","were",
    "this","that","these","those","it","its","be","been","being","with","from",
}


def _first_significant_word(title: str) -> str:
    """Pega primeira palavra ≥3 chars que não seja stopword (lowercase, sem acento desnecessário).
    Usado pra detectar 'mesma entidade central' em 2 manchetes diferentes do mesmo site."""
    if not title:
        return ""
    # Remove acentos pra match robusto (joão→joao)
    import unicodedata
    norm = unicodedata.normalize("NFKD", title)
    norm = "".join(c for c in norm if not unicodedata.combining(c))
    words = re.findall(r"\w+", norm.lower())
    for w in words:
        if w not in _DEDUP_STOPWORDS and len(w) >= 3:
            return w
    return words[0] if words else ""


def _significant_words(title: str, max_n: int = 6) -> set:
    """Retorna SET das primeiras max_n palavras significativas (≥3 chars, não-stopword, sem acento)."""
    if not title:
        return set()
    import unicodedata
    norm = unicodedata.normalize("NFKD", title)
    norm = "".join(c for c in norm if not unicodedata.combining(c))
    words = re.findall(r"\w+", norm.lower())
    sig = [w for w in words if w not in _DEDUP_STOPWORDS and len(w) >= 3]
    return set(sig[:max_n])


def _apply_user_filters(items, filtered_items):
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
    """Dedup do trending em 2 camadas (rápido, custo zero):
    1. Link exato (lowercased)
    2. Primeiros 50 chars da manchete normalizada (cobre títulos quase idênticos)

    Casos mais sutis (mesma fonte + mesma entidade, manchete-prefixo, overlap
    semântico tipo Fonseca x Djokovic da Jovem Pan) são pegados pelo
    EDITOR CHEFE (editorial_review), que vê a edição inteira e tem contexto
    semântico que o regex não tem.
    """
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
# DEDUP CROSS-EDIÇÃO 5D
# ============================================================================
def _load_recently_sent_signatures(user_id, days=5):
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    try:
        res = _supabase_retry(
            lambda: supabase.table("email_items").select("payload")
                .eq("user_id", user_id).eq("kind", "news")
                .gte("created_at", cutoff).execute(),
            label="load_recently_sent_signatures",
        )
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

# ============================================================================
# WELLNESS BLACKLIST — defesa programática EXAUSTIVA
# Wellness = lifestyle preventivo POSITIVO. Nunca doença/dor/sintoma/hospital/vacina/tratamento.
# Camada 2 reforçando a regra do system prompt (Camada 1).
# IMPORTANTE: usa word boundaries (\b) pra evitar falsos positivos
# tipo "versus" → "sus", "discussão" → "sus", "consorcio" → "sor", etc.
# ============================================================================
WELLNESS_THEME_PATTERNS = (
    "wellness", "bem-estar", "bem estar", "bemestar",
    "lifestyle saudável", "lifestyle saudavel",
)
# RAÍZES — viram \braiz\w*\b → pegam todas as variações
# (vacin → vacina/vacinas/vacinação/vacinal/vacinada; imuniz → imunização/imunizar/imunizante; etc.)
WELLNESS_BLACKLIST_ROOTS = (
    # Imunização (todas as variações)
    "vacin", "imuniz", "soroprev",
    # Doenças genéricas (raiz)
    "doenç", "doenc",
    # Sintomas (raiz)
    "sintom",
    # Inflamação (raiz)
    "inflam",
    # Epidemiologia (raízes)
    "epidem", "pandem", "endem",
    # Contaminação/infecção (raízes)
    "contagi", "contagí", "contamin", "infec", "infeç", "infecc",
    # Mortalidade (raiz)
    "óbito", "obito",
    # Cólica (raízes com/sem acento)
    "cólic", "colic",
    # Quimio/radio (raízes)
    "quimio", "radioterap",
    # Antibiótico/antiviral/etc (raiz "anti...")
    "antibiót", "antibiot", "antivir", "antifúng", "antifung",
    "antidepres", "ansiolít", "ansiolit", "analgésic", "analgesic",
)
# PALAVRAS COMPLETAS — viram \bpalavra\b (word boundary exata)
WELLNESS_BLACKLIST_KEYWORDS = (
    # === DOENÇAS NOMEADAS ===
    # Infecciosas/virais
    "gripe", "dengue", "covid", "covid-19", "zika", "chikungunya",
    "sarampo", "varíola", "variola", "varicela", "catapora", "rubéola", "rubeola",
    "caxumba", "tuberculose", "hepatite", "hepatites",
    "hiv", "aids", "sífilis", "sifilis", "gonorreia", "herpes",
    "malária", "malaria", "ebola", "febre amarela",
    # Crônicas/sistêmicas
    "câncer", "cancer", "cancerígeno", "cancerigeno",
    "tumor", "tumores", "carcinoma", "leucemia", "melanoma",
    "metástase", "metastase", "neoplasia",
    "diabetes", "diabético", "diabetico",
    "hipertensão", "hipertensao", "hipertenso",
    "obesidade mórbida", "obesidade morbida",
    "asma", "asmático", "asmatico", "bronquite", "pneumonia", "sinusite",
    "artrite", "artrose", "osteoporose", "fibromialgia",
    "alzheimer", "parkinson", "demência", "demencia",
    "esclerose múltipla", "esclerose multipla", "ela",
    "epilepsia", "lúpus", "lupus", "psoríase", "psoriase",
    "fibrose", "cirrose", "gastrite", "úlcera", "ulcera", "refluxo",
    "enxaqueca", "glaucoma", "catarata",
    # Cardiovasculares
    "avc", "derrame cerebral", "infarto", "infartos",
    "arritmia", "cardiopatia", "insuficiência cardíaca", "insuficiencia cardiaca",
    "trombose", "aneurisma",
    # Mentais (contexto clínico — "doença")
    "esquizofrenia", "esquizofrênico", "esquizofrenico",
    "bipolaridade", "transtorno bipolar", "transtornos mentais",
    "toc", "transtorno mental",
    "depressão clínica", "depressao clinica",
    "ansiedade clínica", "ansiedade clinica",

    # === SINTOMAS / DORES ===
    "dor", "dores",
    "febre", "calafrio", "calafrios",
    "náusea", "nausea", "náuseas", "nauseas",
    "enjoo", "enjôo", "vômito", "vomito", "vômitos", "vomitos",
    "diarreia", "diarréia",
    "tontura", "tonturas", "vertigem", "vertigens",
    "tpm", "ciclo menstrual", "menstrual",
    "fadiga crônica", "fadiga cronica",
    "dispneia", "falta de ar",
    "palpitação", "palpitacao", "palpitações", "palpitacoes",

    # === SISTEMA DE SAÚDE / INSTITUIÇÕES ===
    "oms", "sus", "anvisa", "fiocruz", "ans",
    "ministério da saúde", "ministerio da saude",
    "secretaria de saúde", "secretaria de saude",
    "hospital", "hospitais", "hospitalar",
    "uti", "utis", "upa", "upas",
    "pronto-socorro", "pronto socorro",
    "ambulância", "ambulancia", "ambulâncias", "ambulancias",
    "posto de saúde", "posto de saude",
    "unidade básica de saúde", "unidade basica de saude",
    "plano de saúde", "plano de saude", "convênio médico", "convenio medico",
    "internação", "internacao", "internado", "internada",
    "hospitalização", "hospitalizacao",

    # === TRATAMENTOS E MEDICAÇÕES ===
    "medicamento", "medicamentos",
    "remédio", "remedio", "remédios", "remedios",
    "fármaco", "farmaco", "fármacos", "farmacos",
    "dose", "doses", "reforço vacinal", "reforco vacinal",
    "cirurgia", "cirurgias", "cirúrgico", "cirurgico", "cirúrgica", "cirurgica",
    "operação cirúrgica", "operacao cirurgica",
    "transplante", "transplantes",
    "hemodiálise", "hemodialise", "diálise", "dialise",
    "transfusão", "transfusao",
    "biópsia", "biopsia", "biópsias", "biopsias",

    # === DIAGNÓSTICOS / EXAMES ===
    "diagnóstico", "diagnostico", "diagnósticos", "diagnosticos",
    "prognóstico", "prognostico",
    "ressonância magnética", "ressonancia magnetica",
    "tomografia", "raio-x", "raio x",
    "exame de sangue", "hemograma",

    # === EPIDEMIOLOGIA / MORTALIDADE ===
    "surto", "surtos",
    "mortalidade", "letalidade",
    "falecimento", "morre", "morreu", "morrer",
    "casos confirmados", "caso confirmado",
    "caso suspeito", "casos suspeitos",
    "calendário vacinal", "calendario vacinal",
    "campanha de vacinação", "campanha de vacinacao",

    # === GESTAÇÃO / REPRODUTIVO MÉDICO ===
    "gravidez de risco", "parto prematuro", "aborto espontâneo", "aborto espontaneo",
    "pré-eclampsia", "eclampsia", "pre-eclampsia",
    "menopausa precoce",
)
import re as _re

# Constrói regex unificado: raízes (com \w* pra pegar variações) + palavras inteiras (com \b)
_root_pattern = "|".join(_re.escape(r) for r in WELLNESS_BLACKLIST_ROOTS)
_keyword_pattern = "|".join(_re.escape(k) for k in WELLNESS_BLACKLIST_KEYWORDS)
_WELLNESS_RE = _re.compile(
    r"(?:\b(?:" + _root_pattern + r")\w*\b)|(?:\b(?:" + _keyword_pattern + r")\b)",
    _re.IGNORECASE,
)


def _is_wellness_theme(label):
    """Detecta se o tema é wellness/bem-estar (case insensitive)."""
    if not label:
        return False
    l = label.lower()
    return any(p in l for p in WELLNESS_THEME_PATTERNS)


def _filter_wellness_medical(raw_sections):
    """
    Remove matérias médicas/doenças/dores/vacinas/tratamentos de temas wellness/bem-estar.
    Wellness é APENAS lifestyle preventivo positivo (yoga, alimentação, exercício, sono, etc).

    Roda APÓS curate_news + anti-aluc, ANTES de sections.append final.
    Esquema dos raw_sections: lista de dicts {tema, noticias: [{manchete, resumo, fatos_chave, link, ...}]}.

    Estratégia:
    - Raízes (\\braiz\\w*\\b): pega vacina/vacinação/vacinal/vacinada, doença/doenças/doente, etc.
    - Palavras inteiras (\\bpalavra\\b): evita falsos positivos (versus≠sus, doutorado≠doutor).
    """
    if not raw_sections:
        return 0
    removed = 0
    for sec in raw_sections:
        tema = sec.get("tema", "")
        if not _is_wellness_theme(tema):
            continue
        kept = []
        for n in sec.get("noticias", []):
            content_parts = [
                n.get("manchete") or "",
                n.get("resumo") or "",
            ]
            fk = n.get("fatos_chave") or []
            if isinstance(fk, list):
                content_parts.append(" ".join(str(x) for x in fk))
            content = " ".join(content_parts)
            m = _WELLNESS_RE.search(content)
            if m:
                removed += 1
                log(f"  🚫 wellness blacklist: '{n.get('manchete','?')[:60]}' (match: {m.group(0)!r})")
                continue
            kept.append(n)
        sec["noticias"] = kept
    if removed:
        log(f"  🧹 wellness blacklist: removidas {removed} matéria(s) médica(s) de tema wellness/bem-estar")
    return removed


# ============================================================================
# EDITOR CHEFE — revisão editorial final por Claude Sonnet
# Roda APÓS curate+anti-aluc+filtros+dedup, ANTES do render_email.
# Simula um editor humano sênior lendo a edição final, podendo:
#   - KEEP: manter como está
#   - REWRITE: reescrever manchete/resumo/fatos_chave (mantém link/fonte/img)
#   - DROP: descartar com motivo
# Guard rail: se >30% dos itens forem descartados, ROLLBACK automático.
# ============================================================================
_EDITOR_SYSTEM_PROMPT = """Você é editor-chefe sênior do Recorte ✂, newsletter brasileira premium curada com IA. Sua função: revisar a edição FINAL ANTES de publicar, como um leitor crítico humano leria.

Você é o ÚLTIMO filtro de qualidade. Pode REESCREVER, DESCARTAR ou APROVAR cada item.

📋 CRITÉRIOS DE REVISÃO POR ITEM:
- **COERÊNCIA TEMA↔MATÉRIA**: a matéria realmente cabe nesse tema? (ex: Wellness sobre vacina/dor/doença = DROP, vai pra outro tema)
- **MANCHETE**: clickbait? confusa? promete e não entrega? Falta sujeito? Vaga? → REWRITE
- **RESUMO**: redundante com manchete? jargão técnico sem tradução? frase incompleta? incoerente? → REWRITE
- **FATOS_CHAVE**: cada um deve adicionar info NOVA (não repetir resumo). Devem ser factuais, não opinião. 3-4 itens ideais. → REWRITE
- **FORMATO**: HTML residual (`<strong>`, `<em>`), markdown (`**`, `*`), aspas tortas misturadas, emoji estranho → REWRITE limpando
- **TOM**: deve soar conversacional inteligente (amigo informado contando), não voz de imprensa tradicional, sem floreio jornalístico

📋 CRITÉRIOS NA EDIÇÃO INTEIRA:
- **DUPLICATA**: 2 manchetes sobre o MESMO evento em seções diferentes? → DROP a mais fraca
- **MAINSTREAM em "saiba_antes"**: item com fonte óbvia tipo Folha/G1/Estadão/Valor/UOL/Globo? → DROP (era pra ser exclusivo)
- **REDUNDÂNCIA dentro do mesmo tema**: 2 manchetes muito parecidas? → DROP a fraca

⚖️ LIMITES:
- NÃO INVENTE FATOS: REWRITE usa apenas informação que já está nos campos do item
- NÃO TOQUE em link, fonte, img_url — só mexe em manchete, resumo, fatos_chave
- Em dúvida entre REWRITE e KEEP → KEEP (reescreva só quando claramente melhora)
- Em dúvida entre DROP e KEEP → KEEP (descarte só quando claramente prejudica)
- Manchete: 6-12 palavras ideais
- Texto sempre PT-BR fluente, TEXTO PURO sem nenhuma tag HTML ou markdown
"""


def editorial_review(user_name, sections, trending, undercovered,
                     weekly=False, max_drop_ratio=0.30):
    """
    Revisão editorial final por Claude (modelo MODEL_SMALL=Sonnet) sobre a edição completa.
    Aplica decisões in-place em sections/trending/undercovered.

    Retorna dict com stats: {kept, rewritten, dropped, rolled_back, drops: [motivos]}.

    Modifica:
    - Items REWRITE: atualiza manchete/resumo/fatos_chave in-place
    - Items DROP: remove da lista
    - Sections vazias após DROPs: removidas

    Guard rail: se DROPs > max_drop_ratio do total, ROLLBACK (mantém originais).
    Tolera falhas: API erro / JSON inválido → retorna stats vazios, NÃO bloqueia.
    """
    stats = {"kept": 0, "rewritten": 0, "dropped": 0, "rolled_back": False, "drops": []}

    # Monta lista compacta de items pra Claude
    all_items = []
    for s_idx, sec in enumerate(sections or []):
        for n_idx, n in enumerate(sec.get("noticias", [])):
            all_items.append({
                "id": f"sec_{s_idx}_{n_idx}",
                "kind": f"tema:{sec.get('tema') or sec.get('topic', '?')}",
                "manchete": n.get("manchete", ""),
                "resumo": n.get("resumo", ""),
                "fatos_chave": n.get("fatos_chave", []),
                "fonte": n.get("fonte", ""),
            })
    for t_idx, t in enumerate(trending or []):
        all_items.append({
            "id": f"tre_{t_idx}",
            "kind": "em_alta",
            "manchete": t.get("manchete", ""),
            "resumo": t.get("resumo", ""),
            "fatos_chave": t.get("fatos_chave", []),
            "fonte": t.get("fonte", ""),
        })
    for u_idx, u in enumerate(undercovered or []):
        all_items.append({
            "id": f"und_{u_idx}",
            "kind": "saiba_antes",
            "manchete": u.get("manchete", ""),
            "resumo": u.get("resumo", ""),
            "fatos_chave": u.get("fatos_chave", []),
            "fonte": u.get("fonte", ""),
        })

    if not all_items:
        return stats

    initial_count = len(all_items)
    log(f"  ✏️ editor chefe: revisando {initial_count} items da edição final...")

    user_message = (
        f"Leitor: {user_name}{' (edição SEMANAL — Minha Semana)' if weekly else ''}.\n\n"
        f"Edição completa pra revisar ({initial_count} itens):\n"
        f"{json.dumps({'items': all_items}, ensure_ascii=False, separators=(',', ':'))}\n\n"
        "Pra cada item, retorne decisão. JSON exato neste formato:\n"
        '{"decisions":[\n'
        '  {"id":"<id>","action":"KEEP"},\n'
        '  {"id":"<id>","action":"REWRITE","manchete":"<nova>","resumo":"<novo>","fatos_chave":["...","..."]},\n'
        '  {"id":"<id>","action":"DROP","motivo":"<motivo curto>"}\n'
        ']}\n\n'
        f"Inclua TODOS os {initial_count} itens. Use somente os ids exatos da lista. "
        "Em REWRITE, só inclua campos que mudou (omitir = mantém original). "
        "Responda APENAS o JSON, sem comentários antes/depois."
    )

    try:
        parsed = _call_claude_json(
            user_message, max_tokens=10000, retries=2,
            log_prefix=" (editor)",
            model=MODEL,  # Haiku (mais barato) — editorial review não precisa de Sonnet
            system_prompt=_EDITOR_SYSTEM_PROMPT,
        )
    except Exception as e:
        log(f"  ⚠ editor falhou (não bloqueia): {e}")
        return stats

    if not parsed or "decisions" not in parsed:
        log(f"  ⚠ editor: resposta sem 'decisions', pulando review")
        return stats

    raw_decisions = parsed.get("decisions") or []
    decisions = {}
    for d in raw_decisions:
        if isinstance(d, dict) and isinstance(d.get("id"), str):
            decisions[d["id"]] = d

    # Guard rail: rollback se DROPs > threshold
    n_drops_proposed = sum(1 for d in decisions.values() if d.get("action") == "DROP")
    drop_pct = n_drops_proposed / initial_count if initial_count > 0 else 0
    if drop_pct > max_drop_ratio:
        log(f"  ⚠ editor: {n_drops_proposed}/{initial_count} ({drop_pct:.0%}) descartes > {max_drop_ratio:.0%} — ROLLBACK, mantém originais")
        stats["rolled_back"] = True
        return stats

    # Aplica decisões (marca DROPs com _drop, aplica REWRITEs in-place)
    n_rewritten = 0
    drops_log = []

    def _apply_rewrite(item, d):
        nonlocal n_rewritten
        changed = False
        if d.get("manchete") and d["manchete"] != item.get("manchete"):
            item["manchete"] = d["manchete"]; changed = True
        if d.get("resumo") and d["resumo"] != item.get("resumo"):
            item["resumo"] = d["resumo"]; changed = True
        if isinstance(d.get("fatos_chave"), list) and d["fatos_chave"] != item.get("fatos_chave"):
            item["fatos_chave"] = d["fatos_chave"]; changed = True
        if changed:
            n_rewritten += 1

    # Sections
    for s_idx, sec in enumerate(sections or []):
        for n_idx, n in enumerate(sec.get("noticias", [])):
            d = decisions.get(f"sec_{s_idx}_{n_idx}")
            if not d:
                continue
            action = d.get("action")
            if action == "DROP":
                n["_drop"] = True
                drops_log.append(f"tema/{sec.get('tema','?')}: '{n.get('manchete','?')[:50]}' ({d.get('motivo','?')[:80]})")
            elif action == "REWRITE":
                _apply_rewrite(n, d)

    # Trending
    for t_idx, t in enumerate(trending or []):
        d = decisions.get(f"tre_{t_idx}")
        if not d:
            continue
        action = d.get("action")
        if action == "DROP":
            t["_drop"] = True
            drops_log.append(f"em_alta: '{t.get('manchete','?')[:50]}' ({d.get('motivo','?')[:80]})")
        elif action == "REWRITE":
            _apply_rewrite(t, d)

    # Undercovered
    for u_idx, u in enumerate(undercovered or []):
        d = decisions.get(f"und_{u_idx}")
        if not d:
            continue
        action = d.get("action")
        if action == "DROP":
            u["_drop"] = True
            drops_log.append(f"saiba_antes: '{u.get('manchete','?')[:50]}' ({d.get('motivo','?')[:80]})")
        elif action == "REWRITE":
            _apply_rewrite(u, d)

    # Filtra _drop e remove sections vazias
    n_actual_drops = 0
    if sections:
        for sec in sections:
            before = len(sec.get("noticias", []))
            sec["noticias"] = [n for n in sec.get("noticias", []) if not n.get("_drop")]
            n_actual_drops += before - len(sec["noticias"])
        # Remove sections que ficaram sem nenhuma noticia
        sections[:] = [s for s in sections if s.get("noticias")]
    if trending is not None:
        before_t = len(trending)
        trending[:] = [t for t in trending if not t.get("_drop")]
        n_actual_drops += before_t - len(trending)
    if undercovered is not None:
        before_u = len(undercovered)
        undercovered[:] = [u for u in undercovered if not u.get("_drop")]
        n_actual_drops += before_u - len(undercovered)

    stats["dropped"] = n_actual_drops
    stats["rewritten"] = n_rewritten
    stats["kept"] = initial_count - n_actual_drops - n_rewritten
    stats["drops"] = drops_log

    log(f"  ✏️ editor chefe: kept={stats['kept']} rewritten={stats['rewritten']} dropped={stats['dropped']}/{initial_count} ({n_actual_drops/initial_count:.0%})")
    for drop_msg in drops_log[:8]:
        log(f"     ✗ {drop_msg}")
    if len(drops_log) > 8:
        log(f"     ... e mais {len(drops_log) - 8} drops")

    return stats


def curate_trends(user_name, scope_label, trends, learned_profile="",
                  user_topics_labels=None, filtered_items=None, max_out=None, weekly=False):
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
    # BUFFER ANTI-ALUCINAÇÃO no trending também: pede 2 a mais.
    user_target_trend = total_out
    if max_out is not None and max_out > 0:
        user_target_trend = min(user_target_trend, max_out)
    total_out_with_buffer = user_target_trend + 2
    filter_instruction = ""
    if filtered_items:
        filter_list = ", ".join(f'"{f}"' for f in filtered_items[:20])
        filter_instruction = f"""
🚫 **FILTROS DURO DO USUÁRIO** — proibido incluir trends que envolvam estes temas/veículos:
{filter_list}
Se um trend é de um veículo dessa lista (campo "fonte"), DESCARTE-O integralmente.
"""
    now_brt = datetime.now(BRT)
    date_ctx = get_current_date_context(now_brt)
    system_prompt = f"""{VOICE_PROMPT}
# ============================================
# INSTRUÇÕES ESPECÍFICAS DESTA TAREFA — EM ALTA
# ============================================
📅 **CONTEXTO TEMPORAL**: {date_ctx}
{ANTI_HALLUCINATION_RULE}
Você está montando a seção "🔥 Em Alta" da edição diária do Recorte ✂.
{SAFETY_INSTRUCTIONS}
🚫 **REGRA CRÍTICA DE DEDUPLICAÇÃO**: NUNCA inclua duas manchetes sobre o MESMO evento.
🇧🇷 **REGRA CRÍTICA DE IDIOMA**: TODO conteúdo DEVE estar em **português brasileiro fluente**.
⚠️ **REGRA CRÍTICA DE FORMATO**: Retorne APENAS JSON VÁLIDO, sem markdown.
🚫 **REGRA CRÍTICA DE TEXTO PURO**: TODAS as manchetes, resumos e fatos_chave são TEXTO PURO. NUNCA use tags HTML (<strong>, <em>, <b>, <i>, <span>) nem markdown (**, *, __). O template do email cuida da tipografia — se você inserir uma tag, aparece como texto literal escapado no email do leitor.
📰 **OBJETIVO EDITORIAL**: O leitor entende cada evento sem precisar abrir o link. PORÉM tudo deve vir EXPLICITAMENTE da fonte (regra anti-alucinação acima).
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
{instruction_verb} ({total_out_with_buffer} itens): top stories + redes sociais + viralizações.
{'Como é semanal, dê contexto e mencione a evolução ao longo dos dias quando relevante.' if weekly else 'Seja rico em fatos e contexto.'}
Pra cada item:
- **manchete**: título PT-BR direto, máx 90 chars, sem clickbait
- **resumo**: {('4-5 frases (140-200 palavras)' if weekly else '3-4 frases (100-160 palavras)')}. Envolva 2-3 termos importantes (números/valores/datas/nomes-chave) com `<strong>...</strong>` — renderizados em verde-menta com sublinhado. NÃO envolva frases inteiras.
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
    # SAFETY: remove HTML que Claude possa ter inserido (<strong>, <em>, etc.)
    _strip_html_from_items(items)
    return items
def generate_daily_recap(user_name, sections, trending, learned_profile=""):
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
    now_brt = datetime.now(BRT)
    date_ctx = get_current_date_context(now_brt)
    recap_system = f"""{VOICE_PROMPT}
📅 **CONTEXTO TEMPORAL**: {date_ctx}
{ANTI_HALLUCINATION_RULE}
Você está escrevendo o briefing "Seu dia em 60 segundos" + a quote do dia. As manchetes já passaram por validação anti-alucinação — você pode confiar no que recebe. PORÉM, não invente conexões entre eventos, não acrescente contexto de memória, e não atribua cargos/papéis a pessoas que não estão explícitos nas manchetes. Trabalhe APENAS com o que está nas manchetes recebidas."""
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
                                system_prompt=recap_system)
    if not parsed:
        return {"recap": "", "quote": "", "quote_author": ""}
    return {
        "recap": (parsed.get("recap") or "").strip(),
        "quote": (parsed.get("quote") or "").strip(),
        "quote_author": (parsed.get("quote_author") or "").strip(),
    }
# ============ EMAIL ITEMS + FEEDBACK LINKS ============
def create_email_item(user_id, kind, payload):
    iid = short_id()
    # UPSERT em vez de INSERT pra ser idempotente sob retry.
    # Bug que isso corrige: se a 1ª tentativa do INSERT chega no banco mas
    # a resposta se perde por RemoteProtocolError, o retry refaz o mesmo
    # INSERT com o mesmo `iid` e quebra com "duplicate key (23505)".
    # Com UPSERT + on_conflict="id", o retry é safe: 2ª tentativa "atualiza"
    # o registro com os mesmos valores (no-op funcional) e retorna ok.
    _supabase_retry(
        lambda: supabase.table("email_items").upsert({
            "id": iid,
            "user_id": user_id,
            "kind": kind,
            "payload": payload,
        }, on_conflict="id").execute(),
        label=f"create_email_item({kind})",
    )
    return iid
def _try_decode_gnews_url(url, timeout=4):
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
    try:
        res = _supabase_retry(
            lambda: supabase.table("user_profile").select("*").eq("user_id", user_id).execute(),
            label="user_profile.select",
        )
    except Exception as e:
        log(f"  ⚠ load_profile falhou após retry: {e}")
        return {"learned_text": "", "paused_topics": [], "filtered_items": []}
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
    # PATCH ANTI-ALUCINAÇÃO: gera edition_id cedo pra usar no log de validação
    from tracking import gen_edition_id, save_edition, wrap_links_in_html, finalize_edition
    edition_id = gen_edition_id()
    _topics_pre = _supabase_retry(
        lambda: supabase.table("topics").select("label").eq("user_id", uid).execute(),
        label="topics.select(pre)",
    )
    user_topic_labels = [t["label"] for t in (_topics_pre.data or [])]
    topic_count_for_scaling = len({lbl for lbl in user_topic_labels}) if user_topic_labels else 0
    # Janela de frescor: segunda-feira 48h, demais dias 30h, weekly 168h
    stale_window_h = get_stale_window_hours(weekly, now_brt)
    log(f"  📅 janela frescor: {stale_window_h}h ({'weekly' if weekly else ('segunda' if now_brt.weekday() == 0 else 'daily')})")
    # 1) TRENDING
    trending = []
    trending_label = ""
    raw_trends_combined = []  # mantém todos os trends brutos pra validação anti-alucinação
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
            # PATCH ANTI-ALUCINAÇÃO: passa max_age_hours pro fetch_trending
            raw_trends = fetch_trending(tcountry, weekly=weekly, max_age_hours=stale_window_h)
            log(f"  trends brutos", count=len(raw_trends), scope=tcountry, weekly=weekly)
            raw_trends_combined.extend(raw_trends)  # acumula pra validação posterior
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
        # PATCH ANTI-ALUCINAÇÃO: valida trending contra fontes brutas
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
                # Trunca pra target final após validação
                if len(trending) > TOTAL_TRENDING_BUDGET:
                    trending = trending[:TOTAL_TRENDING_BUDGET]
            except Exception as e:
                log(f"  ⚠ anti-aluc trending falhou (não bloqueia): {e}")
    # 2) NOTÍCIAS POR TEMA
    topics_res = _supabase_retry(
        lambda: supabase.table("topics").select("*").eq("user_id", uid).execute(),
        label="topics.select(full)",
    )
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
            max_age_hours=stale_window_h,
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
    # DEDUP 5D CROSS-EDIÇÃO
    try:
        sent_links, sent_title_sigs = _load_recently_sent_signatures(uid, days=5)
        if sent_links or sent_title_sigs:
            removed_5d = _filter_already_sent(topics_with_news, sent_links, sent_title_sigs)
            if removed_5d > 0:
                log(f"  🔁 dedup 5d: removidas {removed_5d} notícia(s) já enviada(s) recentemente "
                    f"(banco: {len(sent_links)} URLs + {len(sent_title_sigs)} signatures)")
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
        # PATCH ANTI-ALUCINAÇÃO: valida cada notícia curada contra fonte bruta original.
        # 3 camadas: crítico→descarte, moderado→reescrita, leve→permite + log.
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
            # CAMADA 2 (defesa programática): blacklist médica em temas wellness/bem-estar.
            # Camada 1 (system prompt) tenta prevenir, mas curador às vezes deixa passar
            # gripe/dor menstrual/epidemia em Wellness. Esse filtro garante "nunca doenças e dores".
            try:
                _filter_wellness_medical(raw_sections)
            except Exception as e:
                log(f"  ⚠ wellness blacklist falhou (não bloqueia): {e}")
            # Trunca cada seção pra news_per_topic (depois do buffer +2 que pedimos ao Claude)
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
            uc_raw = fetch_undercovered(
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
                    if not is_topic_paused(it.get("title", "")[:80], paused, datetime.now(timezone.utc))
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
                        new_url = _try_decode_gnews_url(url)
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

            # FIX BUG GAZETA: re-aplica filtro BR mainstream APÓS decode
            try:
                before_post = len(uc_raw)
                uc_raw = [it for it in uc_raw if not _is_br_mainstream(it.get("link", ""))]
                removed_post = before_post - len(uc_raw)
                if removed_post > 0:
                    log(f"  🧹 saiba_antes pós-decode: removidos {removed_post} mainstream BR revelados após decode")
            except Exception as e:
                log(f"  ⚠ re-filtro mainstream falhou (não bloqueia): {e}")

            # FILTRO: dedup 5d cross-edição (não enviar repetido em 5 dias)
            try:
                if uc_raw:
                    _sent_links, _sent_sigs = _load_recently_sent_signatures(uid, days=5)
                    if _sent_links or _sent_sigs:
                        before_5d = len(uc_raw)
                        _uc_group = [{"news": uc_raw, "label": "_uc"}]
                        _filter_already_sent(_uc_group, _sent_links, _sent_sigs)
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
                undercovered = curate_undercovered(
                    user["name"], uc_raw, learned,
                    user_topic_labels=user_topic_labels,
                    filtered_items=filtered_items,
                    exclude_links=exclude_links,
                    exclude_titles=exclude_titles,
                    max_out=8,
                    weekly=weekly,
                )
                log(f"  📡 saiba_antes curados count={len(undercovered)}")

                # DEFENSE IN DEPTH: re-filtra mainstream BR pós-cura
                if undercovered:
                    before_final = len(undercovered)
                    undercovered = [u for u in undercovered if not _is_br_mainstream(u.get("link", ""))]
                    removed_final = before_final - len(undercovered)
                    if removed_final > 0:
                        log(f"  🧹 saiba_antes pós-cura: removidos {removed_final} mainstream BR (defense in depth)")

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
    resolve_gnews_urls(sections, trending)
    before_count = len(sections)
    sections = [s for s in sections if s.get("noticias")]
    dropped_empty = before_count - len(sections)
    if dropped_empty > 0:
        log(f"  🧹 removidos {dropped_empty} tema(s) sem notícias")
    sections = add_feedback_links(uid, sections)

    # ============ EDITOR CHEFE — revisão editorial final ============
    # Última camada antes do render. Revisa edição INTEIRA simulando leitor humano.
    # Pode reescrever ou descartar items. Guard rail interno: rollback se >30% DROPs.
    try:
        editorial_review(user["name"], sections, trending, undercovered, weekly=weekly)
        # Sections vazias (todos os items descartados) já foram filtradas dentro do editor.
        # Re-aplica defesa redundante por segurança:
        sections = [s for s in sections if s.get("noticias")]
    except Exception as e:
        log(f"  ⚠ editor chefe falhou (não bloqueia): {e}")

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
    click_base = os.environ.get("CLICK_BASE_URL", "https://recorte.news/c")
    share_base = os.environ.get("SHARE_BASE_URL", f"{SUPABASE_URL}/functions/v1/edition")
    html = render_email(
        user_name=user["name"], date_obj=now_brt,
        trending=trending, trending_label=trending_label,
        undercovered=undercovered,
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
            _supabase_retry(
                lambda: supabase.table("editions").update({"html": html}).eq("id", edition_id).execute(),
                label="editions.update(html)",
            )
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
    try:
        _supabase_retry(
            lambda: supabase.table("users").update(user_updates).eq("id", uid).execute(),
            label="users.update(last_sent_at)",
        )
    except Exception as e:
        log(f"  ⚠ users.update final falhou (não bloqueia o envio): {e}")
    return True
def main():
    now_brt = datetime.now(BRT)
    target_hour = TARGET_HOUR_BRT if TARGET_HOUR_BRT >= 0 else now_brt.hour
    log(f"=== Manhã ☕ V3 run ===", hora=target_hour, dry=DRY_RUN)
    today_start_brt = now_brt.replace(hour=0, minute=0, second=0, microsecond=0)
    res = _supabase_retry(
        lambda: supabase.table("users").select("*").eq("active", True).execute(),
        label="users.select(main)",
    )
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
