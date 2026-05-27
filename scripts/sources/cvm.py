"""
sources/cvm.py — Fatos Relevantes e comunicados ao mercado (BR)

Captura movimentos corporativos das listadas na B3 antes de virarem notícia
"editorializada". Útil pra pegar M&A, IPO, layoffs, mudança de C-level, fato
relevante de earnings antes de aparecer em coluna ou manchete.

ABORDAGEM: a CVM tem o sistema ENET/SAD que NÃO expõe RSS público limpo.
Pra evitar scraper frágil, usamos Google News com queries específicas
('fato relevante', 'comunicado ao mercado', 'CVM divulga', tickers principais)
restringindo idioma e data. Captura ~90% do sinal com 10% do trampo.

PADRÃO DE RETORNO (compatível com fetch_all_sources):
    [{title, link, source, published_at, lang, summary}, ...]
"""

import logging
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.utils import parsedate_to_datetime

try:
    import feedparser
    import urllib.parse
except ImportError:
    feedparser = None

log = logging.getLogger(__name__)

# Queries pra captar Fatos Relevantes / comunicados.
# Combinam termo formal + termos genéricos pra capturar comunicados que
# usam fraseado diverso.
DEFAULT_QUERIES = [
    '"fato relevante" CVM',
    '"comunicado ao mercado" B3',
    '"aviso aos acionistas" empresa brasileira',
    'CVM divulga fato relevante',
    'companhia aberta anuncia fusão OR aquisição OR ofertapública',
]

# Domínios CONFIÁVEIS (relevantes pra fato relevante): financeiros + agregadores.
# Notícia em si pode vir de várias fontes; filtramos por relevância em pós.
TRUSTED_DOMAINS = [
    "valor.globo.com", "infomoney.com.br", "exame.com", "investnews.com.br",
    "neofeed.com.br", "brazilJournal.com", "money-times.com.br",
    "tradingview.com", "investidor10.com.br", "bloomberg.com",
    "reuters.com", "rad.cvm.gov.br",
]


def _parse_date(entry):
    for k in ("published", "updated", "created"):
        v = entry.get(k)
        if not v:
            continue
        try:
            dt = parsedate_to_datetime(v)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            pass
    return None


def _build_gnews_url(query, when="1d"):
    """Monta URL do Google News RSS pra uma query, restringindo PT-BR."""
    q = urllib.parse.quote(f"{query} when:{when}")
    return f"https://news.google.com/rss/search?q={q}&hl=pt-BR&gl=BR&ceid=BR:pt-419"


def _fetch_query(query, max_items=5, when="1d", max_age_hours=36):
    if not feedparser:
        return []
    url = _build_gnews_url(query, when=when)
    try:
        d = feedparser.parse(url)
    except Exception as e:
        log.warning(f"cvm: erro parse query='{query}': {e}")
        return []
    if not d.entries:
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    out = []
    for e in d.entries[:max_items * 2]:
        title = (e.get("title") or "").strip()
        link = (e.get("link") or "").strip()
        if not title or not link:
            continue
        published_iso = _parse_date(e)
        if published_iso:
            try:
                pub_dt = datetime.fromisoformat(published_iso.replace("Z", "+00:00"))
                if pub_dt < cutoff:
                    continue
            except Exception:
                pass

        # Tenta extrair domínio da fonte do Google News snippet
        src = "CVM / B3"
        if hasattr(e, "source") and isinstance(e.source, dict):
            src = e.source.get("title") or src

        out.append({
            "title": title,
            "link": link,
            "source": f"📜 {src}",  # emoji pra marcar visualmente como "corporate"
            "published_at": published_iso,
            "lang": "pt",
            "summary": "",  # GNews não dá summary útil
        })
        if len(out) >= max_items:
            break
    return out


def fetch(max_items_per_query=3, max_age_hours=36, weekly=False):
    """
    Busca Fatos Relevantes recentes via múltiplas queries paralelas.

    Args:
        max_items_per_query: quantos itens por query (default 3)
        max_age_hours: idade máxima (default 36h pra capturar overnight news)
        weekly: se True, janela semana e mais resultados

    Returns:
        Lista deduped por link, ordenada por data desc.
    """
    if not feedparser:
        return []
    when = "7d" if weekly else "2d"
    if weekly:
        max_items_per_query = max_items_per_query * 2

    all_items = []
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {
            ex.submit(_fetch_query, q, max_items_per_query, when, max_age_hours): q
            for q in DEFAULT_QUERIES
        }
        try:
            for fut in as_completed(futures, timeout=15):
                try:
                    items = fut.result()
                    all_items.extend(items)
                except Exception as e:
                    log.warning(f"cvm fetch erro query={futures[fut]}: {e}")
        except TimeoutError:
            log.warning(f"cvm: timeout 15s — usando {len(all_items)} items")

    # Dedup por título normalizado (mesmo fato relevante aparece em várias fontes)
    import re
    seen = set()
    deduped = []
    for it in all_items:
        key = re.sub(r"\W+", "", it["title"]).lower()[:80]
        if key and key not in seen:
            seen.add(key)
            deduped.append(it)

    deduped.sort(key=lambda x: x.get("published_at") or "", reverse=True)
    return deduped
