"""
sources/google_trends.py — termos com spike de busca no dia (RSS oficial)

Por que NÃO pytrends: a biblioteca é instável, sofre rate limit 429 do Google
constantemente, e o endpoint /realtime_trending_searches é frequentemente
desativado. Em vez disso, usamos o RSS público que Google mantém em
trends.google.com/trends/trendingsearches/daily/rss — estável e sem auth.

O RSS retorna ~20 termos trending do dia (por país), com news headlines
relacionadas a cada termo. Cada item vira uma "notícia" no padrão padrão.

PADRÃO DE RETORNO:
    [{title, link, source, published_at, lang, summary, search_volume?}, ...]
"""

import logging
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime

try:
    import feedparser
except ImportError:
    feedparser = None

log = logging.getLogger(__name__)

# Mapeamento país → código geo do Google Trends
GEO_MAP = {
    "BR": "BR",
    "GLOBAL": "US",  # global cai pra US trending
    "US": "US",
    "PT": "PT",
    "AR": "AR",
    "MX": "MX",
    "GB": "GB",
}

# Idioma de display por país (pra etiquetar lang corretamente)
LANG_MAP = {
    "BR": "pt", "PT": "pt",
    "US": "en", "GLOBAL": "en", "GB": "en",
    "AR": "es", "MX": "es",
}


def _parse_date(entry):
    for k in ("published", "updated", "created", "pubDate"):
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


def _extract_volume(entry):
    """Google Trends RSS expõe ht:approx_traffic com volume aproximado de busca."""
    # feedparser flatten o namespace ht: como ht_approx_traffic
    for key in ("ht_approx_traffic", "approx_traffic"):
        v = entry.get(key)
        if v:
            return str(v).strip()
    return None


def _extract_news_headlines(entry, max_headlines=3):
    """Cada trend tem N news items aninhados no XML. Pega títulos + URLs."""
    headlines = []
    # feedparser expõe ht:news_item como uma lista de namespaces
    items = entry.get("ht_news_item", [])
    if isinstance(items, list):
        for it in items[:max_headlines]:
            if isinstance(it, dict):
                t = it.get("ht_news_item_title") or it.get("title")
                u = it.get("ht_news_item_url") or it.get("url")
                if t and u:
                    headlines.append({"title": str(t), "url": str(u)})
    return headlines


def fetch(country="BR", max_items=15, max_age_hours=48):
    """
    Busca trending searches do dia pra um país.

    Args:
        country: BR, GLOBAL, US, etc
        max_items: quantos trends retornar (default 15)
        max_age_hours: idade máxima do trend (default 48h)

    Returns:
        Lista de trends como notícias. Cada trend gera 1 item com a
        primeira news headline relacionada (mais cobertura jornalística).
    """
    if not feedparser:
        return []

    geo = GEO_MAP.get(country, "US")
    lang = LANG_MAP.get(country, "en")
    url = f"https://trends.google.com/trends/trendingsearches/daily/rss?geo={geo}"

    try:
        d = feedparser.parse(url)
    except Exception as e:
        log.warning(f"google_trends: erro parse geo={geo}: {e}")
        return []

    if not d.entries:
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    out = []
    for e in d.entries[:max_items * 2]:
        term = (e.get("title") or "").strip()
        if not term:
            continue

        published_iso = _parse_date(e)
        if published_iso:
            try:
                pub_dt = datetime.fromisoformat(published_iso.replace("Z", "+00:00"))
                if pub_dt < cutoff:
                    continue
            except Exception:
                pass

        volume = _extract_volume(e)
        headlines = _extract_news_headlines(e)

        # Constrói o item: usa a primeira news headline como manchete (mais
        # útil que o termo cru "Botafogo"). Se não tiver headlines, usa o
        # próprio termo + link do Google Trends explore.
        if headlines:
            primary = headlines[0]
            title = primary["title"]
            link = primary["url"]
        else:
            title = f"Em alta: {term}"
            link = f"https://trends.google.com/trends/explore?q={term.replace(' ', '+')}&geo={geo}"

        # Summary: lista os outros termos buscados + volume
        bits = []
        if volume:
            bits.append(f"~{volume} buscas")
        if len(headlines) > 1:
            others = [h["title"] for h in headlines[1:3]]
            if others:
                bits.append("também: " + "; ".join(others)[:160])
        summary = " · ".join(bits)[:280]

        out.append({
            "title": title,
            "link": link,
            "source": "📊 Google Trends",
            "published_at": published_iso,
            "lang": lang,
            "summary": summary,
            "trend_term": term,  # extra: termo original buscado
            "approx_traffic": volume,
        })
        if len(out) >= max_items:
            break

    return out
