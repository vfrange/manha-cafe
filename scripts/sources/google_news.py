"""Fonte: Google News RSS — busca por keyword com filtro de país."""
import re
import feedparser
from urllib.parse import quote_plus

COUNTRY_LOCALES = {
    "BR": {"hl": "pt-BR", "gl": "BR", "ceid": "BR:pt-419"},
    "US": {"hl": "en-US", "gl": "US", "ceid": "US:en"},
    "GB": {"hl": "en-GB", "gl": "GB", "ceid": "GB:en"},
    "FR": {"hl": "fr",    "gl": "FR", "ceid": "FR:fr"},
    "DE": {"hl": "de",    "gl": "DE", "ceid": "DE:de"},
    "ES": {"hl": "es",    "gl": "ES", "ceid": "ES:es"},
    "PT": {"hl": "pt-PT", "gl": "PT", "ceid": "PT:pt-150"},
    "IT": {"hl": "it",    "gl": "IT", "ceid": "IT:it"},
    "NL": {"hl": "nl",    "gl": "NL", "ceid": "NL:nl"},
    "CH": {"hl": "de",    "gl": "CH", "ceid": "CH:de"},
    "SE": {"hl": "sv",    "gl": "SE", "ceid": "SE:sv"},
    "JP": {"hl": "ja",    "gl": "JP", "ceid": "JP:ja"},
    "KR": {"hl": "ko",    "gl": "KR", "ceid": "KR:ko"},
    "CN": {"hl": "zh-CN", "gl": "CN", "ceid": "CN:zh-Hans"},
    "IN": {"hl": "en-IN", "gl": "IN", "ceid": "IN:en"},
    "AE": {"hl": "ar",    "gl": "AE", "ceid": "AE:ar"},
    "IL": {"hl": "he",    "gl": "IL", "ceid": "IL:he"},
    "MX": {"hl": "es-419","gl": "MX", "ceid": "MX:es-419"},
    "AR": {"hl": "es-419","gl": "AR", "ceid": "AR:es-419"},
    "CL": {"hl": "es-419","gl": "CL", "ceid": "CL:es-419"},
    "CO": {"hl": "es-419","gl": "CO", "ceid": "CO:es-419"},
    "CA": {"hl": "en-CA", "gl": "CA", "ceid": "CA:en"},
    "AU": {"hl": "en-AU", "gl": "AU", "ceid": "AU:en"},
    "ZA": {"hl": "en",    "gl": "ZA", "ceid": "ZA:en"},
}


def fetch(query, country="BR", max_items=8):
    if country == "GLOBAL" or country not in COUNTRY_LOCALES:
        url = f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=en&gl=US&ceid=US:en"
    else:
        loc = COUNTRY_LOCALES[country]
        url = f"https://news.google.com/rss/search?q={quote_plus(query)}&hl={loc['hl']}&gl={loc['gl']}&ceid={loc['ceid']}"

    feed = feedparser.parse(url)
    items = []
    for entry in feed.entries[:max_items]:
        title = entry.title
        source = ""
        if " - " in title:
            parts = title.rsplit(" - ", 1)
            title, source = parts[0], parts[1]
        summary = re.sub(r"<[^>]+>", "", entry.get("summary", ""))[:400]
        items.append({
            "title": title.strip(),
            "link": entry.link,
            "source": source.strip() or "Google News",
            "summary": summary.strip(),
            "origin": "google_news",
        })
    return items


def fetch_trends(country="BR", max_items=20):
    """Google Trends Daily por país."""
    if country == "GLOBAL":
        # agrega múltiplos países top
        all_trends = []
        for c in ["US", "GB", "BR", "DE", "JP", "FR", "IN"]:
            all_trends.extend(fetch_trends(c, max_items=8))
        seen = set()
        deduped = []
        for t in all_trends:
            k = t["termo"].lower().strip()
            if k not in seen:
                seen.add(k)
                deduped.append(t)
        return deduped[:30]

    if country not in COUNTRY_LOCALES:
        return []

    url = f"https://trends.google.com/trends/trendingsearches/daily/rss?geo={country}"
    feed = feedparser.parse(url)
    items = []
    for entry in feed.entries[:max_items]:
        buscas = ""
        for k in entry.keys():
            if "approx_traffic" in k:
                buscas = str(entry[k]).strip()
                break
        noticia_link = entry.get("link", "")
        noticia_titulo = ""
        noticia_fonte = ""
        for k in entry.keys():
            if "news_item_title" in k and not noticia_titulo:
                noticia_titulo = str(entry[k]).strip()
            if "news_item_url" in k and not noticia_link:
                noticia_link = str(entry[k]).strip()
            if "news_item_source" in k and not noticia_fonte:
                noticia_fonte = str(entry[k]).strip()
        items.append({
            "termo": entry.title.strip(),
            "buscas": buscas,
            "pais": country,
            "noticia_titulo": noticia_titulo,
            "noticia_link": noticia_link,
            "noticia_fonte": noticia_fonte,
        })
    return items
