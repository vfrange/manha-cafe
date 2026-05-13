"""Fonte: Hacker News API — top stories filtradas por keywords do tema."""
import urllib.request
import json
import re


HN_TOPSTORIES = "https://hacker-news.firebaseio.com/v0/topstories.json"
HN_ITEM = "https://hacker-news.firebaseio.com/v0/item/{id}.json"


def _http_json(url, timeout=8):
    req = urllib.request.Request(url, headers={"User-Agent": "manha-news/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def _keywords(query):
    """Extrai keywords mínimas do query."""
    q = query.lower()
    # remove stopwords PT/EN básicas
    stops = {"de","do","da","e","a","o","em","no","na","para","com","os","as",
             "the","of","and","a","an","in","on","for","with","to","is"}
    tokens = [t for t in re.findall(r"\w+", q) if len(t) > 2 and t not in stops]
    return tokens


def fetch(query, max_items=8, scan_top=80):
    """
    Pega top N stories do HN, filtra as que casam com keywords do query.
    Bom pra temas de tech/AI/business.
    """
    try:
        top_ids = _http_json(HN_TOPSTORIES)[:scan_top]
    except Exception:
        return []

    kws = _keywords(query)
    if not kws:
        return []

    results = []
    for sid in top_ids:
        try:
            item = _http_json(HN_ITEM.format(id=sid), timeout=4)
        except Exception:
            continue
        if not item or item.get("type") != "story":
            continue
        title = (item.get("title") or "").strip()
        if not title:
            continue
        title_low = title.lower()
        if not any(kw in title_low for kw in kws):
            continue
        url = item.get("url") or f"https://news.ycombinator.com/item?id={sid}"
        results.append({
            "title": title,
            "link": url,
            "source": "Hacker News",
            "summary": f"↑ {item.get('score', 0)} pontos · {item.get('descendants', 0)} comentários",
            "origin": "hacker_news",
            "score": item.get("score", 0),
        })
        if len(results) >= max_items:
            break
    return results


def fetch_top_general(max_items=15):
    """Top stories sem filtro — usado em trending tech."""
    try:
        top_ids = _http_json(HN_TOPSTORIES)[:max_items]
    except Exception:
        return []
    results = []
    for sid in top_ids:
        try:
            item = _http_json(HN_ITEM.format(id=sid), timeout=4)
        except Exception:
            continue
        if not item or item.get("type") != "story":
            continue
        title = (item.get("title") or "").strip()
        if not title:
            continue
        url = item.get("url") or f"https://news.ycombinator.com/item?id={sid}"
        results.append({
            "termo": title,
            "buscas": f"↑{item.get('score',0)}",
            "pais": "HN",
            "noticia_titulo": title,
            "noticia_link": url,
            "noticia_fonte": "Hacker News",
        })
    return results
