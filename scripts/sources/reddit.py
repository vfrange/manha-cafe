"""Fonte: Reddit JSON — top posts do dia filtrados por keywords."""
import urllib.request
import json
import re


# Subreddits por categoria
SUBS_BY_CATEGORY = {
    "tech":      ["technology", "artificial", "OpenAI", "MachineLearning", "singularity"],
    "business":  ["business", "finance", "investing", "stocks", "Economics"],
    "world":     ["worldnews", "geopolitics", "europe"],
    "brazil":    ["brasil", "brasilivre"],
    "science":   ["science", "Futurology"],
    "crypto":    ["CryptoCurrency"],
    "us":        ["politics"],
}


def _http_json(url, timeout=8):
    req = urllib.request.Request(url, headers={"User-Agent": "manha-news/1.0 (newsletter)"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def _keywords(query):
    q = query.lower()
    stops = {"de","do","da","e","a","o","em","no","na","para","com","os","as",
             "the","of","and","a","an","in","on","for","with","to","is"}
    return [t for t in re.findall(r"\w+", q) if len(t) > 2 and t not in stops]


def fetch_subreddit(subreddit, limit=15, time_filter="day"):
    url = f"https://www.reddit.com/r/{subreddit}/top.json?limit={limit}&t={time_filter}"
    try:
        data = _http_json(url)
    except Exception:
        return []
    posts = []
    for child in data.get("data", {}).get("children", []):
        d = child.get("data", {})
        if d.get("stickied"):
            continue
        title = (d.get("title") or "").strip()
        if not title:
            continue
        url_post = d.get("url_overridden_by_dest") or f"https://reddit.com{d.get('permalink','')}"
        posts.append({
            "title": title,
            "link": url_post,
            "source": f"r/{subreddit}",
            "summary": f"↑ {d.get('ups',0):,} votos · {d.get('num_comments',0):,} coments",
            "origin": "reddit",
            "score": d.get("ups", 0),
        })
    return posts


def fetch(query, category=None, max_items=6):
    """
    Busca posts do Reddit relevantes pro tema.
    category opcional: 'tech', 'business', 'world', 'brazil', etc.
    """
    kws = _keywords(query)
    if not kws:
        return []

    # decide subreddits
    if category and category in SUBS_BY_CATEGORY:
        subs = SUBS_BY_CATEGORY[category]
    else:
        # heurística leve por palavras-chave
        q_low = query.lower()
        subs = []
        if any(w in q_low for w in ["ai","tech","gpt","artificial","intelig","software"]):
            subs += SUBS_BY_CATEGORY["tech"]
        if any(w in q_low for w in ["econom","financ","market","invest","stock","bolsa"]):
            subs += SUBS_BY_CATEGORY["business"]
        if any(w in q_low for w in ["brasil","brazil"]):
            subs += SUBS_BY_CATEGORY["brazil"]
        if any(w in q_low for w in ["geopol","world","global","russia","china","ukrain"]):
            subs += SUBS_BY_CATEGORY["world"]
        if not subs:
            subs = ["worldnews", "business"]
        subs = list(dict.fromkeys(subs))[:3]  # dedupe, máx 3

    results = []
    for sub in subs:
        posts = fetch_subreddit(sub, limit=15)
        for p in posts:
            title_low = p["title"].lower()
            if any(kw in title_low for kw in kws):
                results.append(p)
                if len(results) >= max_items:
                    break
        if len(results) >= max_items:
            break

    # ordena por score desc
    results.sort(key=lambda x: -x.get("score", 0))
    return results[:max_items]


def fetch_trending_general(max_items=15):
    """Trending geral via r/popular — pra trending alternativo."""
    return [
        {
            "termo": p["title"],
            "buscas": f"↑{p.get('score',0):,}",
            "pais": "reddit",
            "noticia_titulo": p["title"],
            "noticia_link": p["link"],
            "noticia_fonte": p["source"],
        }
        for p in fetch_subreddit("popular", limit=max_items)
    ]
