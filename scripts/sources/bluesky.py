"""
Fonte: Bluesky 'What's Hot' / 'Discover' feed (público, sem auth).
Pega posts virais que estão bombando na plataforma agora.
"""
import re
import requests

# Feed público "Discover" do Bluesky — alta circulação, mistura de assuntos
DISCOVER_FEED = "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.generator/whats-hot"
API_URL = "https://public.api.bsky.app/xrpc/app.bsky.feed.getFeed"

UA = "ManhaCafe/1.0 (newsletter)"


def fetch_trending(max_items=12):
    """
    Retorna posts em alta no Bluesky como items de trending.
    Formato compatível com o resto do pipeline:
    [{"termo": "...", "buscas": "", "pais": "GLOBAL", "noticia_titulo": "...", "noticia_link": "...", "noticia_fonte": "Bluesky"}]
    """
    try:
        resp = requests.get(
            API_URL,
            params={"feed": DISCOVER_FEED, "limit": max_items * 2},
            headers={"User-Agent": UA, "Accept": "application/json"},
            timeout=12
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return []

    items = []
    seen_text = set()
    for feed_item in data.get("feed", []):
        post = feed_item.get("post", {})
        record = post.get("record", {})
        text = record.get("text", "").strip()
        if not text or len(text) < 25:
            continue
        # primeira linha como "manchete" (pega o ponto interessante)
        first_line = text.split("\n", 1)[0][:140]
        key = first_line.lower()[:60]
        if key in seen_text:
            continue
        seen_text.add(key)

        author = post.get("author", {})
        handle = author.get("handle", "")
        uri = post.get("uri", "")
        # converte AT-URI pra URL web do bsky
        post_id = uri.rsplit("/", 1)[-1] if uri else ""
        web_url = f"https://bsky.app/profile/{handle}/post/{post_id}" if handle and post_id else ""

        # tenta extrair link externo do embed (mais valioso que o post nu)
        external_url = ""
        external_title = ""
        embed = record.get("embed", {}) or post.get("embed", {})
        if isinstance(embed, dict):
            ext = embed.get("external", {}) or embed.get("$type", "")
            if isinstance(ext, dict):
                external_url = ext.get("uri", "")
                external_title = ext.get("title", "")

        items.append({
            "termo": (external_title or first_line)[:140],
            "buscas": "",
            "pais": "GLOBAL",
            "noticia_titulo": first_line,
            "noticia_link": external_url or web_url,
            "noticia_fonte": "Bluesky",
            "lang": "en",
        })
        if len(items) >= max_items:
            break
    return items
