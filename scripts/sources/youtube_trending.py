"""
Fonte: YouTube Trending via YouTube Data API v3.
Requer YOUTUBE_API_KEY (free, 10k requests/dia no Google Cloud).
Se a env var não estiver setada, retorna [] silenciosamente.
"""
import os
import requests

API_KEY = os.environ.get("YOUTUBE_API_KEY", "").strip()
API_URL = "https://www.googleapis.com/youtube/v3/videos"

# regionCode → code do YouTube
REGION_MAP = {
    "BR": "BR", "US": "US", "GB": "GB", "GLOBAL": "US",
}


def fetch_trending(country="BR", max_items=12):
    """
    Retorna os top vídeos em alta no YouTube por país.
    Compatível com o pipeline de trending:
    [{"termo": "...", "buscas": "X views", "pais": "BR", "noticia_titulo": "...", "noticia_link": "...", "noticia_fonte": "YouTube"}]
    """
    if not API_KEY:
        return []  # silencioso, evita quebrar quem não configurou

    region = REGION_MAP.get(country, "US")
    try:
        resp = requests.get(API_URL, params={
            "part": "snippet,statistics",
            "chart": "mostPopular",
            "regionCode": region,
            "maxResults": min(max_items, 25),
            "key": API_KEY,
        }, timeout=12)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return []

    items = []
    for video in data.get("items", []):
        sn = video.get("snippet", {})
        stats = video.get("statistics", {})
        title = sn.get("title", "").strip()
        if not title:
            continue
        channel = sn.get("channelTitle", "YouTube")
        video_id = video.get("id", "")
        views = stats.get("viewCount", "")
        views_fmt = _format_views(views)

        items.append({
            "termo": title[:140],
            "buscas": views_fmt,
            "pais": country,
            "noticia_titulo": title[:140],
            "noticia_link": f"https://www.youtube.com/watch?v={video_id}",
            "noticia_fonte": f"YouTube · {channel}",
            "lang": "pt" if country == "BR" else "en",
        })
    return items


def _format_views(n):
    """Formata número grande tipo '1.2M', '850k'."""
    try:
        n = int(n)
    except Exception:
        return ""
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M views"
    if n >= 1_000:
        return f"{n/1_000:.0f}k views"
    return f"{n} views"
