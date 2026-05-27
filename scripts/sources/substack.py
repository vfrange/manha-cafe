"""
sources/substack.py — agregador de newsletters Substack high-signal

Puxa posts recentes de uma lista hardcoded de Substacks (override via env
SUBSTACK_FEEDS=url1,url2). Cada Substack expõe RSS público em /feed.

Uso típico: alimentar a seção "antes de todos" — coisas que estão sendo
discutidas em newsletters anglo, mas ainda não viraram pauta no Brasil.

PADRÃO DE RETORNO (compatível com fetch_all_sources):
    [{title, link, source, published_at (ISO UTC), lang, summary}, ...]
"""

import os
import logging
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.utils import parsedate_to_datetime

try:
    import feedparser
except ImportError:
    feedparser = None

log = logging.getLogger(__name__)

# Lista default — high-signal anglo. Substack tem RSS público em /feed em
# todos eles. Override via env SUBSTACK_FEEDS (csv).
DEFAULT_FEEDS = [
    "https://www.platformer.news/feed",            # Casey Newton — social media, big tech
    "https://www.lennysnewsletter.com/feed",       # Lenny Rachitsky — product mgmt
    "https://www.astralcodexten.com/feed",         # Scott Alexander — rationalism, policy
    "https://www.garbageday.email/feed",           # Ryan Broderick — internet culture
    "https://restofworld.org/feed/latest/",        # Rest of World — global south tech
    "https://every.to/feed.xml",                   # Every — business/tech essays
    "https://www.notboring.co/feed",               # Packy McCormick — biz/tech
    "https://stratechery.com/feed/",               # Ben Thompson — strategy (paywall, mas headline sai)
]


def _parse_date(entry):
    """Tenta extrair data ISO UTC do entry RSS."""
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


def _fetch_one(feed_url, max_items=3, max_age_hours=72):
    """Pega N últimos posts de UM feed Substack, com filtro de idade."""
    if not feedparser:
        return []
    try:
        d = feedparser.parse(feed_url)
    except Exception as e:
        log.warning(f"substack: erro parse {feed_url}: {e}")
        return []
    if not d.entries:
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    feed_title = (d.feed.get("title") or feed_url.split("//")[-1].split("/")[0]).strip()

    out = []
    for e in d.entries[:max_items * 2]:  # busca o dobro pra filtrar por idade
        title = (e.get("title") or "").strip()
        link = (e.get("link") or "").strip()
        if not title or not link:
            continue

        published_iso = _parse_date(e)
        if published_iso:
            try:
                pub_dt = datetime.fromisoformat(published_iso.replace("Z", "+00:00"))
                if pub_dt < cutoff:
                    continue  # mais velho que a janela
            except Exception:
                pass

        # Summary — primeiros 280 chars de description/summary
        summary = (e.get("summary") or e.get("description") or "").strip()
        # remove HTML básico
        import re
        summary = re.sub(r"<[^>]+>", " ", summary)
        summary = re.sub(r"\s+", " ", summary)[:280].strip()

        out.append({
            "title": title,
            "link": link,
            "source": f"Substack · {feed_title}",
            "published_at": published_iso,
            "lang": "en",  # quase todos são em inglês
            "summary": summary,
        })
        if len(out) >= max_items:
            break
    return out


def fetch(max_items_per_feed=2, max_age_hours=72, feeds=None):
    """
    Busca posts recentes de múltiplos Substacks em paralelo.

    Args:
        max_items_per_feed: quantos posts por feed (default 2)
        max_age_hours: idade máxima (default 72h = 3 dias)
        feeds: lista de URLs custom; se None usa DEFAULT_FEEDS ou env SUBSTACK_FEEDS

    Returns:
        Lista combinada de todos os feeds (dedupe por link).
    """
    if feeds is None:
        env_feeds = os.environ.get("SUBSTACK_FEEDS", "").strip()
        if env_feeds:
            feeds = [f.strip() for f in env_feeds.split(",") if f.strip()]
        else:
            feeds = DEFAULT_FEEDS

    if not feeds or not feedparser:
        return []

    all_items = []
    with ThreadPoolExecutor(max_workers=min(8, len(feeds))) as ex:
        futures = {ex.submit(_fetch_one, f, max_items_per_feed, max_age_hours): f for f in feeds}
        try:
            for fut in as_completed(futures, timeout=15):
                try:
                    items = fut.result()
                    all_items.extend(items)
                except Exception as e:
                    log.warning(f"substack fetch erro feed={futures[fut]}: {e}")
        except TimeoutError:
            log.warning(f"substack: timeout 15s — usando {len(all_items)} items dos feeds que responderam")

    # Dedup por link
    seen = set()
    deduped = []
    for it in all_items:
        if it["link"] not in seen:
            seen.add(it["link"])
            deduped.append(it)

    # Sort por data desc (mais recentes primeiro)
    deduped.sort(key=lambda x: x.get("published_at") or "", reverse=True)
    return deduped
