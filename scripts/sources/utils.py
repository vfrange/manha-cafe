"""
URL validation + OG image extraction. Usado por prepare_daily.py e daily_digest.py.

Estratégias:
- HEAD check rápido pra eliminar URLs 4xx/5xx antes de mandar pro Claude
- Extração de og:image via fetch HTML (fallback: twitter:image, primeira img grande)
- Paralelo (asyncio) com timeout 3s — não bloqueia o prepare
- Cache em memória durante a run (evita refetch da mesma URL)
- Blacklist de URLs genéricas (banners de portal, share defaults, placeholders)
"""
import re
import asyncio
import logging
from typing import Optional, Dict, List
from urllib.parse import urlparse, urljoin

import aiohttp

log = logging.getLogger(__name__)

# Cache em memória — vive só durante a execução do script
_url_cache: Dict[str, Dict] = {}
_image_cache: Dict[str, Optional[str]] = {}

# Limites
HEAD_TIMEOUT = 3.0           # 3s pra HEAD check
GET_TIMEOUT = 8.0            # 8s pra fetch HTML (mais cobertura de publishers lentos)
MAX_CONCURRENT = 10          # max 10 conexões simultâneas
# User-Agent FIXO pra HEAD checks (sites são tolerantes em HEAD).
USER_AGENT = "Mozilla/5.0 (compatible; RecorteBot/1.0; +https://recorte.news)"


# ============================================================================
# BLACKLIST DE IMAGENS GENÉRICAS — banners de portal que não são da matéria
# ============================================================================
# Quando matéria não tem og:image próprio, o publisher serve og:image PADRÃO
# do site inteiro (banner "videos em alta no G1", share default, etc).
# Esses padrões aparecem na URL da imagem — detectamos e descartamos.
GENERIC_IMAGE_PATTERNS = [
    # G1 / Globo (banner "vídeos em alta")
    r'videos[-_]?em[-_]?alta',
    r'g1[-_]?logo',
    r'globo[-_]?logo',
    # G1 og:image padrão "preview-share-min.png" servido em s3.glbimg.com
    # quando matéria não tem og:image próprio. Bug detectado 21/05/2026.
    r'preview[-_]?share[-_]?min',
    # TradingView placeholder "news-story.jpg" em static/images/illustrations
    # servido como og:image quando a matéria não tem capa. Bug 21/05/2026.
    r'news[-_]?story\.(?:jpg|jpeg|png|webp)',
    r'/illustrations/news[-_]?story',
    # Padrões universais de fallback/share default
    r'/share[-_]?default',
    r'/og[-_]?default',
    r'/og[-_]?image[-_]?default',
    r'/default[-_]?share',
    r'/social[-_]?default',
    r'/placeholder',
    r'placeholder[-_]',
    r'/banner[-_]?share',
    r'site[-_]?banner',
    r'/home[-_]?image',
    r'fallback[-_]?image',
    r'no[-_]?image',
    r'noimage\b',
    r'logo[-_]?share',
    r'og[-_]share',
    r'twitter[-_]share',
    r'social[-_]preview',
    # Imagens muito pequenas pelo nome (favicon-like)
    r'_\d{1,2}x\d{1,2}\.',  # _16x16, _32x32
    # Logos / brand assets
    r'/logo\.(?:png|jpg|jpeg|webp|svg)',
    r'[-_]logo\.(?:png|jpg|jpeg|webp|svg)',
    r'/brand[-_]',
    # Padrões de homepage/cover
    r'/cover[-_]?default',
    r'homepage[-_]?cover',
]


def _is_generic_image(url: str) -> bool:
    """Detecta imagens genéricas/placeholder que não são da matéria específica.

    Retorna True se a URL bate em algum padrão de banner/share-default conhecido.
    """
    if not url or not isinstance(url, str):
        return False
    url_lower = url.lower()
    return any(re.search(pat, url_lower) for pat in GENERIC_IMAGE_PATTERNS)


# Regex pra encontrar og:image / twitter:image em HTML
OG_IMAGE_RE = re.compile(
    r'<meta\s+(?:property|name)\s*=\s*[\'"](?:og:image|twitter:image)(?::\w+)?[\'"]\s+content\s*=\s*[\'"]([^\'"]+)[\'"]',
    re.IGNORECASE,
)
OG_IMAGE_REVERSED_RE = re.compile(
    r'<meta\s+content\s*=\s*[\'"]([^\'"]+)[\'"]\s+(?:property|name)\s*=\s*[\'"](?:og:image|twitter:image)(?::\w+)?[\'"]',
    re.IGNORECASE,
)
# Fallback 1: <meta itemprop="image" content="..."> (Schema.org)
ITEMPROP_IMAGE_RE = re.compile(
    r'<meta\s+itemprop\s*=\s*[\'"]image[\'"]\s+content\s*=\s*[\'"]([^\'"]+)[\'"]',
    re.IGNORECASE,
)
# Fallback 2: <link rel="image_src" href="..."> (HTML antigo, Facebook-style)
LINK_IMAGE_SRC_RE = re.compile(
    r'<link\s+rel\s*=\s*[\'"]image_src[\'"]\s+href\s*=\s*[\'"]([^\'"]+)[\'"]',
    re.IGNORECASE,
)


def is_valid_url(url: str) -> bool:
    """Sanity check básico — URL parseable, tem host, é http(s)."""
    if not url or not isinstance(url, str):
        return False
    try:
        p = urlparse(url)
        return p.scheme in ("http", "https") and bool(p.netloc) and "." in p.netloc
    except Exception:
        return False


async def _check_url(session: aiohttp.ClientSession, url: str) -> Dict:
    """HEAD check de uma URL."""
    if url in _url_cache:
        return _url_cache[url]

    result = {"url": url, "status": None, "ok": False, "final_url": url}

    if not is_valid_url(url):
        result["status"] = 0
        _url_cache[url] = result
        return result

    try:
        async with session.head(
            url, allow_redirects=True,
            timeout=aiohttp.ClientTimeout(total=HEAD_TIMEOUT),
            headers={"User-Agent": USER_AGENT},
        ) as resp:
            result["status"] = resp.status
            result["ok"] = 200 <= resp.status < 400
            result["final_url"] = str(resp.url)
    except asyncio.TimeoutError:
        result["status"] = -1
        result["ok"] = True
    except aiohttp.ClientError as e:
        log.debug(f"  ⚠ HEAD falhou {url}: {e}")
        result["status"] = -2
        result["ok"] = False
    except Exception as e:
        log.debug(f"  ⚠ HEAD exception {url}: {e}")
        result["status"] = -3
        result["ok"] = True

    _url_cache[url] = result
    return result


async def _check_urls_batch(urls: List[str]) -> Dict[str, Dict]:
    if not urls:
        return {}
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [_check_url(session, url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    out = {}
    for url, res in zip(urls, results):
        if isinstance(res, Exception):
            out[url] = {"url": url, "status": -99, "ok": True, "final_url": url}
        else:
            out[url] = res
    return out


def filter_valid_urls(news_items: List[Dict], url_key: str = "link") -> List[Dict]:
    if not news_items:
        return news_items
    urls = [item.get(url_key, "") for item in news_items]
    urls = [u for u in urls if u]
    if not urls:
        return []
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            results = loop.run_until_complete(_check_urls_batch(urls))
        finally:
            loop.close()
    except Exception as e:
        log.warning(f"  ⚠ URL validation falhou em lote: {e}")
        return news_items

    valid = []
    for item in news_items:
        url = item.get(url_key, "")
        r = results.get(url)
        if r and r.get("ok"):
            if r.get("final_url") and r["final_url"] != url:
                item[url_key] = r["final_url"]
            valid.append(item)
        else:
            log.info(f"  ✗ URL descartada: {url} (status={r.get('status') if r else 'unknown'})")
    return valid


async def _fetch_og_image(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    """Faz GET no URL e extrai og:image. Filtra imagens genéricas via blacklist."""
    if url in _image_cache:
        return _image_cache[url]

    if not is_valid_url(url):
        _image_cache[url] = None
        return None

    try:
        async with session.get(
            url, allow_redirects=True,
            timeout=aiohttp.ClientTimeout(total=GET_TIMEOUT),
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            },
        ) as resp:
            if resp.status >= 400:
                _image_cache[url] = None
                return None
            content_type = resp.headers.get("Content-Type", "").lower()
            if "html" not in content_type:
                _image_cache[url] = None
                return None
            chunk = await resp.content.read(80 * 1024)
            html = chunk.decode("utf-8", errors="ignore")
            final_url = str(resp.url)
    except Exception as e:
        log.debug(f"  ⚠ fetch og:image falhou {url}: {e}")
        _image_cache[url] = None
        return None

    match = (
        OG_IMAGE_RE.search(html) or
        OG_IMAGE_REVERSED_RE.search(html) or
        ITEMPROP_IMAGE_RE.search(html) or
        LINK_IMAGE_SRC_RE.search(html)
    )
    if not match:
        _image_cache[url] = None
        return None

    img_url = match.group(1).strip()
    if img_url.startswith("//"):
        img_url = "https:" + img_url
    elif img_url.startswith("/"):
        img_url = urljoin(final_url, img_url)
    elif not img_url.startswith("http"):
        img_url = urljoin(final_url, img_url)

    if not is_valid_url(img_url):
        _image_cache[url] = None
        return None

    # BLACKLIST: descarta imagem genérica (banner do portal, share default, etc).
    # Sem imagem é melhor que imagem errada — o template colapsa silenciosamente.
    if _is_generic_image(img_url):
        log.info(f"  🚫 imagem genérica descartada: {img_url[:80]}")
        _image_cache[url] = None
        return None

    _image_cache[url] = img_url
    return img_url


async def _fetch_images_batch(urls: List[str]) -> Dict[str, Optional[str]]:
    if not urls:
        return {}
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [_fetch_og_image(session, url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    out = {}
    for url, res in zip(urls, results):
        if isinstance(res, Exception):
            out[url] = None
        else:
            out[url] = res
    return out


def extract_images(urls: List[str]) -> Dict[str, Optional[str]]:
    if not urls:
        return {}
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(_fetch_images_batch(urls))
        finally:
            loop.close()
    except Exception as e:
        log.warning(f"  ⚠ image extraction falhou em lote: {e}")
        return {url: None for url in urls}


# ============ HELPERS PRA CHECK DE IMAGEM ============
async def _check_image_url(session: aiohttp.ClientSession, url: str) -> bool:
    """Verifica que a imagem responde 200, é image/* E não é genérica."""
    # Pre-check: se já está na blacklist por URL, descarta sem fazer request
    if _is_generic_image(url):
        return False
    try:
        async with session.head(
            url, allow_redirects=True,
            timeout=aiohttp.ClientTimeout(total=HEAD_TIMEOUT),
            headers={"User-Agent": USER_AGENT},
        ) as resp:
            if not (200 <= resp.status < 400):
                return False
            ct = resp.headers.get("Content-Type", "").lower()
            return ct.startswith("image/")
    except Exception:
        return False


def validate_images(image_urls: List[str]) -> Dict[str, bool]:
    if not image_urls:
        return {}
    image_urls = [u for u in image_urls if u]
    if not image_urls:
        return {}

    async def _run():
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [_check_image_url(session, u) for u in image_urls]
            return await asyncio.gather(*tasks, return_exceptions=True)

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            results = loop.run_until_complete(_run())
        finally:
            loop.close()
    except Exception as e:
        log.warning(f"  ⚠ image validation falhou: {e}")
        return {u: False for u in image_urls}

    return {u: (r if isinstance(r, bool) else False) for u, r in zip(image_urls, results)}


# ============ HELPER: EXTRAÇÃO DE IMG DE FEEDPARSER ENTRY (estratégia A) ============
def extract_img_from_entry(entry) -> Optional[str]:
    """Extrai URL da imagem de uma entry do feedparser. Aplica blacklist genérica."""
    if not entry:
        return None

    def _check_and_return(url):
        """Helper: valida URL e checa blacklist antes de retornar."""
        if url and is_valid_url(url) and not _is_generic_image(url):
            return url
        return None

    # 1. media_content (Google News, Yahoo)
    media_content = entry.get("media_content", []) or []
    for m in media_content:
        url = m.get("url") if isinstance(m, dict) else None
        result = _check_and_return(url)
        if result:
            return result

    # 2. media_thumbnail (alguns RSS BR)
    media_thumbnail = entry.get("media_thumbnail", []) or []
    for m in media_thumbnail:
        url = m.get("url") if isinstance(m, dict) else None
        result = _check_and_return(url)
        if result:
            return result

    # 3. enclosures (RSS clássico)
    enclosures = entry.get("enclosures", []) or []
    for enc in enclosures:
        url = enc.get("url") if isinstance(enc, dict) else (enc.get("href") if isinstance(enc, dict) else None)
        ctype = (enc.get("type", "") if isinstance(enc, dict) else "").lower()
        if url and (ctype.startswith("image/") or any(url.lower().endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".webp", ".gif"])):
            result = _check_and_return(url)
            if result:
                return result

    # 4. links com rel=enclosure e type image
    links = entry.get("links", []) or []
    for link in links:
        if not isinstance(link, dict):
            continue
        rel = (link.get("rel", "") or "").lower()
        ctype = (link.get("type", "") or "").lower()
        href = link.get("href", "")
        if rel == "enclosure" and ctype.startswith("image/") and href:
            result = _check_and_return(href)
            if result:
                return result

    # 5. Fallback: <img src="..."> no summary/content
    html_blob = entry.get("summary", "") or ""
    if not html_blob:
        content = entry.get("content", []) or []
        if content and isinstance(content, list) and isinstance(content[0], dict):
            html_blob = content[0].get("value", "")
    if html_blob:
        m = re.search(r'<img[^>]+src=[\'"]([^\'"]+)[\'"]', html_blob, re.IGNORECASE)
        if m:
            img_url = m.group(1)
            if img_url.startswith("//"):
                img_url = "https:" + img_url
            result = _check_and_return(img_url)
            if result:
                return result

    return None
