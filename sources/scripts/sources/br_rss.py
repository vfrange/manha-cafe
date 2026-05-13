"""
Fonte: RSS direto de veículos brasileiros, organizados por categoria.
Cada categoria agrupa feeds de múltiplas fontes pra dar redundância e qualidade.
"""
import re
import feedparser


# Mapeamento categoria → lista de feeds
BR_FEEDS = {
    "geral": [
        "https://g1.globo.com/rss/g1/",
        "https://feeds.folha.uol.com.br/folha/topofthehour/rss091.xml",
        "https://www.cnnbrasil.com.br/feed/",
        "https://rss.uol.com.br/feed/noticias.xml",
    ],
    "economia": [
        "https://g1.globo.com/rss/g1/economia/",
        "https://feeds.folha.uol.com.br/mercado/rss091.xml",
        "https://www.infomoney.com.br/feed/",
        "https://www.cnnbrasil.com.br/economia/feed/",
        "https://neofeed.com.br/feed/",
        "https://exame.com/feed/",
    ],
    "politica": [
        "https://g1.globo.com/rss/g1/politica/",
        "https://feeds.folha.uol.com.br/poder/rss091.xml",
        "https://www.cnnbrasil.com.br/politica/feed/",
    ],
    "tecnologia": [
        "https://g1.globo.com/rss/g1/tecnologia/",
        "https://feeds.folha.uol.com.br/tec/rss091.xml",
        "https://olhardigital.com.br/feed/",
        "https://canaltech.com.br/rss/",
    ],
    "mundo": [
        "https://g1.globo.com/rss/g1/mundo/",
        "https://feeds.folha.uol.com.br/mundo/rss091.xml",
        "https://www.cnnbrasil.com.br/internacional/feed/",
    ],
    "esportes": [
        "https://ge.globo.com/rss/ge/",
        "https://feeds.folha.uol.com.br/esporte/rss091.xml",
    ],
    "negocios": [
        "https://neofeed.com.br/feed/",
        "https://exame.com/feed/",
        "https://www.infomoney.com.br/mercados/feed/",
    ],
}

# Heurística pra detectar categoria a partir do query
CATEGORY_KEYWORDS = {
    "economia":   ["econom","financ","bolsa","selic","inflac","pib","dólar","mercado","banco","ipo"],
    "politica":   ["polít","congresso","governo","presidente","ministr","stf","eleic","lula","senado"],
    "tecnologia": ["tech","tecno","ia ","inteligência artificial","software","app","startup","google","apple","microsoft","openai"],
    "mundo":      ["geopol","internacional","guerra","mundial","china","russia","ucrânia","oriente médio"],
    "esportes":   ["futebol","esporte","copa","brasileir","libertadores","olímp"],
    "negocios":   ["m&a","fus","aquisic","empresa","ceo","negócio","food service","varejo","ifood","b2b"],
}


def detect_category(query):
    q = query.lower()
    for cat, kws in CATEGORY_KEYWORDS.items():
        if any(kw in q for kw in kws):
            return cat
    return "geral"


def _keywords(query):
    q = query.lower()
    stops = {"de","do","da","e","a","o","em","no","na","para","com","os","as",
             "brasil","brazil","brasileiro","mundo","global"}
    return [t for t in re.findall(r"\w+", q) if len(t) > 2 and t not in stops]


def fetch(query, category=None, max_items=8, max_per_feed=4):
    """
    Busca em feeds RSS BR pela categoria detectada,
    filtra entradas que casam com keywords do query.
    """
    if not category:
        category = detect_category(query)
    feeds = BR_FEEDS.get(category, BR_FEEDS["geral"])

    kws = _keywords(query)
    results = []
    for feed_url in feeds:
        try:
            f = feedparser.parse(feed_url, request_headers={"User-Agent": "manha-news/1.0"})
        except Exception:
            continue
        fonte = (f.feed.get("title") or "RSS").strip()
        # nome curto da fonte
        if "globo" in feed_url.lower(): fonte = "G1"
        elif "folha" in feed_url.lower(): fonte = "Folha"
        elif "cnnbrasil" in feed_url.lower(): fonte = "CNN Brasil"
        elif "uol" in feed_url.lower(): fonte = "UOL"
        elif "infomoney" in feed_url.lower(): fonte = "InfoMoney"
        elif "neofeed" in feed_url.lower(): fonte = "NeoFeed"
        elif "exame" in feed_url.lower(): fonte = "Exame"
        elif "olhardigital" in feed_url.lower(): fonte = "Olhar Digital"
        elif "canaltech" in feed_url.lower(): fonte = "Canaltech"

        count = 0
        for entry in f.entries:
            if count >= max_per_feed:
                break
            title = (entry.get("title") or "").strip()
            if not title:
                continue
            # se tem keywords no query, filtra; senão pega top do feed
            if kws:
                title_low = title.lower()
                summary_low = re.sub(r"<[^>]+>", "", entry.get("summary","")).lower()
                if not any(kw in title_low or kw in summary_low for kw in kws):
                    continue
            summary = re.sub(r"<[^>]+>", "", entry.get("summary",""))[:400].strip()
            results.append({
                "title": title,
                "link": entry.get("link",""),
                "source": fonte,
                "summary": summary,
                "origin": "br_rss",
            })
            count += 1
        if len(results) >= max_items:
            break
    return results[:max_items]
