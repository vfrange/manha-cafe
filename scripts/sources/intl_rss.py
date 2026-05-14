"""
Fonte: RSS direto dos grandes veículos internacionais.
Cobertura plural ideológica e geográfica.
Só deve ser usado pra temas curados generalistas (economia, tech, etc),
NÃO pra temas customs específicos (que não terão match).
"""
import re
import feedparser

# Cada feed: (id, nome, url, idioma, viés_aproximado)
FEEDS = {
    # Anglófonos top
    "nyt_home":      ("New York Times",      "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml",                   "en", "centro-esquerda"),
    "nyt_world":     ("NYT World",           "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",                      "en", "centro-esquerda"),
    "nyt_business":  ("NYT Business",        "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",                   "en", "centro-esquerda"),
    "wapo_world":    ("Washington Post",     "https://feeds.washingtonpost.com/rss/world",                                  "en", "centro-esquerda"),
    "wsj_world":     ("Wall Street Journal", "https://feeds.a.dj.com/rss/RSSWorldNews.xml",                                 "en", "centro-direita"),
    "wsj_business":  ("WSJ Business",        "https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml",                             "en", "centro-direita"),
    "bbc_world":     ("BBC News",            "https://feeds.bbci.co.uk/news/world/rss.xml",                                 "en", "centro"),
    "bbc_business":  ("BBC Business",        "https://feeds.bbci.co.uk/news/business/rss.xml",                              "en", "centro"),
    "guardian_world":("The Guardian",        "https://www.theguardian.com/world/rss",                                       "en", "esquerda"),
    "ft_world":      ("Financial Times",     "https://www.ft.com/world?format=rss",                                         "en", "centro"),
    "bloomberg":     ("Bloomberg Markets",   "https://feeds.bloomberg.com/markets/news.rss",                                "en", "centro"),
    "economist":     ("The Economist",       "https://www.economist.com/the-world-this-week/rss.xml",                       "en", "centro-liberal"),
    "reuters_top":   ("Reuters Top",         "https://feeds.reuters.com/reuters/topNews",                                   "en", "centro"),
    # Europa não-anglófona
    "lemonde":       ("Le Monde",            "https://www.lemonde.fr/rss/une.xml",                                          "fr", "centro-esquerda"),
    "spiegel":       ("Der Spiegel",         "https://www.spiegel.de/international/index.rss",                              "en", "centro-esquerda"),  # int'l section é em inglês
    "elpais":        ("El País",             "https://feeds.elpais.com/mrss-s/pages/ep/site/elpais.com/portada",            "es", "centro-esquerda"),
}

# Quais feeds usar por tema (label do tema → lista de feed ids)
TOPIC_FEEDS = {
    "Economia":                ["nyt_business", "wsj_business", "bbc_business", "ft_world", "bloomberg", "economist", "spiegel", "elpais"],
    "Mercados financeiros":    ["wsj_business", "ft_world", "bloomberg", "economist"],
    "Negócios & M&A":          ["nyt_business", "wsj_business", "ft_world", "bloomberg", "economist"],
    "Tech & IA":               ["nyt_home", "wsj_world", "bbc_world", "guardian_world", "economist"],
    "Ciência & saúde":         ["nyt_world", "guardian_world", "bbc_world", "economist"],
    "Cultura & entretenimento":["nyt_home", "guardian_world", "lemonde", "elpais"],
    "Esportes":                ["bbc_world", "guardian_world", "lemonde"],
    "Política":                ["nyt_world", "wapo_world", "wsj_world", "bbc_world", "guardian_world", "ft_world", "lemonde", "spiegel", "elpais"],
    "Geopolítica":             ["nyt_world", "wapo_world", "bbc_world", "guardian_world", "ft_world", "lemonde", "spiegel", "elpais", "economist", "reuters_top"],
    "Auto & mobilidade":       ["nyt_business", "wsj_business", "bbc_business"],
    "Trabalho & carreira":     ["nyt_business", "wsj_business", "bbc_business", "ft_world"],
    "Consumo & marcas":        ["nyt_business", "wsj_business", "bbc_business"],
    "Food service & varejo":   ["nyt_business", "wsj_business", "bbc_business"],
    "Imobiliário":             ["nyt_business", "wsj_business", "ft_world", "bloomberg"],
    "Sustentabilidade & ESG":  ["nyt_world", "guardian_world", "bbc_world", "ft_world", "economist"],
    "Educação":                ["nyt_home", "guardian_world", "economist"],
    # Curiosidades, customs (Hamburguerias, Tributário etc) → não usa intl rss
}


def fetch_for_topic(topic_label, max_per_feed=3):
    """
    Busca matérias dos feeds internacionais relevantes pro tema.
    Retorna lista no formato compatível com o resto do pipeline.
    """
    feed_ids = TOPIC_FEEDS.get(topic_label, [])
    if not feed_ids:
        return []

    out = []
    for fid in feed_ids:
        info = FEEDS.get(fid)
        if not info:
            continue
        name, url, lang, bias = info
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:max_per_feed]:
                title = entry.get("title", "").strip()
                if not title:
                    continue
                summary = re.sub(r"<[^>]+>", "", entry.get("summary", ""))[:400]
                out.append({
                    "title": title,
                    "link": entry.get("link", ""),
                    "source": name,
                    "summary": summary.strip(),
                    "origin": f"intl_rss:{fid}",
                    "lang": lang,
                    "bias_hint": bias,
                })
        except Exception:
            continue
    return out
