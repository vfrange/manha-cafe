"""
Camada de segurança do Recorte.
Três funções principais:
- is_safe_topic(query) — bloqueia cadastro de temas explicitamente perigosos
- is_safe_news(news_item) — filtra matéria bruta antes de mandar pro Claude (pre-filter)
- is_safe_curated(item)   — valida output do Claude (post-filter)
- SAFETY_INSTRUCTIONS     — texto pra injetar nos prompts do Claude
"""
import re

# ============================================================================
# A. KEYWORDS DE BLOQUEIO (regex case-insensitive)
# ============================================================================

# Temas que o cadastro/manage REJEITA explicitamente (operacional, não factual)
BLOCKED_TOPIC_PATTERNS = [
    # How-to de armas
    (r"\b(como\s+(?:fazer|construir|montar|fabricar)\s+(?:arma|fuzil|bomba|explosivo|granada))", "armas/explosivos how-to"),
    (r"\b(arma|fuzil|pistola|munição)\s+(?:caseir[ao]|artesanal|3d)", "arma caseira"),
    (r"\b(?:tutorial|guia|receita)\s+(?:de\s+)?(?:bomba|explosivo|c4)", "bomba/explosivo"),

    # How-to de drogas
    (r"\bcomo\s+(?:fazer|sintetizar|produzir|cozinhar)\s+(?:cocaína|crack|meta|fentanil|lsd|metanfetamin|heroína|mdma)", "síntese de drogas"),
    (r"\b(?:receita|tutorial|guia)\s+(?:de\s+)?(?:cocaína|crack|meta|fentanil|lsd|metanfetamin)", "receita de droga"),

    # Hacking malicioso
    (r"\bcomo\s+(?:hackear|invadir)\s+(?:conta|whatsapp|instagram|facebook|email|wifi|senha)", "invasão de conta"),
    (r"\b(?:tutorial|guia|curso)\s+(?:de\s+)?(?:hack(?:ing|er)|cracking|phishing|carding)", "hacking ofensivo"),

    # Fraude e falsificação
    (r"\bcomo\s+(?:falsificar|forjar|fraudar|burlar)\s+(?:cpf|rg|nota|cnh|passaporte|documento)", "falsificação"),
    (r"\b(?:gerador|tutorial)\s+(?:de\s+)?(?:cpf|cnpj|cartão)\s+(?:falso|fake)", "fraude documental"),

    # Dark web operacional
    (r"\bcomo\s+(?:acessar|usar|comprar\s+em|navegar\s+(?:n[ao])?)\s+(?:dark\s*web|deep\s*web|silk\s*road|tor)", "dark web how-to"),

    # Suicídio operacional (método)
    (r"\b(?:método|forma|como|jeito)\s+(?:de\s+|para\s+|pra\s+)?(?:suicídio|me\s+matar|se\s+matar|cometer\s+suicídio)", "suicídio método"),
    (r"\bcomo\s+(?:morrer|me\s+matar|se\s+matar)\b", "auto-extermínio"),

    # Pedofilia / abuso infantil — ZERO tolerância
    (r"\b(?:pedofilia|pornografia\s+infantil|child\s+porn|csam|lolic[ao]n)", "abuso infantil"),
    (r"\b(?:sex|sexo|nu[ad])\s+(?:com\s+)?(?:menor|criança|adolescente|infantil)", "sex menor"),

    # Discurso de ódio organizado
    (r"\b(?:supremacia|supremacist[ao]s?)\s+(?:branc[ao]|aria|racial)", "supremacismo"),
    (r"\b(?:neonaz|nazismo|hitler\s+tinha\s+razão|holocaust(?:o)?\s+(?:foi\s+)?(?:mentira|farsa))", "neonazismo/negacionismo"),
    (r"\b(?:expurgar|exterminar|aniquilar)\s+(?:negros|judeus|gays|lgbt|árabes|nordestinos)", "incitação ao genocídio"),

    # Doxxing
    (r"\b(?:endereço|cpf|telefone|cep|rua)\s+(?:de|d[ao])\s+\w+", "doxxing"),

    # Pornografia
    (r"\b(?:porno|pornô|pornografia|xvideos|onlyfans\s+vaza|nudes\s+vaza)", "pornografia"),
]

# Domínios bloqueados — fontes conhecidas de extremismo, pirataria, etc.
BLOCKED_DOMAINS = {
    # Adicionar conforme necessário; lista inicial conservadora
    "stormfront.org", "dailystormer.in", "8kun.top", "kiwifarms.net",
    "voat.co", "gab.com",  # extremismo
}

# ============================================================================
# B. PRE-FILTER — matérias brutas (vindas das fontes RSS/API antes do Claude)
# ============================================================================

PREFILTER_PATTERNS = [
    # Pornografia em manchetes
    (r"\b(?:porno|pornô|onlyfans|nudes\s+vaz)\b", "pornografia"),
    # Conteúdo sexual com menores
    (r"\b(?:abuso|estupro)\s+(?:de\s+)?(?:menor|criança|adolescente)", "abuso infantil (pode ser jornalismo mas é gatilho — bloqueia por padrão)"),
    # Discurso de ódio explícito
    (r"\b(?:morte\s+aos|matem\s+os|expurguem\s+os)\s+\w+", "incitação"),
]


def _matches_any(text: str, patterns) -> tuple:
    """Retorna (True, motivo) se algum pattern bate, senão (False, None)."""
    if not text:
        return (False, None)
    txt = text.lower()
    for pattern, reason in patterns:
        if re.search(pattern, txt, re.IGNORECASE):
            return (True, reason)
    return (False, None)


# ============================================================================
# C. FUNÇÕES PÚBLICAS
# ============================================================================

def is_safe_topic(query: str) -> tuple:
    """
    Valida tema digitado no cadastro/manage.
    Retorna (True, None) se OK, (False, motivo) se bloqueado.
    """
    if not query or not query.strip():
        return (True, None)
    blocked, reason = _matches_any(query, BLOCKED_TOPIC_PATTERNS)
    if blocked:
        return (False, f"Tema rejeitado por política de uso: {reason}. Recorte não fornece conteúdo operacional sobre temas sensíveis. Tente um tema mais amplo.")
    return (True, None)


def is_safe_news(news_item: dict) -> bool:
    """Pre-filter: remove matérias com sinais óbvios de conteúdo proibido."""
    text = " ".join([
        str(news_item.get("title", "")),
        str(news_item.get("manchete", "")),
        str(news_item.get("resumo", "")),
        str(news_item.get("description", "")),
        str(news_item.get("url", "")),
        str(news_item.get("link", "")),
    ])
    # Domínio
    link = (news_item.get("link") or news_item.get("url") or "").lower()
    for blocked in BLOCKED_DOMAINS:
        if blocked in link:
            return False
    # Conteúdo
    blocked, _ = _matches_any(text, PREFILTER_PATTERNS)
    return not blocked


def is_safe_curated(curated_item: dict) -> bool:
    """Post-filter: re-valida output do Claude antes de incluir no email."""
    text = " ".join([
        str(curated_item.get("manchete", "")),
        str(curated_item.get("resumo", "")),
        " ".join(curated_item.get("fatos_chave", []) or []),
    ])
    blocked, _ = _matches_any(text, PREFILTER_PATTERNS)
    return not blocked


# ============================================================================
# D. INSTRUÇÕES PRO CLAUDE (injetadas em prompts de curate_news / curate_trends)
# ============================================================================

SAFETY_INSTRUCTIONS = """
🛡️ **REGRAS NÃO-NEGOCIÁVEIS DE SEGURANÇA**:

Você DEVE excluir totalmente do output qualquer matéria que:

1. **Conteúdo operacional perigoso** — instruções, tutoriais, "como fazer" para:
   - Armas, explosivos, bombas
   - Síntese de drogas (cocaína, fentanil, meta, etc.)
   - Hacking ofensivo, invasão de contas, phishing, carding
   - Fraude documental, falsificação de CPF/CNH/passaporte
   - Acesso operacional à dark web, marketplaces ilegais
   - Métodos de suicídio ou automutilação

   (Notícias FACTUAIS sobre esses temas — ex: "PF apreende fuzis", "STF debate descriminalização" — podem entrar normalmente. O que NÃO entra é instrução operacional.)

2. **Conteúdo sexual ou exploração**:
   - Pornografia, conteúdo sexualmente explícito
   - QUALQUER menção sexual envolvendo menores de 18 anos
   - Vazamento de nudes ou imagens íntimas

3. **Discurso de ódio organizado**:
   - Supremacismo racial, neonazismo, apologia ao fascismo
   - Incitação a violência contra grupos (LGBT, negros, judeus, etc.)
   - Negacionismo histórico (Holocausto, ditaduras, escravidão)

4. **Apologia ao terror/milícia**:
   - Glorificação de atentados, atos terroristas
   - Defesa de milícias armadas, grupos paramilitares ilegais

5. **Doxxing / privacidade**:
   - Dados pessoais (endereço, CPF, telefone) de pessoas privadas
   - Fotos íntimas vazadas sem consentimento

6. **Desinformação médica grave**:
   - Anti-vacina ativa (contra consenso científico)
   - Curas falsas pra câncer, HIV, doenças crônicas

7. **Suicídio - tratamento especial**:
   - Pode mencionar fato (ex: "X cometeu suicídio") MAS:
   - NUNCA descrever método, local específico ou instruções
   - NUNCA glorificar

8. **Sinais financeiros**:
   - Recomendação direta de compra/venda de ativo específico
   - Pump-and-dump, "moeda X vai 100x"
   - (Análise de mercado é OK; recomendação direta NÃO)

⚠️ Se uma matéria viola qualquer um desses pontos, SIMPLESMENTE NÃO INCLUA NO OUTPUT.
Não tente "amenizar" ou "filtrar" o conteúdo — descarte completamente.
Substitua por outra matéria relevante das fontes disponíveis.

✅ Quando estiver em dúvida (matéria sobre tema sensível mas factual e jornalística): INCLUA, mas SEM detalhes operacionais. Ex: "PF apreende 3kg de cocaína em SP" SIM. "Como apreendedores identificaram a droga" detalhado a ponto de ensinar a esconder NÃO.
"""


# ============================================================================
# E. CLASSIFICAÇÃO DE VIÉS POLÍTICO (instrução pro Claude)
# ============================================================================

POLITICAL_BIAS_INSTRUCTIONS = """
🏛️ **CLASSIFICAÇÃO DE VIÉS** (apenas para temas: Política, Geopolítica, Governo, Eleições, Legislativo, STF, Congresso):

Para CADA notícia desses temas, adicione o campo `"pol_bias"` com UM destes 4 valores:

- **"factual"**: relato dos fatos sem viés perceptível (datas, números, decisões registradas, votos contabilizados)
- **"centro"**: análise equilibrada, traz múltiplas perspectivas, contexto neutro
- **"esq"**: matéria que enquadra eventos sob ótica progressista/de esquerda (foco em desigualdade, direitos sociais, crítica a Bolsonaro/direita, apoio a Lula/governo, etc.)
- **"dir"**: matéria que enquadra eventos sob ótica conservadora/de direita (foco em segurança, mercado, crítica a Lula/esquerda, apoio a Bolsonaro/oposição, etc.)

⚖️ Importante:
- Classifique a MATÉRIA (texto), não o veículo
- O mesmo veículo pode publicar texto factual de manhã e opinativo à tarde — julgue cada matéria
- Em caso de dúvida → "factual"
- NÃO mencione o viés no resumo nem nos fatos. O chip aparece silenciosamente.

Temas NÃO políticos NÃO precisam do campo `pol_bias`. Omita ou deixe null.
"""


# ============================================================================
# F. TEMAS QUE ATIVAM CLASSIFICAÇÃO POLÍTICA
# ============================================================================
POLITICAL_TOPICS = {
    "política", "politica", "governo", "geopolítica", "geopolitica",
    "eleições", "eleicoes", "stf", "congresso", "senado", "câmara",
    "camara", "planalto", "legislativo", "judiciário", "judiciario",
    "ministério", "ministerio", "diplomacia",
}


def is_political_topic(topic: str) -> bool:
    if not topic:
        return False
    t = topic.lower().strip()
    for p in POLITICAL_TOPICS:
        if p in t:
            return True
    return False
