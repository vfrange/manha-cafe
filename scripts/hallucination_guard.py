"""
hallucination_guard.py — Detecção programática de alucinação na curadoria.

3 camadas de severidade:
1. CRÍTICO  (descarte) : cargo de poder atribuído a nome NÃO presente na fonte.
                         Ex: "presidente Dória" quando a fonte só diz "Dória".
                         Caso clássico do bug 22/05/2026 (homônimo São Paulo FC).
2. MODERADO (reescrita): números/percentuais/valores monetários/datas no resumo
                         que NÃO aparecem na fonte. 1 retry pedindo pro Claude
                         (Haiku 4.5, ~R$ 0,01/chamada) reescrever sem inflar.
3. LEVE     (permite + log): cargo de figura pública conhecida via whitelist
                             (Lula presidente, Trump presidente EUA, etc).

Garante volume mantendo precisão. Custo médio: ~R$ 0,01-0,03/edição (só camada 2).

Tabela: hallucination_log (SQL no migrations/).
"""

import re
import json
import unicodedata
import logging
from typing import Optional, Tuple

log = logging.getLogger(__name__)


# ============================================================================
# CONFIG
# ============================================================================

# Cargos de poder — disparam descarte CRÍTICO se atribuídos a nome não-fonte
POWER_TITLES = [
    "presidente", "vice-presidente", "vice presidente", "ex-presidente", "ex presidente",
    "ceo", "chairman", "chair", "presidente do conselho",
    "ministro", "ministra", "ex-ministro", "ex-ministra",
    "governador", "governadora", "ex-governador", "ex-governadora",
    "prefeito", "prefeita", "ex-prefeito", "ex-prefeita",
    "deputado", "deputada", "deputado federal", "deputada federal",
    "senador", "senadora",
    "diretor", "diretora", "diretor-presidente",
    "secretário", "secretaria", "secretária",
    "primeiro-ministro", "primeira-ministra", "primeiro ministro", "primeira ministra",
    "chanceler",
    "procurador-geral", "procuradora-geral", "procurador geral",
    "promotor", "promotora",
    "juiz", "juíza", "juiza", "desembargador", "desembargadora", "ministro do stf",
    "embaixador", "embaixadora",
    "comandante",
    "fundador", "fundadora", "founder", "cofundador", "cofundadora", "co-fundador",
    "dono", "dona", "proprietário", "proprietária",
    "papa", "rei", "rainha",
]


# Whitelist — figuras públicas estáveis (cargo de domínio público).
# Mantém conservadora. Atualizar manualmente quando mudar.
# Match parcial: se o resumo cita "Lula presidente", e "lula" está aqui, libera.
PUBLIC_FIGURES_WHITELIST = {
    # BR — Executivo Federal
    "lula": "presidente do brasil",
    "luiz inácio lula da silva": "presidente do brasil",
    "alckmin": "vice-presidente do brasil",
    "geraldo alckmin": "vice-presidente do brasil",
    "haddad": "ministro da fazenda",
    "fernando haddad": "ministro da fazenda",
    "lewandowski": "ministro da justiça",
    "ricardo lewandowski": "ministro da justiça",
    "rui costa": "ministro da casa civil",
    "padilha": "ministro da saúde",
    "alexandre padilha": "ministro da saúde",
    "marina silva": "ministra do meio ambiente",
    "anielle franco": "ministra da igualdade racial",
    # BR — STF
    "barroso": "presidente do stf",
    "luís roberto barroso": "presidente do stf",
    "moraes": "ministro do stf",
    "alexandre de moraes": "ministro do stf",
    "fachin": "ministro do stf",
    "gilmar mendes": "ministro do stf",
    "dino": "ministro do stf",
    "flávio dino": "ministro do stf",
    # BR — Congresso
    "pacheco": "ex-presidente do senado",
    "rodrigo pacheco": "ex-presidente do senado",
    "lira": "ex-presidente da câmara",
    "arthur lira": "ex-presidente da câmara",
    "hugo motta": "presidente da câmara",
    "alcolumbre": "presidente do senado",
    "davi alcolumbre": "presidente do senado",
    # BR — Governadores
    "tarcísio": "governador de são paulo",
    "tarcisio": "governador de são paulo",
    "tarcísio de freitas": "governador de são paulo",
    "ratinho junior": "governador do paraná",
    "ratinho jr": "governador do paraná",
    "zema": "governador de minas gerais",
    "romeu zema": "governador de minas gerais",
    "cláudio castro": "governador do rio",
    "claudio castro": "governador do rio",
    # BR — Clubes (fix do bug Dória)
    "casares": "presidente do são paulo fc",
    "julio casares": "presidente do são paulo fc",
    "leila pereira": "presidente do palmeiras",
    "augusto melo": "presidente do corinthians",
    "rodolfo landim": "ex-presidente do flamengo",
    "bap": "presidente do flamengo",
    "luiz eduardo baptista": "presidente do flamengo",
    # EUA
    "trump": "presidente dos eua",
    "donald trump": "presidente dos eua",
    "biden": "ex-presidente dos eua",
    "joe biden": "ex-presidente dos eua",
    "harris": "ex-vice dos eua",
    "kamala harris": "ex-vice dos eua",
    "vance": "vice dos eua",
    "jd vance": "vice dos eua",
    # Big Tech
    "musk": "ceo de tesla e spacex",
    "elon musk": "ceo de tesla e spacex",
    "bezos": "fundador da amazon",
    "jeff bezos": "fundador da amazon",
    "zuckerberg": "ceo da meta",
    "mark zuckerberg": "ceo da meta",
    "altman": "ceo da openai",
    "sam altman": "ceo da openai",
    "nadella": "ceo da microsoft",
    "satya nadella": "ceo da microsoft",
    "pichai": "ceo do google",
    "sundar pichai": "ceo do google",
    "tim cook": "ceo da apple",
    "jensen huang": "ceo da nvidia",
    "huang": "ceo da nvidia",
    # Mundo
    "macron": "presidente da frança",
    "emmanuel macron": "presidente da frança",
    "merz": "chanceler da alemanha",
    "friedrich merz": "chanceler da alemanha",
    "scholz": "ex-chanceler da alemanha",
    "starmer": "primeiro-ministro do reino unido",
    "keir starmer": "primeiro-ministro do reino unido",
    "milei": "presidente da argentina",
    "javier milei": "presidente da argentina",
    "petro": "presidente da colômbia",
    "gustavo petro": "presidente da colômbia",
    "boric": "presidente do chile",
    "gabriel boric": "presidente do chile",
    "sheinbaum": "presidente do méxico",
    "claudia sheinbaum": "presidente do méxico",
    "xi jinping": "presidente da china",
    "putin": "presidente da rússia",
    "vladimir putin": "presidente da rússia",
    "zelensky": "presidente da ucrânia",
    "zelenski": "presidente da ucrânia",
    "modi": "primeiro-ministro da índia",
    "narendra modi": "primeiro-ministro da índia",
    "netanyahu": "primeiro-ministro de israel",
    "benjamin netanyahu": "primeiro-ministro de israel",
    "ishiba": "primeiro-ministro do japão",
    "papa leão": "papa do vaticano",
    "papa francisco": "ex-papa do vaticano",
}


# ============================================================================
# HELPERS DE NORMALIZAÇÃO
# ============================================================================

def _norm(text: str) -> str:
    """Minúsculas, sem acentos, espaços colapsados. Pra comparar match."""
    if not text:
        return ""
    s = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", s.lower().strip())


def _name_in_whitelist(name_norm: str) -> bool:
    """Match parcial contra whitelist (sobrenome ou nome completo)."""
    if not name_norm:
        return False
    for key in PUBLIC_FIGURES_WHITELIST:
        key_norm = _norm(key)
        if key_norm == name_norm:
            return True
        # Sobrenome contido (ex: "lula" dentro de "luiz inácio lula da silva")
        if len(key_norm) >= 4 and key_norm in name_norm:
            return True
        if len(name_norm) >= 4 and name_norm in key_norm:
            return True
    return False


# ============================================================================
# CAMADA 1 — CRÍTICO: cargo de poder atribuído a nome ausente da fonte
# ============================================================================

def _find_power_titles_with_names(text: str) -> list:
    """
    Acha padrões "cargo + Nome (Capitalizado)" no texto.
    Retorna lista de tuplas (cargo_norm, nome_norm, snippet).
    """
    if not text:
        return []
    findings = []
    for title in POWER_TITLES:
        # Padrão: cargo + opcional "do/da/de/dos/das" + Nome capitalizado
        # Captura até 3 palavras capitalizadas (cobre "Luiz Inácio Lula da Silva")
        pattern = re.compile(
            r"\b" + re.escape(title) + r"\b\s+(?:do |da |de |dos |das )?"
            r"([A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][\w\-áàâãéêíóôõúç]+"
            r"(?:\s+(?:de |da |do |dos |das )?[A-ZÁÀÂÃÉÊÍÓÔÕÚÇ][\w\-áàâãéêíóôõúç]+){0,3})",
            re.IGNORECASE,
        )
        for m in pattern.finditer(text):
            name = m.group(1).strip()
            findings.append((_norm(title), _norm(name), m.group(0)))
    return findings


def _check_critical_inflation(generated_text: str, source_text: str) -> Optional[dict]:
    """
    CAMADA 1: detecta cargo atribuído a nome SEM aparecer na fonte.
    Retorna dict de erro ou None se ok.
    """
    titles_in_generated = _find_power_titles_with_names(generated_text)
    if not titles_in_generated:
        return None

    source_norm = _norm(source_text)

    for title_norm, name_norm, snippet in titles_in_generated:
        # Whitelist: figura pública conhecida → passa
        if _name_in_whitelist(name_norm):
            continue
        # Cargo está na fonte? E nome (ou parte) também?
        title_in_source = title_norm in source_norm
        # Match nome: testa nome completo OU primeira/última palavra
        name_parts = [p for p in name_norm.split() if len(p) >= 3]
        name_in_source = any(p in source_norm for p in name_parts)

        if title_in_source and name_in_source:
            continue  # Tudo na fonte, ok

        # Cargo ausente da fonte → inflação crítica
        if not title_in_source:
            return {
                "severity": "critical",
                "reason": f"cargo '{title_norm}' atribuído a '{name_norm}' não está na fonte",
                "snippet": snippet,
            }
        # Nome ausente da fonte (cargo está, mas atribuído a pessoa errada)
        if title_in_source and not name_in_source:
            return {
                "severity": "critical",
                "reason": f"nome '{name_norm}' associado a '{title_norm}' não está na fonte",
                "snippet": snippet,
            }
    return None


# ============================================================================
# CAMADA 2 — MODERADO: números/datas/valores inflados (passível de reescrita)
# ============================================================================

def _extract_quantitative_facts(text: str) -> list:
    """Extrai percentuais, valores monetários, datas específicas, números 4+ dígitos."""
    if not text:
        return []
    items = []
    # Percentuais: 12%, 12,5%, 12.5%
    items.extend(re.findall(r"\d+[.,]?\d*\s*%", text))
    # Valores monetários: R$ 1,2 bi, US$ 500 milhões, € 2 trilhões
    items.extend(re.findall(
        r"(?:R\$|US\$|€|£|\$)\s*[\d.,]+(?:\s*(?:bi(?:lhões|lhão)?|mi(?:lhões|lhão)?|tri(?:lhões|lhão)?|mil))?",
        text, re.IGNORECASE,
    ))
    # Datas: 15/03, 15/03/2026, 15-03-2026
    items.extend(re.findall(r"\b\d{1,2}[/\-]\d{1,2}(?:[/\-]\d{2,4})?\b", text))
    # Datas por extenso: 15 de março
    items.extend(re.findall(
        r"\b\d{1,2}\s+de\s+(?:janeiro|fevereiro|março|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)(?:\s+de\s+\d{4})?\b",
        text, re.IGNORECASE,
    ))
    # Números 4+ dígitos sem unidade (ex "12.500 vítimas")
    items.extend(re.findall(r"\b\d{4,}(?:[.,]\d+)*\b", text))
    return [it.strip() for it in items if it.strip()]


def _check_moderate_inflation(generated_text: str, source_text: str) -> Optional[dict]:
    """
    CAMADA 2: detecta números/percentuais/datas no resumo que não aparecem na fonte.
    Retorna dict com inflated_items ou None se ok.
    """
    gen_facts = _extract_quantitative_facts(generated_text)
    if not gen_facts:
        return None

    source_norm_text = source_text or ""
    # Versões variadas pra comparar (vírgula vs ponto, com/sem espaço)
    source_variants = {
        _norm(source_norm_text),
        _norm(source_norm_text).replace(",", "."),
        _norm(source_norm_text).replace(",", ".").replace(" ", ""),
    }

    inflated = []
    for fact in gen_facts:
        fact_norm = _norm(fact)
        fact_variants = {
            fact_norm,
            fact_norm.replace(",", "."),
            fact_norm.replace(",", ".").replace(" ", ""),
        }
        # Se alguma variante do fato bate com alguma variante da fonte → ok
        if any(any(f in s for s in source_variants) for f in fact_variants):
            continue
        # Permite anos atuais (2020-2030) — contexto temporal genérico
        if re.fullmatch(r"\d{4}", fact_norm):
            try:
                if 2020 <= int(fact_norm) <= 2030:
                    continue
            except ValueError:
                pass
        inflated.append(fact)

    if inflated:
        return {
            "severity": "moderate",
            "reason": f"números/datas no resumo não aparecem na fonte",
            "inflated_items": inflated[:5],  # cap pra log
        }
    return None


# ============================================================================
# ENTRADA PRINCIPAL — VALIDAÇÃO DE UM ITEM
# ============================================================================

def validate_resumo(resumo: str, fatos_chave, source_title: str, source_summary: str) -> dict:
    """
    Valida um resumo+fatos contra a fonte (título + summary brutos).

    Returns: {"severity": "ok"|"moderate"|"critical", "reason": str, ...details}
    """
    if not resumo:
        return {"severity": "ok", "reason": ""}

    fatos_str = " ".join(fatos_chave) if isinstance(fatos_chave, list) else (fatos_chave or "")
    generated_text = f"{resumo} {fatos_str}"
    source_text = f"{source_title or ''} {source_summary or ''}"

    # CAMADA 1: crítico
    critical = _check_critical_inflation(generated_text, source_text)
    if critical:
        return critical

    # CAMADA 2: moderado
    moderate = _check_moderate_inflation(generated_text, source_text)
    if moderate:
        return moderate

    return {"severity": "ok", "reason": ""}


# ============================================================================
# REESCRITA (CAMADA 2) — pede pro Claude remover inflações
# ============================================================================

def rewrite_inflated(claude_client, model: str, resumo: str, fatos_chave,
                     source_title: str, source_summary: str, inflated_items: list) -> Optional[dict]:
    """
    Pede pro Claude reescrever resumo+fatos REMOVENDO os números inflados.
    Retorna {"resumo": str, "fatos_chave": list} ou None se falhar.
    1 chamada Haiku, ~R$ 0,01.
    """
    fatos_json = json.dumps(fatos_chave or [], ensure_ascii=False)
    inflated_json = json.dumps(inflated_items, ensure_ascii=False)

    prompt = f"""REESCREVA um resumo de notícia REMOVENDO informações que não estão na fonte original.

FONTE ORIGINAL:
Título: {source_title}
Summary: {source_summary}

RESUMO GERADO:
{resumo}

FATOS-CHAVE GERADOS:
{fatos_json}

PROBLEMA: estes elementos aparecem no resumo/fatos mas NÃO estão na fonte e devem ser REMOVIDOS:
{inflated_json}

TAREFA: Reescreva o resumo e fatos-chave REMOVENDO TODOS os números, percentuais, valores e datas que não aparecem na fonte. Mantenha o tom natural e o sentido geral. NÃO adicione nada novo. Se um fato-chave inteiro só contém número inflado, remova esse fato.

Responda APENAS com JSON válido, sem markdown:
{{"resumo": "...", "fatos_chave": ["...", "..."]}}"""

    try:
        resp = claude_client.messages.create(
            model=model,
            max_tokens=1200,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        parsed = json.loads(text)
        if parsed.get("resumo"):
            return {
                "resumo": parsed["resumo"].strip(),
                "fatos_chave": parsed.get("fatos_chave", []) or [],
            }
    except Exception as e:
        log.warning(f"rewrite_inflated falhou: {e}")
    return None


# ============================================================================
# LOG NO SUPABASE
# ============================================================================

def log_hallucination(supabase, user_id, edition_id, item, validation, action):
    """
    Salva auditoria na tabela hallucination_log.
    Não bloqueia o fluxo se a tabela não existir ou der erro.
    action: 'discarded' | 'rewritten' | 'logged'
    """
    try:
        supabase.table("hallucination_log").insert({
            "user_id": user_id,
            "edition_id": edition_id,
            "manchete": (item.get("manchete") or "")[:300],
            "resumo": (item.get("resumo") or "")[:1500],
            "fonte": (item.get("fonte") or "")[:100],
            "link": (item.get("link") or "")[:500],
            "severity": validation.get("severity", "unknown"),
            "reason": (validation.get("reason") or "")[:500],
            "action": action,
            "details": validation,
        }).execute()
    except Exception as e:
        log.debug(f"hallucination_log insert ignorado: {e}")


# ============================================================================
# PIPELINE — VALIDA + LIMPA SEÇÕES (de news por tema)
# ============================================================================

def _find_source(item, topics_raw_news):
    """Acha a notícia bruta original pelo link (ou título, fallback)."""
    link = (item.get("link") or "").strip()
    title = (item.get("manchete") or "").strip()
    title_norm = _norm(title)[:40]

    if not topics_raw_news:
        return None

    for group in topics_raw_news:
        for n in (group.get("news") or []):
            if link and (n.get("link") or "").strip() == link:
                return n
        # Fallback por título parecido
        if title_norm:
            for n in (group.get("news") or []):
                n_title_norm = _norm(n.get("title") or "")[:40]
                if n_title_norm and n_title_norm == title_norm:
                    return n
    return None


def validate_and_clean_sections(sections, topics_raw_news, supabase, user_id, edition_id,
                                 claude_client, model) -> dict:
    """
    Valida cada notícia de cada seção. Modifica sections in-place.

    Pipeline por notícia:
      1. Acha fonte original.
      2. Roda validate_resumo.
      3. critical → descarta + log.
      4. moderate → tenta reescrever (1x). Se reescrita ok, mantém. Senão descarta.
      5. ok → mantém.

    Returns stats: dict com contagens.
    """
    stats = {"total": 0, "ok": 0, "rewritten": 0,
             "discarded_critical": 0, "discarded_moderate": 0, "no_source": 0}

    for section in sections:
        if not section.get("noticias"):
            continue
        cleaned = []
        for item in section["noticias"]:
            stats["total"] += 1
            source = _find_source(item, topics_raw_news)
            if not source:
                # Sem fonte localizada — não dá pra validar, mantém defensivamente
                cleaned.append(item)
                stats["no_source"] += 1
                continue

            source_title = source.get("title") or ""
            source_summary = source.get("summary") or ""

            validation = validate_resumo(
                item.get("resumo") or "",
                item.get("fatos_chave") or [],
                source_title, source_summary,
            )

            if validation["severity"] == "critical":
                log_hallucination(supabase, user_id, edition_id, item, validation, "discarded")
                stats["discarded_critical"] += 1
                continue

            if validation["severity"] == "moderate":
                inflated = validation.get("inflated_items", [])
                rewritten = rewrite_inflated(
                    claude_client, model,
                    item.get("resumo") or "", item.get("fatos_chave") or [],
                    source_title, source_summary, inflated,
                )
                if rewritten:
                    # Revalida a reescrita
                    rev = validate_resumo(
                        rewritten["resumo"], rewritten["fatos_chave"],
                        source_title, source_summary,
                    )
                    if rev["severity"] == "ok":
                        item["resumo"] = rewritten["resumo"]
                        item["fatos_chave"] = rewritten["fatos_chave"]
                        log_hallucination(supabase, user_id, edition_id, item, validation, "rewritten")
                        cleaned.append(item)
                        stats["rewritten"] += 1
                        continue
                # Reescrita falhou ou ainda problemática → descarta
                log_hallucination(supabase, user_id, edition_id, item, validation, "discarded")
                stats["discarded_moderate"] += 1
                continue

            # ok
            cleaned.append(item)
            stats["ok"] += 1

        section["noticias"] = cleaned

    return stats


# ============================================================================
# PIPELINE — VALIDA + LIMPA TRENDING (Em Alta)
# ============================================================================

def validate_and_clean_trending(trending, raw_trends, supabase, user_id, edition_id,
                                 claude_client, model) -> dict:
    """Mesma lógica das sections, mas em lista plana de trending."""
    stats = {"total": 0, "ok": 0, "rewritten": 0,
             "discarded_critical": 0, "discarded_moderate": 0, "no_source": 0}

    if not trending:
        return stats

    # Adapta raw_trends pra formato esperado por _find_source
    pseudo_groups = [{"news": raw_trends}] if raw_trends else []

    cleaned = []
    for item in trending:
        stats["total"] += 1
        source = _find_source(item, pseudo_groups)
        if not source:
            cleaned.append(item)
            stats["no_source"] += 1
            continue

        source_title = source.get("title") or source.get("manchete") or ""
        source_summary = source.get("summary") or source.get("contexto") or ""

        validation = validate_resumo(
            item.get("resumo") or item.get("contexto") or "",
            item.get("fatos_chave") or [],
            source_title, source_summary,
        )

        if validation["severity"] == "critical":
            log_hallucination(supabase, user_id, edition_id, item, validation, "discarded")
            stats["discarded_critical"] += 1
            continue

        if validation["severity"] == "moderate":
            inflated = validation.get("inflated_items", [])
            rewritten = rewrite_inflated(
                claude_client, model,
                item.get("resumo") or "", item.get("fatos_chave") or [],
                source_title, source_summary, inflated,
            )
            if rewritten:
                rev = validate_resumo(
                    rewritten["resumo"], rewritten["fatos_chave"],
                    source_title, source_summary,
                )
                if rev["severity"] == "ok":
                    item["resumo"] = rewritten["resumo"]
                    item["fatos_chave"] = rewritten["fatos_chave"]
                    log_hallucination(supabase, user_id, edition_id, item, validation, "rewritten")
                    cleaned.append(item)
                    stats["rewritten"] += 1
                    continue
            log_hallucination(supabase, user_id, edition_id, item, validation, "discarded")
            stats["discarded_moderate"] += 1
            continue

        cleaned.append(item)
        stats["ok"] += 1

    trending.clear()
    trending.extend(cleaned)
    return stats


# ============================================================================
# CONTEXTO TEMPORAL — pra injetar no system prompt
# ============================================================================

def get_current_date_context(now_brt) -> str:
    """Retorna texto pronto pra system prompt: 'Hoje é quinta-feira, 22 de maio de 2026.'"""
    dias = ["segunda-feira", "terça-feira", "quarta-feira", "quinta-feira",
            "sexta-feira", "sábado", "domingo"]
    meses = ["janeiro", "fevereiro", "março", "abril", "maio", "junho",
             "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]
    dia_semana = dias[now_brt.weekday()]
    return f"Hoje é {dia_semana}, {now_brt.day} de {meses[now_brt.month-1]} de {now_brt.year}."


# ============================================================================
# REGRA ANTI-ALUCINAÇÃO — texto pronto pra colar em qualquer system prompt
# ============================================================================

ANTI_HALLUCINATION_RULE = """
🛡 **REGRA ABSOLUTA ANTI-ALUCINAÇÃO** (não-negociável):
Use APENAS o que está EXPLICITAMENTE escrito no título e summary da matéria fornecida. Você está PROIBIDO de:
- Completar cargos de pessoas mencionadas. Se a fonte diz apenas "Dória", NÃO escreva "presidente Dória", "diretor Dória", "ministro Dória" etc. Escreva apenas "Dória".
- Acrescentar contexto histórico, biográfico, político ou profissional de sua memória sobre as pessoas, empresas ou entidades citadas.
- Inferir nome completo, sobrenome, partido, empresa, time, cargo, nacionalidade ou afiliação que não aparece TEXTUALMENTE na fonte.
- Inventar números, percentuais, valores monetários ou datas que não aparecem na fonte.
- Conectar a pessoa citada a eventos, declarações ou contextos do seu conhecimento prévio.

Trate cada matéria como conhecimento ISOLADO. Você NÃO sabe NADA sobre as pessoas além do que a fonte específica diz. Se não estiver no texto-fonte, NÃO pode estar no resumo.
"""
