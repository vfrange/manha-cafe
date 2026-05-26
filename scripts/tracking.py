"""
Helpers de tracking + editions.

- wrap_links_in_html: substitui <a href="https://..."> por <a href="/c/{short_id}">
  e cria registros em link_clicks pra rastrear cliques.
- save_edition: insere registro em editions e retorna o id (UUID).
- finalize_edition: marca sent_at após envio pelo Resend.
"""
import re
import os
import uuid
import logging
from typing import Optional, List, Dict
from urllib.parse import urlparse

from feedback_token import short_id

# Reusa o _supabase_retry do daily_digest pra ter retry consistente em todo
# o pipeline (backoff exponencial em RemoteProtocolError / disconnect HTTP/2).
# Lazy import pra evitar ciclo no tempo de carregamento dos módulos.
def _get_retry():
    try:
        from daily_digest import _supabase_retry
        return _supabase_retry
    except Exception:
        # Fallback: se daily_digest não puder ser importado, executa sem retry.
        return lambda fn, label="", **kw: fn()

log = logging.getLogger(__name__)

# Domínios que NÃO devem ser wrapeados (links internos)
INTERNAL_DOMAINS = {
    "recorte.news",
    "supabase.co",      # edge functions
    "supabase.in",
}

# Schemes pulados (mailto:, tel:, javascript:, anchors)
SKIP_SCHEMES = {"mailto", "tel", "javascript", "data"}


def _is_external_link(url: str) -> bool:
    """True se é um link externo que deve ser rastreado."""
    if not url or not isinstance(url, str):
        return False
    url = url.strip()
    if not url or url.startswith("#"):
        return False
    try:
        p = urlparse(url)
    except Exception:
        return False
    if p.scheme.lower() in SKIP_SCHEMES:
        return False
    if not p.scheme or not p.netloc:
        return False
    # Domínio interno?
    netloc = p.netloc.lower()
    for d in INTERNAL_DOMAINS:
        if netloc == d or netloc.endswith("." + d):
            return False
    return True


def wrap_links_in_html(
    html: str,
    user_id: str,
    edition_id: str,
    supabase_client,
    click_base_url: str,
    link_metadata: Optional[Dict[str, Dict]] = None,
) -> str:
    """
    Substitui todos os links externos no HTML por wrappers /c/{short_id} que rastreiam cliques.

    Args:
        html: HTML do email
        user_id: UUID do user
        edition_id: UUID da edição
        supabase_client: cliente Supabase pra inserir em link_clicks
        click_base_url: ex. "https://recorte.news/c" — base URL do redirector
        link_metadata: dict {url: {source, topic_label, position}} opcional pra enriquecer cliques.

    Returns:
        HTML com hrefs substituídos. Insere registros em link_clicks em batch.
    """
    if not html or not user_id or not edition_id:
        return html

    link_metadata = link_metadata or {}
    _supabase_retry = _get_retry()

    # Match <a href="..."> — captura aspas simples ou duplas, ignora maiúsculas
    href_pattern = re.compile(r'<a([^>]*?)\shref=([\'"])([^\'"]+)\2', re.IGNORECASE)

    # Mapa url → short_id já gerado (mesmo URL na mesma edição = mesmo short_id)
    url_to_short: Dict[str, str] = {}
    inserts: List[Dict] = []

    def _replace(match):
        attrs_before = match.group(1)
        quote = match.group(2)
        url = match.group(3).strip()

        if not _is_external_link(url):
            return match.group(0)  # não wrappa

        # Mesmo URL na mesma edição → mesmo short_id (evita duplicar registros)
        if url in url_to_short:
            sid = url_to_short[url]
        else:
            sid = short_id(10)
            url_to_short[url] = sid
            meta = link_metadata.get(url, {})
            inserts.append({
                "short_id": sid,
                "user_id": user_id,
                "edition_id": edition_id,
                "target_url": url,
                "source": meta.get("source"),
                "topic_label": meta.get("topic_label"),
                "position": meta.get("position"),
            })

        wrapped = f"{click_base_url.rstrip('/')}/{sid}"
        return f'<a{attrs_before} href={quote}{wrapped}{quote}'

    new_html = href_pattern.sub(_replace, html)

    # Insert em batch — com fallback de inserts individuais se batch falhar.
    # CRÍTICO: se inserts falharem, os /c/{short_id} no HTML viram links mortos
    # (edge function /c/ não acha → mostra "Notícia saiu do ar" pra TODOS).
    # Pra mitigar:
    #   1. batch com retry (cobre disconnect HTTP/2)
    #   2. se batch ainda falhar, tenta 1 por 1 com retry em cada
    #   3. links que ainda assim falharem, "des-wrappa" pra apontar pra URL original
    if inserts:
        try:
            _supabase_retry(
                lambda: supabase_client.table("link_clicks").insert(inserts).execute(),
                label="link_clicks.insert(batch)",
            )
        except Exception as e:
            log.warning(f"  ⚠ batch insert link_clicks falhou após retry: {e} — retentando individuais")
            ok_short_ids = set()
            for row in inserts:
                try:
                    _supabase_retry(
                        lambda r=row: supabase_client.table("link_clicks").insert(r).execute(),
                        label="link_clicks.insert(single)",
                    )
                    ok_short_ids.add(row["short_id"])
                except Exception as e2:
                    log.warning(f"    ⚠ falha individual short_id={row['short_id']}: {e2}")
            # Pra cada short_id que NÃO foi inserido, "des-wrappa" o link no HTML
            # (substitui /c/{short_id} pela URL original) — evita /c/ links mortos
            for row in inserts:
                if row["short_id"] not in ok_short_ids:
                    bad_wrapped = f"{click_base_url.rstrip('/')}/{row['short_id']}"
                    new_html = new_html.replace(bad_wrapped, row["target_url"])
            if not ok_short_ids:
                log.error(f"  ✗ NENHUM link_click inserido — todos os links no email apontam direto")

    return new_html


def save_edition(
    supabase_client,
    user_id: str,
    kind: str,
    subject: str,
    html: str,
    scheduled_for: str,
    queue_id: Optional[str] = None,
    edition_id: Optional[str] = None,
) -> str:
    """
    Insere registro na tabela editions. Retorna o id.

    Args:
        edition_id: se passado, usa esse UUID (útil pra reservar o ID antes de montar HTML).
        kind: 'daily' | 'weekly' | 'welcome'
    """
    if edition_id is None:
        edition_id = str(uuid.uuid4())

    _supabase_retry = _get_retry()
    try:
        _supabase_retry(
            lambda: supabase_client.table("editions").insert({
                "id": edition_id,
                "user_id": user_id,
                "queue_id": queue_id,
                "kind": kind,
                "subject": subject,
                "html": html,
                "scheduled_for": scheduled_for,
            }).execute(),
            label="editions.insert",
        )
    except Exception as e:
        log.error(f"  ✗ falha ao salvar edition após retry: {e}")
        raise

    return edition_id


def finalize_edition(supabase_client, edition_id: str, resend_id: Optional[str] = None):
    """Marca a edition como enviada (sent_at = agora)."""
    from datetime import datetime, timezone
    _supabase_retry = _get_retry()
    try:
        upd = {"sent_at": datetime.now(timezone.utc).isoformat()}
        if resend_id:
            upd["resend_id"] = resend_id
        _supabase_retry(
            lambda: supabase_client.table("editions").update(upd).eq("id", edition_id).execute(),
            label="editions.update(sent_at)",
        )
    except Exception as e:
        log.warning(f"  ⚠ falha ao finalizar edition {edition_id} após retry: {e}")


def gen_edition_id() -> str:
    """Gera um UUID novo pra ser usado como edition_id."""
    return str(uuid.uuid4())
