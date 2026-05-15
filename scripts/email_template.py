"""
Renderiza o HTML do email diário do Recorte ✂.
V4: identidade visual verde-menta dominante + Fraunces + Mulish.

Paleta:
- Verde-menta #6EE7B7 (masthead + signoff)
- Amarelo #FFD60A (TTS player + highlights)
- Marinho #0A2540 (texto principal + detalhes)
- Verde-esmeralda #10B981 (chips de coverage + accents)
- Verde-floresta #047857 (números + texto verde escuro)
- Verde-menta lavado #D1FAE5 (fundo "Por que importa")
- Creme #FFFAF0 (background base)

Compatibilidade: HTML tabelar + inline styles + fallback de fontes pra Gmail,
Outlook, Apple Mail, iOS Mail. Fraunces e Mulish via Google Fonts (renderizam
em Gmail web, Apple Mail, Outlook web; fallback Georgia/Helvetica em Android).
"""

import html as html_lib
import re
import unicodedata


# ============================================================================
# ÍCONES POR TEMA — SVGs minimalistas linha (16x16, herdam cor via currentColor)
# Inline no HTML, sem fetch externo. Funcionam em todos clientes de email.
# ============================================================================
def _svg(path_d):
    """Wraps um <path> em SVG 16x16 monocromático."""
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="14" height="14" '
        'fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" '
        f'stroke-linejoin="round" style="vertical-align:-2px;margin-right:6px;">{path_d}</svg>'
    )

TOPIC_ICONS = {
    # Economia — gráfico de barras crescente
    "economia": _svg('<path d="M3 21V11M9 21V7M15 21V13M21 21V4"/><line x1="3" y1="21" x2="21" y2="21"/>'),
    # Mercados financeiros — candle / setas mercado
    "mercado":  _svg('<polyline points="3 17 9 11 13 15 21 7"/><polyline points="14 7 21 7 21 14"/>'),
    # Tech & IA — chip
    "tech":     _svg('<rect x="4" y="4" width="16" height="16" rx="2"/><rect x="9" y="9" width="6" height="6"/><line x1="9" y1="2" x2="9" y2="4"/><line x1="15" y1="2" x2="15" y2="4"/><line x1="9" y1="20" x2="9" y2="22"/><line x1="15" y1="20" x2="15" y2="22"/><line x1="2" y1="9" x2="4" y2="9"/><line x1="2" y1="15" x2="4" y2="15"/><line x1="20" y1="9" x2="22" y2="9"/><line x1="20" y1="15" x2="22" y2="15"/>'),
    # Geopolítica — globo
    "geopol":   _svg('<circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15 15 0 0 1 4 10 15 15 0 0 1-4 10 15 15 0 0 1-4-10 15 15 0 0 1 4-10z"/>'),
    # Política — coluna/capitólio (3 pilares)
    "polit":    _svg('<line x1="3" y1="22" x2="21" y2="22"/><line x1="3" y1="18" x2="21" y2="18"/><line x1="6" y1="18" x2="6" y2="6"/><line x1="12" y1="18" x2="12" y2="6"/><line x1="18" y1="18" x2="18" y2="6"/><polyline points="3 6 12 2 21 6"/>'),
    # Governo (alias)
    "governo":  _svg('<line x1="3" y1="22" x2="21" y2="22"/><line x1="3" y1="18" x2="21" y2="18"/><line x1="6" y1="18" x2="6" y2="6"/><line x1="12" y1="18" x2="12" y2="6"/><line x1="18" y1="18" x2="18" y2="6"/><polyline points="3 6 12 2 21 6"/>'),
    # Food service — hambúrguer
    "food":     _svg('<path d="M3 11h18a0 0 0 0 1 0 0 7 7 0 0 1-7 7h-4a7 7 0 0 1-7-7"/><path d="M21 8H3a9 9 0 0 1 9-6 9 9 0 0 1 9 6z"/><line x1="6" y1="15" x2="6.01" y2="15"/><line x1="10" y1="15" x2="10.01" y2="15"/><line x1="14" y1="15" x2="14.01" y2="15"/>'),
    # Hambúrguer (alias)
    "hamburg":  _svg('<path d="M3 11h18a0 0 0 0 1 0 0 7 7 0 0 1-7 7h-4a7 7 0 0 1-7-7"/><path d="M21 8H3a9 9 0 0 1 9-6 9 9 0 0 1 9 6z"/><line x1="6" y1="15" x2="6.01" y2="15"/><line x1="10" y1="15" x2="10.01" y2="15"/><line x1="14" y1="15" x2="14.01" y2="15"/>'),
    # Varejo (alias food service)
    "varejo":   _svg('<path d="M6 2 3 6v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V6l-3-4z"/><line x1="3" y1="6" x2="21" y2="6"/><path d="M16 10a4 4 0 0 1-8 0"/>'),
    # Negócios & M&A — handshake / fusão
    "negocio":  _svg('<path d="M11 17l-5-5 5-5"/><path d="M13 7l5 5-5 5"/><line x1="6" y1="12" x2="18" y2="12"/>'),
    "fusoes":   _svg('<path d="M11 17l-5-5 5-5"/><path d="M13 7l5 5-5 5"/><line x1="6" y1="12" x2="18" y2="12"/>'),
    # Imobiliário — casa
    "imob":     _svg('<path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/>'),
    # Esportes — bola futebol
    "esport":   _svg('<circle cx="12" cy="12" r="10"/><path d="M12 2L9 7l6 0z"/><path d="M22 12l-5-3 0 6z"/><path d="M12 22l3-5-6 0z"/><path d="M2 12l5 3 0-6z"/>'),
    # SPFC / São Paulo Futebol Clube / Seleção (alias)
    "futebol":  _svg('<circle cx="12" cy="12" r="10"/><path d="M12 2L9 7l6 0z"/><path d="M22 12l-5-3 0 6z"/><path d="M12 22l3-5-6 0z"/><path d="M2 12l5 3 0-6z"/>'),
    "spfc":     _svg('<circle cx="12" cy="12" r="10"/><path d="M12 2L9 7l6 0z"/><path d="M22 12l-5-3 0 6z"/><path d="M12 22l3-5-6 0z"/><path d="M2 12l5 3 0-6z"/>'),
    "selecao":  _svg('<circle cx="12" cy="12" r="10"/><path d="M12 2L9 7l6 0z"/><path d="M22 12l-5-3 0 6z"/><path d="M12 22l3-5-6 0z"/><path d="M2 12l5 3 0-6z"/>'),
    "copa":     _svg('<path d="M6 9H4.5a2.5 2.5 0 0 1 0-5H6"/><path d="M18 9h1.5a2.5 2.5 0 0 0 0-5H18"/><path d="M4 22h16"/><path d="M10 14.66V17c0 .55-.47.98-.97 1.21C7.85 18.75 7 20.24 7 22"/><path d="M14 14.66V17c0 .55.47.98.97 1.21C16.15 18.75 17 20.24 17 22"/><path d="M18 2H6v7a6 6 0 0 0 12 0V2Z"/>'),
    # Cultura & entretenimento — claquete
    "cultura":  _svg('<polygon points="3 8 21 8 21 20 3 20 3 8"/><polyline points="3 8 5 4 9 4 7 8 11 8 9 4 13 4 11 8 15 8 13 4 17 4 15 8 19 8 17 4 21 4"/>'),
    "entretenimento": _svg('<polygon points="3 8 21 8 21 20 3 20 3 8"/><polyline points="3 8 5 4 9 4 7 8 11 8 9 4 13 4 11 8 15 8 13 4 17 4 15 8 19 8 17 4 21 4"/>'),
    # Ciência & saúde — DNA helix simplificado
    "ciencia":  _svg('<path d="M4 3h16"/><path d="M4 21h16"/><path d="M4 8c4 3 12 3 16 0"/><path d="M4 16c4-3 12-3 16 0"/>'),
    "saude":    _svg('<path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"/>'),
    "pesquisa": _svg('<path d="M4 3h16"/><path d="M4 21h16"/><path d="M4 8c4 3 12 3 16 0"/><path d="M4 16c4-3 12-3 16 0"/>'),
    # Sustentabilidade & ESG — folha
    "sustent":  _svg('<path d="M11 20A7 7 0 0 1 9.8 6.1C15.5 5 17 4.48 19.2 2.96c.8 3.69 1.32 7.07.36 11.36-1.04 4.69-4.55 7.06-8.56 5.68z"/><path d="M2 21c0-3 1.85-5.36 5.08-6"/>'),
    "esg":      _svg('<path d="M11 20A7 7 0 0 1 9.8 6.1C15.5 5 17 4.48 19.2 2.96c.8 3.69 1.32 7.07.36 11.36-1.04 4.69-4.55 7.06-8.56 5.68z"/><path d="M2 21c0-3 1.85-5.36 5.08-6"/>'),
    "clima":    _svg('<path d="M11 20A7 7 0 0 1 9.8 6.1C15.5 5 17 4.48 19.2 2.96c.8 3.69 1.32 7.07.36 11.36-1.04 4.69-4.55 7.06-8.56 5.68z"/><path d="M2 21c0-3 1.85-5.36 5.08-6"/>'),
    # Auto & mobilidade — carro
    "auto":     _svg('<path d="M16 3h-8a2 2 0 0 0-2 2v5h12V5a2 2 0 0 0-2-2z"/><path d="M3 14v3a1 1 0 0 0 1 1h2"/><circle cx="7" cy="17" r="2"/><circle cx="17" cy="17" r="2"/><path d="M19 17h2a1 1 0 0 0 1-1v-3l-2-3h-4"/>'),
    "mobilidade": _svg('<path d="M16 3h-8a2 2 0 0 0-2 2v5h12V5a2 2 0 0 0-2-2z"/><path d="M3 14v3a1 1 0 0 0 1 1h2"/><circle cx="7" cy="17" r="2"/><circle cx="17" cy="17" r="2"/><path d="M19 17h2a1 1 0 0 0 1-1v-3l-2-3h-4"/>'),
    # Educação — capelo
    "educacao": _svg('<path d="M22 10v6M2 10l10-5 10 5-10 5z"/><path d="M6 12v5c3 3 9 3 12 0v-5"/>'),
    "ensino":   _svg('<path d="M22 10v6M2 10l10-5 10 5-10 5z"/><path d="M6 12v5c3 3 9 3 12 0v-5"/>'),
    # Trabalho & carreira — maleta
    "trabalho": _svg('<rect x="2" y="7" width="20" height="14" rx="2" ry="2"/><path d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16"/>'),
    "carreira": _svg('<rect x="2" y="7" width="20" height="14" rx="2" ry="2"/><path d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16"/>'),
    "rh":       _svg('<rect x="2" y="7" width="20" height="14" rx="2" ry="2"/><path d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16"/>'),
    "startup":  _svg('<path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/><path d="M12 15l-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/><path d="M9 12H4s.55-3.03 2-4c1.62-1.08 5 0 5 0"/><path d="M12 15v5s3.03-.55 4-2c1.08-1.62 0-5 0-5"/>'),
    "starups":  _svg('<path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/><path d="M12 15l-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/><path d="M9 12H4s.55-3.03 2-4c1.62-1.08 5 0 5 0"/><path d="M12 15v5s3.03-.55 4-2c1.08-1.62 0-5 0-5"/>'),
    # Consumo & marcas — sacola de compras
    "consumo":  _svg('<path d="M6 2 3 6v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V6l-3-4z"/><line x1="3" y1="6" x2="21" y2="6"/><path d="M16 10a4 4 0 0 1-8 0"/>'),
    "marcas":   _svg('<path d="M6 2 3 6v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V6l-3-4z"/><line x1="3" y1="6" x2="21" y2="6"/><path d="M16 10a4 4 0 0 1-8 0"/>'),
    "marketing":_svg('<path d="M6 2 3 6v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V6l-3-4z"/><line x1="3" y1="6" x2="21" y2="6"/><path d="M16 10a4 4 0 0 1-8 0"/>'),
    # Curiosidades — lâmpada
    "curiosidade": _svg('<path d="M9 18h6"/><path d="M10 22h4"/><path d="M15.09 14c.18-.98.65-1.74 1.41-2.5A4.65 4.65 0 0 0 18 8 6 6 0 0 0 6 8c0 1 .23 2.23 1.5 3.5A4.61 4.61 0 0 1 8.91 14"/>'),
}

# Default = tesoura (símbolo da marca pra temas personalizados sem match)
DEFAULT_TOPIC_ICON = _svg(
    '<circle cx="6" cy="6" r="3"/>'
    '<circle cx="6" cy="18" r="3"/>'
    '<line x1="20" y1="4" x2="8.12" y2="15.88"/>'
    '<line x1="14.47" y1="14.48" x2="20" y2="20"/>'
    '<line x1="8.12" y1="8.12" x2="12" y2="12"/>'
)


def _get_topic_icon(label):
    """Acha o ícone do tema. Match parcial em lowercase. Default = tesoura."""
    if not label:
        return DEFAULT_TOPIC_ICON
    label_norm = label.lower().strip()
    # Remove acentos
    label_norm = "".join(c for c in unicodedata.normalize("NFKD", label_norm)
                        if not unicodedata.combining(c))
    for key, svg in TOPIC_ICONS.items():
        if key in label_norm:
            return svg
    return DEFAULT_TOPIC_ICON


# ============================================================================
# PALETA
# ============================================================================
COLORS = {
    "mint":          "#6EE7B7",  # verde-menta dominante
    "mint_bg_light": "#D1FAE5",  # verde lavado pra fundos
    "mint_deep":     "#10B981",  # verde-esmeralda saturado (chip, borda)
    "mint_dark":     "#047857",  # verde-floresta (números, texto verde)
    "yellow":        "#FFD60A",  # amarelo accent quente
    "yellow_bg":     "#FFF5BD",  # amarelo lavado pra highlights
    "ink":           "#0A2540",  # marinho — texto principal
    "ink_soft":      "#4A5568",  # cinza-azulado pra texto secundário
    "ink_muted":     "#8A95A8",  # cinza claro pra meta
    "bg":            "#FFFAF0",  # creme base
    "bg_2":          "#F4F1EA",  # creme escuro pra sections
    "line":          "#E8E1D0",  # hairline creme
    "red":           "#C8102E",  # vermelho — "menos como essa"
}

# Stack tipográfica com fallbacks robustos
SERIF_FONT = "'Fraunces', Georgia, 'Times New Roman', serif"
SANS_FONT = "'Mulish', -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"
MONO_FONT = "'JetBrains Mono', 'SF Mono', Monaco, Consolas, monospace"


def _esc(s):
    return html_lib.escape(s or "")


# ============================================================================
# FAIXA TRICOLOR — elemento de marca recorrente
# ============================================================================
def _render_tricolor_band():
    """Faixa horizontal verde-esmeralda + amarelo + marinho de 4px."""
    return f"""<tr><td height="4" style="line-height:0;font-size:0;padding:0;">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>
        <td width="33%" height="4" bgcolor="{COLORS['mint_deep']}" style="background:{COLORS['mint_deep']};line-height:0;font-size:0;">&nbsp;</td>
        <td width="33%" height="4" bgcolor="{COLORS['yellow']}" style="background:{COLORS['yellow']};line-height:0;font-size:0;">&nbsp;</td>
        <td width="34%" height="4" bgcolor="{COLORS['ink']}" style="background:{COLORS['ink']};line-height:0;font-size:0;">&nbsp;</td>
      </tr></table>
    </td></tr>"""


# ============================================================================
# TRENDING SECTION
# ============================================================================
def _render_trending_section(trending, scope_label, email_mode="coado"):
    if not trending:
        return ""

    items_html = ""
    for idx, item in enumerate(trending):
        # Suporta formato NOVO (manchete/resumo/fatos_chave) e formato VELHO (termo/contexto)
        manchete = _esc(item.get("manchete") or item.get("termo", ""))
        resumo_raw = item.get("resumo") or item.get("contexto", "")
        if not manchete or not resumo_raw:
            continue

        is_espresso = (email_mode == "espresso")
        if is_espresso:
            import re as _re
            m = _re.split(r'(?<=[.!?])\s+', resumo_raw, maxsplit=1)
            resumo_show = m[0] if m else resumo_raw
            if len(resumo_show) > 180:
                resumo_show = resumo_show[:177].rstrip() + "..."
        else:
            resumo_show = resumo_raw
        resumo = _esc(resumo_show)

        fatos = item.get("fatos_chave") or []
        if is_espresso:
            fatos = []  # esconde fatos no modo espresso

        link = item.get("link", "")
        fonte = item.get("fonte", "")
        buscas = item.get("buscas", "")

        # Chip de "↑ X buscas" se vier
        buscas_html = ""
        if buscas:
            buscas_html = f'<span style="display:inline-block;background:{COLORS["mint"]};color:{COLORS["ink"]};font-family:{SANS_FONT};font-weight:800;font-size:10px;letter-spacing:0.08em;text-transform:uppercase;padding:2px 8px;margin-bottom:10px;">↑ {_esc(buscas)}</span><br/>'

        # Fatos-chave
        fatos_html = ""
        if isinstance(fatos, list) and fatos:
            bullets = "".join(
                f'<tr><td valign="top" style="padding:0 8px 6px 0;color:{COLORS["mint_dark"]};font-family:{SANS_FONT};font-weight:800;font-size:14px;line-height:1.4;">›</td>'
                f'<td style="padding-bottom:6px;font-family:{SANS_FONT};font-size:14px;line-height:1.5;color:{COLORS["ink_soft"]};">{_esc(f)}</td></tr>'
                for f in fatos[:5]
            )
            fatos_html = f'''<tr><td style="padding:0 0 14px 0;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:{COLORS['bg_2']};padding:14px 16px;">
                <tr><td colspan="2" style="font-family:{SANS_FONT};font-weight:800;font-size:10px;letter-spacing:0.18em;text-transform:uppercase;color:{COLORS['ink_muted']};padding-bottom:8px;">FATOS-CHAVE</td></tr>
                {bullets}
              </table>
            </td></tr>'''

        # Chip de idioma
        lang_chip = ""
        lang = (item.get("lang") or "").lower()
        lang_map = {"en":"🇺🇸 EN","fr":"🇫🇷 FR","de":"🇩🇪 DE","es":"🇪🇸 ES","it":"🇮🇹 IT","ja":"🇯🇵 JA","zh":"🇨🇳 ZH","ko":"🇰🇷 KO"}
        if lang in lang_map:
            lang_chip = f'<span style="display:inline-block;background:{COLORS["bg_2"]};color:{COLORS["ink_muted"]};font-family:{SANS_FONT};font-weight:700;font-size:10px;letter-spacing:0.06em;padding:2px 7px;margin-left:8px;border:1px solid {COLORS["line"]};">{lang_map[lang]}</span>'

        link_html = ""
        if link:
            fonte_suffix = ""
            if fonte:
                fonte_suffix = f'<span style="color:{COLORS["ink"]};font-weight:800;">&nbsp;·&nbsp;{_esc(fonte)}</span>'
            link_html = f'<tr><td style="font-family:{SANS_FONT};font-size:12px;color:{COLORS["ink_muted"]};padding-bottom:8px;"><a href="{_esc(link)}" style="color:{COLORS["ink"]};text-decoration:none;font-weight:800;border-bottom:2.5px solid {COLORS["mint_deep"]};padding-bottom:1px;margin-right:6px;">Ler matéria →</a>{fonte_suffix}{lang_chip}</td></tr>'

        items_html += f"""
        <tr><td style="padding:0 0 28px 0;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr><td>{buscas_html}<div style="font-family:{SERIF_FONT};font-weight:700;font-size:22px;line-height:1.22;color:{COLORS['ink']};letter-spacing:-0.015em;margin-bottom:10px;">{manchete}</div></td></tr>
            <tr><td style="font-family:{SANS_FONT};font-size:15px;line-height:1.55;color:{COLORS['ink_soft']};padding-bottom:14px;">{resumo}</td></tr>
            {fatos_html}
            {link_html}
          </table>
        </td></tr>"""

    return f"""
    <tr><td style="padding:32px 36px 8px 36px;">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:8px;border-bottom:4px solid {COLORS['bg_2']};">
        <tr><td style="padding:0 0 22px 0;">
          <table role="presentation" cellpadding="0" cellspacing="0" border="0"><tr>
            <td style="background:{COLORS['mint_dark']};padding:5px 12px;font-family:{SANS_FONT};font-size:11px;font-weight:800;letter-spacing:0.2em;text-transform:uppercase;color:{COLORS['mint']};">🔥 Em alta hoje</td>
            <td style="padding-left:12px;font-family:{SERIF_FONT};font-style:italic;font-size:12px;color:{COLORS['ink_muted']};">{_esc(scope_label)}</td>
          </tr></table>
        </td></tr>
        {items_html}
      </table>
    </td></tr>"""


def _render_daily_recap(recap_text):
    """Bloco 'Seu dia em 60 segundos' — entre hero e Em Alta."""
    if not recap_text or not recap_text.strip():
        return ""
    return f"""
    <tr><td style="padding:0 36px 24px 36px;">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:{COLORS['mint_bg_light']};border-left:6px solid {COLORS['mint_dark']};">
        <tr><td style="padding:22px 24px;">
          <div style="font-family:{SANS_FONT};font-size:11px;font-weight:800;letter-spacing:0.22em;text-transform:uppercase;color:{COLORS['mint_dark']};margin-bottom:12px;">☕ Seu dia em 60 segundos</div>
          <div style="font-family:{SERIF_FONT};font-size:16px;line-height:1.65;color:{COLORS['ink']};">{_esc(recap_text)}</div>
        </td></tr>
      </table>
    </td></tr>"""


# ============================================================================
# NEWS SECTIONS
# ============================================================================
def _slugify(text):
    """Slug simples pra usar como id de âncora."""
    s = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-zA-Z0-9]+", "-", s).strip("-").lower()
    return s or "tema"


def _render_news_sections(sections, email_mode="coado"):
    out = ""
    for idx, sec in enumerate(sections):
        slug = _slugify(sec.get("topic", f"tema-{idx}"))
        country_chip = ""
        if sec.get("country_label"):
            country_chip = f"""<span style="font-family:{SERIF_FONT};font-style:italic;font-size:11px;color:{COLORS['ink_muted']};margin-left:10px;">· {_esc(sec["country_label"])}</span>"""

        pause_btn = ""
        if sec.get("fb_pause_url"):
            pause_btn = f"""<td align="right" style="font-family:{SERIF_FONT};font-style:italic;font-size:11px;color:{COLORS['ink_muted']};">
              <a href="{_esc(sec['fb_pause_url'])}" style="color:{COLORS['ink_muted']};text-decoration:none;border-bottom:1px dashed {COLORS['ink_muted']};">⏸ pausar 7d</a>
            </td>"""

        noticias_html = ""
        for n in sec["noticias"]:
            # Defensivo: pula itens sem campos mínimos (Claude às vezes devolve item incompleto)
            if not n.get("manchete") or not n.get("resumo"):
                continue

            is_espresso = (email_mode == "espresso")

            # Resumo: completo no coado, 1ª frase no espresso
            resumo_full = n.get("resumo", "").strip()
            if is_espresso:
                # Pega 1ª frase (ou primeiros ~120 chars se não tiver pontuação)
                import re as _re
                m = _re.split(r'(?<=[.!?])\s+', resumo_full, maxsplit=1)
                resumo_display = m[0] if m else resumo_full
                if len(resumo_display) > 180:
                    resumo_display = resumo_display[:177].rstrip() + "..."
            else:
                resumo_display = resumo_full

            # Fatos-chave (bullets) — só no Café Coado
            fatos_html = ""
            if not is_espresso and n.get("fatos_chave"):
                fatos = n["fatos_chave"] if isinstance(n["fatos_chave"], list) else []
                if fatos:
                    bullets = "".join(
                        f'<tr><td valign="top" style="padding:0 8px 6px 0;color:{COLORS["mint_dark"]};font-family:{SANS_FONT};font-weight:800;font-size:14px;line-height:1.4;">›</td>'
                        f'<td style="padding-bottom:6px;font-family:{SANS_FONT};font-size:14px;line-height:1.5;color:{COLORS["ink_soft"]};">{_esc(f)}</td></tr>'
                        for f in fatos[:5]
                    )
                    fatos_html = f"""<tr><td style="padding-bottom:14px;">
                      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:{COLORS['bg_2']};padding:14px 16px;">
                        <tr><td colspan="2" style="font-family:{SANS_FONT};font-weight:800;font-size:10px;letter-spacing:0.18em;text-transform:uppercase;color:{COLORS['ink_muted']};padding-bottom:8px;">FATOS-CHAVE</td></tr>
                        {bullets}
                      </table>
                    </td></tr>"""

            # Feedback +/-
            fb_btns = ""
            if n.get("fb_more_url") and n.get("fb_less_url"):
                fb_btns = f"""<tr><td style="padding-top:14px;border-top:1px solid {COLORS['line']};font-family:{SANS_FONT};font-size:11px;font-weight:700;">
                  <a href="{_esc(n['fb_more_url'])}" style="color:{COLORS['mint_deep']};text-decoration:none;">＋ mais como essa</a>
                  &nbsp;&nbsp;
                  <a href="{_esc(n['fb_less_url'])}" style="color:{COLORS['red']};text-decoration:none;">— menos como essa</a>
                </td></tr>"""

            # Chip de viés político (apenas em notícias políticas)
            bias_chips = ""
            pol_bias = (n.get("pol_bias") or "").lower().strip()
            POL_LABELS = {
                "esq":     ("Esquerda", "#7C2D12"),
                "centro":  ("Centro",   "#334155"),
                "dir":     ("Direita",  "#1E3A8A"),
                "factual": ("Factual",  "#047857"),
            }
            if pol_bias in POL_LABELS:
                label, txt_color = POL_LABELS[pol_bias]
                bias_chips = f'<span style="display:inline-block;background:{COLORS["bg_2"]};border:1px solid {COLORS["line"]};color:{txt_color};font-family:{SANS_FONT};font-weight:800;font-size:10px;letter-spacing:0.04em;padding:3px 9px;margin-right:6px;">⚖ {label}</span>'

            # Chip de idioma — só aparece se NÃO for PT (default assumido)
            lang_chip = ""
            lang = (n.get("lang") or "").lower()
            lang_map = {
                "en": "🇺🇸 EN", "fr": "🇫🇷 FR", "de": "🇩🇪 DE",
                "es": "🇪🇸 ES", "it": "🇮🇹 IT", "ja": "🇯🇵 JA",
                "zh": "🇨🇳 ZH", "ar": "🇦🇪 AR", "ko": "🇰🇷 KO", "he": "🇮🇱 HE",
            }
            if lang in lang_map:
                lang_chip = f'<span style="display:inline-block;background:{COLORS["bg_2"]};color:{COLORS["ink_muted"]};font-family:{SANS_FONT};font-weight:700;font-size:10px;letter-spacing:0.06em;padding:2px 7px;margin-left:8px;border:1px solid {COLORS["line"]};">{lang_map[lang]}</span>'

            noticias_html += f"""
            <tr><td style="padding:0 0 32px 0;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr><td style="font-family:{SERIF_FONT};font-weight:700;font-size:{'22' if is_espresso else '26'}px;line-height:1.18;color:{COLORS['ink']};letter-spacing:-0.02em;padding-bottom:{'8' if is_espresso else '14'}px;">{_esc(n.get('manchete',''))}</td></tr>
                <tr><td style="font-family:{SANS_FONT};font-size:{'14' if is_espresso else '16'}px;line-height:1.6;color:{COLORS['ink_soft']};padding-bottom:{'10' if is_espresso else '16'}px;">{_esc(resumo_display)}</td></tr>
                {fatos_html}
                <tr><td style="font-family:{SANS_FONT};font-size:12px;color:{COLORS['ink_muted']};padding-bottom:8px;">
                  <a href="{_esc(n.get('link','#'))}" style="color:{COLORS['ink']};text-decoration:none;font-weight:800;border-bottom:2.5px solid {COLORS['mint_deep']};padding-bottom:1px;margin-right:12px;">Ler matéria →</a>
                  <span style="color:{COLORS['ink']};font-weight:800;">{_esc(n.get('fonte','') or 'Fonte')}</span>
                  {lang_chip}
                </td></tr>
                <tr><td style="padding-top:8px;padding-bottom:4px;">{bias_chips}</td></tr>
                {fb_btns}
              </table>
            </td></tr>"""

        out += f"""
        <tr><td style="padding:0 36px;" id="tema-{slug}">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:8px;border-bottom:4px solid {COLORS['bg_2']};">
            <tr><td style="padding:28px 0 4px 0;">
              <!-- Linha de corte tracejada + tesoura — separador editorial entre capítulos -->
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr><td style="border-top:3px dashed {COLORS['ink']};font-size:0;line-height:0;">&nbsp;</td></tr>
                <tr><td align="center" style="line-height:1;">
                  <div style="display:inline-block;background:{COLORS['bg']};margin-top:-19px;padding:0 18px;font-size:30px;color:{COLORS['mint_deep']};line-height:1;">✂</div>
                </td></tr>
              </table>
              <div style="font-family:{MONO_FONT};font-size:9px;letter-spacing:0.3em;color:{COLORS['mint_dark']};text-transform:uppercase;text-align:right;margin-top:4px;font-weight:700;">recortado pra você</div>
            </td></tr>
            <tr><td style="padding:8px 0 18px 0;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr>
                  <td><table role="presentation" cellpadding="0" cellspacing="0" border="0"><tr>
                    <td style="background:{COLORS['ink']};border:1.5px solid {COLORS['ink']};padding:4px 12px;font-family:{SANS_FONT};font-size:11px;font-weight:800;letter-spacing:0.18em;text-transform:uppercase;color:{COLORS['mint']};">{_get_topic_icon(sec['topic'])} {_esc(sec['topic'])}</td>
                    <td>{country_chip}</td>
                  </tr></table></td>
                  {pause_btn}
                </tr>
              </table>
            </td></tr>
            <tr><td style="padding-top:10px;"><table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">{noticias_html}</table></td></tr>
          </table>
        </td></tr>"""
    return out


def _render_toc(trending, sections, position="top"):
    """
    Índice navegável. position='top' (estilo destaque verde-menta) ou 'bottom' (compacto, marinho).
    Top: chips brancos com contagem.
    Bottom: chips translúcidos sobre marinho.
    """
    if not trending and not sections:
        return ""

    if position == "top":
        return _render_toc_top(trending, sections)
    return _render_toc_bottom(trending, sections)


def _render_toc_top(trending, sections):
    chips = []
    if trending:
        chips.append(
            f'<a href="#em-alta" style="display:inline-block;background:{COLORS["mint_dark"]};color:#FFF;text-decoration:none;'
            f'padding:9px 13px;font-family:{SANS_FONT};font-size:11px;font-weight:800;letter-spacing:0.08em;'
            f'text-transform:uppercase;margin:3px 5px 3px 0;border:2px solid {COLORS["mint_dark"]};">'
            f'🔥 Em Alta <span style="color:{COLORS["mint"]};font-weight:700;">· {len(trending)}</span></a>'
        )
    for idx, sec in enumerate(sections):
        slug = _slugify(sec.get("topic", f"tema-{idx}"))
        label = sec.get("topic", "")
        count = len(sec.get("noticias", []))
        count_html = f' <span style="color:{COLORS["mint_dark"]};font-weight:700;">· {count}</span>' if count else ''
        chips.append(
            f'<a href="#tema-{slug}" style="display:inline-block;background:#FFFFFF;color:{COLORS["ink"]};text-decoration:none;'
            f'padding:9px 13px;font-family:{SANS_FONT};font-size:11px;font-weight:800;letter-spacing:0.08em;'
            f'text-transform:uppercase;margin:3px 5px 3px 0;border:2px solid {COLORS["ink"]};">'
            f'{_esc(label)}{count_html}</a>'
        )

    chips_html = "".join(chips)
    return f"""
        <tr><td style="padding:0 36px 28px 36px;" id="topo">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:{COLORS['mint_bg_light']};border:2.5px solid {COLORS['mint_dark']};">
            <tr><td style="padding:18px 20px;">
              <div style="font-family:{SANS_FONT};font-size:10px;font-weight:800;letter-spacing:0.22em;text-transform:uppercase;color:{COLORS['mint_dark']};margin-bottom:10px;">📑 Navegação rápida</div>
              <div>{chips_html}</div>
            </td></tr>
          </table>
        </td></tr>"""


def _render_toc_bottom(trending, sections):
    """TOC compacto no rodapé, fundo marinho, chips translúcidos."""
    chips = []
    if trending:
        chips.append(
            f'<a href="#em-alta" style="display:inline-block;background:transparent;color:#FFFFFF;text-decoration:none;'
            f'padding:6px 11px;font-family:{SANS_FONT};font-size:11px;font-weight:700;letter-spacing:0.08em;'
            f'text-transform:uppercase;margin:3px 4px 3px 0;border:1.5px solid {COLORS["mint"]};">'
            f'🔥 Em Alta</a>'
        )
    for idx, sec in enumerate(sections):
        slug = _slugify(sec.get("topic", f"tema-{idx}"))
        label = sec.get("topic", "")
        chips.append(
            f'<a href="#tema-{slug}" style="display:inline-block;background:transparent;color:#FFFFFF;text-decoration:none;'
            f'padding:6px 11px;font-family:{SANS_FONT};font-size:11px;font-weight:700;letter-spacing:0.08em;'
            f'text-transform:uppercase;margin:3px 4px 3px 0;border:1.5px solid rgba(255,255,255,0.4);">'
            f'{_esc(label)}</a>'
        )

    chips_html = "".join(chips)
    return f"""
        <tr><td style="padding:0 36px 28px 36px;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:{COLORS['ink']};">
            <tr><td style="padding:16px 20px;">
              <div style="font-family:{SANS_FONT};font-size:10px;font-weight:800;letter-spacing:0.22em;text-transform:uppercase;color:{COLORS['mint']};margin-bottom:9px;">📑 Pula pra outra seção</div>
              <div>{chips_html}</div>
            </td></tr>
          </table>
        </td></tr>"""


# ============================================================================
# MAIN RENDER
# ============================================================================
def render_email(user_name, date_obj, trending=None, trending_label="",
                 sections=None, manage_url="#", tts_url=None, tts_duration=None,
                 user_id=None, daily_recap=None,
                 daily_quote="", daily_quote_author="",
                 email_mode="coado", weekly_mode=False):
    """
    Renderiza o HTML completo do email diário.

    Args:
        user_name: nome do usuário (usa primeiro nome na saudação)
        date_obj: datetime do envio
        trending: list[dict] com termo/contexto/buscas/link/fonte
        trending_label: label da seção trending (ex: "cenário global · 5 termos")
        sections: list[dict] com topic/country_label/noticias/fb_pause_url
        manage_url: URL pra ajustar preferências (com tokens)
        tts_url: URL opcional do áudio TTS (se ausente, esconde player)
        tts_duration: string tipo "5:32" (opcional)
        email_mode: "coado" (default, análise completa) ou "espresso" (manchete + 1 frase)
    """
    trending = trending or []
    sections = sections or []
    first_name = user_name.split()[0] if user_name else "leitor"
    email_mode = (email_mode or "coado").lower()
    if email_mode not in ("coado", "espresso"):
        email_mode = "coado"

    meses = ["janeiro","fevereiro","março","abril","maio","junho",
             "julho","agosto","setembro","outubro","novembro","dezembro"]
    dias = ["segunda-feira","terça-feira","quarta-feira","quinta-feira",
            "sexta-feira","sábado","domingo"]
    weekday = dias[date_obj.weekday()]
    date_short = date_obj.strftime("%d/%m/%Y")
    issue_num = f"{date_obj.timetuple().tm_yday}"

    if date_obj.hour < 12:
        saudacao = "Bom dia"
    elif date_obj.hour < 18:
        saudacao = "Boa tarde"
    else:
        saudacao = "Boa noite"

    total_noticias = sum(len(s["noticias"]) for s in sections)
    # intro_count: usado SÓ no H1 do hero — mostra apenas notícias (mais limpo)
    if total_noticias:
        intro_count = f"{total_noticias} notícia{'s' if total_noticias != 1 else ''}"
    else:
        intro_count = "novidades"

    stat_noticias = total_noticias or 0
    stat_trending = len(trending)
    stat_temas = len(sections)
    # Minutos: ~25s/manchete espresso, ~50s/manchete coado
    secs_each = 25 if email_mode == "espresso" else 50
    stat_minutos = max(2, round((stat_noticias + stat_trending) * secs_each / 60))

    # Textos do hero — variam entre daily e weekly
    if weekly_mode:
        hero_h1 = f'<span style="background:linear-gradient(180deg,transparent 60%,{COLORS["mint"]} 60%);padding:0 2px;">Sua semana</span><br/>em {intro_count}.'
        hero_subtitle = "Os fatos que marcaram seus temas nos últimos 7 dias, com análise mais profunda. Bom fim de semana."
        mode_badge = "🗞 RECORTE DA SEMANA"
    else:
        hero_h1 = f'Hoje em <span style="background:linear-gradient(180deg,transparent 60%,{COLORS["mint"]} 60%);padding:0 2px;">{intro_count}</span><br/>só pra você.'
        hero_subtitle = "A cada toque em ＋ ou —, eu aprendo o que importa pra você. Bom café."
        mode_badge = "⚡ ESPRESSO" if email_mode == "espresso" else "☕ CAFÉ COADO"

    # TTS player (opcional)
    tts_html = ""
    if tts_url:
        duration_str = tts_duration or "—"
        tts_html = f"""
        <tr><td style="background:{COLORS['yellow']};padding:14px 36px;border-bottom:2px solid {COLORS['ink']};">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>
            <td width="38" valign="middle" style="padding-right:14px;">
              <a href="{_esc(tts_url)}" style="text-decoration:none;">
                <div style="width:38px;height:38px;background:{COLORS['ink']};color:{COLORS['yellow']};border-radius:50%;text-align:center;line-height:38px;font-family:{SANS_FONT};font-size:14px;font-weight:700;">▶</div>
              </a>
            </td>
            <td valign="middle">
              <div style="font-family:{SERIF_FONT};font-weight:700;font-size:14px;color:{COLORS['ink']};letter-spacing:-0.005em;">Ouça esta edição</div>
              <div style="font-family:{SANS_FONT};font-size:11px;color:{COLORS['ink']};opacity:0.7;font-weight:500;">narrada por uma voz IA · personalizada pra você</div>
            </td>
            <td valign="middle" align="right" style="font-family:{MONO_FONT};font-size:12px;color:{COLORS['ink']};font-weight:600;background:rgba(10,37,64,0.1);padding:4px 8px;">{_esc(duration_str)}</td>
          </tr></table>
        </td></tr>"""

    trending_html = _render_trending_section(trending, trending_label, email_mode=email_mode)
    sections_html = _render_news_sections(sections, email_mode=email_mode)
    recap_html = _render_daily_recap(daily_recap)

    # Quote do dia — pequeno bloco editorial entre o hero e o recap
    quote_html = ""
    if daily_quote:
        author_html = ""
        if daily_quote_author:
            author_html = f'<div style="font-family:{SANS_FONT};font-size:11px;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;color:{COLORS["mint_dark"]};margin-top:10px;">— {_esc(daily_quote_author)}</div>'
        quote_html = f"""
        <tr><td style="padding:0 36px 24px;" class="px-mob">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:{COLORS['ink']};">
            <tr><td style="padding:22px 26px;">
              <div style="font-family:{SERIF_FONT};font-style:italic;font-weight:500;font-size:18px;line-height:1.4;color:{COLORS['mint']};letter-spacing:-0.01em;">
                {_esc(daily_quote)}
              </div>
              {author_html}
            </td></tr>
          </table>
        </td></tr>"""

    manage_link = manage_url

    google_fonts_link = '<link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,400;9..144,600;9..144,700;9..144,800;9..144,900&family=Mulish:wght@400;500;600;700;800&family=JetBrains+Mono:wght@500&display=swap" rel="stylesheet">'

    return f"""<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" lang="pt-BR"><head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="color-scheme" content="light">
<meta name="supported-color-schemes" content="light">
<title>Recorte ✂ · {date_short}</title>
{google_fonts_link}
<!--[if mso]><style type="text/css">body, table, td {{font-family: Georgia, 'Times New Roman', serif !important;}} .mso-sans {{font-family: Arial, Helvetica, sans-serif !important;}}</style><![endif]-->
<style>
  @media only screen and (max-width:600px){{
    .container {{ width:100% !important; max-width:100% !important; }}
    .px-mob {{ padding-left:20px !important; padding-right:20px !important; }}
    .hero-h1 {{ font-size:36px !important; line-height:1.05 !important; }}
    .stat-num {{ font-size:20px !important; }}
  }}
</style>
</head>
<body style="margin:0;padding:0;background:{COLORS['bg']};font-family:{SANS_FONT};-webkit-font-smoothing:antialiased;">

<!-- preheader: visível no preview do inbox -->
<div style="display:none;font-size:1px;color:{COLORS['bg']};line-height:1px;max-height:0;max-width:0;opacity:0;overflow:hidden;">
  {saudacao}, {_esc(first_name)}. {"Sua semana em " + intro_count + " — antes do café." if weekly_mode else "Hoje em " + intro_count + " só pra você — em 5 minutos."}
</div>

<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:{COLORS['bg']};">
  <tr><td align="center" style="padding:24px 16px;">
    <table role="presentation" class="container" width="640" cellpadding="0" cellspacing="0" border="0" style="max-width:640px;background:{COLORS['bg']};">

      <!-- MASTHEAD verde-menta -->
      <tr><td style="background:{COLORS['mint']};padding:26px 36px 22px;" class="px-mob">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>
          <td valign="middle">
            <table role="presentation" cellpadding="0" cellspacing="0" border="0"><tr>
              <td valign="middle" style="padding-right:14px;">
                <div style="width:42px;height:42px;background:{COLORS['bg']};border:2.5px solid {COLORS['ink']};border-radius:50%;text-align:center;line-height:38px;font-family:{SERIF_FONT};font-style:italic;font-weight:700;font-size:24px;color:{COLORS['ink']};letter-spacing:-0.05em;">r<span style="color:{COLORS['mint_deep']};">.</span></div>
              </td>
              <td valign="middle" style="font-family:{SERIF_FONT};font-weight:700;font-size:30px;letter-spacing:-0.025em;color:{COLORS['ink']};">Recorte</td>
            </tr></table>
          </td>
          <td valign="middle" align="right" style="font-family:{SERIF_FONT};font-style:italic;font-size:13px;color:{COLORS['mint_dark']};">
            {weekday},<br/><strong style="color:{COLORS['ink']};font-style:normal;font-weight:600;">{date_obj.day} de {meses[date_obj.month-1]}</strong>
            <div style="font-family:{MONO_FONT};font-size:10px;letter-spacing:0.18em;text-transform:uppercase;color:{COLORS['mint_dark']};margin-top:4px;font-weight:500;font-style:normal;">EDIÇÃO Nº {issue_num} · {mode_badge}</div>
          </td>
        </tr></table>
      </td></tr>

      {_render_tricolor_band()}

      {tts_html}

      <!-- HERO -->
      <tr><td style="padding:44px 36px 28px;" class="px-mob">
        <div style="font-family:{SERIF_FONT};font-style:italic;font-size:15px;color:{COLORS['mint_dark']};margin-bottom:12px;">— {saudacao}, {_esc(first_name)}.</div>
        <h1 class="hero-h1" style="font-family:{SERIF_FONT};font-weight:900;font-size:44px;line-height:1.0;letter-spacing:-0.04em;color:{COLORS['ink']};margin:0 0 18px 0;">{hero_h1}</h1>
        <p style="font-family:{SANS_FONT};font-size:16px;line-height:1.55;color:{COLORS['ink_soft']};margin:0 0 24px 0;max-width:520px;">{hero_subtitle}</p>

        <!-- Quick stats bar -->
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-top:1px solid {COLORS['line']};border-bottom:1px solid {COLORS['line']};">
          <tr>
            <td align="center" style="padding:14px 4px;">
              <div class="stat-num" style="font-family:{SERIF_FONT};font-weight:800;font-size:24px;color:{COLORS['mint_dark']};line-height:1;letter-spacing:-0.02em;">{stat_noticias}</div>
              <div style="font-family:{SANS_FONT};font-size:10px;font-weight:600;letter-spacing:0.1em;text-transform:uppercase;color:{COLORS['ink_muted']};margin-top:5px;">Notícias</div>
            </td>
            <td width="1" style="background:{COLORS['line']};">&nbsp;</td>
            <td align="center" style="padding:14px 4px;">
              <div class="stat-num" style="font-family:{SERIF_FONT};font-weight:800;font-size:24px;color:{COLORS['mint_dark']};line-height:1;letter-spacing:-0.02em;">{stat_trending}</div>
              <div style="font-family:{SANS_FONT};font-size:10px;font-weight:600;letter-spacing:0.1em;text-transform:uppercase;color:{COLORS['ink_muted']};margin-top:5px;">Em alta</div>
            </td>
            <td width="1" style="background:{COLORS['line']};">&nbsp;</td>
            <td align="center" style="padding:14px 4px;">
              <div class="stat-num" style="font-family:{SERIF_FONT};font-weight:800;font-size:24px;color:{COLORS['mint_dark']};line-height:1;letter-spacing:-0.02em;">{stat_temas}</div>
              <div style="font-family:{SANS_FONT};font-size:10px;font-weight:600;letter-spacing:0.1em;text-transform:uppercase;color:{COLORS['ink_muted']};margin-top:5px;">Temas seus</div>
            </td>
            <td width="1" style="background:{COLORS['line']};">&nbsp;</td>
            <td align="center" style="padding:14px 4px;">
              <div class="stat-num" style="font-family:{SERIF_FONT};font-weight:800;font-size:24px;color:{COLORS['mint_dark']};line-height:1;letter-spacing:-0.02em;">{stat_minutos}'</div>
              <div style="font-family:{SANS_FONT};font-size:10px;font-weight:600;letter-spacing:0.1em;text-transform:uppercase;color:{COLORS['ink_muted']};margin-top:5px;">Leitura</div>
            </td>
          </tr>
        </table>
      </td></tr>

      {quote_html}
      {recap_html}
      {trending_html}
      {sections_html}

      {_render_tricolor_band()}

      <!-- SIGN OFF verde-menta -->
      <tr><td style="background:{COLORS['mint']};padding:36px 36px 30px;text-align:center;" class="px-mob">
        <div style="font-family:{SERIF_FONT};font-style:italic;font-size:19px;line-height:1.5;color:{COLORS['ink']};margin-bottom:20px;padding:0 20px;">
          <span style="color:{COLORS['mint_dark']};font-size:24px;font-weight:700;vertical-align:-8px;">“</span>A notícia certa, na hora certa, é o melhor café da manhã. ☕<span style="color:{COLORS['mint_dark']};font-size:24px;font-weight:700;vertical-align:-8px;">”</span>
        </div>
        <div style="font-family:{SANS_FONT};font-weight:800;font-size:11px;letter-spacing:0.22em;text-transform:uppercase;color:{COLORS['mint_dark']};">— Recorte ✂ &nbsp;·&nbsp; até amanhã às 6h</div>
      </td></tr>

      <!-- FOOTER -->
      <tr><td style="background:{COLORS['bg_2']};padding:24px 36px;text-align:center;" class="px-mob">
        <div style="font-family:{SANS_FONT};font-size:11px;color:{COLORS['ink_muted']};line-height:1.7;">
          Você está recebendo porque se cadastrou em <strong style="color:{COLORS['ink']};">Recorte ✂</strong>.<br/>
          <a href="{_esc(manage_link)}" style="color:{COLORS['ink_soft']};text-decoration:underline;font-weight:700;">⚙ Ajustar minhas preferências</a>
          <br/><br/>
          <span style="font-size:10px;color:{COLORS['ink_muted']};opacity:0.8;">Curadoria editorial por agentes de IA especialistas · 200+ fontes brasileiras e internacionais · Conteúdo de terceiros. Direitos reservados aos veículos originais.</span>
          <div style="margin-top:10px;font-family:{MONO_FONT};font-size:9px;letter-spacing:0.18em;text-transform:uppercase;color:{COLORS['ink_muted']};opacity:0.6;">Última coleta · {date_obj.strftime('%d/%m %H:%M')} BRT</div>
        </div>
      </td></tr>

    </table>
  </td></tr>
</table>
</body></html>"""
