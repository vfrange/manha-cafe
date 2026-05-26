"""
Renderiza o HTML do email diário do Recorte ✂.
V4: identidade visual verde-menta dominante + Fraunces + Mulish.
"""

import html as html_lib
import re
import unicodedata


def _svg(path_d):
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="14" height="14" '
        'fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" '
        f'stroke-linejoin="round" style="vertical-align:-2px;margin-right:6px;">{path_d}</svg>'
    )

TOPIC_ICONS = {
    "economia": _svg('<path d="M3 21V11M9 21V7M15 21V13M21 21V4"/><line x1="3" y1="21" x2="21" y2="21"/>'),
    "mercado":  _svg('<polyline points="3 17 9 11 13 15 21 7"/><polyline points="14 7 21 7 21 14"/>'),
    "tech":     _svg('<rect x="4" y="4" width="16" height="16" rx="2"/><rect x="9" y="9" width="6" height="6"/><line x1="9" y1="2" x2="9" y2="4"/><line x1="15" y1="2" x2="15" y2="4"/><line x1="9" y1="20" x2="9" y2="22"/><line x1="15" y1="20" x2="15" y2="22"/><line x1="2" y1="9" x2="4" y2="9"/><line x1="2" y1="15" x2="4" y2="15"/><line x1="20" y1="9" x2="22" y2="9"/><line x1="20" y1="15" x2="22" y2="15"/>'),
    "geopol":   _svg('<circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15 15 0 0 1 4 10 15 15 0 0 1-4 10 15 15 0 0 1-4-10 15 15 0 0 1 4-10z"/>'),
    "polit":    _svg('<line x1="3" y1="22" x2="21" y2="22"/><line x1="3" y1="18" x2="21" y2="18"/><line x1="6" y1="18" x2="6" y2="6"/><line x1="12" y1="18" x2="12" y2="6"/><line x1="18" y1="18" x2="18" y2="6"/><polyline points="3 6 12 2 21 6"/>'),
    "governo":  _svg('<line x1="3" y1="22" x2="21" y2="22"/><line x1="3" y1="18" x2="21" y2="18"/><line x1="6" y1="18" x2="6" y2="6"/><line x1="12" y1="18" x2="12" y2="6"/><line x1="18" y1="18" x2="18" y2="6"/><polyline points="3 6 12 2 21 6"/>'),
    "food":     _svg('<path d="M3 11h18a0 0 0 0 1 0 0 7 7 0 0 1-7 7h-4a7 7 0 0 1-7-7"/><path d="M21 8H3a9 9 0 0 1 9-6 9 9 0 0 1 9 6z"/><line x1="6" y1="15" x2="6.01" y2="15"/><line x1="10" y1="15" x2="10.01" y2="15"/><line x1="14" y1="15" x2="14.01" y2="15"/>'),
    "hamburg":  _svg('<path d="M3 11h18a0 0 0 0 1 0 0 7 7 0 0 1-7 7h-4a7 7 0 0 1-7-7"/><path d="M21 8H3a9 9 0 0 1 9-6 9 9 0 0 1 9 6z"/><line x1="6" y1="15" x2="6.01" y2="15"/><line x1="10" y1="15" x2="10.01" y2="15"/><line x1="14" y1="15" x2="14.01" y2="15"/>'),
    "varejo":   _svg('<path d="M6 2 3 6v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V6l-3-4z"/><line x1="3" y1="6" x2="21" y2="6"/><path d="M16 10a4 4 0 0 1-8 0"/>'),
    "negocio":  _svg('<path d="M11 17l-5-5 5-5"/><path d="M13 7l5 5-5 5"/><line x1="6" y1="12" x2="18" y2="12"/>'),
    "fusoes":   _svg('<path d="M11 17l-5-5 5-5"/><path d="M13 7l5 5-5 5"/><line x1="6" y1="12" x2="18" y2="12"/>'),
    "imob":     _svg('<path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/>'),
    "esport":   _svg('<circle cx="12" cy="12" r="10"/><path d="M12 2L9 7l6 0z"/><path d="M22 12l-5-3 0 6z"/><path d="M12 22l3-5-6 0z"/><path d="M2 12l5 3 0-6z"/>'),
    "futebol":  _svg('<circle cx="12" cy="12" r="10"/><path d="M12 2L9 7l6 0z"/><path d="M22 12l-5-3 0 6z"/><path d="M12 22l3-5-6 0z"/><path d="M2 12l5 3 0-6z"/>'),
    "spfc":     _svg('<circle cx="12" cy="12" r="10"/><path d="M12 2L9 7l6 0z"/><path d="M22 12l-5-3 0 6z"/><path d="M12 22l3-5-6 0z"/><path d="M2 12l5 3 0-6z"/>'),
    "selecao":  _svg('<circle cx="12" cy="12" r="10"/><path d="M12 2L9 7l6 0z"/><path d="M22 12l-5-3 0 6z"/><path d="M12 22l3-5-6 0z"/><path d="M2 12l5 3 0-6z"/>'),
    "copa":     _svg('<path d="M6 9H4.5a2.5 2.5 0 0 1 0-5H6"/><path d="M18 9h1.5a2.5 2.5 0 0 0 0-5H18"/><path d="M4 22h16"/><path d="M10 14.66V17c0 .55-.47.98-.97 1.21C7.85 18.75 7 20.24 7 22"/><path d="M14 14.66V17c0 .55.47.98.97 1.21C16.15 18.75 17 20.24 17 22"/><path d="M18 2H6v7a6 6 0 0 0 12 0V2Z"/>'),
    "cultura":  _svg('<polygon points="3 8 21 8 21 20 3 20 3 8"/><polyline points="3 8 5 4 9 4 7 8 11 8 9 4 13 4 11 8 15 8 13 4 17 4 15 8 19 8 17 4 21 4"/>'),
    "entretenimento": _svg('<polygon points="3 8 21 8 21 20 3 20 3 8"/><polyline points="3 8 5 4 9 4 7 8 11 8 9 4 13 4 11 8 15 8 13 4 17 4 15 8 19 8 17 4 21 4"/>'),
    "ciencia":  _svg('<path d="M4 3h16"/><path d="M4 21h16"/><path d="M4 8c4 3 12 3 16 0"/><path d="M4 16c4-3 12-3 16 0"/>'),
    "saude":    _svg('<path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"/>'),
    "pesquisa": _svg('<path d="M4 3h16"/><path d="M4 21h16"/><path d="M4 8c4 3 12 3 16 0"/><path d="M4 16c4-3 12-3 16 0"/>'),
    "sustent":  _svg('<path d="M11 20A7 7 0 0 1 9.8 6.1C15.5 5 17 4.48 19.2 2.96c.8 3.69 1.32 7.07.36 11.36-1.04 4.69-4.55 7.06-8.56 5.68z"/><path d="M2 21c0-3 1.85-5.36 5.08-6"/>'),
    "esg":      _svg('<path d="M11 20A7 7 0 0 1 9.8 6.1C15.5 5 17 4.48 19.2 2.96c.8 3.69 1.32 7.07.36 11.36-1.04 4.69-4.55 7.06-8.56 5.68z"/><path d="M2 21c0-3 1.85-5.36 5.08-6"/>'),
    "clima":    _svg('<path d="M11 20A7 7 0 0 1 9.8 6.1C15.5 5 17 4.48 19.2 2.96c.8 3.69 1.32 7.07.36 11.36-1.04 4.69-4.55 7.06-8.56 5.68z"/><path d="M2 21c0-3 1.85-5.36 5.08-6"/>'),
    "auto":     _svg('<path d="M16 3h-8a2 2 0 0 0-2 2v5h12V5a2 2 0 0 0-2-2z"/><path d="M3 14v3a1 1 0 0 0 1 1h2"/><circle cx="7" cy="17" r="2"/><circle cx="17" cy="17" r="2"/><path d="M19 17h2a1 1 0 0 0 1-1v-3l-2-3h-4"/>'),
    "mobilidade": _svg('<path d="M16 3h-8a2 2 0 0 0-2 2v5h12V5a2 2 0 0 0-2-2z"/><path d="M3 14v3a1 1 0 0 0 1 1h2"/><circle cx="7" cy="17" r="2"/><circle cx="17" cy="17" r="2"/><path d="M19 17h2a1 1 0 0 0 1-1v-3l-2-3h-4"/>'),
    "educacao": _svg('<path d="M22 10v6M2 10l10-5 10 5-10 5z"/><path d="M6 12v5c3 3 9 3 12 0v-5"/>'),
    "ensino":   _svg('<path d="M22 10v6M2 10l10-5 10 5-10 5z"/><path d="M6 12v5c3 3 9 3 12 0v-5"/>'),
    "trabalho": _svg('<rect x="2" y="7" width="20" height="14" rx="2" ry="2"/><path d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16"/>'),
    "carreira": _svg('<rect x="2" y="7" width="20" height="14" rx="2" ry="2"/><path d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16"/>'),
    "rh":       _svg('<rect x="2" y="7" width="20" height="14" rx="2" ry="2"/><path d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16"/>'),
    "startup":  _svg('<path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/><path d="M12 15l-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/><path d="M9 12H4s.55-3.03 2-4c1.62-1.08 5 0 5 0"/><path d="M12 15v5s3.03-.55 4-2c1.08-1.62 0-5 0-5"/>'),
    "starups":  _svg('<path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/><path d="M12 15l-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/><path d="M9 12H4s.55-3.03 2-4c1.62-1.08 5 0 5 0"/><path d="M12 15v5s3.03-.55 4-2c1.08-1.62 0-5 0-5"/>'),
    "consumo":  _svg('<path d="M6 2 3 6v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V6l-3-4z"/><line x1="3" y1="6" x2="21" y2="6"/><path d="M16 10a4 4 0 0 1-8 0"/>'),
    "marcas":   _svg('<path d="M6 2 3 6v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V6l-3-4z"/><line x1="3" y1="6" x2="21" y2="6"/><path d="M16 10a4 4 0 0 1-8 0"/>'),
    "marketing":_svg('<path d="M6 2 3 6v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V6l-3-4z"/><line x1="3" y1="6" x2="21" y2="6"/><path d="M16 10a4 4 0 0 1-8 0"/>'),
    "curiosidade": _svg('<path d="M9 18h6"/><path d="M10 22h4"/><path d="M15.09 14c.18-.98.65-1.74 1.41-2.5A4.65 4.65 0 0 0 18 8 6 6 0 0 0 6 8c0 1 .23 2.23 1.5 3.5A4.61 4.61 0 0 1 8.91 14"/>'),
}

DEFAULT_TOPIC_ICON = _svg(
    '<circle cx="6" cy="6" r="3"/>'
    '<circle cx="6" cy="18" r="3"/>'
    '<line x1="20" y1="4" x2="8.12" y2="15.88"/>'
    '<line x1="14.47" y1="14.48" x2="20" y2="20"/>'
    '<line x1="8.12" y1="8.12" x2="12" y2="12"/>'
)


def _get_topic_icon(label):
    if not label:
        return DEFAULT_TOPIC_ICON
    label_norm = label.lower().strip()
    label_norm = "".join(c for c in unicodedata.normalize("NFKD", label_norm)
                        if not unicodedata.combining(c))
    for key, svg in TOPIC_ICONS.items():
        if key in label_norm:
            return svg
    return DEFAULT_TOPIC_ICON


COLORS = {
    "mint":          "#6EE7B7",
    "mint_bg_light": "#D1FAE5",
    "mint_deep":     "#10B981",
    "mint_dark":     "#047857",
    "yellow":        "#FFD60A",
    "yellow_bg":     "#FFF5BD",
    "ink":           "#0A2540",
    "ink_soft":      "#4A5568",
    "ink_muted":     "#8A95A8",
    "bg":            "#FFFAF0",
    "bg_2":          "#F4F1EA",
    "line":          "#E8E1D0",
    "red":           "#BE1622",
}

SERIF_FONT = "'Gambarino', Georgia, 'Times New Roman', serif"
SANS_FONT = "'General Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"
SANS_DISPLAY = "'Switzer', -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"
MONO_FONT = "'JetBrains Mono', 'SF Mono', Monaco, Consolas, monospace"


def _esc(s):
    return html_lib.escape(s or "")


def _safe_url(url, fallback="#"):
    if not url or not isinstance(url, str):
        return fallback
    url_clean = url.strip()
    if not url_clean:
        return fallback
    allowed_prefixes = ('http://', 'https://', 'mailto:')
    if not any(url_clean.lower().startswith(p) for p in allowed_prefixes):
        return fallback
    return html_lib.escape(url_clean)


def _render_inline_html(text):
    """Escapa HTML mas mantém <strong> e <em>, e estiliza <strong> em verde-menta
    + underline (MUDANÇA #4 — links visuais inspirados no LAIOB).

    O Claude curador é instruído (em daily_digest.py) a marcar 2-3 termos importantes
    do resumo com <strong>...</strong>. Esses termos viram destaques visuais coloridos
    sem virarem hyperlinks navegáveis (não há URL inline — apenas estilização).
    """
    if not text:
        return ""
    safe = html_lib.escape(text)
    # Desfaz só as tags permitidas inline
    safe = safe.replace("&lt;strong&gt;", "<strong>").replace("&lt;/strong&gt;", "</strong>")
    safe = safe.replace("&lt;em&gt;", "<em>").replace("&lt;/em&gt;", "</em>")
    # Aplica style inline em verde-menta + underline nos <strong>
    safe = re.sub(
        r"<strong>([^<]+)</strong>",
        rf'<strong style="color:{COLORS["mint_deep"]};text-decoration:underline;text-decoration-thickness:1.5px;text-underline-offset:2px;font-weight:600;">\1</strong>',
        safe,
    )
    return safe


def _render_tricolor_band():
    return f"""<tr><td height="4" style="line-height:0;font-size:0;padding:0;">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>
        <td width="33%" height="4" bgcolor="{COLORS['mint_deep']}" style="background:{COLORS['mint_deep']};line-height:0;font-size:0;">&nbsp;</td>
        <td width="33%" height="4" bgcolor="{COLORS['yellow']}" style="background:{COLORS['yellow']};line-height:0;font-size:0;">&nbsp;</td>
        <td width="34%" height="4" bgcolor="{COLORS['ink']}" style="background:{COLORS['ink']};line-height:0;font-size:0;">&nbsp;</td>
      </tr></table>
    </td></tr>"""


def _render_welcome_block():
    yellow_bg = COLORS.get("yellow_bg", "#FFF5BD")
    mint_bg = COLORS.get("mint_bg_light", "#D1FAE5")
    return f"""<tr><td style="padding:32px 36px 0;" class="px-mob">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:{yellow_bg};background:linear-gradient(135deg,{yellow_bg} 0%,{mint_bg} 100%);border:2px solid {COLORS['ink']};box-shadow:6px 6px 0 {COLORS['ink']};">
    <tr><td style="padding:36px 32px;" class="px-mob">
      <table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:18px;"><tr>
        <td style="background:{COLORS['ink']};border:1.5px solid {COLORS['ink']};padding:5px 12px;font-family:{SANS_FONT};font-size:10px;font-weight:800;letter-spacing:0.22em;text-transform:uppercase;color:{COLORS['mint']};"><span style="color:{COLORS['mint']};">✂︎</span> Sua primeira edição</td>
      </tr></table>
      <h1 style="font-family:{SERIF_FONT};font-weight:400;font-style:italic;font-size:38px;line-height:1.05;letter-spacing:-0.03em;color:{COLORS['ink']};margin:0 0 14px 0;">
        Você acabou de <em style="font-style:italic;font-weight:700;color:{COLORS['mint_dark']};">cortar</em><br/>
        <span style="background:linear-gradient(180deg,transparent 65%,{COLORS['yellow']} 65%);padding:0 3px;">o ruído.</span>
      </h1>
      <p style="font-family:{SERIF_FONT};font-weight:400;font-style:italic;font-size:18px;line-height:1.5;color:{COLORS['ink_soft']};margin:0 0 24px 0;">
        Daqui pra frente, notícia é feita <strong style="color:{COLORS['ink']};font-weight:700;">pra você</strong>. Não pra "todo mundo".
      </p>
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:20px;table-layout:fixed;">
        <tr>
          <td width="32%" height="118" style="background:#fff;border:1.5px solid {COLORS['ink']};padding:18px 12px;text-align:center;vertical-align:top;mso-line-height-rule:exactly;min-height:118px;" class="feat-mob">
            <div class="feat-emoji" style="font-size:26px;line-height:1;margin-bottom:8px;">☕</div>
            <div class="feat-title" style="font-family:{SANS_FONT};font-size:11px;font-weight:800;letter-spacing:0.12em;text-transform:uppercase;color:{COLORS['ink']};margin-bottom:5px;">Toda manhã</div>
            <div class="feat-desc" style="font-family:{SANS_FONT};font-size:12px;line-height:1.35;color:{COLORS['ink_muted']};font-weight:500;">Antes do café, 6h</div>
          </td>
          <td width="2%" style="font-size:0;line-height:0;">&nbsp;</td>
          <td width="32%" height="118" style="background:#fff;border:1.5px solid {COLORS['ink']};padding:18px 12px;text-align:center;vertical-align:top;mso-line-height-rule:exactly;min-height:118px;" class="feat-mob">
            <div class="feat-emoji" style="font-size:26px;line-height:1;margin-bottom:8px;">🗞</div>
            <div class="feat-title" style="font-family:{SANS_FONT};font-size:11px;font-weight:800;letter-spacing:0.12em;text-transform:uppercase;color:{COLORS['ink']};margin-bottom:5px;">Aos domingos</div>
            <div class="feat-desc" style="font-family:{SANS_FONT};font-size:12px;line-height:1.35;color:{COLORS['ink_muted']};font-weight:500;">A semana, recortada</div>
          </td>
          <td width="2%" style="font-size:0;line-height:0;">&nbsp;</td>
          <td width="32%" height="118" style="background:#fff;border:1.5px solid {COLORS['ink']};padding:18px 12px;text-align:center;vertical-align:top;mso-line-height-rule:exactly;min-height:118px;" class="feat-mob">
            <div class="feat-emoji" style="font-size:26px;line-height:1;margin-bottom:8px;">🎛</div>
            <div class="feat-title" style="font-family:{SANS_FONT};font-size:11px;font-weight:800;letter-spacing:0.12em;text-transform:uppercase;color:{COLORS['ink']};margin-bottom:5px;">Você no controle</div>
            <div class="feat-desc" style="font-family:{SANS_FONT};font-size:12px;line-height:1.35;color:{COLORS['ink_muted']};font-weight:500;">Ajusta quando quiser</div>
          </td>
        </tr>
      </table>
      <div style="border-top:1px solid {COLORS['ink']};padding-top:18px;font-family:{SERIF_FONT};font-style:italic;font-size:15px;line-height:1.55;color:{COLORS['ink_soft']};">
        Cada manhã, a gente lê o mundo, cruza com os <strong style="font-style:normal;color:{COLORS['ink']};font-weight:700;">seus temas</strong>, tira o que você filtrou, e monta uma edição que só existe pra <strong style="font-style:normal;color:{COLORS['ink']};font-weight:700;">um leitor</strong> — você.
      </div>
    </td></tr>
  </table>
</td></tr>
<tr><td style="padding:24px 36px;" class="px-mob">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:separate;">
    <tr>
      <td valign="middle" style="padding-top:15px;line-height:0;font-size:0;">
        <div style="border-top:3px dashed {COLORS['ink']};font-size:0;line-height:0;height:0;">&nbsp;</div>
      </td>
      <td valign="middle" align="center" width="60" style="font-size:30px;color:{COLORS['mint_dark']};line-height:1;padding:0 8px;mso-line-height-rule:exactly;">✂︎</td>
      <td valign="middle" style="padding-top:15px;line-height:0;font-size:0;">
        <div style="border-top:3px dashed {COLORS['ink']};font-size:0;line-height:0;height:0;">&nbsp;</div>
      </td>
    </tr>
  </table>
</td></tr>"""


def _render_news_image(img_url, alt_text, topic_id=None, size_mode="hero"):
    if not img_url or not isinstance(img_url, str):
        return ""
    img_url_clean = img_url.strip()
    if not (img_url_clean.lower().startswith('http://') or
            img_url_clean.lower().startswith('https://')):
        return ""
    if size_mode == "thumb":
        width = 280
        height = 158
        img_class = "news-img-thumb"
        td_style = "padding:0 0 12px 0;font-size:0;line-height:0;text-align:center;"
        table_align = 'align="center"'
        table_width = str(width)
    else:
        width = 560
        height = 315
        img_class = "news-img"
        td_style = "padding:0 0 12px 0;font-size:0;line-height:0;"
        table_align = ""
        table_width = "100%"
    img_url_esc = _esc(img_url_clean)
    return f"""
    <tr><td style="{td_style}">
      <table role="presentation" width="{table_width}" cellpadding="0" cellspacing="0" border="0" {table_align}>
        <tr><td style="font-size:0;line-height:0;">
          <img src="{img_url_esc}" alt="" width="{width}" height="{height}" border="0"
               class="{img_class}"
               style="display:block;width:100%;max-width:{width}px;height:{height}px;object-fit:cover;object-position:center;border:0;border-radius:8px;outline:none;text-decoration:none;-ms-interpolation-mode:bicubic;" />
        </td></tr>
      </table>
    </td></tr>"""


def _render_trending_section(trending, scope_label, email_mode="coado"):
    if not trending:
        return ""
    items_html = ""
    for idx, item in enumerate(trending):
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
        resumo = _render_inline_html(resumo_show)
        fatos = item.get("fatos_chave") or []
        if is_espresso:
            fatos = []
        link = item.get("link", "")
        fonte = item.get("fonte", "")
        buscas = item.get("buscas", "")
        buscas_html = ""
        if buscas:
            buscas_html = f'<span style="display:inline-block;background:{COLORS["mint"]};color:{COLORS["ink"]};font-family:{SANS_FONT};font-weight:800;font-size:10px;letter-spacing:0.08em;text-transform:uppercase;padding:2px 8px;margin-bottom:10px;">↑ {_esc(buscas)}</span><br/>'
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
            link_html = f'<tr><td style="font-family:{SANS_FONT};font-size:12px;color:{COLORS["ink_muted"]};padding-bottom:8px;"><a href="{_safe_url(link)}" style="color:{COLORS["ink"]};text-decoration:none;font-weight:800;border-bottom:2.5px solid {COLORS["mint_deep"]};padding-bottom:1px;margin-right:6px;">Ler matéria →</a>{fonte_suffix}{lang_chip}</td></tr>'
        items_html += f"""
        <tr><td style="padding:0 0 20px 0;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border:1.5px solid {COLORS['mint']};border-radius:15px;background:#FFFFFF;">
            <tr><td style="padding:20px;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr><td style="padding-bottom:14px;"><div style="font-family:{SERIF_FONT};font-weight:700;font-style:italic;font-size:24px;line-height:1.22;color:{COLORS['ink']};letter-spacing:-0.015em;" class="dark-text">{manchete}</div></td></tr>
                {_render_news_image(item.get('img_url'), manchete, topic_id='trending') if item.get('img_url') else ''}
                <tr><td style="font-family:{SANS_FONT};font-size:15px;line-height:1.55;color:{COLORS['ink_soft']};padding-bottom:14px;{'padding-top:14px;' if item.get('img_url') else ''}" class="dark-text-soft">{buscas_html}{resumo}</td></tr>
                {fatos_html}
                {link_html}
              </table>
            </td></tr>
          </table>
        </td></tr>"""
    return f"""
    <tr><td style="padding:32px 36px 8px 36px;" class="px-mob">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:8px;border-bottom:4px solid {COLORS['bg_2']};">
        <tr><td style="padding:0 0 22px 0;">
          <table role="presentation" cellpadding="0" cellspacing="0" border="0"><tr>
            <td style="background:{COLORS['mint_dark']};padding:5px 12px;font-family:{SANS_FONT};font-size:11px;font-weight:800;letter-spacing:0.2em;text-transform:uppercase;color:{COLORS['mint']};">🔥 Em alta hoje</td>
          </tr></table>
        </td></tr>
        {items_html}
      </table>
    </td></tr>"""


def _render_daily_recap(recap_text):
    if not recap_text or not recap_text.strip():
        return ""
    return f"""
    <tr><td style="padding:0 36px 24px 36px;" class="px-mob">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:{COLORS['mint_bg_light']};border-left:6px solid {COLORS['mint_dark']};">
        <tr><td style="padding:22px 24px;">
          <div style="font-family:{SANS_FONT};font-size:11px;font-weight:800;letter-spacing:0.22em;text-transform:uppercase;color:{COLORS['mint_dark']};margin-bottom:12px;">☕ Seu dia em 60 segundos</div>
          <div style="font-family:{SERIF_FONT};font-size:16px;line-height:1.65;color:{COLORS['ink']};">{_esc(recap_text)}</div>
        </td></tr>
      </table>
    </td></tr>"""


def _slugify(text):
    s = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-zA-Z0-9]+", "-", s).strip("-").lower()
    return s or "tema"


def _render_news_sections(sections, email_mode="coado"):
    out = ""
    for idx, sec in enumerate(sections):
        # ====================================================================
        # GUARD: pula temas SEM NENHUMA notícia válida (com manchete + resumo).
        # ====================================================================
        # Sem isso, o cabeçalho do tema (nome, ícone, separador tesoura) ainda
        # renderiza mesmo com 0 notícias renderizáveis, deixando "esqueleto vazio".
        # Fix do bug 20/05/2026: temas SPFC e Tech & IA aparecendo vazios no email
        # (causa: todas as notícias do tema foram puladas no for interno por falta
        # de manchete/resumo, mas o cabeçalho do tema seguia sendo renderizado).
        valid_news = [
            n for n in sec.get("noticias", [])
            if n.get("manchete") and n.get("resumo")
        ]
        if not valid_news:
            continue
        # ====================================================================

        slug = _slugify(sec.get("topic", f"tema-{idx}"))
        country_chip = ""

        pause_btn = ""
        if sec.get("fb_pause_url"):
            pause_btn = f"""<td align="right" style="font-family:{SERIF_FONT};font-style:italic;font-size:11px;color:{COLORS['ink_muted']};">
              <a href="{_safe_url(sec['fb_pause_url'])}" style="color:{COLORS['ink_muted']};text-decoration:none;border-bottom:1px dashed {COLORS['ink_muted']};">⏸ pausar 7d</a>
            </td>"""

        noticias_html = ""
        rendered_count = 0
        for n in sec["noticias"]:
            if not n.get("manchete") or not n.get("resumo"):
                continue
            has_image = bool(n.get("img_url"))
            rendered_count += 1
            is_espresso = (email_mode == "espresso")
            resumo_full = n.get("resumo", "").strip()
            if is_espresso:
                import re as _re
                m = _re.split(r'(?<=[.!?])\s+', resumo_full, maxsplit=1)
                resumo_display = m[0] if m else resumo_full
                if len(resumo_display) > 180:
                    resumo_display = resumo_display[:177].rstrip() + "..."
            else:
                resumo_display = resumo_full
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
            fb_btns = ""
            if n.get("fb_more_url") and n.get("fb_less_url"):
                fb_btns = f"""<tr><td style="padding-top:14px;border-top:1px solid {COLORS['line']};font-family:{SANS_FONT};font-size:11px;font-weight:700;">
                  <a href="{_safe_url(n['fb_more_url'])}" style="color:{COLORS['mint_deep']};text-decoration:none;">＋ mais como essa</a>
                  &nbsp;&nbsp;
                  <a href="{_safe_url(n['fb_less_url'])}" style="color:{COLORS['red']};text-decoration:none;">— menos como essa</a>
                </td></tr>"""
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
            <tr><td style="padding:0 0 22px 0;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border:1.5px solid {COLORS['mint']};border-radius:15px;background:#FFFFFF;">
                <tr><td style="padding:22px;">
                  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                    <tr><td style="font-family:{SERIF_FONT};font-weight:700;font-style:italic;font-size:{'24' if is_espresso else '28'}px;line-height:1.18;color:{COLORS['ink']};letter-spacing:-0.02em;padding-bottom:{'12' if is_espresso else '16'}px;" class="dark-text">{_esc(n.get('manchete',''))}</td></tr>
                    {_render_news_image(n.get('img_url'), n.get('manchete',''), topic_id=sec.get('topic_id') or sec.get('topic'), size_mode='hero') if has_image else ''}
                    <tr><td style="font-family:{SANS_FONT};font-size:{'14' if is_espresso else '16'}px;line-height:1.6;color:{COLORS['ink_soft']};padding-bottom:{'10' if is_espresso else '16'}px;{'padding-top:14px;' if has_image else ''}" class="dark-text-soft">{_render_inline_html(resumo_display)}</td></tr>
                    {fatos_html}
                    <tr><td style="font-family:{SANS_FONT};font-size:12px;color:{COLORS['ink_muted']};padding-bottom:8px;" class="dark-text-muted">
                      <a href="{_safe_url(n.get('link'))}" style="color:{COLORS['ink']};text-decoration:none;font-weight:800;border-bottom:2.5px solid {COLORS['mint_deep']};padding-bottom:1px;margin-right:12px;" class="dark-text">Ler matéria →</a>
                      <span style="color:{COLORS['ink']};font-weight:800;" class="dark-text">{_esc(n.get('fonte','') or 'Fonte')}</span>
                      {lang_chip}
                    </td></tr>
                    <tr><td style="padding-top:8px;padding-bottom:4px;">{bias_chips}</td></tr>
                    {fb_btns}
                  </table>
                </td></tr>
              </table>
            </td></tr>"""

        # Defensivo extra: se mesmo após o for nada foi renderizado, pula o tema.
        # (não deveria acontecer porque temos o guard `valid_news` no topo, mas
        # garantia dupla pra evitar "esqueleto vazio" sob qualquer circunstância.)
        if rendered_count == 0:
            continue

        out += f"""
        <tr><td style="padding:0 36px;" id="tema-{slug}" class="sec-edge">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:8px;border-bottom:4px solid {COLORS['bg_2']};">
            <tr><td style="padding:28px 0 4px 0;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:separate;">
                <tr>
                  <td valign="middle" style="padding-top:15px;line-height:0;font-size:0;">
                    <div style="border-top:3px dashed {COLORS['ink']};font-size:0;line-height:0;height:0;">&nbsp;</div>
                  </td>
                  <td valign="middle" align="center" width="60" style="font-size:30px;color:{COLORS['mint_deep']};line-height:1;padding:0 8px;mso-line-height-rule:exactly;">✂︎</td>
                  <td valign="middle" style="padding-top:15px;line-height:0;font-size:0;">
                    <div style="border-top:3px dashed {COLORS['ink']};font-size:0;line-height:0;height:0;">&nbsp;</div>
                  </td>
                </tr>
              </table>
              <div style="font-family:{MONO_FONT};font-size:9px;letter-spacing:0.3em;color:{COLORS['mint_dark']};text-transform:uppercase;text-align:right;margin-top:4px;font-weight:700;">recortado pra você</div>
            </td></tr>
            <tr><td style="padding:8px 0 18px 0;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr>
                  <td><table role="presentation" cellpadding="0" cellspacing="0" border="0"><tr>
                    <td style="background:{COLORS['ink']};border:2px solid {COLORS['ink']};padding:8px 18px;font-family:{SANS_FONT};font-size:13px;font-weight:800;letter-spacing:0.20em;text-transform:uppercase;color:{COLORS['mint']};box-shadow:3px 3px 0 {COLORS['ink_muted']};">{_get_topic_icon(sec['topic'])} {_esc(sec['topic'])}</td>
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
        <tr><td style="padding:0 36px 28px 36px;" id="topo" class="px-mob">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:{COLORS['mint_bg_light']};border:2.5px solid {COLORS['mint_dark']};">
            <tr><td style="padding:18px 20px;">
              <div style="font-family:{SANS_FONT};font-size:10px;font-weight:800;letter-spacing:0.22em;text-transform:uppercase;color:{COLORS['mint_dark']};margin-bottom:10px;">📑 Navegação rápida</div>
              <div>{chips_html}</div>
            </td></tr>
          </table>
        </td></tr>"""


def _render_toc_bottom(trending, sections):
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
        <tr><td style="padding:0 36px 28px 36px;" class="px-mob">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:{COLORS['ink']};">
            <tr><td style="padding:16px 20px;">
              <div style="font-family:{SANS_FONT};font-size:10px;font-weight:800;letter-spacing:0.22em;text-transform:uppercase;color:{COLORS['mint']};margin-bottom:9px;">📑 Pula pra outra seção</div>
              <div>{chips_html}</div>
            </td></tr>
          </table>
        </td></tr>"""


def _minify_email_html(html):
    """Minificação leve do HTML do email pra reduzir tamanho e evitar Gmail clipping
    (limite 102KB). Conservadora: não altera conteúdo visível, só remove whitespace
    desnecessário entre tags.

    - Remove indentação e quebras de linha ENTRE tags
    - Colapsa múltiplos espaços em um só
    - Preserva conteúdo de texto e comentários condicionais (Outlook)
    """
    if not html:
        return html
    # Protege comentários condicionais Outlook (<!--[if mso]> ... <![endif]-->)
    placeholders = []
    def _stash(m):
        placeholders.append(m.group(0))
        return f"___MSO_COMMENT_{len(placeholders)-1}___"
    protected = re.sub(r'<!--\[if[^>]*?\]>.*?<!\[endif\]-->', _stash, html, flags=re.DOTALL)

    # Remove whitespace entre tags adjacentes (>< com whitespace no meio)
    minified = re.sub(r'>\s+<', '><', protected)
    # Colapsa múltiplos espaços (mas só fora de atributos — heurística simples)
    minified = re.sub(r'\n\s*', ' ', minified)
    minified = re.sub(r'  +', ' ', minified)

    # Restaura comentários protegidos
    for i, comment in enumerate(placeholders):
        minified = minified.replace(f"___MSO_COMMENT_{i}___", comment)

    return minified


def render_email(user_name, date_obj, trending=None, trending_label="",
                 sections=None, manage_url="#", tts_url=None, tts_duration=None,
                 user_id=None, daily_recap=None,
                 daily_quote="", daily_quote_author="",
                 email_mode="coado", weekly_mode=False,
                 user_tz="America/Sao_Paulo", saudacao_mode="auto",
                 filtered_items_count=0, is_welcome=False,
                 unsub_url="#",
                 edition_id=None,
                 share_base_url="https://recorte.news/r"):
    trending = trending or []
    sections = sections or []

    # ====================================================================
    # DEFENSIVO: filtra sections sem nenhuma notícia válida ANTES do render.
    # Camada de segurança caso prepare_daily.py / daily_digest.py deixem
    # passar tema com noticias=[] ou só com itens incompletos.
    # ====================================================================
    sections = [
        s for s in sections
        if any(n.get("manchete") and n.get("resumo") for n in s.get("noticias", []))
    ]
    # ====================================================================

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

    if saudacao_mode == "manha":
        saudacao = "Bom dia"
    elif saudacao_mode in ("domingo", "sabado"):
        saudacao = "Bom domingo"
    elif saudacao_mode == "neutro":
        saudacao = "Oi"
    else:
        try:
            from zoneinfo import ZoneInfo
            local_dt = date_obj.astimezone(ZoneInfo(user_tz)) if date_obj.tzinfo else date_obj
            hour = local_dt.hour
        except Exception:
            hour = date_obj.hour
        if 5 <= hour < 12:
            saudacao = "Bom dia"
        elif 12 <= hour < 18:
            saudacao = "Boa tarde"
        elif 18 <= hour < 24:
            saudacao = "Boa noite"
        else:
            saudacao = "Olá"

    total_noticias = sum(len(s["noticias"]) for s in sections) + len(trending)
    if total_noticias:
        intro_count = f"{total_noticias} notícia{'s' if total_noticias != 1 else ''}"
    else:
        intro_count = "novidades"

    stat_noticias = total_noticias or 0
    stat_trending = len(trending)
    stat_temas = len(sections)
    secs_each = 10 if email_mode == "espresso" else 20
    stat_minutos = max(2, round(stat_noticias * secs_each / 60))

    if weekly_mode:
        hero_h1 = f'<span style="background:linear-gradient(180deg,transparent 60%,{COLORS["mint"]} 60%);padding:0 2px;">Sua semana</span><br/>em {intro_count}.'
        hero_subtitle = "Sua semana inteira, recortada pra você. Pega o café — temos um tempinho."
        mode_badge = "🗞 DOMINGO · RECORTE DA SEMANA"
    else:
        hero_h1 = f'Hoje em <span style="background:linear-gradient(180deg,transparent 60%,{COLORS["mint"]} 60%);padding:0 2px;">{intro_count}</span><br/>só pra você.'
        hero_subtitle = "A cada toque em ＋ ou —, a gente entende melhor o que importa pra você. Bom café."
        mode_badge = "⚡ ESPRESSO" if email_mode == "espresso" else "☕ CAFÉ COADO"

    tts_html = ""
    if tts_url:
        duration_str = tts_duration or "—"
        tts_html = f"""
        <tr><td style="background:{COLORS['yellow']};padding:14px 36px;border-bottom:2px solid {COLORS['ink']};" class="px-mob">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>
            <td width="38" valign="middle" style="padding-right:14px;">
              <a href="{_safe_url(tts_url)}" style="text-decoration:none;">
                <div style="width:38px;height:38px;background:{COLORS['ink']};color:{COLORS['yellow']};border-radius:50%;text-align:center;line-height:38px;font-family:{SANS_FONT};font-size:14px;font-weight:700;">▶</div>
              </a>
            </td>
            <td valign="middle">
              <div style="font-family:{SERIF_FONT};font-weight:400;font-style:italic;font-size:14px;color:{COLORS['ink']};letter-spacing:-0.005em;">Ouça esta edição</div>
              <div style="font-family:{SANS_FONT};font-size:11px;color:{COLORS['ink']};opacity:0.7;font-weight:500;">narrada pra você</div>
            </td>
            <td valign="middle" align="right" style="font-family:{MONO_FONT};font-size:12px;color:{COLORS['ink']};font-weight:600;background:rgba(10,37,64,0.1);padding:4px 8px;">{_esc(duration_str)}</td>
          </tr></table>
        </td></tr>"""

    trending_html = _render_trending_section(trending, trending_label, email_mode=email_mode)
    sections_html = _render_news_sections(sections, email_mode=email_mode)
    recap_html = _render_daily_recap(daily_recap)

    quote_html = ""
    if daily_quote:
        author_html = ""
        if daily_quote_author:
            author_html = f'<div style="font-family:{SANS_FONT};font-size:11px;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;color:{COLORS["mint_dark"]};margin-top:10px;">— {_esc(daily_quote_author)}</div>'
        quote_html = f"""
        <tr><td style="padding:0 36px 24px;" class="px-mob">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="{COLORS['ink']}" style="background:{COLORS['ink']};">
            <tr><td bgcolor="{COLORS['ink']}" style="background:{COLORS['ink']};padding:22px 26px;">
              <div style="font-family:{SERIF_FONT};font-style:italic;font-weight:500;font-size:18px;line-height:1.4;color:{COLORS['mint']};letter-spacing:-0.01em;">
                {_esc(daily_quote)}
              </div>
              {author_html}
            </td></tr>
          </table>
        </td></tr>"""

    manage_link = manage_url
    unsub_link = unsub_url or "#"

    if edition_id:
        share_url = f"{share_base_url.rstrip('/')}/{edition_id}"
    else:
        share_url = "https://recorte.news"
    from urllib.parse import quote as _q
    if weekly_mode:
        share_msg = f"✂ Minha semana, recortada: 7 dias do mundo lidos pra mim, numa edição única de domingo. É o Recorte:"
    elif is_welcome:
        share_msg = f"✂ Olha que incrível: toda manhã, antes do café, chega um jornal feito SÓ pra mim. Ele cura por tema que eu seleciono. É o Recorte:"
    else:
        share_msg = f"✂ Olha o que recebi hoje: alguém leu as notícias do mundo, recortou pelos meus interesses, e deixou na minha caixa antes das 6h. Chega de ficar rolando o feed. É o Recorte:"
    share_wa_text = _q(f"{share_msg} {share_url}")
    share_x_text = _q(f"{share_msg} {share_url}")

    web_fonts_link = '<link rel="stylesheet" href="https://api.fontshare.com/v2/css?f[]=switzer@400,500,700,800,900&f[]=general-sans@400,500,600,700&f[]=gambarino@400i,400&display=swap">'

    _html = f"""<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" lang="pt-BR"><head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="color-scheme" content="light only">
<meta name="supported-color-schemes" content="light">
<meta name="format-detection" content="telephone=no, date=no, address=no, email=no">
<meta name="x-apple-disable-message-reformatting">
<title>Recorte ✂ · {date_short}</title>
{web_fonts_link}
<!--[if mso]><style type="text/css">body, table, td {{font-family: Georgia, 'Times New Roman', serif !important;}} .mso-sans {{font-family: Arial, Helvetica, sans-serif !important;}} table {{mso-table-lspace:0pt; mso-table-rspace:0pt;}}</style><![endif]-->
<style>
  /* Outlook.com / Hotmail: evita o wrap .ExternalClass quebrar line-height */
  .ExternalClass {{ width: 100%; }}
  .ExternalClass, .ExternalClass p, .ExternalClass span,
  .ExternalClass font, .ExternalClass td, .ExternalClass div {{ line-height: inherit; }}
  /* Reset universal: remove espaçamento extra em tables (Outlook desktop) */
  table, td {{ mso-table-lspace: 0pt; mso-table-rspace: 0pt; border-collapse: collapse; }}
  /* Reset img: evita "borda" em Outlook + outline azul ao redor em alguns clients */
  img {{ -ms-interpolation-mode: bicubic; border: 0; outline: none; text-decoration: none; }}
  /* Apple Mail / iOS: evita auto-link em números, datas, endereços */
  a[x-apple-data-detectors] {{
    color: inherit !important; text-decoration: none !important;
    font-size: inherit !important; font-family: inherit !important;
    font-weight: inherit !important; line-height: inherit !important;
  }}
  @media only screen and (max-width:600px){{
    .container {{ width:100% !important; max-width:100% !important; }}
    .px-mob {{ padding-left:20px !important; padding-right:20px !important; }}
    .hero-h1 {{ font-size:36px !important; line-height:1.05 !important; }}
    .edition-hero {{ font-size:30px !important; }}
    .stat-num {{ font-size:20px !important; }}
    .news-img {{ width:100% !important; height:200px !important; max-height:200px !important; object-fit:cover !important; }}
    .news-img-thumb {{ width:100% !important; max-width:280px !important; height:140px !important; max-height:140px !important; object-fit:cover !important; }}
    .share-btn {{ display:block !important; width:100% !important; margin:6px 0 !important; }}
    .feat-mob {{ height:130px !important; min-height:130px !important; padding:14px 6px !important; vertical-align:top !important; }}
    .feat-mob .feat-emoji {{ font-size:22px !important; margin-bottom:6px !important; }}
    .feat-mob .feat-title {{ font-size:9.5px !important; letter-spacing:0.04em !important; line-height:1.15 !important; }}
    .feat-mob .feat-desc {{ font-size:10.5px !important; line-height:1.25 !important; }}
    /* Fix iPhone: zera padding lateral da TD outer pra cabeçalho/cards ocuparem largura total */
    .outer-td {{ padding-left:0 !important; padding-right:0 !important; }}
    /* Fix iPhone v3: zera padding lateral das TDs de seção (cards full-width em mobile) */
    .sec-edge {{ padding-left:0 !important; padding-right:0 !important; }}
  }}
  :root {{
    color-scheme: light only;
    supported-color-schemes: light;
  }}
  u + .body a {{ color: inherit; }}
  .news-img {{ display:block; width:100%; max-width:560px; height:315px; object-fit:cover; border:0; border-radius:8px; }}
  .news-img-thumb {{ display:block; width:100%; max-width:280px; height:158px; object-fit:cover; border:0; border-radius:8px; margin:0 auto; }}
</style>
</head>
<body bgcolor="{COLORS['bg']}" style="margin:0;padding:0;background:{COLORS['bg']};font-family:{SANS_FONT};-webkit-font-smoothing:antialiased;" class="body">

<div style="display:none;font-size:1px;color:{COLORS['bg']};line-height:1px;max-height:0;max-width:0;opacity:0;overflow:hidden;">
  {saudacao}, {_esc(first_name)}. {"Sua semana em " + intro_count + " — antes do café." if weekly_mode else "Hoje em " + intro_count + " só pra você — em 5 minutos."}
</div>

<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="{COLORS['bg']}" style="background:{COLORS['bg']};">
  <tr><td align="center" class="outer-td" style="padding:24px 16px;">
    <table role="presentation" class="container" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="{COLORS['bg']}" style="width:100%;max-width:640px;background:{COLORS['bg']};">

      <tr><td bgcolor="{COLORS['mint']}" style="background:{COLORS['mint']};padding:26px 36px 22px;" class="px-mob">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>
          <td valign="middle">
            <table role="presentation" cellpadding="0" cellspacing="0" border="0"><tr>
              <td valign="middle" style="padding-right:14px;">
                <div style="width:42px;height:42px;background:{COLORS['bg']};border:2.5px solid {COLORS['ink']};border-radius:50%;text-align:center;line-height:38px;font-family:{SANS_DISPLAY};font-weight:900;font-size:22px;color:{COLORS['ink']};letter-spacing:-0.04em;">R<span style="color:{COLORS['mint_deep']};">.</span></div>
              </td>
              <td valign="middle" style="font-family:{SANS_DISPLAY};font-weight:900;font-size:30px;letter-spacing:-0.035em;color:{COLORS['ink']};">Recorte<span style="color:{COLORS['mint_deep']};margin-left:2px;">✂︎</span></td>
            </tr></table>
          </td>
          <td valign="middle" align="right" style="font-family:{SERIF_FONT};font-style:italic;font-size:13px;color:{COLORS['mint_dark']};">
            {weekday},<br/><strong style="color:{COLORS['ink']};font-style:normal;font-weight:600;">{date_obj.day} de {meses[date_obj.month-1]}</strong>
            <div style="font-family:{MONO_FONT};font-size:10px;letter-spacing:0.18em;text-transform:uppercase;color:{COLORS['mint_dark']};margin-top:4px;font-weight:500;font-style:normal;">EDIÇÃO Nº {issue_num} · {mode_badge}</div>
          </td>
        </tr></table>
      </td></tr>

      {_render_tricolor_band()}

      {_render_welcome_block() if is_welcome else ""}

      {tts_html}

      <tr><td style="padding:44px 36px 28px;" class="px-mob">
        <div class="edition-hero" style="font-family:{SERIF_FONT};font-style:italic;font-weight:400;font-size:38px;line-height:1;color:{COLORS['mint_dark']};margin-bottom:14px;letter-spacing:-0.03em;">edição <span style="color:{COLORS['ink']};font-style:normal;font-weight:500;">#{issue_num}</span></div>
        <div style="font-family:{SERIF_FONT};font-style:italic;font-size:15px;color:{COLORS['mint_dark']};margin-bottom:12px;">— {saudacao}, {_esc(first_name)}.</div>
        <h1 class="hero-h1" style="font-family:{SERIF_FONT};font-weight:400;font-style:italic;font-size:44px;line-height:1.0;letter-spacing:-0.04em;color:{COLORS['ink']};margin:0 0 18px 0;">{hero_h1}</h1>
        <p style="font-family:{SANS_FONT};font-size:16px;line-height:1.55;color:{COLORS['ink_soft']};margin:0 0 24px 0;max-width:520px;">{hero_subtitle}</p>

        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-top:1px solid {COLORS['line']};border-bottom:1px solid {COLORS['line']};">
          <tr>
            <td align="center" style="padding:14px 4px;">
              <div class="stat-num" style="font-family:{SERIF_FONT};font-weight:400;font-style:italic;font-size:24px;color:{COLORS['mint_dark']};line-height:1;letter-spacing:-0.02em;">{stat_noticias}</div>
              <div style="font-family:{SANS_FONT};font-size:10px;font-weight:600;letter-spacing:0.1em;text-transform:uppercase;color:{COLORS['ink_muted']};margin-top:5px;">Notícias</div>
            </td>
            <td width="1" style="background:{COLORS['line']};">&nbsp;</td>
            <td align="center" style="padding:14px 4px;">
              <div class="stat-num" style="font-family:{SERIF_FONT};font-weight:400;font-style:italic;font-size:24px;color:{COLORS['mint_dark']};line-height:1;letter-spacing:-0.02em;">{stat_trending}</div>
              <div style="font-family:{SANS_FONT};font-size:10px;font-weight:600;letter-spacing:0.1em;text-transform:uppercase;color:{COLORS['ink_muted']};margin-top:5px;">Em alta</div>
            </td>
            <td width="1" style="background:{COLORS['line']};">&nbsp;</td>
            <td align="center" style="padding:14px 4px;">
              <div class="stat-num" style="font-family:{SERIF_FONT};font-weight:400;font-style:italic;font-size:24px;color:{COLORS['mint_dark']};line-height:1;letter-spacing:-0.02em;">{stat_temas}</div>
              <div style="font-family:{SANS_FONT};font-size:10px;font-weight:600;letter-spacing:0.1em;text-transform:uppercase;color:{COLORS['ink_muted']};margin-top:5px;">Temas seus</div>
            </td>
            <td width="1" style="background:{COLORS['line']};">&nbsp;</td>
            <td align="center" style="padding:14px 4px;">
              <div class="stat-num" style="font-family:{SERIF_FONT};font-weight:400;font-style:italic;font-size:24px;color:{COLORS['mint_dark']};line-height:1;letter-spacing:-0.02em;">{stat_minutos}'</div>
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

      <tr><td bgcolor="{COLORS['mint']}" style="background:{COLORS['mint']};padding:36px 36px 30px;text-align:center;" class="px-mob">
        <div style="font-family:{SERIF_FONT};font-style:italic;font-size:19px;line-height:1.5;color:{COLORS['ink']};margin-bottom:20px;padding:0 20px;">
          <span style="color:{COLORS['mint_dark']};font-size:24px;font-weight:700;vertical-align:-8px;">“</span>A notícia certa, na hora certa, é o melhor café da manhã. ☕<span style="color:{COLORS['mint_dark']};font-size:24px;font-weight:700;vertical-align:-8px;">”</span>
        </div>
        <div style="font-family:{SANS_FONT};font-weight:800;font-size:11px;letter-spacing:0.22em;text-transform:uppercase;color:{COLORS['mint_dark']};">— Recorte ✂ &nbsp;·&nbsp; até amanhã às 6h</div>
      </td></tr>

      <tr><td style="padding:24px 36px 8px 36px;text-align:center;" class="px-mob">
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" align="center" style="margin:0 auto;">
          <tr><td align="center" style="padding-bottom:12px;font-family:{SERIF_FONT};font-style:italic;font-size:15px;color:{COLORS['ink_soft']};" class="dark-text-soft">
            Curtiu? <strong style="color:{COLORS['ink']};" class="dark-text">Encaminha pra um amigo ✂</strong>
          </td></tr>
          <tr><td align="center">
            <table role="presentation" cellpadding="0" cellspacing="0" border="0"><tr>
              <td style="padding:0 4px;">
                <a class="share-btn" href="https://wa.me/?text={share_wa_text}"
                   style="display:inline-block;background:#25D366;color:#ffffff;font-family:{SANS_FONT};font-weight:800;font-size:12px;letter-spacing:0.08em;text-transform:uppercase;text-decoration:none;padding:10px 18px;border-radius:4px;mso-padding-alt:0;">
                  💬 WhatsApp
                </a>
              </td>
              <td style="padding:0 4px;">
                <a class="share-btn" href="https://twitter.com/intent/tweet?text={share_x_text}"
                   style="display:inline-block;background:#000000;color:#ffffff;font-family:{SANS_FONT};font-weight:800;font-size:12px;letter-spacing:0.08em;text-transform:uppercase;text-decoration:none;padding:10px 18px;border-radius:4px;">
                  𝕏 Compartilhar
                </a>
              </td>
              <td style="padding:0 4px;">
                <a class="share-btn" href="{share_url}"
                   style="display:inline-block;background:{COLORS['mint_deep']};color:#ffffff;font-family:{SANS_FONT};font-weight:800;font-size:12px;letter-spacing:0.08em;text-transform:uppercase;text-decoration:none;padding:10px 18px;border-radius:4px;">
                  🔗 Abrir online
                </a>
              </td>
            </tr></table>
          </td></tr>
        </table>
      </td></tr>

      <tr><td bgcolor="{COLORS['bg_2']}" style="background:{COLORS['bg_2']};padding:28px 36px;text-align:center;" class="px-mob">
        <div style="font-family:{SANS_FONT};font-size:11px;color:{COLORS['ink_muted']};line-height:1.7;" class="dark-text-muted">
          Você está recebendo porque se cadastrou em <strong style="color:{COLORS['ink']};" class="dark-text">Recorte ✂</strong>.
          {('<br/><span style="font-family:' + MONO_FONT + ';font-size:10px;color:' + COLORS['ink_muted'] + ';opacity:0.7;letter-spacing:0.08em;">' + str(filtered_items_count) + ' filtro' + ('s' if filtered_items_count != 1 else '') + ' ativo' + ('s' if filtered_items_count != 1 else '') + ' · você no controle</span>') if filtered_items_count > 0 else ''}
          <br/><br/>
          <a href="{_safe_url(manage_link)}" style="color:{COLORS['ink_soft']};text-decoration:underline;font-weight:700;font-size:12px;" class="dark-text-soft">⚙ Ajustar minhas preferências</a>
          &nbsp;&nbsp;·&nbsp;&nbsp;
          <a href="{_safe_url(unsub_link)}" style="color:{COLORS['ink_soft']};text-decoration:underline;font-weight:700;font-size:12px;" class="dark-text-soft">✕ Cancelar inscrição</a>
          <br/><br/>
          <span style="font-size:10px;color:{COLORS['ink_muted']};opacity:0.8;line-height:1.65;" class="dark-text-muted">A gente lê o mundo todo dia, pra você · Conteúdo de terceiros. Direitos reservados aos veículos originais.</span>
          <br/><br/>
          <a href="https://recorte.news/termos.html" style="color:{COLORS['ink_muted']};text-decoration:underline;font-size:10px;opacity:0.7;" class="dark-text-muted">Termos de Uso</a>
          &nbsp;·&nbsp;
          <a href="https://recorte.news/politica-privacidade.html" style="color:{COLORS['ink_muted']};text-decoration:underline;font-size:10px;opacity:0.7;" class="dark-text-muted">Política de Privacidade</a>
          &nbsp;·&nbsp;
          <a href="mailto:contato@recorte.news" style="color:{COLORS['ink_muted']};text-decoration:underline;font-size:10px;opacity:0.7;" class="dark-text-muted">Contato</a>
          <div style="margin-top:14px;font-size:10px;color:{COLORS['ink_muted']};opacity:0.65;line-height:1.6;" class="dark-text-muted">
            Recorte ✂ · Operado pela Equipe Recorte ✂ · São Paulo/SP · Brasil
          </div>
          <div style="margin-top:6px;font-family:{MONO_FONT};font-size:9px;letter-spacing:0.18em;text-transform:uppercase;color:{COLORS['ink_muted']};opacity:0.55;" class="dark-text-muted">Última coleta · {date_obj.strftime('%d/%m %H:%M')} BRT</div>
        </div>
      </td></tr>

    </table>
  </td></tr>
</table>
</body></html>"""
    return _minify_email_html(_html)
