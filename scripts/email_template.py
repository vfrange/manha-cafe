"""
Renderiza o HTML do email diário do Manhã ☕.
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
def _render_trending_section(trending, scope_label):
    if not trending:
        return ""

    items_html = ""
    for idx, item in enumerate(trending):
        # Suporta formato NOVO (manchete/resumo/fatos_chave) e formato VELHO (termo/contexto)
        manchete = _esc(item.get("manchete") or item.get("termo", ""))
        resumo = _esc(item.get("resumo") or item.get("contexto", ""))
        fatos = item.get("fatos_chave") or []
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


def _render_news_sections(sections):
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
            # Fatos-chave (bullets) — opcional
            fatos_html = ""
            if n.get("fatos_chave"):
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

            # "Por que importa" — opcional
            why_html = ""
            if n.get("why_matters"):
                why_html = f"""<tr><td style="padding-bottom:16px;">
                  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:{COLORS['mint_bg_light']};border-left:4px solid {COLORS['mint_deep']};">
                    <tr><td style="padding:14px 18px;">
                      <div style="font-family:{SANS_FONT};font-weight:800;font-size:10px;letter-spacing:0.2em;text-transform:uppercase;color:{COLORS['mint_dark']};margin-bottom:5px;">↳ Por que importa pra você</div>
                      <div style="font-family:{SERIF_FONT};font-style:italic;font-size:15px;line-height:1.5;color:{COLORS['ink']};">{_esc(n['why_matters'])}</div>
                    </td></tr>
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

            # Bias chips
            bias_chips = ""
            if n.get("bias"):
                bias_chips += f"""<span style="display:inline-block;background:{COLORS['bg']};border:1px solid {COLORS['line']};color:{COLORS['ink_soft']};font-family:{SANS_FONT};font-weight:700;font-size:10px;letter-spacing:0.04em;padding:3px 8px;margin-right:4px;">⚖ {_esc(n['bias'])}</span>"""
            if n.get("coverage_count"):
                bias_chips += f"""<span style="display:inline-block;background:{COLORS['mint_deep']};border:1px solid {COLORS['mint_deep']};color:#FFFFFF;font-family:{SANS_FONT};font-weight:700;font-size:10px;letter-spacing:0.04em;padding:3px 8px;">↔ {_esc(str(n['coverage_count']))} fontes</span>"""

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
                <tr><td style="font-family:{SERIF_FONT};font-weight:700;font-size:26px;line-height:1.18;color:{COLORS['ink']};letter-spacing:-0.02em;padding-bottom:14px;">{_esc(n['manchete'])}</td></tr>
                <tr><td style="font-family:{SANS_FONT};font-size:16px;line-height:1.6;color:{COLORS['ink_soft']};padding-bottom:16px;">{_esc(n['resumo'])}</td></tr>
                {fatos_html}
                {why_html}
                <tr><td style="font-family:{SANS_FONT};font-size:12px;color:{COLORS['ink_muted']};padding-bottom:8px;">
                  <a href="{_esc(n['link'])}" style="color:{COLORS['ink']};text-decoration:none;font-weight:800;border-bottom:2.5px solid {COLORS['mint_deep']};padding-bottom:1px;margin-right:12px;">Ler matéria →</a>
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
            <tr><td style="padding:28px 0 18px 0;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr>
                  <td><table role="presentation" cellpadding="0" cellspacing="0" border="0"><tr>
                    <td style="background:{COLORS['mint_bg_light']};border:1.5px solid {COLORS['mint_deep']};padding:4px 12px;font-family:{SANS_FONT};font-size:11px;font-weight:800;letter-spacing:0.18em;text-transform:uppercase;color:{COLORS['mint_dark']};">★ {_esc(sec['topic'])}</td>
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
                 user_id=None, daily_recap=None):
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
    """
    trending = trending or []
    sections = sections or []
    first_name = user_name.split()[0] if user_name else "leitor"

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
    intro_parts = []
    if trending: intro_parts.append(f"{len(trending)} em alta")
    if total_noticias: intro_parts.append(f"{total_noticias} notícias")
    intro_count = " e ".join(intro_parts) if intro_parts else "novidades"

    stat_noticias = total_noticias or 0
    stat_trending = len(trending)
    stat_temas = len(sections)
    stat_minutos = max(2, (stat_noticias * 1) + (stat_trending // 2))

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

    trending_html = _render_trending_section(trending, trending_label)
    sections_html = _render_news_sections(sections)
    recap_html = _render_daily_recap(daily_recap)

    manage_link = manage_url

    google_fonts_link = '<link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,400;9..144,600;9..144,700;9..144,800;9..144,900&family=Mulish:wght@400;500;600;700;800&family=JetBrains+Mono:wght@500&display=swap" rel="stylesheet">'

    return f"""<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" lang="pt-BR"><head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="color-scheme" content="light">
<meta name="supported-color-schemes" content="light">
<title>Manhã ☕ · {date_short}</title>
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
  {saudacao}, {_esc(first_name)}. Hoje tem {intro_count} dos seus temas — em 5 minutos.
</div>

<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:{COLORS['bg']};">
  <tr><td align="center" style="padding:24px 16px;">
    <table role="presentation" class="container" width="640" cellpadding="0" cellspacing="0" border="0" style="max-width:640px;background:{COLORS['bg']};">

      <!-- MASTHEAD verde-menta -->
      <tr><td style="background:{COLORS['mint']};padding:26px 36px 22px;" class="px-mob">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>
          <td valign="middle">
            <table role="presentation" cellpadding="0" cellspacing="0" border="0"><tr>
              <td valign="middle" style="padding-right:12px;">
                <div style="width:38px;height:38px;background:{COLORS['ink']};color:{COLORS['yellow']};border-radius:50%;text-align:center;line-height:38px;font-family:{SANS_FONT};font-size:16px;">☕</div>
              </td>
              <td valign="middle" style="font-family:{SERIF_FONT};font-weight:900;font-size:30px;letter-spacing:-0.04em;color:{COLORS['ink']};">Manhã</td>
            </tr></table>
          </td>
          <td valign="middle" align="right" style="font-family:{SERIF_FONT};font-style:italic;font-size:13px;color:{COLORS['mint_dark']};">
            {weekday},<br/><strong style="color:{COLORS['ink']};font-style:normal;font-weight:600;">{date_obj.day} de {meses[date_obj.month-1]}</strong>
            <div style="font-family:{MONO_FONT};font-size:10px;letter-spacing:0.18em;text-transform:uppercase;color:{COLORS['mint_dark']};margin-top:4px;font-weight:500;font-style:normal;">EDIÇÃO Nº {issue_num}</div>
          </td>
        </tr></table>
      </td></tr>

      {_render_tricolor_band()}

      {tts_html}

      <!-- HERO -->
      <tr><td style="padding:44px 36px 28px;" class="px-mob">
        <div style="font-family:{SERIF_FONT};font-style:italic;font-size:15px;color:{COLORS['mint_dark']};margin-bottom:12px;">— {saudacao}, {_esc(first_name)}.</div>
        <h1 class="hero-h1" style="font-family:{SERIF_FONT};font-weight:900;font-size:44px;line-height:1.0;letter-spacing:-0.04em;color:{COLORS['ink']};margin:0 0 18px 0;">Hoje tem <span style="background:linear-gradient(180deg,transparent 60%,{COLORS['mint']} 60%);padding:0 2px;">{intro_count}</span> dos seus temas.</h1>
        <p style="font-family:{SANS_FONT};font-size:16px;line-height:1.55;color:{COLORS['ink_soft']};margin:0 0 24px 0;max-width:520px;">A cada toque em ＋ ou —, eu aprendo o que importa pra você. Bom café.</p>

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

      {recap_html}
      {trending_html}
      {sections_html}

      {_render_tricolor_band()}

      <!-- SIGN OFF verde-menta -->
      <tr><td style="background:{COLORS['mint']};padding:36px 36px 30px;text-align:center;" class="px-mob">
        <div style="font-family:{SERIF_FONT};font-style:italic;font-size:19px;line-height:1.5;color:{COLORS['ink']};margin-bottom:20px;padding:0 20px;">
          <span style="color:{COLORS['mint_dark']};font-size:24px;font-weight:700;vertical-align:-8px;">“</span>A informação certa, na hora certa, é o melhor café da manhã.<span style="color:{COLORS['mint_dark']};font-size:24px;font-weight:700;vertical-align:-8px;">”</span>
        </div>
        <div style="font-family:{SANS_FONT};font-weight:800;font-size:11px;letter-spacing:0.22em;text-transform:uppercase;color:{COLORS['mint_dark']};">— Manhã ☕ &nbsp;·&nbsp; até amanhã às 7h</div>
      </td></tr>

      <!-- FOOTER -->
      <tr><td style="background:{COLORS['bg_2']};padding:24px 36px;text-align:center;" class="px-mob">
        <div style="font-family:{SANS_FONT};font-size:11px;color:{COLORS['ink_muted']};line-height:1.7;">
          Você está recebendo porque se cadastrou em <strong style="color:{COLORS['ink']};">Manhã ☕</strong>.<br/>
          <a href="{_esc(manage_link)}" style="color:{COLORS['ink_soft']};text-decoration:underline;font-weight:700;">⚙ Ajustar minhas preferências</a>
          <br/><br/>
          <span style="font-size:10px;color:{COLORS['ink_muted']};opacity:0.8;">Resumos por Claude AI · Fontes: Google News, Bluesky, YouTube, Reddit, Hacker News e 25+ veículos BR e internacionais</span>
        </div>
      </td></tr>

    </table>
  </td></tr>
</table>
</body></html>"""
