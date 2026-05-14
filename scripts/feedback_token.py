"""Geração e validação de tokens HMAC para os links de feedback e manage nos e-mails."""
import os
import time
import hmac
import hashlib
import secrets


def short_id(length=10):
    """ID curto base64-url-safe pra usar como key em email_items."""
    return secrets.token_urlsafe(length)[:length]


def sign(item_id: str, signal: int, secret: str = None) -> str:
    """Gera token HMAC-SHA256 truncado em 12 chars."""
    secret = secret or os.environ.get("FEEDBACK_SECRET", "")
    if not secret:
        raise ValueError("FEEDBACK_SECRET ausente")
    msg = f"{item_id}:{signal}".encode("utf-8")
    return hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()[:12]


def verify(item_id: str, signal: int, token: str, secret: str = None) -> bool:
    expected = sign(item_id, signal, secret)
    return hmac.compare_digest(expected, token)


def feedback_url(base_url: str, item_id: str, signal: int) -> str:
    """Monta URL completa do botão de feedback."""
    token = sign(item_id, signal)
    return f"{base_url}?i={item_id}&s={signal}&t={token}"


# ============ MANAGE TOKEN ============
def manage_sign(user_id: str, exp: int, secret: str = None) -> str:
    """HMAC do payload manage|user_id|exp truncado em 24 chars (mais entropia que feedback)."""
    secret = secret or os.environ.get("FEEDBACK_SECRET", "")
    if not secret:
        raise ValueError("FEEDBACK_SECRET ausente")
    msg = f"manage|{user_id}|{exp}".encode("utf-8")
    return hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()[:24]


def manage_url(base_url: str, user_id: str, ttl_days: int = 30) -> str:
    """Monta URL assinada pra página /manage. Expira em ttl_days."""
    if not base_url or base_url == "#":
        return "#"
    exp = int(time.time()) + ttl_days * 86400
    token = manage_sign(user_id, exp)
    # base_url pode ser https://x.github.io/manha-cafe/ ou https://x.github.io/manha-cafe/manage.html
    base = base_url.rstrip("/")
    if not base.endswith(".html"):
        base = base + "/manage.html"
    return f"{base}?u={user_id}&exp={exp}&t={token}"

