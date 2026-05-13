"""Geração e validação de tokens HMAC para os links de feedback nos e-mails."""
import os
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
