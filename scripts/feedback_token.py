"""Geração e validação de tokens HMAC para os links de feedback e manage nos e-mails."""
import os
import time
import hmac
import hashlib
import secrets


def short_id(length=10):
    return secrets.token_urlsafe(length)[:length]


def sign(item_id: str, signal: int, secret: str = None) -> str:
    secret = secret or os.environ.get("FEEDBACK_SECRET", "")
    if not secret:
        raise ValueError("FEEDBACK_SECRET ausente")
    msg = f"{item_id}:{signal}".encode("utf-8")
    return hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()[:12]


def verify(item_id: str, signal: int, token: str, secret: str = None) -> bool:
    expected = sign(item_id, signal, secret)
    return hmac.compare_digest(expected, token)


def feedback_url(base_url: str, item_id: str, signal: int) -> str:
    token = sign(item_id, signal)
    return f"{base_url}?i={item_id}&s={signal}&t={token}"


def manage_sign(user_id: str, exp: int, secret: str = None) -> str:
    secret = secret or os.environ.get("FEEDBACK_SECRET", "")
    if not secret:
        raise ValueError("FEEDBACK_SECRET ausente")
    msg = f"manage|{user_id}|{exp}".encode("utf-8")
    return hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()[:24]


def manage_url(base_url: str, user_id: str, ttl_days: int = 30) -> str:
    if not base_url or base_url == "#":
        return "#"
    exp = int(time.time()) + ttl_days * 86400
    token = manage_sign(user_id, exp)
    base = base_url.rstrip("/")
    if not base.endswith(".html"):
        base = base + "/manage.html"
    return f"{base}?u={user_id}&exp={exp}&t={token}"


def unsub_sign(user_id: str, secret: str = None) -> str:
    secret = secret or os.environ.get("FEEDBACK_SECRET", "")
    if not secret:
        raise ValueError("FEEDBACK_SECRET ausente")
    msg = f"unsub|{user_id}".encode("utf-8")
    return hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()[:24]


def unsub_verify(user_id: str, token: str, secret: str = None) -> bool:
    expected = unsub_sign(user_id, secret)
    return hmac.compare_digest(expected, token)


def unsub_url(supabase_url: str, user_id: str) -> str:
    if not supabase_url or not user_id:
        return "#"
    base = supabase_url.rstrip("/")
    token = unsub_sign(user_id)
    return f"{base}/functions/v1/unsubscribe?u={user_id}&t={token}"
