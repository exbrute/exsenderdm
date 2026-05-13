"""Общие пути сессий и создание TelegramClient для main и бота управления."""

from __future__ import annotations

import logging
import os

from telethon import TelegramClient

from proxy_util import parse_proxy

logger = logging.getLogger(__name__)


def session_path(account_id: str) -> str:
    base = os.path.dirname(os.path.abspath(__file__))
    if account_id == "default":
        legacy = os.path.join(base, "userbot_session")
        if os.path.isfile(legacy + ".session"):
            return legacy
    sessions_dir = os.path.join(base, "sessions")
    os.makedirs(sessions_dir, exist_ok=True)
    return os.path.join(sessions_dir, account_id)


def make_telethon_client(
    account_id: str,
    api_id: int,
    api_hash: str,
    *,
    proxy_raw: str | None = None,
) -> TelegramClient:
    cr = int(os.getenv("TELETHON_CONNECTION_RETRIES", "5"))
    rd = float(os.getenv("TELETHON_RETRY_DELAY", "1"))
    proxy = parse_proxy(proxy_raw)
    if proxy_raw and proxy is None:
        logger.warning(
            "Userbot [%s]: прокси не разобран (%r) — запуск без прокси.",
            account_id,
            proxy_raw,
        )
    return TelegramClient(
        session_path(account_id),
        api_id,
        api_hash,
        connection_retries=max(1, cr),
        retry_delay=rd,
        proxy=proxy,
    )


async def connect_client_with_fallback(
    client: TelegramClient,
    *,
    account_id: str,
    api_id: int,
    api_hash: str,
    proxy_raw: str | None,
    allow_direct_fallback: bool = False,
) -> TelegramClient:
    """
    Подключает клиент с fallback:
    - как задано;
    - если proxy без схемы и socks не взлетел: пробует http://proxy;
    - опционально: прямое подключение без прокси.
    """
    last_err: Exception | None = None
    try:
        await client.connect()
        return client
    except Exception as e:
        last_err = e

    proxy_s = (proxy_raw or "").strip()
    if proxy_s and "://" not in proxy_s:
        try:
            alt = make_telethon_client(
                account_id,
                api_id,
                api_hash,
                proxy_raw=f"http://{proxy_s}",
            )
            await alt.connect()
            logger.warning(
                "Userbot [%s]: прокси без схемы поднялся как http://",
                account_id,
            )
            return alt
        except Exception as e:
            last_err = e

    if allow_direct_fallback:
        try:
            direct = make_telethon_client(
                account_id,
                api_id,
                api_hash,
                proxy_raw=None,
            )
            await direct.connect()
            logger.warning("Userbot [%s]: подключён без прокси (fallback).", account_id)
            return direct
        except Exception as e:
            last_err = e

    if last_err is not None:
        raise last_err
    raise RuntimeError("Не удалось подключить клиент")
