"""ЙОД: дважды /start боту (например @Spambot) для снятия ограничений."""

from __future__ import annotations

import asyncio
import html
import logging
import os
import re

logger = logging.getLogger(__name__)


def parse_yod_bot_username(raw: str) -> str | None:
    """@bot, username или t.me/bot для ЙОД."""
    s = (raw or "").strip()
    if not s:
        return None
    m = re.search(
        r"(?:https?://)?(?:t\.me|telegram\.me)/([a-zA-Z][a-zA-Z0-9_]{3,})(?:[\s/?#]|$)",
        s,
        re.I,
    )
    if m:
        u = m.group(1)
        if u.lower() in (
            "joinchat",
            "addlist",
            "share",
            "socks",
            "proxy",
            "c",
        ):
            return None
        return u
    if s.startswith("@"):
        u = s[1:].split()[0].split("?")[0].strip()
    else:
        u = s.split()[0].split("?")[0].strip()
    if re.match(r"^[a-zA-Z][a-zA-Z0-9_]{3,}$", u):
        return u
    return None


def yod_bot_from_env() -> str | None:
    return parse_yod_bot_username(os.getenv("YOD_BOT", ""))


def _esc(s: str) -> str:
    return html.escape(str(s), quote=True)


async def yod_run_double_start(tc, *, uname: str) -> tuple[bool, str]:
    from telethon.tl.types import User as TgUser

    try:
        ent = await tc.get_entity(uname)
    except Exception as e:
        return False, f"Не найден: {_esc(e)}"
    if not isinstance(ent, TgUser) or not getattr(ent, "bot", False):
        return (
            False,
            "Укажите аккаунт <b>бота</b> (в Telegram он помечен как bot).",
        )
    interval = max(1.0, float(os.getenv("YOD_START_INTERVAL_SEC", "3")))
    try:
        await tc.send_message(ent, "/start")
        await asyncio.sleep(interval)
        await tc.send_message(ent, "/start")
    except Exception as e:
        return False, f"Ошибка отправки: {_esc(e)}"
    at = f"@{ent.username}" if ent.username else str(ent.id)
    return True, f"✅ Готово: два раза <code>/start</code> → <code>{_esc(at)}</code>."
