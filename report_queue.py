"""Очередь отчётов в Bot API для админов (фоновая отправка)."""

from __future__ import annotations

import asyncio
import html
import logging

logger = logging.getLogger(__name__)

_queue: asyncio.Queue[tuple[str, str]] | None = None


def get_report_queue() -> asyncio.Queue[tuple[str, str]]:
    global _queue
    if _queue is None:
        _queue = asyncio.Queue(maxsize=500)
    return _queue


def enqueue_report(slot: str, body_html: str) -> None:
    """Не блокирует spam_loop; пропуск при переполнении."""
    text = f"<b>Слот <code>{html.escape(slot)}</code></b>\n{body_html}"
    try:
        get_report_queue().put_nowait((slot, text))
    except asyncio.QueueFull:
        logger.warning("Очередь отчётов полна, отчёт пропущен.")


async def drain_reports_forever(bot, admin_ids: set[int]) -> None:
    q = get_report_queue()
    while True:
        _slot, msg = await q.get()
        if not admin_ids:
            continue
        for uid in admin_ids:
            try:
                await bot.send_message(uid, msg, parse_mode="HTML")
            except Exception:
                logger.debug("Не удалось отправить отчёт админу %s", uid, exc_info=True)
