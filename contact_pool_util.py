"""Общий пул контактов: автосинхронизация и текст для UI."""

from __future__ import annotations

import logging
from collections.abc import Callable

from telethon import TelegramClient

from group_dialogs import parse_contacts_into_shared_pool
from state import MultiAccountState

logger = logging.getLogger(__name__)


async def sync_session_contacts_to_pool(
    client: TelegramClient,
    multi: MultiAccountState,
    persist: Callable[[], None],
    *,
    account_id: str = "",
) -> tuple[int, int]:
    """Скан адресной книги сессии → общий пул. Возвращает (добавлено, всего в очереди)."""
    added, total = await parse_contacts_into_shared_pool(client, multi, persist)
    logger.info(
        "Пул контактов [%s]: +%s новых, в очереди %s",
        account_id or "?",
        added,
        total,
    )
    return added, total


def pool_stats_line(multi: MultiAccountState) -> str:
    p = len(multi.shared_dm_pending_user_ids or [])
    s = len(multi.shared_dm_sent_user_ids or [])
    on = "✅ вкл" if multi.shared_pool_mailing else "❌ выкл"
    return f"📇 <b>Пул контактов</b>: ждут <b>{p}</b> · отправлено <b>{s}</b> · рассылка {on}"


def pool_panel_body(multi: MultiAccountState, *, active_slot: str | None = None) -> str:
    p = len(multi.shared_dm_pending_user_ids or [])
    s = len(multi.shared_dm_sent_user_ids or [])
    on = "✅ включена" if multi.shared_pool_mailing else "❌ выключена"
    slot = (
        f"Активный слот: <code>{active_slot}</code>\n\n"
        if active_slot
        else ""
    )
    return (
        f"<b>Общий пул рассылки</b>\n\n"
        f"{slot}"
        "Сюда попадают <b>контакты</b> из адресной книги каждой авторизованной сессии "
        "(обновляются автоматически при входе и переключении слота).\n\n"
        f"В очереди: <b>{p}</b>\n"
        f"Уже получали ЛС: <b>{s}</b>\n"
        f"Рассылка по пулу: <b>{on}</b>\n\n"
        "<i>После отправки пользователь убирается из очереди.</i>"
    )
