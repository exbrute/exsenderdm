from __future__ import annotations

from collections.abc import Callable

from telethon import TelegramClient, helpers
from typing import Iterable, Optional

from telethon.tl import functions, types
from telethon.tl.types import Channel, Chat, User

from state import MultiAccountState, RuntimeState, enabled_ls_source_ids

_MAX_PARTICIPANTS_CHUNK = 200
_DM_PENDING_CAP = 50_000
_MESSAGE_SENDER_SCAN_LIMIT = 15_000


def append_users_to_shared_pool(
    multi: MultiAccountState,
    persist: Callable[[], None],
    users: Iterable[User],
    me_id: int | None,
) -> tuple[int, int]:
    """Добавить пользователей в общий пул рассылки (между слотами)."""
    sent = set(multi.shared_dm_sent_user_ids or [])
    pending = list(multi.shared_dm_pending_user_ids or [])
    in_pending: set[int] = set(pending)
    added = 0
    for u in users:
        if len(pending) >= _DM_PENDING_CAP:
            break
        if getattr(u, "bot", False) or getattr(u, "deleted", False):
            continue
        if me_id is not None and u.id == me_id:
            continue
        if u.id in sent or u.id in in_pending:
            continue
        pending.append(u.id)
        in_pending.add(u.id)
        added += 1
    multi.shared_dm_pending_user_ids = pending
    persist()
    return added, len(pending)


async def parse_contacts_into_shared_pool(
    client: TelegramClient,
    multi: MultiAccountState,
    persist: Callable[[], None],
) -> tuple[int, int]:
    """
    Адресная книга Telegram (контакты текущей сессии) → общий пул.
    Уже отправленные (shared_dm_sent) и уже в очереди не дублируются.
    """
    me = await client.get_me()
    my_id = me.id if me else None
    collected: list[User] = []
    try:
        result = await client(
            functions.contacts.GetContactsRequest(hash=0)
        )
        for u in result.users:
            if isinstance(u, User):
                collected.append(u)
    except Exception:
        async for u in client.iter_contacts():
            if isinstance(u, User):
                collected.append(u)
    return append_users_to_shared_pool(multi, persist, collected, my_id)


def append_users_to_dm_pending(
    state: RuntimeState,
    source_peer_id: int,
    persist: Callable[[], None],
    users: Iterable[User],
    me_id: int | None,
) -> tuple[int, int]:
    """Добавить объекты User в очередь ЛС этого источника (дедуп, лимит)."""
    cfg = state.cfg(source_peer_id)
    sent = set(cfg.dm_sent_user_ids or [])
    pending = list(cfg.dm_pending_user_ids or [])
    in_pending: set[int] = set(pending)
    added = 0
    for u in users:
        if len(pending) >= _DM_PENDING_CAP:
            break
        if getattr(u, "bot", False) or getattr(u, "deleted", False):
            continue
        if me_id is not None and u.id == me_id:
            continue
        if u.id in sent or u.id in in_pending:
            continue
        pending.append(u.id)
        in_pending.add(u.id)
        added += 1
    cfg.dm_pending_user_ids = pending
    cfg.dm_fetch_offset = 0
    state.set_cfg(source_peer_id, cfg)
    persist()
    return added, len(pending)


async def parse_recent_message_senders_into_pending(
    client: TelegramClient,
    state: RuntimeState,
    source_peer_id: int,
    persist: Callable[[], None],
    *,
    limit: int = _MESSAGE_SENDER_SCAN_LIMIT,
) -> tuple[int, int]:
    """
    Уникальные отправители из последних ``limit`` сообщений группы / супергруппы.
    """
    me = await client.get_me()
    my_id = me.id if me else None
    by_uid: dict[int, User] = {}
    lim = max(1, min(int(limit), 50_000))
    async for msg in client.iter_messages(source_peer_id, limit=lim):
        u: User | None = None
        if isinstance(msg.sender, User):
            u = msg.sender
        elif msg.sender_id and msg.sender_id > 0 and msg.sender_id not in by_uid:
            try:
                ent = await client.get_entity(msg.sender_id)
            except Exception:
                continue
            if isinstance(ent, User):
                u = ent
        if u is None:
            continue
        if u.id not in by_uid:
            by_uid[u.id] = u
    return append_users_to_dm_pending(
        state, source_peer_id, persist, by_uid.values(), my_id
    )


async def parse_ls_dialogs_users_into_pending(
    client: TelegramClient,
    state: RuntimeState,
    persist: Callable[[], None],
) -> tuple[int, int, int | None]:
    """
    Пользователи из личных диалогов (ЛС) → очередь **первого** включённого источника.
    Возвращает (добавлено, размер очереди, peer_id источника или None).
    """
    ids = enabled_ls_source_ids(state)
    if not ids:
        return 0, 0, None
    target = ids[0]
    me = await client.get_me()
    my_id = me.id if me else None
    collected: list[User] = []
    async for dialog in client.iter_dialogs():
        e = dialog.entity
        if not isinstance(e, User):
            continue
        if getattr(e, "bot", False) or getattr(e, "deleted", False):
            continue
        if my_id is not None and e.id == my_id:
            continue
        collected.append(e)
    added, total = append_users_to_dm_pending(
        state, target, persist, collected, my_id
    )
    return added, total, target


def _html_esc(x: object) -> str:
    from html import escape

    return escape(str(x), quote=True)


async def resolve_group_source_from_link(
    client: TelegramClient, link: str
) -> tuple[int | None, str, str]:
    """
    Разрешить ссылку в peer_id группы/супергруппы.
    Возвращает (peer_id, title, err_html) — при ошибке peer_id None и заполнен err_html.
    """
    from telethon_join import join_chat_or_channel_by_link, parse_tme_post_link

    s = (link or "").strip()
    if not s:
        return None, "", "Пустая ссылка."

    if parse_tme_post_link(s):
        return None, "", "Это ссылка на <b>пост</b>, а не на группу."

    from telethon_join import _addlist_slug  # noqa: PLC2701

    if _addlist_slug(s):
        return (
            None,
            "",
            "Укажите ссылку на <b>одну</b> группу. Списки <code>addlist</code> сюда не подходят.",
        )

    peer_id, msg = await join_chat_or_channel_by_link(client, s)
    if peer_id is None:
        return None, "", msg

    try:
        ent = await client.get_entity(peer_id)
    except Exception as e:
        return None, "", f"Не удалось открыть чат: {_html_esc(e)}"

    if isinstance(ent, Channel):
        if getattr(ent, "broadcast", False):
            return (
                None,
                "",
                "Это канал (только посты). Нужна <b>группа</b> или <b>супергруппа</b>.",
            )
        title = (ent.title or "").strip() or str(peer_id)
    elif isinstance(ent, Chat):
        title = (ent.title or "").strip() or str(peer_id)
    else:
        return None, "", "Ссылка ведёт не в группу."

    return int(peer_id), title, ""


async def fetch_participants_chunk(
    client: TelegramClient, source_peer_id: int, offset: int, limit: int
) -> list[User]:
    """Участники группы/супергруппы порциями (для рассылки в ЛС).

    В Telethon 1.36+ у ``get_participants`` нет аргумента ``offset`` (он давал
    TypeError и тихо превращался в пустой список). Для супергрупп используем
    ``GetParticipantsRequest`` с полем ``offset``, как во внутреннем итераторе.
    """
    off = max(0, int(offset))
    lim = min(max(1, int(limit)), _MAX_PARTICIPANTS_CHUNK)

    inp = await client.get_input_entity(source_peer_id)
    ty = helpers._entity_type(inp)

    if ty == helpers._EntityType.CHANNEL:
        result = await client(
            functions.channels.GetParticipantsRequest(
                channel=inp,
                filter=types.ChannelParticipantsSearch(""),
                offset=off,
                limit=lim,
                hash=0,
            )
        )
        if not result.participants:
            return []
        user_map = {u.id: u for u in result.users}
        out: list[User] = []
        for p in result.participants:
            if isinstance(p, types.ChannelParticipantLeft):
                continue
            if isinstance(p, types.ChannelParticipantBanned):
                peer = p.peer
                if not isinstance(peer, types.PeerUser):
                    continue
                uid = peer.user_id
            else:
                uid = p.user_id
            u = user_map.get(uid)
            if u is not None and isinstance(u, User):
                out.append(u)
        return out

    if ty == helpers._EntityType.CHAT:
        all_users = await client.get_participants(source_peer_id)
        slc = [u for u in all_users if isinstance(u, User)]
        return slc[off : off + lim]

    return []


async def parse_group_members_into_pending(
    client: TelegramClient,
    state: RuntimeState,
    source_peer_id: int,
    persist: Callable[[], None],
) -> tuple[int, int]:
    """
    Пройти участников группы и добавить user_id в очередь рассылки.
    Пропускаются: боты, удалённые, вы; уже в dm_sent_user_ids или уже в очереди.
    Возвращает (сколько новых добавлено, итоговый размер очереди).
    """
    me = await client.get_me()
    my_id = me.id if me else None
    cfg = state.cfg(source_peer_id)
    sent = set(cfg.dm_sent_user_ids or [])
    pending = list(cfg.dm_pending_user_ids or [])
    in_pending: set[int] = set(pending)
    added = 0
    offset = 0
    while len(pending) < _DM_PENDING_CAP:
        chunk = await fetch_participants_chunk(
            client, source_peer_id, offset, _MAX_PARTICIPANTS_CHUNK
        )
        if not chunk:
            break
        for u in chunk:
            if getattr(u, "bot", False):
                continue
            if getattr(u, "deleted", False):
                continue
            if my_id is not None and u.id == my_id:
                continue
            if u.id in sent or u.id in in_pending:
                continue
            pending.append(u.id)
            in_pending.add(u.id)
            added += 1
            if len(pending) >= _DM_PENDING_CAP:
                break
        offset += len(chunk)
    cfg.dm_pending_user_ids = pending
    cfg.dm_fetch_offset = 0
    state.set_cfg(source_peer_id, cfg)
    persist()
    return added, len(pending)


async def list_group_chats(client: TelegramClient) -> list[tuple[int, str]]:
    rows: list[tuple[int, str]] = []
    async for dialog in client.iter_dialogs():
        e = dialog.entity
        if isinstance(e, User):
            continue
        if isinstance(e, Channel):
            if getattr(e, "broadcast", False):
                continue
            title = (e.title or "").strip() or str(e.id)
        elif isinstance(e, Chat):
            title = (e.title or "").strip() or str(e.id)
        else:
            continue
        rows.append((dialog.id, title))
    rows.sort(key=lambda x: x[1].casefold())
    return rows


async def group_chat_title(
    client: TelegramClient, peer_id: int
) -> Optional[str]:
    """Название группы / супергруппы по id диалога, иначе None."""
    try:
        ent = await client.get_entity(peer_id)
    except Exception:
        return None
    if isinstance(ent, Channel) and not getattr(ent, "broadcast", False):
        return (ent.title or "").strip() or None
    if isinstance(ent, Chat):
        return (ent.title or "").strip() or None
    return None


async def list_broadcast_channels(client: TelegramClient) -> list[tuple[int, str]]:
    rows: list[tuple[int, str]] = []
    async for dialog in client.iter_dialogs():
        e = dialog.entity
        if isinstance(e, Channel) and getattr(e, "broadcast", False):
            title = (e.title or "").strip() or str(e.id)
            rows.append((dialog.id, title))
    rows.sort(key=lambda x: x[1].casefold())
    return rows
