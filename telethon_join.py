"""Вступление userbot в чат / канал по публичной ссылке или invite."""

from __future__ import annotations

import logging
import re
from typing import Tuple

from telethon import TelegramClient, utils
from telethon.errors.rpcerrorlist import UserAlreadyParticipantError
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest

logger = logging.getLogger(__name__)
_ADDLIST_LIMIT = 97

_INVITE_PLUS = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me)/\+([A-Za-z0-9_-]+)", re.I
)
_INVITE_JOINCHAT = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me)/joinchat/([A-Za-z0-9_-]+)", re.I
)
_PUBLIC = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me)/([a-zA-Z][a-zA-Z0-9_]{3,})(?:[\s/?#]|$)", re.I
)
_ADDLIST = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me)/addlist/([A-Za-z0-9_-]+)(?:[\s/?#]|$)",
    re.I,
)
_RESERVED = frozenset(
    {
        "joinchat",
        "addstickers",
        "share",
        "socks",
        "proxy",
        "iv",
        "login",
        "addlist",
    }
)


def _invite_hash(text: str) -> str | None:
    s = (text or "").strip()
    m = _INVITE_PLUS.search(s)
    if m:
        return m.group(1)
    m = _INVITE_JOINCHAT.search(s)
    if m:
        return m.group(1)
    if s.startswith("+") and len(s) > 2:
        h = s[1:].split()[0].split("?")[0]
        if re.match(r"^[A-Za-z0-9_-]+$", h):
            return h
    return None


def _public_username(text: str) -> str | None:
    s = (text or "").strip()
    m = _PUBLIC.search(s)
    if m:
        u = m.group(1)
        if u.lower() not in _RESERVED:
            return u
    if re.match(r"^[a-zA-Z][a-zA-Z0-9_]{3,}$", s) and s.lower() not in _RESERVED:
        return s
    return None


def _addlist_slug(text: str) -> str | None:
    s = (text or "").strip()
    m = _ADDLIST.search(s)
    if not m:
        return None
    return m.group(1)


async def join_chat_or_channel_by_link(
    client: TelegramClient, link_or_text: str
) -> Tuple[int | None, str]:
    """
    Возвращает (peer_id для диалогов Telethon, сообщение для пользователя).
    При ошибке peer_id == None.
    """
    if not client.is_connected():
        return None, "Userbot не подключён."

    h = _invite_hash(link_or_text)
    if h:
        try:
            upd = await client(ImportChatInviteRequest(h))
        except Exception as e:
            name = type(e).__name__
            logger.warning("ImportChatInviteRequest: %s", e)
            if "Invite" in name and "Expired" in name:
                return None, "Ссылка-приглашение истекла или недействительна."
            if "Invite" in name and "Invalid" in name:
                return None, "Неверная ссылка-приглашение."
            if "Flood" in name:
                return None, "Слишком часто — подождите и попробуйте снова."
            return None, f"Не удалось вступить: {e}"
        if getattr(upd, "chats", None):
            ch = upd.chats[0]
            pid = utils.get_peer_id(ch)
            return int(pid), f"Готово. Чат в списке (<code>{pid}</code>)."
        return None, "Вступили, но не удалось определить id — обновите список чатов."

    username = _public_username(link_or_text)
    if username:
        try:
            ent = await client.get_entity(username)
        except Exception as e:
            logger.warning("get_entity %s: %s", username, e)
            return None, f"Канал или группа не найдены: {e}"
        try:
            await client(JoinChannelRequest(ent))
        except UserAlreadyParticipantError:
            pass
        except Exception as e:
            logger.warning("JoinChannelRequest: %s", e)
            return None, f"Не удалось подписаться/вступить: {e}"
        pid = utils.get_peer_id(ent)
        return int(pid), f"Готово. Уже в канале/группе (<code>{pid}</code>)."

    return (
        None,
        "Пришлите ссылку вида <code>https://t.me/+…</code>, "
        "<code>https://t.me/joinchat/…</code> или <code>https://t.me/username</code>.",
    )


async def join_chats_from_addlist(
    client: TelegramClient, link_or_text: str
) -> tuple[bool, str]:
    """
    Обрабатывает t.me/addlist/<slug>.
    Возвращает (handled, message): handled=False, если это не addlist-ссылка.
    """
    slug = _addlist_slug(link_or_text)
    if not slug:
        return False, ""
    if not client.is_connected():
        return True, "Userbot не подключён."

    try:
        from telethon.tl.functions.chatlists import (
            CheckChatlistInviteRequest,
            JoinChatlistInviteRequest,
        )
    except Exception:
        return (
            True,
            "Текущая версия Telethon не поддерживает addlist. Обновите зависимости и перезапустите скрипт.",
        )

    try:
        invite = await client(CheckChatlistInviteRequest(slug=slug))
    except Exception as e:
        name = type(e).__name__
        logger.warning("CheckChatlistInviteRequest: %s", e)
        if "Expired" in name or "Invalid" in name:
            return True, "Ссылка addlist недействительна или устарела."
        if "Flood" in name:
            return True, "Слишком часто — подождите и попробуйте снова."
        return True, f"Не удалось прочитать addlist: {e}"

    all_peers = list(getattr(invite, "peers", None) or [])
    total = len(all_peers)
    selected_peers = all_peers[:_ADDLIST_LIMIT] if total > _ADDLIST_LIMIT else all_peers
    skipped = max(0, total - len(selected_peers))

    input_peers = await _to_input_peers_for_chatlist(client, invite, selected_peers)
    selected_entities = _entities_for_chatlist_peers(invite, selected_peers)

    if input_peers:
        try:
            await client(JoinChatlistInviteRequest(slug=slug, peers=input_peers))
        except Exception as e:
            name = type(e).__name__
            logger.warning("JoinChatlistInviteRequest: %s", e)
            if "Flood" in name:
                return True, "Слишком часто — Telegram временно ограничил вступления (FloodWait)."
            logger.warning("JoinChatlistInviteRequest fallback to per-chat joins: %s", e)
    elif selected_peers:
        logger.warning("addlist: не удалось собрать InputPeer, пробуем прямой join по entity")

    joined = 0
    already = 0
    failed = 0
    for ent in selected_entities:
        try:
            await client(JoinChannelRequest(ent))
            joined += 1
        except UserAlreadyParticipantError:
            already += 1
        except Exception as e:
            failed += 1
            logger.debug("per-chat JoinChannelRequest failed: %s", e, exc_info=True)

    processed = len(selected_peers)
    base_msg = (
        f"Готово: обработано {processed} чатов из списка."
        f" Вступили в {joined}."
    )
    if already > 0:
        base_msg += f" Уже были в {already}."
    if failed > 0:
        base_msg += f" Ошибок вступления: {failed}."
    if skipped > 0:
        base_msg += f" Пропущено {skipped} (лимит {_ADDLIST_LIMIT})."
    return True, base_msg


async def _to_input_peers_for_chatlist(client: TelegramClient, invite, peers: list) -> list:
    """
    missing_peers из CheckChatlistInvite обычно Peer*.
    JoinChatlistInviteRequest ожидает InputPeer*, поэтому конвертируем.
    """
    if not peers:
        return []

    by_id: dict[int, object] = {}
    for ent in list(getattr(invite, "chats", None) or []):
        try:
            by_id[int(utils.get_peer_id(ent))] = ent
        except Exception:
            continue
    for ent in list(getattr(invite, "users", None) or []):
        try:
            by_id[int(utils.get_peer_id(ent))] = ent
        except Exception:
            continue

    out: list = []
    for p in peers:
        try:
            pid = int(utils.get_peer_id(p))
        except Exception:
            continue
        ent = by_id.get(pid)
        if ent is not None:
            try:
                out.append(await client.get_input_entity(ent))
                continue
            except Exception:
                logger.debug("get_input_entity from invite entity failed", exc_info=True)
        try:
            out.append(await client.get_input_entity(pid))
        except Exception:
            logger.debug("get_input_entity by peer id failed: %s", pid, exc_info=True)
            continue
    return out


def _entities_for_chatlist_peers(invite, peers: list) -> list:
    """
    Сопоставляет Peer* из invite.peers с entity из invite.chats.
    Для прямого JoinChannelRequest нужны сущности каналов/супергрупп.
    """
    if not peers:
        return []
    by_id: dict[int, object] = {}
    for ent in list(getattr(invite, "chats", None) or []):
        try:
            by_id[int(utils.get_peer_id(ent))] = ent
        except Exception:
            continue
    out: list = []
    seen: set[int] = set()
    for p in peers:
        try:
            pid = int(utils.get_peer_id(p))
        except Exception:
            continue
        ent = by_id.get(pid)
        if ent is None:
            continue
        if not hasattr(ent, "access_hash"):
            continue
        if pid in seen:
            continue
        seen.add(pid)
        out.append(ent)
    return out


_TME_POST_PRIVATE = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me)/c/(\d+)/(\d+)(?:[\s?#/]|$)",
    re.I,
)
_TME_POST_PUBLIC = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me)/([a-zA-Z][a-zA-Z0-9_]{3,})/(\d+)(?:[\s?#/]|$)",
    re.I,
)


def parse_tme_post_link(text: str) -> tuple[str | int, int] | None:
    """(username str или int peer_id, message_id) или None."""
    s = (text or "").strip()
    m = _TME_POST_PRIVATE.search(s)
    if m:
        inner = int(m.group(1))
        mid = int(m.group(2))
        return (-100 * inner, mid)
    m = _TME_POST_PUBLIC.search(s)
    if m:
        u = m.group(1)
        if u.lower() in _RESERVED or u.lower() == "c":
            return None
        return (u, int(m.group(2)))
    return None


async def resolve_tme_post_for_forward(
    client: TelegramClient, text: str
) -> tuple[int, int] | None:
    """Проверяет ссылку и доступ; возвращает (peer_id, message_id) для state."""
    if not client.is_connected():
        return None
    raw = parse_tme_post_link(text)
    if not raw:
        return None
    peer_spec, mid = raw
    if isinstance(peer_spec, str):
        try:
            ent = await client.get_entity(peer_spec)
        except Exception as e:
            logger.warning("get_entity для ссылки на пост (%s): %s", peer_spec, e)
            return None
        pid = int(utils.get_peer_id(ent))
    else:
        pid = int(peer_spec)
    try:
        got = await client.get_messages(pid, ids=mid)
    except Exception as e:
        logger.warning("get_messages %s id=%s: %s", pid, mid, e)
        return None
    if isinstance(got, (list, tuple)):
        m = got[0] if got else None
    else:
        m = got
    if m is None:
        return None
    return pid, mid
