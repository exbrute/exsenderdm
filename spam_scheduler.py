from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Callable
from typing import NamedTuple, Optional

from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    InputUserDeactivatedError,
    PeerFloodError,
    SlowModeWaitError,
    UserDeactivatedError,
    UserIsBlockedError,
    UserPrivacyRestrictedError,
)

from report_queue import enqueue_report
from state import (
    MultiAccountState,
    RuntimeState,
    effective_send_text,
    enabled_ls_source_ids,
    pool_text_chat_id,
    random_interval_seconds,
)
from yod_util import yod_bot_from_env, yod_run_double_start

logger = logging.getLogger(__name__)

_spam_loop_started: set[str] = set()
_yod_after_peer_flood_last: dict[str, float] = {}

_GAP = 1.5
_FIRST_FIRE_SEC = 3.0
_FORBIDDEN_BACKOFF_SEC = 3600.0
_PEER_FLOOD_BACKOFF_SEC = 3600.0
_DM_SENT_CAP = 50_000
_OPENING_GAP_SEC = 0.9
_SHARED_POOL_SCHED_ID = -1


async def _maybe_yod_after_peer_flood(
    client: TelegramClient,
    account_key: str,
    report: Callable[[str], None],
    loop: asyncio.AbstractEventLoop,
) -> None:
    """Два /start боту из YOD_BOT при ограничении ЛС (PeerFlood / спамблок)."""
    raw = os.getenv("YOD_ON_PEER_FLOOD", "1").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return
    uname = yod_bot_from_env()
    if not uname:
        return
    now = loop.time()
    cooldown = max(60.0, float(os.getenv("YOD_PEER_FLOOD_COOLDOWN_SEC", "600")))
    last = _yod_after_peer_flood_last.get(account_key, 0.0)
    if now - last < cooldown:
        logger.debug("ЙОД после PeerFlood: cooldown слот %s", account_key)
        return
    _yod_after_peer_flood_last[account_key] = now
    try:
        ok, msg = await yod_run_double_start(client, uname=uname)
    except Exception as e:
        logger.exception("yod_run_double_start после PeerFlood")
        report(
            f"⚠️ <b>ЙОД</b> (авто после спамблока): "
            f"<code>{_html_esc(type(e).__name__)}</code>"
        )
        return
    if ok:
        report(f"🧪 <b>ЙОД</b> (авто после PeerFlood): {msg}")
    else:
        report(f"⚠️ <b>ЙОД</b> авто не удался: {msg}")


def _is_write_forbidden_chat(e: BaseException) -> bool:
    n = type(e).__name__
    return n in (
        "ChatWriteForbiddenError",
        "UserBannedInChannelError",
        "ChannelPrivateError",
        "ChatGuestSendForbiddenError",
    )


def _is_bad_peer(e: BaseException) -> bool:
    return type(e).__name__ in ("PeerIdInvalidError", "InputUserDeactivatedError")


def _skip_user(state: RuntimeState, source_pid: int, uid: int, persist: Callable[[], None]) -> None:
    cfg = state.cfg(source_pid)
    lst = list(cfg.dm_sent_user_ids or [])
    if uid not in lst:
        lst.append(uid)
        if len(lst) > _DM_SENT_CAP:
            lst = lst[-(_DM_SENT_CAP // 2) :]
        cfg.dm_sent_user_ids = lst
        state.set_cfg(source_pid, cfg)
        persist()


class _SendPlan(NamedTuple):
    is_forward: bool
    forward_from: Optional[int]
    forward_msg_ids: tuple[int, ...]
    text: Optional[str]
    entities: Optional[list]


async def _resolve_send_plan(
    client: TelegramClient, state: RuntimeState, source_pid: int
) -> _SendPlan:
    c = state.cfg(source_pid)
    peer: Optional[int] = None
    mid: Optional[int] = None
    use_forward: bool = False
    extra: str = ""
    if c.source_channel_id is not None:
        peer = c.source_channel_id
        mid = c.source_message_id
        use_forward = c.source_forward
        extra = c.extra_text or ""
    elif state.global_source_channel_id is not None:
        peer = state.global_source_channel_id
        mid = state.global_source_message_id
        use_forward = state.default_source_forward
        extra = c.extra_text or ""

    if peer is None:
        t = effective_send_text(state, source_pid, pick_random_variant=True)
        return _SendPlan(False, None, (), t, None)

    try:
        if mid is not None:
            fetched = await client.get_messages(peer, ids=mid)
            if isinstance(fetched, (list, tuple)):
                m = fetched[0] if fetched else None
            else:
                m = fetched
        else:
            msg = await client.get_messages(peer, limit=1)
            if isinstance(msg, (list, tuple)):
                m = msg[0] if msg else None
            else:
                m = msg
        if m:
            if use_forward:
                return _SendPlan(True, peer, (m.id,), None, None)
            body = (m.message or "").strip()
            ex = (extra or "").strip()
            if ex and body:
                text = body + "\n" + ex
            elif ex:
                text = ex
            else:
                text = body
            return _SendPlan(False, None, (), text, m.entities)
    except Exception as e:
        logger.warning("Источник поста %s: %s", peer, e)
    t = effective_send_text(state, source_pid, pick_random_variant=True)
    return _SendPlan(False, None, (), t, None)


def _next_uid_from_pending_queue(
    state: RuntimeState, source_pid: int, persist: Callable[[], None]
) -> int | None:
    cfg = state.cfg(source_pid)
    pending = list(cfg.dm_pending_user_ids or [])
    sent = set(cfg.dm_sent_user_ids or [])
    changed = False
    while pending and pending[-1] in sent:
        pending.pop()
        changed = True
    if changed:
        cfg.dm_pending_user_ids = pending
        state.set_cfg(source_pid, cfg)
        persist()
    if not pending:
        return None
    return pending[-1]


def _pop_pending_tail_for_uid(
    state: RuntimeState, source_pid: int, uid: int, persist: Callable[[], None]
) -> None:
    cfg = state.cfg(source_pid)
    pending = list(cfg.dm_pending_user_ids or [])
    if pending and pending[-1] == uid:
        pending.pop()
    elif uid in pending:
        pending.remove(uid)
    cfg.dm_pending_user_ids = pending
    state.set_cfg(source_pid, cfg)
    persist()


def _next_uid_from_shared_pool(
    multi: MultiAccountState, persist: Callable[[], None]
) -> int | None:
    pending = list(multi.shared_dm_pending_user_ids or [])
    sent = set(multi.shared_dm_sent_user_ids or [])
    changed = False
    while pending and pending[-1] in sent:
        pending.pop()
        changed = True
    if changed:
        multi.shared_dm_pending_user_ids = pending
        persist()
    if not pending:
        return None
    return pending[-1]


def _pop_shared_pending_uid(
    multi: MultiAccountState, uid: int, persist: Callable[[], None]
) -> None:
    pending = list(multi.shared_dm_pending_user_ids or [])
    if pending and pending[-1] == uid:
        pending.pop()
    elif uid in pending:
        pending.remove(uid)
    multi.shared_dm_pending_user_ids = pending
    persist()


def _mark_shared_sent(
    multi: MultiAccountState, uid: int, persist: Callable[[], None]
) -> None:
    lst = list(multi.shared_dm_sent_user_ids or [])
    if uid not in lst:
        lst.append(uid)
        if len(lst) > _DM_SENT_CAP:
            lst = lst[-(_DM_SENT_CAP // 2) :]
        multi.shared_dm_sent_user_ids = lst
        persist()


def _skip_shared_user(
    multi: MultiAccountState, uid: int, persist: Callable[[], None]
) -> None:
    _mark_shared_sent(multi, uid, persist)


def start_spam_loop_background(
    client: TelegramClient,
    state: RuntimeState,
    persist: Callable[[], None],
    account_key: str,
    *,
    multi: MultiAccountState | None = None,
) -> bool:
    if account_key in _spam_loop_started:
        logger.debug("spam_loop уже запущен для слота %s", account_key)
        return False
    _spam_loop_started.add(account_key)

    async def _runner() -> None:
        try:
            await spam_loop(
                client,
                state,
                persist=persist,
                account_key=account_key,
                multi=multi,
            )
        finally:
            _spam_loop_started.discard(account_key)

    loop = asyncio.get_running_loop()
    loop.create_task(_runner())
    return True


async def spam_loop(
    client: TelegramClient,
    state: RuntimeState,
    *,
    persist: Callable[[], None],
    account_key: str,
    multi: MultiAccountState | None = None,
) -> None:
    next_fire: dict[int, float] = {}
    loop = asyncio.get_running_loop()
    prev_connected: bool | None = None
    prev_authorized: bool | None = None
    last_auth_error_name: str | None = None

    def report(body: str) -> None:
        enqueue_report(account_key, body)

    while True:
        await asyncio.sleep(2)
        try:
            connected = client.is_connected()

            if prev_connected is True and not connected:
                report(
                    "🔌 <b>Разрыв соединения</b> с Telegram (сеть, прокси или обрыв)."
                )
            if prev_connected is False and connected:
                report("🔌 <b>Соединение восстановлено.</b>")
            prev_connected = connected

            if not connected:
                next_fire.clear()
                continue

            try:
                authorized = await client.is_user_authorized()
            except Exception as e:
                ename = type(e).__name__
                if last_auth_error_name != ename:
                    last_auth_error_name = ename
                    report(
                        "⚠️ <b>Ошибка проверки сессии:</b> "
                        f"<code>{_html_esc(ename)}</code>\n"
                        f"<i>{_html_esc(str(e)[:220])}</i>"
                    )
                authorized = False
            else:
                last_auth_error_name = None

            if prev_authorized is True and not authorized:
                report(
                    "🔐 <b>Сессия недоступна</b> (выход, отзыв сессии или ограничение аккаунта). "
                    "Проверьте слот в приложении Telegram и в боте управления."
                )
            if prev_authorized is False and authorized:
                report("✅ <b>Сессия снова в порядке</b> (авторизация восстановлена).")
            prev_authorized = authorized

            if not authorized:
                next_fire.clear()
                continue

            if not state.spam_running:
                next_fire.clear()
                continue

            now = loop.time()
            ids = enabled_ls_source_ids(state)
            pool_on = bool(
                multi
                and multi.shared_pool_mailing
                and (multi.shared_dm_pending_user_ids or [])
            )
            sched_ids: list[int] = list(ids)
            if pool_on:
                sched_ids.append(_SHARED_POOL_SCHED_ID)

            for pid in list(next_fire.keys()):
                if pid == _SHARED_POOL_SCHED_ID:
                    if not pool_on:
                        del next_fire[pid]
                elif pid not in ids:
                    del next_fire[pid]

            if not sched_ids:
                continue

            for pid in sched_ids:
                if pid not in next_fire:
                    if pid == _SHARED_POOL_SCHED_ID:
                        next_fire[pid] = now + _FIRST_FIRE_SEC
                    else:
                        c0 = state.cfg(pid)
                        delay_sec = max(0.0, float(c0.start_delay_sec or 0))
                        if delay_sec > 0:
                            next_fire[pid] = now + delay_sec
                        else:
                            next_fire[pid] = now + _FIRST_FIRE_SEC

            due = [pid for pid in sched_ids if next_fire.get(pid, 0) <= now]
            due.sort(key=lambda x: next_fire.get(x, 0))

            for source_pid in due:
                if not state.spam_running:
                    break
                from_pool = source_pid == _SHARED_POOL_SCHED_ID
                if not from_pool and source_pid not in enabled_ls_source_ids(state):
                    continue
                uid: int | None = None
                cfg_chat = pool_text_chat_id(state) if from_pool else source_pid
                try:
                    cfg = state.cfg(cfg_chat)
                    src_label = "общий пул" if from_pool else str(source_pid)

                    def _after_fail_user(u: int) -> None:
                        if from_pool and multi is not None:
                            _skip_shared_user(multi, u, persist)
                            _pop_shared_pending_uid(multi, u, persist)
                        else:
                            _skip_user(state, source_pid, u, persist)
                            _pop_pending_tail_for_uid(state, source_pid, u, persist)

                    def _after_sent_user(u: int) -> None:
                        if from_pool and multi is not None:
                            _mark_shared_sent(multi, u, persist)
                            _pop_shared_pending_uid(multi, u, persist)
                        else:
                            c3 = state.cfg(source_pid)
                            lst = list(c3.dm_sent_user_ids or [])
                            if u not in lst:
                                lst.append(u)
                                if len(lst) > _DM_SENT_CAP:
                                    lst = lst[-(_DM_SENT_CAP // 2) :]
                                c3.dm_sent_user_ids = lst
                            state.set_cfg(source_pid, c3)
                            persist()
                            _pop_pending_tail_for_uid(state, source_pid, u, persist)

                    if not from_pool:
                        lim = cfg.message_limit
                        if lim is not None and int(lim) > 0:
                            if cfg.messages_sent >= int(lim):
                                c2 = state.cfg(source_pid)
                                c2.enabled = False
                                state.set_cfg(source_pid, c2)
                                persist()
                                del next_fire[source_pid]
                                report(
                                    f"✅ Лимит ЛС для источника <code>{source_pid}</code> достигнут — источник выключен."
                                )
                                logger.info(
                                    "Лимит ЛС %s для источника %s", lim, source_pid
                                )
                                continue

                    if from_pool:
                        if multi is None:
                            continue
                        uid = _next_uid_from_shared_pool(multi, persist)
                    else:
                        uid = _next_uid_from_pending_queue(state, source_pid, persist)
                    if uid is None:
                        logger.info(
                            "%s: очередь ЛС пуста.",
                            src_label,
                        )
                        next_fire[source_pid] = loop.time() + 300.0
                        await asyncio.sleep(_GAP)
                        continue

                    plan = await _resolve_send_plan(client, state, cfg_chat)
                    opening = (cfg.dm_opening or "").strip()

                    if plan.is_forward:
                        if not plan.forward_msg_ids or plan.forward_from is None:
                            logger.warning(
                                "Пересылка: нет сообщения в канале-источнике, пропуск %s",
                                src_label,
                            )
                            _after_fail_user(uid)
                            next_fire[source_pid] = loop.time() + random_interval_seconds(
                                state, cfg_chat
                            )
                            continue
                        if opening:
                            await client.send_message(uid, opening)
                            await asyncio.sleep(_OPENING_GAP_SEC)
                        await client.forward_messages(
                            uid,
                            list(plan.forward_msg_ids),
                            from_peer=plan.forward_from,
                        )
                    else:
                        text, entities = plan.text, plan.entities
                        if not text:
                            logger.warning("Пустой текст, пропуск %s", src_label)
                            _after_fail_user(uid)
                            next_fire[source_pid] = loop.time() + random_interval_seconds(
                                state, cfg_chat
                            )
                            continue
                        if opening:
                            await client.send_message(uid, opening)
                            await asyncio.sleep(_OPENING_GAP_SEC)
                        if entities:
                            try:
                                await client.send_message(
                                    uid, text, formatting_entities=entities
                                )
                            except TypeError:
                                await client.send_message(uid, text)
                        else:
                            await client.send_message(uid, text)

                    logger.info("ЛС отправлено user=%s (%s)", uid, src_label)
                    if not from_pool:
                        c3 = state.cfg(source_pid)
                        lim2 = c3.message_limit
                        if lim2 is not None and int(lim2) > 0:
                            c3.messages_sent = int(c3.messages_sent or 0) + 1
                            hit = c3.messages_sent >= int(lim2)
                            if hit:
                                c3.enabled = False
                                report(
                                    f"✅ Лимит ЛС для источника <code>{source_pid}</code> достигнут — источник выключен."
                                )
                            state.set_cfg(source_pid, c3)
                            persist()
                            if hit:
                                _after_sent_user(uid)
                                del next_fire[source_pid]
                                await asyncio.sleep(_GAP)
                                continue
                    _after_sent_user(uid)

                    next_fire[source_pid] = loop.time() + random_interval_seconds(
                        state, cfg_chat
                    )
                    await asyncio.sleep(_GAP)
                    continue

                except PeerFloodError as e:
                    w = int(getattr(e, "seconds", 0) or 0) + 60
                    report(
                        f"🚫 <b>PeerFlood</b> при ЛС (источник <code>{source_pid}</code>).\n"
                        f"Пауза ~{max(w, _PEER_FLOOD_BACKOFF_SEC)} с. "
                        "Telegram <b>ограничил личные сообщения</b> с этого аккаунта "
                        "(жалобы, массовые ЛС, спам-блок по поведению)."
                    )
                    await _maybe_yod_after_peer_flood(client, account_key, report, loop)
                    next_fire[source_pid] = loop.time() + float(
                        max(w, _PEER_FLOOD_BACKOFF_SEC)
                    )
                    continue
                except (FloodWaitError, SlowModeWaitError) as e:
                    wait = int(getattr(e, "seconds", 0) or 0) + 2
                    report(
                        f"⏳ FloodWait {wait}s · источник <code>{source_pid}</code>."
                    )
                    next_fire[source_pid] = loop.time() + float(max(wait, 1))
                    continue
                except (UserPrivacyRestrictedError, UserIsBlockedError) as e:
                    logger.info("Пропуск user %s: %s", uid, e)
                    if uid is not None:
                        if from_pool and multi is not None:
                            _skip_shared_user(multi, uid, persist)
                            _pop_shared_pending_uid(multi, uid, persist)
                        else:
                            _skip_user(state, source_pid, uid, persist)
                            _pop_pending_tail_for_uid(state, source_pid, uid, persist)
                    next_fire[source_pid] = loop.time() + random_interval_seconds(
                        state, cfg_chat if from_pool else source_pid
                    )
                    continue
                except (UserDeactivatedError, InputUserDeactivatedError) as e:
                    logger.info("Деактивированный user: %s", e)
                    if uid is not None:
                        if from_pool and multi is not None:
                            _skip_shared_user(multi, uid, persist)
                            _pop_shared_pending_uid(multi, uid, persist)
                        else:
                            _skip_user(state, source_pid, uid, persist)
                            _pop_pending_tail_for_uid(state, source_pid, uid, persist)
                    next_fire[source_pid] = loop.time() + random_interval_seconds(
                        state, cfg_chat if from_pool else source_pid
                    )
                    continue
                except Exception as e:
                    if _is_write_forbidden_chat(e):
                        report(
                            f"⛔ Нет доступа к источнику <code>{source_pid}</code>: {_html_esc(type(e).__name__)}"
                        )
                        next_fire[source_pid] = loop.time() + _FORBIDDEN_BACKOFF_SEC
                        if uid is not None:
                            _pop_pending_tail_for_uid(state, source_pid, uid, persist)
                        await asyncio.sleep(_GAP)
                        continue
                    if _is_bad_peer(e):
                        c4 = state.cfg(source_pid)
                        c4.enabled = False
                        state.set_cfg(source_pid, c4)
                        persist()
                        del next_fire[source_pid]
                        if uid is not None:
                            _pop_pending_tail_for_uid(state, source_pid, uid, persist)
                        report(
                            f"❌ Источник <code>{source_pid}</code> отключён: некорректный peer."
                        )
                        await asyncio.sleep(_GAP)
                        continue
                    logger.warning("ЛС источник %s: %s", source_pid, e)
                    if uid is not None:
                        _skip_user(state, source_pid, uid, persist)
                        _pop_pending_tail_for_uid(state, source_pid, uid, persist)
                    next_fire[source_pid] = loop.time() + random_interval_seconds(
                        state, source_pid
                    )
                    await asyncio.sleep(_GAP)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception("spam_loop: %s", e)
            report(f"⚠️ Ошибка цикла рассылки: {_html_esc(e)}")


def _html_esc(x: object) -> str:
    from html import escape

    return escape(str(x), quote=True)
