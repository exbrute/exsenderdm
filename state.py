import json
import logging
import os
import re
import random
import tempfile
import threading
from dataclasses import asdict, dataclass, field
from typing import Any, Optional

STATE_PATH = os.path.join(os.path.dirname(__file__), "runtime_state.json")
_state_lock = threading.Lock()
logger = logging.getLogger(__name__)


def _int_list(raw: Any) -> list[int]:
    if not isinstance(raw, list):
        return []
    out: list[int] = []
    for x in raw:
        try:
            out.append(int(x))
        except (TypeError, ValueError):
            continue
    return out


def _custom_interval_sec_from_dict(d: dict[str, Any]) -> Optional[float]:
    if d.get("custom_interval_sec") is not None:
        v = float(d["custom_interval_sec"])
        return v if v > 0 else None
    if d.get("custom_interval_min") is not None:
        return float(d["custom_interval_min"]) * 60.0
    return None


def _start_delay_sec_from_dict(d: dict[str, Any]) -> Optional[float]:
    if d.get("start_delay_sec") is not None:
        v = float(d["start_delay_sec"])
        return None if v <= 0 else v
    if d.get("start_delay_min") is not None:
        v = float(d["start_delay_min"]) * 60.0
        return None if v <= 0 else v
    return None


@dataclass
class ChatSpamConfig:
    enabled: bool = False
    custom_message: Optional[str] = None
    text_variants: list[str] = field(default_factory=list)
    custom_interval_sec: Optional[float] = None
    custom_interval_jitter: Optional[float] = None
    extra_text: str = ""
    source_channel_id: Optional[int] = None
    # Конкретный пост канала (если None — берётся последний). Задаётся ссылкой на сообщение в боте.
    source_message_id: Optional[int] = None
    # При True и заданном source_channel_id — пересылка последнего поста; иначе копирование текста.
    source_forward: bool = False
    message_limit: Optional[int] = None
    messages_sent: int = 0
    start_delay_sec: Optional[float] = None
    # Рассылка в ЛС: группа с ✅ — источник участников.
    dm_fetch_offset: int = 0
    # Очередь user_id после «Парс → очередь»; рассылка берёт только отсюда.
    dm_pending_user_ids: list[int] = field(default_factory=list)
    dm_sent_user_ids: list[int] = field(default_factory=list)
    # Отдельное короткое сообщение перед основным (например смайлик).
    dm_opening: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "custom_message": self.custom_message,
            "text_variants": list(self.text_variants or []),
            "custom_interval_sec": self.custom_interval_sec,
            "custom_interval_jitter": self.custom_interval_jitter,
            "extra_text": self.extra_text,
            "source_channel_id": self.source_channel_id,
            "source_message_id": self.source_message_id,
            "source_forward": self.source_forward,
            "message_limit": self.message_limit,
            "messages_sent": self.messages_sent,
            "start_delay_sec": self.start_delay_sec,
            "dm_fetch_offset": int(self.dm_fetch_offset or 0),
            "dm_pending_user_ids": list(self.dm_pending_user_ids or []),
            "dm_sent_user_ids": list(self.dm_sent_user_ids or []),
            "dm_opening": str(self.dm_opening or ""),
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ChatSpamConfig":
        cij = d.get("custom_interval_jitter")
        tv = d.get("text_variants")
        if isinstance(tv, list):
            variants = [str(x) for x in tv if str(x).strip()]
        else:
            variants = []
        return cls(
            enabled=bool(d.get("enabled", False)),
            custom_message=d.get("custom_message"),
            text_variants=variants,
            custom_interval_sec=_custom_interval_sec_from_dict(d),
            custom_interval_jitter=(
                float(cij) if cij is not None else None
            ),
            extra_text=str(d.get("extra_text", "") or ""),
            source_channel_id=(
                int(d["source_channel_id"])
                if d.get("source_channel_id") is not None
                else None
            ),
            source_message_id=(
                int(d["source_message_id"])
                if d.get("source_message_id") is not None
                else None
            ),
            source_forward=bool(d.get("source_forward", False)),
            message_limit=(
                int(d["message_limit"])
                if d.get("message_limit") is not None
                else None
            ),
            messages_sent=int(d.get("messages_sent", 0) or 0),
            start_delay_sec=_start_delay_sec_from_dict(d),
            dm_fetch_offset=int(d.get("dm_fetch_offset", 0) or 0),
            dm_pending_user_ids=_int_list(d.get("dm_pending_user_ids")),
            dm_sent_user_ids=_int_list(d.get("dm_sent_user_ids")),
            dm_opening=str(d.get("dm_opening", "") or ""),
        )


@dataclass
class RuntimeState:
    spam_running: bool = False
    default_message: str = ""
    default_interval_sec: float = 300.0
    default_interval_jitter: float = 0.0
    # Прокси для Telethon этого слота (логин:пароль@ip:port, ip:port, socks5://..., http://...).
    proxy: Optional[str] = None
    # Для новых привязок «канал / id / ссылка на пост» в чате (в самом чате можно переключить).
    default_source_forward: bool = True
    # Один источник для всех чатов, у которых в настройках чата не задан свой канал.
    global_source_channel_id: Optional[int] = None
    global_source_message_id: Optional[int] = None
    chat_configs: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Источники для парса и ЛС: порядок peer_id групп, добавленных по ссылке (строки для JSON).
    ls_source_order: list[str] = field(default_factory=list)
    # Подписи peer_id → название / ссылка для меню.
    ls_source_labels: dict[str, str] = field(default_factory=dict)

    def cfg(self, chat_id: int) -> ChatSpamConfig:
        raw = self.chat_configs.get(str(chat_id))
        if not raw:
            return ChatSpamConfig()
        return ChatSpamConfig.from_dict(raw)

    def set_cfg(self, chat_id: int, cfg: ChatSpamConfig) -> None:
        self.chat_configs[str(chat_id)] = cfg.to_dict()


@dataclass
class MultiAccountState:
    """Несколько userbot-аккаунтов в одном runtime_state.json."""

    active_account_id: str = "default"
    account_order: list[str] = field(default_factory=list)
    accounts: dict[str, RuntimeState] = field(default_factory=dict)
    # Общий пул получателей ЛС (все слоты): контакты и др. сканы.
    shared_dm_pending_user_ids: list[int] = field(default_factory=list)
    shared_dm_sent_user_ids: list[int] = field(default_factory=list)
    # Рассылка из общего пула (настройки текста/интервала — стандарт или первый ✅ источник).
    shared_pool_mailing: bool = False


def active_runtime_state(multi: MultiAccountState) -> RuntimeState:
    return multi.accounts[multi.active_account_id]


def _runtime_state_from_flat_dict(raw: dict[str, Any]) -> RuntimeState:
    chat_configs: dict[str, dict[str, Any]] = {}
    if isinstance(raw.get("chat_configs"), dict):
        chat_configs = {str(k): dict(v) for k, v in raw["chat_configs"].items()}

    if not chat_configs and raw.get("mirror_to_chat_ids"):
        for x in raw["mirror_to_chat_ids"]:
            cid = str(int(x))
            chat_configs[cid] = ChatSpamConfig(enabled=True).to_dict()
    elif not chat_configs and raw.get("target_chat_id") is not None:
        chat_configs[str(int(raw["target_chat_id"]))] = ChatSpamConfig(
            enabled=True
        ).to_dict()

    spam_running = bool(raw.get("spam_running", raw.get("mirroring_enabled", False)))
    dj = float(raw.get("default_interval_jitter", 0) or 0)
    dj = max(0.0, min(0.95, dj))
    if raw.get("default_interval_sec") is not None:
        di = float(raw["default_interval_sec"] or 0)
    else:
        di = float(raw.get("default_interval_min", 5) or 5) * 60.0
    if di <= 0:
        di = 300.0
    dsf = raw.get("default_source_forward")
    default_source_forward = True if dsf is None else bool(dsf)
    gch = raw.get("global_source_channel_id")
    gmid = raw.get("global_source_message_id")
    ls_order_raw = raw.get("ls_source_order")
    if isinstance(ls_order_raw, list) and ls_order_raw:
        ls_source_order = [str(x).strip() for x in ls_order_raw if str(x).strip()]
    else:
        ls_source_order = []
    ls_lab_raw = raw.get("ls_source_labels")
    if isinstance(ls_lab_raw, dict):
        ls_source_labels = {
            str(k).strip(): str(v).strip()[:500]
            for k, v in ls_lab_raw.items()
            if str(k).strip()
        }
    else:
        ls_source_labels = {}
    if not ls_source_order and chat_configs:
        ls_source_order = sorted(
            chat_configs.keys(),
            key=lambda k: int(k) if re.match(r"^-?\d+$", str(k)) else 0,
        )
    return RuntimeState(
        spam_running=spam_running,
        default_message=str(raw.get("default_message", "") or ""),
        default_interval_sec=di,
        default_interval_jitter=dj,
        proxy=(str(raw["proxy"]).strip() or None) if raw.get("proxy") else None,
        default_source_forward=default_source_forward,
        global_source_channel_id=(int(gch) if gch is not None else None),
        global_source_message_id=(int(gmid) if gmid is not None else None),
        chat_configs=chat_configs,
        ls_source_order=ls_source_order,
        ls_source_labels=ls_source_labels,
    )


def _read_state_json_raw() -> dict[str, Any] | None:
    if not os.path.isfile(STATE_PATH):
        return None
    try:
        with _state_lock:
            with open(STATE_PATH, encoding="utf-8") as f:
                return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        backup = STATE_PATH + ".broken"
        try:
            if os.path.isfile(STATE_PATH):
                os.replace(STATE_PATH, backup)
        except OSError:
            pass
        logger.warning(
            "Не удалось прочитать %s (%s). Создан новый state; бэкап: %s",
            STATE_PATH,
            e,
            backup if os.path.isfile(backup) else "нет",
        )
        return None


def load_multi_account_state() -> MultiAccountState:
    env_ids = default_account_ids_from_env()
    if not env_ids:
        env_ids = ["default"]
    raw = _read_state_json_raw()
    if raw is None:
        return MultiAccountState(
            active_account_id=env_ids[0],
            account_order=list(env_ids),
            accounts={aid: RuntimeState() for aid in env_ids},
        )

    if isinstance(raw.get("accounts"), dict):
        accounts: dict[str, RuntimeState] = {}
        for k, v in raw["accounts"].items():
            aid = str(k).strip()
            if not aid:
                continue
            if isinstance(v, dict):
                accounts[aid] = _runtime_state_from_flat_dict(v)
            else:
                accounts[aid] = RuntimeState()
        if not accounts:
            accounts[env_ids[0]] = _runtime_state_from_flat_dict(raw)

        file_order_raw = raw.get("account_order")
        if isinstance(file_order_raw, list) and any(str(x).strip() for x in file_order_raw):
            order = [
                str(x).strip()
                for x in file_order_raw
                if str(x).strip()
            ]
        else:
            order = []
            for e in env_ids:
                if e in accounts:
                    order.append(e)
            for k in accounts:
                if k not in order:
                    order.append(k)

        for e in env_ids:
            if e not in accounts:
                accounts[e] = RuntimeState()
        for k in accounts:
            if k not in order:
                order.append(k)

        active = str(raw.get("active_account_id") or "").strip()
        if not active or active not in accounts:
            active = order[0] if order else next(iter(accounts))
        return MultiAccountState(
            active_account_id=active,
            account_order=order,
            accounts=accounts,
            shared_dm_pending_user_ids=_int_list(raw.get("shared_dm_pending_user_ids")),
            shared_dm_sent_user_ids=_int_list(raw.get("shared_dm_sent_user_ids")),
            shared_pool_mailing=bool(raw.get("shared_pool_mailing", False)),
        )

    st = _runtime_state_from_flat_dict(raw)
    accounts = {env_ids[0]: st}
    for a in env_ids[1:]:
        accounts[a] = RuntimeState()
    return MultiAccountState(
        active_account_id=env_ids[0],
        account_order=list(env_ids),
        accounts=accounts,
    )


def save_multi_account_state(multi: MultiAccountState) -> None:
    seen: set[str] = set()
    order: list[str] = []
    for x in multi.account_order:
        if x in multi.accounts and x not in seen:
            order.append(x)
            seen.add(x)
    for k in multi.accounts:
        if k not in seen:
            order.append(k)
            seen.add(k)
    multi.account_order = order
    if multi.active_account_id not in multi.accounts:
        multi.active_account_id = order[0] if order else "default"

    payload = json.dumps(
        {
            "active_account_id": multi.active_account_id,
            "account_order": list(multi.account_order),
            "accounts": {k: asdict(v) for k, v in multi.accounts.items()},
            "shared_dm_pending_user_ids": list(multi.shared_dm_pending_user_ids or []),
            "shared_dm_sent_user_ids": list(multi.shared_dm_sent_user_ids or []),
            "shared_pool_mailing": bool(multi.shared_pool_mailing),
        },
        indent=2,
        ensure_ascii=False,
    )
    d = os.path.dirname(STATE_PATH) or "."
    with _state_lock:
        fd, tmp_path = tempfile.mkstemp(
            suffix=".json.tmp", prefix="state_", dir=d, text=True
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(payload)
            os.replace(tmp_path, STATE_PATH)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise


def default_account_ids_from_env() -> list[str]:
    raw = os.getenv("ACCOUNTS", "default").strip()
    parts = [x.strip() for x in raw.split(",") if x.strip()]
    return parts or ["default"]


def load_state() -> RuntimeState:
    """Совместимость: активный аккаунт из multi-файла (см. ACCOUNTS в .env)."""
    return active_runtime_state(load_multi_account_state())


def enabled_chat_ids(state: RuntimeState) -> list[int]:
    out: list[int] = []
    for k, v in state.chat_configs.items():
        if v.get("enabled"):
            try:
                out.append(int(k))
            except ValueError:
                continue
    return out


def ls_source_peer_ids(state: RuntimeState) -> list[int]:
    out: list[int] = []
    seen: set[int] = set()
    for s in state.ls_source_order:
        try:
            pid = int(s.strip())
        except ValueError:
            continue
        if pid in seen:
            continue
        seen.add(pid)
        out.append(pid)
    return out


def enabled_ls_source_ids(state: RuntimeState) -> list[int]:
    return [pid for pid in ls_source_peer_ids(state) if state.cfg(pid).enabled]


def effective_interval_sec(state: RuntimeState, chat_id: int) -> float:
    c = state.cfg(chat_id)
    if c.custom_interval_sec is not None and c.custom_interval_sec > 0:
        return float(c.custom_interval_sec)
    return max(1.0, float(state.default_interval_sec or 300))


def effective_jitter(state: RuntimeState, chat_id: int) -> float:
    c = state.cfg(chat_id)
    j = c.custom_interval_jitter
    if j is None:
        j = state.default_interval_jitter
    j = float(j or 0)
    return max(0.0, min(0.95, j))


def random_interval_seconds(state: RuntimeState, chat_id: int) -> float:
    base_sec = effective_interval_sec(state, chat_id)
    j = effective_jitter(state, chat_id)
    if j <= 0:
        return base_sec
    factor = 1.0 + random.uniform(-j, j)
    factor = max(0.1, factor)
    return base_sec * factor


def effective_body_text(
    state: RuntimeState, chat_id: int, *, pick_random_variant: bool = False
) -> str:
    c = state.cfg(chat_id)
    vars_non_empty = [str(x).strip() for x in (c.text_variants or []) if str(x).strip()]
    if vars_non_empty:
        if pick_random_variant:
            return random.choice(vars_non_empty)
        return vars_non_empty[0]
    base = (
        c.custom_message
        if c.custom_message is not None and str(c.custom_message).strip()
        else state.default_message
    )
    return (base or "").strip()


def effective_send_text(
    state: RuntimeState, chat_id: int, *, pick_random_variant: bool = False
) -> str:
    body = effective_body_text(state, chat_id, pick_random_variant=pick_random_variant)
    extra = (state.cfg(chat_id).extra_text or "").strip()
    if extra and body:
        return body + "\n" + extra
    return body or extra


def chat_has_resolved_text(state: RuntimeState, chat_id: int) -> bool:
    c = state.cfg(chat_id)
    if c.source_channel_id is not None:
        return True
    if state.global_source_channel_id is not None:
        return True
    vars_non_empty = [str(x).strip() for x in (c.text_variants or []) if str(x).strip()]
    if vars_non_empty:
        return True
    return bool(effective_body_text(state, chat_id).strip())


def pool_text_chat_id(state: RuntimeState) -> int:
    """Чат настроек для рассылки из общего пула (первый ✅ источник или стандарт)."""
    ids = enabled_ls_source_ids(state)
    return ids[0] if ids else 0


def validate_spam_start(
    state: RuntimeState,
    client_connected: bool,
    multi: MultiAccountState | None = None,
) -> tuple[bool, str]:
    from proxy_util import parse_proxy

    if not client_connected:
        return False, "Userbot не подключён к Telegram (нет сессии или сеть)."
    if state.proxy and str(state.proxy).strip() and parse_proxy(state.proxy) is None:
        return (
            False,
            "Прокси указан, но формат неверный. Используйте логин:пароль@ip:порт или ip:порт.",
        )
    pool_on = bool(
        multi
        and multi.shared_pool_mailing
        and (multi.shared_dm_pending_user_ids or [])
    )
    ids = enabled_ls_source_ids(state)
    if not ids and not pool_on:
        return (
            False,
            "Нет источников с ✅ и пустой общий пул — добавьте группу или "
            "«📇 Контакты → общий пул» и включите рассылку по пулу.",
        )
    if pool_on:
        pid = pool_text_chat_id(state)
        if effective_interval_sec(state, pid) <= 0:
            return False, "Интервал для общего пула должен быть &gt; 0 сек."
        if not chat_has_resolved_text(state, pid):
            return (
                False,
                "Задайте текст для общего пула (стандарт в Настройках или источник с ✅).",
            )
    for cid in ids:
        if not (state.cfg(cid).dm_pending_user_ids or []):
            if not pool_on:
                return (
                    False,
                    "Пустая очередь ЛС для источника <code>"
                    f"{cid}</code>. Откройте «Парс» → ⚙ этого чата → "
                    "<b>Парс → очередь ЛС</b> или включите общий пул.",
                )
            continue
    if state.default_interval_sec <= 0:
        for cid in ids:
            ci = state.cfg(cid).custom_interval_sec
            if ci is None or ci <= 0:
                return (
                    False,
                    "Задайте стандартный интервал &gt; 0 (Настройки) или свой для каждого включённого чата "
                    "<b>в секундах</b>.",
                )
    for cid in ids:
        if effective_interval_sec(state, cid) <= 0:
            return False, f"Интервал для чата <code>{cid}</code> должен быть &gt; 0 сек."
        c = state.cfg(cid)
        if c.source_channel_id is not None:
            continue
        if not chat_has_resolved_text(state, cid):
            return (
                False,
                f"Пустой текст для чата <code>{cid}</code>. Задайте стандартный текст, кастомный, варианты, общий или чатовый канал-источник.",
            )
    return True, ""

