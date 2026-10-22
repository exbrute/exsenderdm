"""Импорт сессии Telethon из папки Telegram Desktop (tdata) через opentele."""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
import zipfile
from pathlib import Path

from opentele.api import API
from opentele.exception import TDataWrongPasscode
from opentele.td import TDesktop
from opentele.tl import TelegramClient as OpenTeleClient


class TDataNeedPasscode(Exception):
    """Папка tdata зашифрована локальным паролем Telegram Desktop."""


class TDataImportError(Exception):
    pass


def find_tdata_root(base: Path) -> Path | None:
    """Найти каталог tdata (есть key_datas или map*) внутри распакованного архива."""
    base = base.resolve()
    markers = ("key_datas", "map0", "map1", "map")
    if any((base / m).exists() for m in markers):
        return base
    best: Path | None = None
    for root, _dirs, files in os.walk(base):
        p = Path(root)
        if any(m in files for m in markers):
            if best is None or len(p.parts) < len(best.parts):
                best = p
    return best


def extract_upload_to_temp(file_path: str) -> Path:
    """Распаковать zip или вернуть каталог, если загружен как папка (не используется Bot API)."""
    p = Path(file_path)
    if p.is_dir():
        return p
    dest = Path(tempfile.mkdtemp(prefix="tdata_up_"))
    with zipfile.ZipFile(p, "r") as zf:
        zf.extractall(dest)
    return dest


def _load_tdesktop(td_path: str, passcode: str) -> TDesktop:
    try:
        tdesk = TDesktop(td_path, passcode=passcode)
    except TDataWrongPasscode as e:
        raise TDataNeedPasscode() from e
    except Exception as e:
        raise TDataImportError(f"Не удалось прочитать tdata: {type(e).__name__}") from e
    if not tdesk.isLoaded() or not tdesk.accounts:
        raise TDataImportError("В tdata нет аккаунтов или файл повреждён.")
    return tdesk


async def import_tdata_to_session(
    tdata_dir: str | Path,
    session_base: str | Path,
    *,
    api_id: int,
    api_hash: str,
    passcode: str | None = None,
    timeout_sec: float = 180.0,
) -> None:
    """
    Конвертировать tdata → файл сессии Telethon (session_base без .session).
    """
    td_path = Path(tdata_dir)
    if not td_path.is_dir():
        raise TDataImportError("Каталог tdata не найден.")

    session_stem = str(session_base)
    if session_stem.endswith(".session"):
        session_stem = session_stem[: -len(".session")]

    Path(session_stem).parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-journal", "-wal", "-shm"):
        cand = session_stem + ".session" + suffix
        if os.path.isfile(cand):
            os.remove(cand)

    async def _run() -> None:
        tdesk = await asyncio.to_thread(
            _load_tdesktop, str(td_path), passcode or ""
        )
        api = API.TelegramDesktop.Generate(system="windows")
        client = await OpenTeleClient.FromTDesktop(
            tdesk,
            session=session_stem,
            api=api,
        )
        try:
            await client.connect()
            if not await client.is_user_authorized():
                raise TDataImportError("Сессия из tdata не авторизована.")
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass

    try:
        await asyncio.wait_for(_run(), timeout=timeout_sec)
    except asyncio.TimeoutError as e:
        raise TDataImportError(
            f"Импорт tdata занял больше {int(timeout_sec)} с — прервано."
        ) from e

    _ = api_id
    _ = api_hash


def cleanup_tree(path: str | Path | None) -> None:
    if not path:
        return
    p = Path(path)
    if p.is_dir():
        shutil.rmtree(p, ignore_errors=True)
