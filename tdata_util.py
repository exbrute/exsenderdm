"""Импорт сессии Telethon из папки Telegram Desktop (tdata) через opentele."""

from __future__ import annotations

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


async def import_tdata_to_session(
    tdata_dir: str | Path,
    session_base: str | Path,
    *,
    api_id: int,
    api_hash: str,
    passcode: str | None = None,
) -> None:
    """
    Конвертировать tdata → файл сессии Telethon (session_base без .session).
    """
    td_path = Path(tdata_dir)
    if not td_path.is_dir():
        raise TDataImportError("Каталог tdata не найден.")

    api = API.TelegramDesktop.Generate(system="windows")
    try:
        tdesk = TDesktop(str(td_path), passcode=passcode or "")
    except TDataWrongPasscode as e:
        raise TDataNeedPasscode() from e
    except Exception as e:
        raise TDataImportError(f"Не удалось прочитать tdata: {type(e).__name__}") from e

    if not tdesk.isLoaded() or not tdesk.accounts:
        raise TDataImportError("В tdata нет аккаунтов или файл повреждён.")

    session_stem = str(session_base)
    if session_stem.endswith(".session"):
        session_stem = session_stem[: -len(".session")]

    Path(session_stem).parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-journal", "-wal", "-shm"):
        cand = session_stem + ".session" + suffix
        if os.path.isfile(cand):
            os.remove(cand)

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

    # opentele пишет sqlite-сессию; дальше используем обычный Telethon с API_ID/API_HASH из .env
    _ = api_id
    _ = api_hash


def cleanup_tree(path: str | Path | None) -> None:
    if not path:
        return
    p = Path(path)
    if p.is_dir():
        shutil.rmtree(p, ignore_errors=True)
