"""Bio и приватность профиля при добавлении аккаунта."""

from __future__ import annotations

import logging
import os

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.functions.account import SetPrivacyRequest, UpdateProfileRequest
from telethon.tl.types import InputPrivacyKeyAbout, InputPrivacyValueAllowAll

logger = logging.getLogger(__name__)

DEFAULT_PROFILE_BIO = "@exdevjejfksbot <3"


def profile_bio_from_env() -> str:
    raw = os.getenv("PROFILE_BIO", DEFAULT_PROFILE_BIO).strip()
    return raw or DEFAULT_PROFILE_BIO


async def apply_account_profile_onboarding(client: TelegramClient) -> tuple[bool, str]:
    """
    Установить «О себе» и видимость для всех.
    Возвращает (успех, сообщение для лога/отчёта).
    """
    bio = profile_bio_from_env()
    if len(bio) > 70:
        bio = bio[:70]

    try:
        await client(
            UpdateProfileRequest(about=bio)
        )
        await client(
            SetPrivacyRequest(
                key=InputPrivacyKeyAbout(),
                rules=[InputPrivacyValueAllowAll()],
            )
        )
        return True, f"Bio: {bio}"
    except FloodWaitError as e:
        wait = int(getattr(e, "seconds", 0) or 0)
        return False, f"FloodWait {wait}s при смене bio"
    except Exception as e:
        logger.exception("apply_account_profile_onboarding")
        return False, type(e).__name__
