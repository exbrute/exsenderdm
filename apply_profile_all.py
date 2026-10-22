#!/usr/bin/env python3
"""Однократно: bio + приватность для всех авторизованных слотов."""

from __future__ import annotations

import asyncio
import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


async def main() -> int:
    from account_profile import apply_account_profile_onboarding
    from state import RuntimeState, load_multi_account_state
    from telethon_accounts import connect_client_with_fallback, make_telethon_client

    api_id = int(os.environ["API_ID"])
    api_hash = os.environ["API_HASH"]
    multi = load_multi_account_state()
    ids = list(dict.fromkeys(multi.account_order + list(multi.accounts.keys())))
    if not ids:
        logger.info("Нет слотов.")
        return 0

    ok_n = 0
    for aid in ids:
        if aid not in multi.accounts:
            multi.accounts[aid] = RuntimeState()
        proxy = multi.accounts[aid].proxy
        client = make_telethon_client(aid, api_id, api_hash, proxy_raw=proxy)
        try:
            client = await connect_client_with_fallback(
                client,
                account_id=aid,
                api_id=api_id,
                api_hash=api_hash,
                proxy_raw=proxy,
                allow_direct_fallback=True,
            )
        except Exception as e:
            logger.warning("[%s] connect: %s", aid, e)
            continue
        try:
            if not await client.is_user_authorized():
                logger.warning("[%s] не авторизован", aid)
                continue
            me = await client.get_me()
            ok, detail = await apply_account_profile_onboarding(client)
            if ok:
                ok_n += 1
                logger.info(
                    "[%s] OK @%s (%s)",
                    aid,
                    me.username or "-",
                    me.id,
                )
            else:
                logger.warning("[%s] fail: %s", aid, detail)
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass

    logger.info("Готово: %s/%s слотов.", ok_n, len(ids))
    return 0 if ok_n else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
