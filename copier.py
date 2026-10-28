"""
Telegram Channel Copier — Railway Deployment
"""

import os
import json
import asyncio
import pickle
import logging
import sys
from pathlib import Path

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, ChannelPrivateError, ChatWriteForbiddenError
from telethon.tl.types import MessageMediaWebPage


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("copier")


def require_env(key: str) -> str:
    val = os.environ.get(key, "").strip()
    if not val:
        log.critical("Missing required environment variable: %s", key)
        sys.exit(1)
    return val


def optional_env(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


API_ID         = int(require_env("API_ID"))
API_HASH       = require_env("API_HASH")
SESSION_STRING = require_env("SESSION_STRING")
SOURCE_CHANNEL = require_env("SOURCE_CHANNEL")
DEST_CHANNELS  = [c.strip() for c in require_env("DEST_CHANNELS").split(",") if c.strip()]

DATA_DIR         = Path(optional_env("DATA_DIR", "/data"))
DELAY_SECONDS    = float(optional_env("DELAY_SECONDS", "0.5"))
RETRIES          = int(optional_env("RETRIES", "5"))
BATCH_SAVE_EVERY = int(optional_env("BATCH_SAVE_EVERY", "50"))
FLOOD_EXTRA_WAIT = int(optional_env("FLOOD_EXTRA_WAIT", "3"))
SKIP_PROTECTED   = optional_env("SKIP_PROTECTED", "true").lower() != "false"
MODE             = optional_env("MODE", "full").lower()

DATA_DIR.mkdir(parents=True, exist_ok=True)
MEDIA_DIR  = DATA_DIR / "downloads"
STATE_FILE = DATA_DIR / "copy_progress.json"
CACHE_FILE = DATA_DIR / "message_cache.pkl"
MEDIA_DIR.mkdir(parents=True, exist_ok=True)

log.info("DATA_DIR : %s", DATA_DIR)
log.info("SOURCE   : %s", SOURCE_CHANNEL)
log.info("DEST(s)  : %s", ", ".join(DEST_CHANNELS))
log.info("MODE     : %s", MODE)


def load_state() -> dict:
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(state: dict) -> None:
    tmp = str(STATE_FILE) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
    Path(tmp).replace(STATE_FILE)


def load_cache() -> dict:
    if CACHE_FILE.exists():
        with open(CACHE_FILE, "rb") as f:
            return pickle.load(f)
    return {}


def save_cache(cache: dict) -> None:
    tmp = str(CACHE_FILE) + ".tmp"
    with open(tmp, "wb") as f:
        pickle.dump(cache, f)
    Path(tmp).replace(CACHE_FILE)


async def cache_messages(client, source_entity, cache_key, last_cached_id):
    cache     = load_cache()
    msg_store = cache.setdefault(cache_key, {})
    new_count = 0

    log.info(
        "[PHASE 1] Caching | already=%d | resume_after_id=%d",
        len(msg_store), last_cached_id,
    )

    try:
        async for msg in client.iter_messages(
            source_entity,
            reverse=True,
            min_id=last_cached_id,
            wait_time=2,
        ):
            if not msg or msg.action:
                continue
            if SKIP_PROTECTED and getattr(msg, "noforwards", False):
                log.debug("SKIP protected id=%d", msg.id)
                continue

            msg_store[msg.id] = msg
            new_count += 1

            if new_count % BATCH_SAVE_EVERY == 0:
                save_cache(cache)
                log.info("  Cached %d new (total=%d)...", new_count, len(msg_store))

    except ChannelPrivateError:
        log.warning("Source went private — saving what we have.")
    except asyncio.CancelledError:
        log.warning("Caching cancelled — progress saved.")
    except Exception as exc:
        log.error("Cache error: %s", exc, exc_info=True)
    finally:
        save_cache(cache)

    log.info("[PHASE 1] Done | new=%d | total=%d", new_count, len(msg_store))
    return msg_store


async def download_with_retry(client, msg):
    for attempt in range(1, RETRIES + 1):
        try:
            path = await client.download_media(msg, file=str(MEDIA_DIR) + "/")
            if path:
                return path
            raise RuntimeError("download_media returned None")
        except FloodWaitError as exc:
            wait = exc.seconds + FLOOD_EXTRA_WAIT
            log.warning("FloodWait %ds (download)...", wait)
            await asyncio.sleep(wait)
        except Exception as exc:
            log.warning("Download attempt %d/%d: %s", attempt, RETRIES, exc)
            if attempt < RETRIES:
                await asyncio.sleep(attempt * 3)
    return None


async def send_one(client, msg, dest_entities):
    text  = msg.raw_text or ""
    media = msg.media

    for dest in dest_entities:
        for attempt in range(1, RETRIES + 1):
            try:
                if not media or isinstance(media, MessageMediaWebPage):
                    if text:
                        await client.send_message(
                            dest,
                            text,
                            formatting_entities=msg.entities,
                            link_preview=isinstance(media, MessageMediaWebPage),
                        )
                else:
                    path = await download_with_retry(client, msg)
                    if not path:
                        log.error("MSG %d: download failed permanently", msg.id)
                        return False
                    try:
                        await client.send_file(
                            dest,
                            path,
                            caption=text or None,
                            formatting_entities=msg.entities,
                        )
                    finally:
                        try:
                            Path(path).unlink(missing_ok=True)
                        except OSError:
                            pass
                break

            except FloodWaitError as exc:
                wait = exc.seconds + FLOOD_EXTRA_WAIT
                log.warning("FloodWait %ds (send)...", wait)
                await asyncio.sleep(wait)

            except ChatWriteForbiddenError:
                log.error("MSG %d: no write permission in dest", msg.id)
                return False

            except Exception as exc:
                log.warning("Send attempt %d/%d msg %d: %s", attempt, RETRIES, msg.id, exc)
                if attempt < RETRIES:
                    await asyncio.sleep(attempt * 2)
                else:
                    log.error("MSG %d: send failed permanently.", msg.id)
                    return False

    return True


async def transfer_from_cache(client, msg_store, dest_entities, state, state_key):
    transferred_ids = set(state.get(state_key + "_done", []))
    sorted_ids      = sorted(msg_store.keys())
    remaining       = [i for i in sorted_ids if i not in transferred_ids]

    log.info(
        "[PHASE 2] Transfer | total=%d | done=%d | remaining=%d",
        len(sorted_ids), len(transferred_ids), len(remaining),
    )

    success = 0
    failed  = 0

    for msg_id in remaining:
        msg = msg_store.get(msg_id)
        if not msg:
            continue

        try:
            ok = await send_one(client, msg, dest_entities)
        except Exception as exc:
            log.error("MSG %d: unexpected error: %s", msg_id, exc, exc_info=True)
            ok = False

        if ok:
            success += 1
            transferred_ids.add(msg_id)
            state[state_key + "_done"]      = list(transferred_ids)
            state[state_key + "_last_sent"] = msg_id
            save_state(state)
            log.info(
                "  OK id=%-8d | sent=%-5d | failed=%-4d | remain=%d",
                msg_id, success, failed,
                len(remaining) - success - failed,
            )
        else:
            failed += 1
            log.warning("  FAIL id=%-8d | sent=%-5d | failed=%-4d", msg_id, success, failed)

        await asyncio.sleep(DELAY_SECONDS)

    log.info("[PHASE 2] Done | success=%d | failed=%d", success, failed)
    return success, failed


async def main():
    log.info("Starting Telegram Copier...")

    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.start()

    me = await client.get_me()
    log.info("Logged in as: %s (id=%d)", me.first_name, me.id)

    try:
        source_entity = await client.get_entity(
            int(SOURCE_CHANNEL) if SOURCE_CHANNEL.lstrip("-").isdigit() else SOURCE_CHANNEL
        )
        log.info("Source: %s (id=%d)", getattr(source_entity, "title", SOURCE_CHANNEL), source_entity.id)
    except Exception as exc:
        log.critical("Cannot resolve SOURCE_CHANNEL '%s': %s", SOURCE_CHANNEL, exc)
        await client.disconnect()
        sys.exit(1)

    dest_entities = []
    for dest_str in DEST_CHANNELS:
        try:
            ent = await client.get_entity(
                int(dest_str) if dest_str.lstrip("-").isdigit() else dest_str
            )
            dest_entities.append(ent)
            log.info("Dest: %s (id=%d)", getattr(ent, "title", dest_str), ent.id)
        except Exception as exc:
            log.error("Cannot resolve dest '%s': %s — skipping.", dest_str, exc)

    if not dest_entities:
        log.critical("No valid destinations. Exiting.")
        await client.disconnect()
        sys.exit(1)

    dest_ids = ",".join(str(e.id) for e in dest_entities)
    base_key = f"{source_entity.id}:{dest_ids}"
    state    = load_state()

    last_cached_id = int(state.get(base_key + "_last_cached", 0))

    if MODE in ("full", "cache_only"):
        msg_store = await cache_messages(client, source_entity, base_key, last_cached_id)
        if msg_store:
            state[base_key + "_last_cached"] = max(msg_store.keys())
            save_state(state)
        if MODE == "cache_only":
            log.info("MODE=cache_only done.")
            await client.disconnect()
            return

    if MODE in ("full", "transfer_only"):
        cache     = load_cache()
        msg_store = cache.get(base_key, {})
        if not msg_store:
            log.critical("Cache empty. Run MODE=cache_only first.")
            await client.disconnect()
            sys.exit(1)

        await transfer_from_cache(client, msg_store, dest_entities, state, base_key)

    await client.disconnect()
    log.info("Done. Exiting cleanly.")


if __name__ == "__main__":
    asyncio.run(main())
