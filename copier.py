import os
import json
import asyncio
import pickle
from telethon import TelegramClient
from telethon.errors import FloodWaitError, ChannelPrivateError
from telethon.tl.types import MessageMediaWebPage

API_ID   = 37725843
API_HASH = "fda38d832e85b7f746d2c5a67674f07b"

SESSION      = "copier_session"
STATE_FILE   = "copy_progress.json"
CACHE_FILE   = "message_cache.pkl"
MEDIA_DIR    = "downloads"
RETRIES      = 5

os.makedirs(MEDIA_DIR, exist_ok=True)
client = TelegramClient(SESSION, API_ID, API_HASH)


# ── STATE ──────────────────────────────────────────────────────────────────────

def load_state():
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_state(state):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, STATE_FILE)

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "rb") as f:
            return pickle.load(f)
    return {}

def save_cache(cache):
    tmp = CACHE_FILE + ".tmp"
    with open(tmp, "wb") as f:
        pickle.dump(cache, f)
    os.replace(tmp, CACHE_FILE)


# ── DIALOG PICKER ──────────────────────────────────────────────────────────────

async def choose_dialog(dialogs, title, multiple=False):
    print(f"\n--- {title} ---")
    for i, d in enumerate(dialogs, 1):
        print(f"{i}. {d.name}  (ID: {d.id})")
    while True:
        try:
            raw  = input("Numbers: ").strip()
            nums = [int(x.strip()) for x in raw.split(",")]
            if not multiple and len(nums) != 1:
                print("Exactly one select karo.")
                continue
            if all(1 <= n <= len(dialogs) for n in nums):
                return [dialogs[n - 1] for n in dict.fromkeys(nums)]
        except ValueError:
            pass
        print("Invalid.")


# ── PHASE 1 — CACHE ALL MESSAGES FROM SOURCE ───────────────────────────────────

async def cache_messages(source, cache_key, last_cached_id):
    """
    Source se saare messages pickle cache mein save karo.
    Agar source chhoot jaaye, cache already safe hai.
    """
    cache = load_cache()
    if cache_key not in cache:
        cache[cache_key] = {}

    msg_store   = cache[cache_key]
    new_count   = 0
    total_count = len(msg_store)

    print(f"\n[PHASE 1] Source se messages cache ho rahe hain...")
    print(f"  Already cached : {total_count}")
    print(f"  Resume after ID: {last_cached_id}")

    try:
        async for msg in client.iter_messages(
            source.entity,
            reverse=True,
            min_id=last_cached_id,
            wait_time=2,
        ):
            if not msg or msg.action:
                continue
            if getattr(msg, "noforwards", False):
                print(f"  SKIP {msg.id}: protected")
                continue

            # store message object
            msg_store[msg.id] = msg
            new_count += 1

            if new_count % 50 == 0:
                save_cache(cache)
                print(f"  Cached {total_count + new_count} messages...", end="\r")

    except ChannelPrivateError:
        print("\n  [!] Source se nikal gaye! Jo cache hua hai woh safe hai.")
    except KeyboardInterrupt:
        print("\n  [!] Cache paused by user.")
    except Exception as e:
        print(f"\n  [!] Cache error: {e}")
    finally:
        save_cache(cache)

    total_count += new_count
    print(f"\n  Cache complete. Total messages: {total_count}")
    return msg_store


# ── MEDIA DOWNLOAD WITH RETRY ──────────────────────────────────────────────────

async def download_with_retry(msg):
    for attempt in range(1, RETRIES + 1):
        try:
            path = await client.download_media(msg, file=MEDIA_DIR + "/")
            if path:
                return path
            raise RuntimeError("No file returned")
        except FloodWaitError as e:
            print(f"  FloodWait: {e.seconds}s ruk rahe hain...")
            await asyncio.sleep(e.seconds + 2)
        except Exception as e:
            print(f"  Download attempt {attempt}/{RETRIES}: {e}")
            if attempt < RETRIES:
                await asyncio.sleep(attempt * 3)
    return None


# ── SEND ONE MESSAGE ───────────────────────────────────────────────────────────

async def send_one(msg, destinations):
    text  = msg.raw_text or ""
    media = msg.media

    for dest in destinations:
        try:
            if not media or isinstance(media, MessageMediaWebPage):
                if text:
                    await client.send_message(
                        dest.entity,
                        text,
                        formatting_entities=msg.entities,
                        link_preview=isinstance(media, MessageMediaWebPage),
                    )
            else:
                path = await download_with_retry(msg)
                if not path:
                    print(f"  FAILED msg {msg.id}: download fail")
                    return False
                try:
                    await client.send_file(
                        dest.entity,
                        path,
                        caption=text or None,
                        formatting_entities=msg.entities,
                    )
                finally:
                    try:
                        os.remove(path)
                    except OSError:
                        pass
        except FloodWaitError as e:
            print(f"  FloodWait on send: {e.seconds}s")
            await asyncio.sleep(e.seconds + 2)
            return False
        except Exception as e:
            print(f"  Send error msg {msg.id}: {e}")
            return False

    return True


# ── PHASE 2 — TRANSFER FROM CACHE ─────────────────────────────────────────────

async def transfer_from_cache(msg_store, destinations, state, state_key):
    """
    Cache se sorted order mein sab messages destinations pe bhejo.
    Already transferred IDs dobara nahi jayenge.
    """
    transferred_ids = set(state.get(state_key + "_done", []))
    sorted_ids      = sorted(msg_store.keys())
    remaining       = [i for i in sorted_ids if i not in transferred_ids]

    print(f"\n[PHASE 2] Cache se transfer shuru...")
    print(f"  Total cached   : {len(sorted_ids)}")
    print(f"  Already done   : {len(transferred_ids)}")
    print(f"  Remaining      : {len(remaining)}")

    success = 0
    failed  = 0

    for msg_id in remaining:
        msg = msg_store.get(msg_id)
        if not msg:
            continue

        try:
            ok = await send_one(msg, destinations)
        except Exception as e:
            print(f"  ERROR msg {msg_id}: {e}")
            ok = False

        if ok:
            success += 1
            transferred_ids.add(msg_id)
            state[state_key + "_done"]      = list(transferred_ids)
            state[state_key + "_last_sent"] = msg_id
            save_state(state)
            print(
                f"  OK {msg_id} | "
                f"sent={success} | "
                f"failed={failed} | "
                f"remain={len(remaining)-success-failed}",
                end="\r"
            )
        else:
            failed += 1
            print(f"\n  SKIP {msg_id} | failed={failed}")

        await asyncio.sleep(0.5)

    print(f"\n\n[DONE] Success: {success}  Failed: {failed}")
    return success, failed


# ── MAIN ───────────────────────────────────────────────────────────────────────

async def main():
    await client.start()
    me = await client.get_me()
    print(f"Logged in: {me.first_name} (ID: {me.id})")

    # collect all broadcast channels
    all_channels = []
    async for d in client.iter_dialogs():
        if getattr(d.entity, "broadcast", False):
            all_channels.append(d)

    if not all_channels:
        print("Koi broadcast channel nahi mila.")
        return

    # source — any channel
    source = (await choose_dialog(all_channels, "SOURCE CHANNEL (jahan se copy karna hai)"))[0]

    # destination — only channels where you're admin/creator
    owned = [
        d for d in all_channels
        if getattr(d.entity, "creator", False)
        or getattr(d.entity, "admin_rights", None)
    ]
    if not owned:
        print("Koi owned/admin channel nahi mila destination ke liye.")
        return

    destinations = await choose_dialog(
        owned, "DESTINATION CHANNEL(S) — multiple ke liye comma se", multiple=True
    )

    state    = load_state()
    dest_ids = ",".join(str(d.id) for d in destinations)
    base_key = f"{source.id}:{dest_ids}"

    # last cached message ID — resume caching
    last_cached_id = int(state.get(base_key + "_last_cached", 0))

    print(f"\n  Source      : {source.name}")
    print(f"  Destination : {', '.join(d.name for d in destinations)}")

    print("\n[MENU]")
    print("  1. Phase 1 — Source se messages cache karo")
    print("  2. Phase 2 — Cache se destinations pe transfer karo")
    print("  3. Full run — Pehle cache phir transfer (recommended)")
    choice = input("\nChoice (1/2/3): ").strip()

    if choice in ("1", "3"):
        msg_store = await cache_messages(source, base_key, last_cached_id)

        # save last cached id
        if msg_store:
            state[base_key + "_last_cached"] = max(msg_store.keys())
            save_state(state)

        print(f"\n  Messages cache mein: {len(msg_store)}")

        if choice == "1":
            print("Cache complete. Option 2 se transfer karo jab ready ho.")
            return

    # load cache for phase 2
    cache     = load_cache()
    msg_store = cache.get(base_key, {})

    if not msg_store:
        print("Cache empty. Pehle option 1 se cache karo.")
        return

    if input(f"\n{len(msg_store)} messages cache mein hain. Transfer shuru karo? (YES): ").strip() != "YES":
        print("Cancelled.")
        return

    await transfer_from_cache(msg_store, destinations, state, base_key)
    print("\nProgress saved. Dobara run karo agle batch ke liye.")


with client:
    client.loop.run_until_complete(main())
