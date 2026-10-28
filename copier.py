
import os
import json
import asyncio
import pickle

from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, ChannelPrivateError
from telethon.tl.types import MessageMediaWebPage

# ── RAILWAY ENVIRONMENT ──────────────────────────────────────────

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION_STRING = os.environ["SESSION_STRING"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_USER_ID = int(os.environ["ADMIN_USER_ID"])

STATE_FILE = "copy_progress.json"
CACHE_FILE = "message_cache.pkl"
MEDIA_DIR = "downloads"
RETRIES = 5

os.makedirs(MEDIA_DIR, exist_ok=True)

# User account: existing authenticated session
client = TelegramClient(
    StringSession(SESSION_STRING),
    API_ID,
    API_HASH
)

# Bot account: Telegram control panel
bot = TelegramClient("control_bot", API_ID, API_HASH)

pause_event = asyncio.Event()
pause_event.set()

stop_event = asyncio.Event()
job_task = None

channels = []
source = None
destinations = []
selected_dest_ids = set()
current_admin_chat = None


# ── STATE ────────────────────────────────────────────────────────

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


async def admin_message(text, buttons=None):
    if current_admin_chat is not None:
        await bot.send_message(
            current_admin_chat,
            text,
            buttons=buttons
        )


# ── CHANNEL DISCOVERY ────────────────────────────────────────────

async def get_channels():
    result = []
    async for d in client.iter_dialogs():
        if getattr(d.entity, "broadcast", False):
            result.append(d)
    return result


def get_owned_channels():
    return [
        d for d in channels
        if getattr(d.entity, "creator", False)
        or getattr(d.entity, "admin_rights", None)
    ]


# ── TELEGRAM BUTTON MENUS ────────────────────────────────────────

async def show_sources(event):
    global channels
    channels = await get_channels()

    if not channels:
        await event.respond("Koi accessible broadcast channel nahi mila.")
        return

    buttons = [
        [Button.inline(d.name[:50], f"src:{d.id}".encode())]
        for d in channels
    ]
    buttons.append([Button.inline("Cancel", b"cancel")])

    await event.respond(
        "SOURCE CHANNEL select karo:",
        buttons=buttons
    )


async def show_destinations(event):
    owned = get_owned_channels()

    if not owned:
        await event.respond(
            "Koi creator/admin broadcast channel destination ke liye nahi mila."
        )
        return

    buttons = []
    for d in owned:
        mark = "✓ " if d.id in selected_dest_ids else ""
        buttons.append([
            Button.inline(
                (mark + d.name)[:50],
                f"dst:{d.id}".encode()
            )
        ])

    buttons.append([
        Button.inline("Confirm destinations", b"dest_confirm")
    ])
    buttons.append([Button.inline("Cancel", b"cancel")])

    await event.respond(
        "DESTINATION CHANNELS select karo. Multiple channels select kar sakte ho:",
        buttons=buttons
    )


async def refresh_destinations(event):
    owned = get_owned_channels()
    buttons = []

    for d in owned:
        mark = "✓ " if d.id in selected_dest_ids else ""
        buttons.append([
            Button.inline(
                (mark + d.name)[:50],
                f"dst:{d.id}".encode()
            )
        ])

    buttons.append([
        Button.inline("Confirm destinations", b"dest_confirm")
    ])
    buttons.append([Button.inline("Cancel", b"cancel")])

    await event.edit(
        "Destination select karo (✓ = selected):",
        buttons=buttons
    )


# ── PHASE 1: CACHE SOURCE MESSAGES ───────────────────────────────

async def cache_messages(source_dialog, cache_key, last_cached_id):
    cache = load_cache()
    if cache_key not in cache:
        cache[cache_key] = {}

    msg_store = cache[cache_key]
    new_count = 0

    await admin_message(
        f"📥 Cache started\nSource: {source_dialog.name}\n"
        f"Already cached: {len(msg_store)}"
    )

    try:
        async for msg in client.iter_messages(
            source_dialog.entity,
            reverse=True,
            min_id=last_cached_id,
            wait_time=2
        ):
            await pause_event.wait()
            if stop_event.is_set():
                break

            if not msg or msg.action:
                continue

            if getattr(msg, "noforwards", False):
                continue

            msg_store[msg.id] = msg
            new_count += 1

            if new_count % 25 == 0:
                save_cache(cache)
                await admin_message(
                    f"📥 Cached {len(msg_store)} messages..."
                )

    except ChannelPrivateError:
        await admin_message(
            "⚠️ Source channel access nahi hai. "
            "Jo messages cache ho chuke hain, woh save kar diye gaye hain."
        )
    except Exception as e:
        await admin_message(f"⚠️ Cache error: {type(e).__name__}: {e}")
    finally:
        save_cache(cache)

    return msg_store


# ── MEDIA DOWNLOAD WITH RETRIES ─────────────────────────────────

async def download_with_retry(msg):
    for attempt in range(1, RETRIES + 1):
        try:
            path = await client.download_media(
                msg,
                file=MEDIA_DIR + "/"
            )
            if path:
                return path
            raise RuntimeError("No file returned")

        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 2)

        except Exception as e:
            await admin_message(
                f"Download attempt {attempt}/{RETRIES} failed: {e}"
            )
            if attempt < RETRIES:
                await asyncio.sleep(attempt * 3)

    return None


# ── SEND ONE MESSAGE ─────────────────────────────────────────────

async def send_one(msg, destination_list):
    text = msg.raw_text or ""
    media = msg.media

    for dest in destination_list:
        try:
            if not media or isinstance(media, MessageMediaWebPage):
                if text:
                    await client.send_message(
                        dest.entity,
                        text,
                        formatting_entities=msg.entities,
                        link_preview=isinstance(media, MessageMediaWebPage)
                    )
            else:
                path = await download_with_retry(msg)

                if not path:
                    return False

                try:
                    await client.send_file(
                        dest.entity,
                        path,
                        caption=text or None,
                        formatting_entities=msg.entities
                    )
                finally:
                    try:
                        os.remove(path)
                    except OSError:
                        pass

        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 2)
            return False

        except Exception as e:
            await admin_message(
                f"Send error for message {msg.id}: {e}"
            )
            return False

    return True


# ── PHASE 2: TRANSFER FROM CACHE ─────────────────────────────────

async def transfer_from_cache(msg_store, destination_list, state, state_key):
    transferred_ids = set(state.get(state_key + "_done", []))
    sorted_ids = sorted(msg_store.keys())
    remaining = [i for i in sorted_ids if i not in transferred_ids]

    success = 0
    failed = 0

    await admin_message(
        f"🚀 Transfer started\nCached: {len(sorted_ids)}\n"
        f"Remaining: {len(remaining)}"
    )

    for msg_id in remaining:
        await pause_event.wait()

        if stop_event.is_set():
            break

        msg = msg_store.get(msg_id)
        if not msg:
            continue

        try:
            ok = await send_one(msg, destination_list)
        except Exception as e:
            await admin_message(f"Transfer error {msg_id}: {e}")
            ok = False

        if ok:
            success += 1
            transferred_ids.add(msg_id)
            state[state_key + "_done"] = list(transferred_ids)
            state[state_key + "_last_sent"] = msg_id
            save_state(state)
        else:
            failed += 1

        if (success + failed) % 10 == 0:
            await admin_message(
                f"📊 Progress\nSuccess: {success}\nFailed: {failed}\n"
                f"Remaining: {len(remaining) - success - failed}"
            )

        await asyncio.sleep(0.5)

    await admin_message(
        f"🏁 Transfer finished or stopped\n"
        f"Success: {success}\nFailed: {failed}\n"
        f"Progress saved."
    )

    return success, failed


# ── FULL JOB ─────────────────────────────────────────────────────

async def run_job():
    global source, destinations

    state = load_state()
    dest_ids = ",".join(str(d.id) for d in destinations)
    base_key = f"{source.id}:{dest_ids}"

    last_cached_id = int(state.get(base_key + "_last_cached", 0))

    await admin_message(
        f"Selected source: {source.name}\n"
        f"Destinations: {', '.join(d.name for d in destinations)}"
    )

    msg_store = await cache_messages(
        source,
        base_key,
        last_cached_id
    )

    if msg_store:
        state[base_key + "_last_cached"] = max(msg_store.keys())
        save_state(state)

    if stop_event.is_set():
        await admin_message("⏹️ Stopped after caching. Cache saved.")
        return

    if not msg_store:
        await admin_message("Cache empty. Transfer nahi kiya gaya.")
        return

    await transfer_from_cache(
        msg_store,
        destinations,
        state,
        base_key
    )


# ── BOT COMMANDS ─────────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r"^/start$"))
async def start_handler(event):
    global current_admin_chat

    if event.sender_id != ADMIN_USER_ID:
        return

    current_admin_chat = event.chat_id

    await event.respond(
        "Channel Copier Control Panel\n\n"
        "Source aur destination select karke transfer start karo.",
        buttons=[
            [Button.inline("Select source channel", b"choose_source")],
            [Button.inline("Status", b"status")],
            [Button.inline("Pause", b"pause"),
             Button.inline("Resume", b"resume")],
            [Button.inline("Stop job", b"stop")]
        ]
    )


@bot.on(events.NewMessage(pattern=r"^/status$"))
async def status_handler(event):
    if event.sender_id != ADMIN_USER_ID:
        return

    status = "RUNNING" if job_task and not job_task.done() else "IDLE"
    pause_status = "PAUSED" if not pause_event.is_set() else "NOT PAUSED"

    await event.respond(
        f"Status: {status}\nPause: {pause_status}"
    )


@bot.on(events.NewMessage(pattern=r"^/pause$"))
async def pause_handler(event):
    if event.sender_id != ADMIN_USER_ID:
        return

    pause_event.clear()
    await event.respond("⏸️ Pause requested.")


@bot.on(events.NewMessage(pattern=r"^/resume$"))
async def resume_handler(event):
    if event.sender_id != ADMIN_USER_ID:
        return

    pause_event.set()
    await event.respond("▶️ Resume requested.")


@bot.on(events.NewMessage(pattern=r"^/stop$"))
async def stop_handler(event):
    if event.sender_id != ADMIN_USER_ID:
        return

    stop_event.set()
    pause_event.set()
    await event.respond("⏹️ Stop requested. Current operation may finish first.")


@bot.on(events.CallbackQuery)
async def callback_handler(event):
    global source, destinations, selected_dest_ids
    global job_task, current_admin_chat

    if event.sender_id != ADMIN_USER_ID:
        await event.answer("Unauthorized", alert=True)
        return

    current_admin_chat = event.chat_id
    data = event.data.decode()

    if data == "cancel":
        await event.answer("Cancelled")
        await event.edit("Cancelled.")
        return

    if data == "choose_source":
        await event.answer()
        await show_sources(event)
        return

    if data.startswith("src:"):
        source_id = int(data.split(":", 1)[1])
        source = next((d for d in channels if d.id == source_id), None)

        if source is None:
            await event.answer("Source not found. Try /start again.", alert=True)
            return

        selected_dest_ids.clear()
        await event.answer(f"Source: {source.name}")
        await show_destinations(event)
        return

    if data.startswith("dst:"):
        dest_id = int(data.split(":", 1)[1])

        if dest_id in selected_dest_ids:
            selected_dest_ids.remove(dest_id)
        else:
            selected_dest_ids.add(dest_id)

        await event.answer()
        await refresh_destinations(event)
        return

    if data == "dest_confirm":
        if source is None:
            await event.answer("Select source first.", alert=True)
            return

        owned = get_owned_channels()
        destinations = [d for d in owned if d.id in selected_dest_ids]

        if not destinations:
            await event.answer("Select at least one destination.", alert=True)
            return

        if job_task and not job_task.done():
            await event.answer("A job is already running.", alert=True)
            return

        stop_event.clear()
        pause_event.set()

        await event.answer("Starting job")
        await event.edit(
            f"Ready to start\nSource: {source.name}\n"
            f"Destinations: {', '.join(d.name for d in destinations)}",
            buttons=[
                [Button.inline("START FULL RUN", b"run_job")],
                [Button.inline("Cancel", b"cancel")]
            ]
        )
        return

    if data == "run_job":
        if source is None or not destinations:
            await event.answer("Choose source and destinations first.", alert=True)
            return

        if job_task and not job_task.done():
            await event.answer("A job is already running.", alert=True)
            return

        stop_event.clear()
        pause_event.set()
        job_task = asyncio.create_task(run_job())

        await event.answer("Job started")
        await event.edit("🚀 Full run started. Use /status, /pause, /resume or /stop.")
        return

    if data == "status":
        status = "RUNNING" if job_task and not job_task.done() else "IDLE"
        pause_status = "PAUSED" if not pause_event.is_set() else "NOT PAUSED"
        await event.answer(f"{status} / {pause_status}", alert=True)
        return

    if data == "pause":
        pause_event.clear()
        await event.answer("Pause requested")
        return

    if data == "resume":
        pause_event.set()
        await event.answer("Resume requested")
        return

    if data == "stop":
        stop_event.set()
        pause_event.set()
        await event.answer("Stop requested")
        return


# ── STARTUP ──────────────────────────────────────────────────────

async def main():
    await client.connect()

    if not await client.is_user_authorized():
        raise RuntimeError(
            "SESSION_STRING is not authorized. Generate a valid user session first."
        )

    me = await client.get_me()
    print(f"User logged in: {me.first_name} (ID: {me.id})")

    await bot.start(bot_token=BOT_TOKEN)
    print("Control bot started.")

    await bot.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
                        
import os
import json
import asyncio
import pickle

from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, ChannelPrivateError
from telethon.tl.types import MessageMediaWebPage

# ── RAILWAY ENVIRONMENT ──────────────────────────────────────────

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION_STRING = os.environ["SESSION_STRING"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_USER_ID = int(os.environ["ADMIN_USER_ID"])

STATE_FILE = "copy_progress.json"
CACHE_FILE = "message_cache.pkl"
MEDIA_DIR = "downloads"
RETRIES = 5

os.makedirs(MEDIA_DIR, exist_ok=True)

# User account: existing authenticated session
client = TelegramClient(
    StringSession(SESSION_STRING),
    API_ID,
    API_HASH
)

# Bot account: Telegram control panel
bot = TelegramClient("control_bot", API_ID, API_HASH)

pause_event = asyncio.Event()
pause_event.set()

stop_event = asyncio.Event()
job_task = None

channels = []
source = None
destinations = []
selected_dest_ids = set()
current_admin_chat = None


# ── STATE ────────────────────────────────────────────────────────

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


async def admin_message(text, buttons=None):
    if current_admin_chat is not None:
        await bot.send_message(
            current_admin_chat,
            text,
            buttons=buttons
        )


# ── CHANNEL DISCOVERY ────────────────────────────────────────────

async def get_channels():
    result = []
    async for d in client.iter_dialogs():
        if getattr(d.entity, "broadcast", False):
            result.append(d)
    return result


def get_owned_channels():
    return [
        d for d in channels
        if getattr(d.entity, "creator", False)
        or getattr(d.entity, "admin_rights", None)
    ]


# ── TELEGRAM BUTTON MENUS ────────────────────────────────────────

async def show_sources(event):
    global channels
    channels = await get_channels()

    if not channels:
        await event.respond("Koi accessible broadcast channel nahi mila.")
        return

    buttons = [
        [Button.inline(d.name[:50], f"src:{d.id}".encode())]
        for d in channels
    ]
    buttons.append([Button.inline("Cancel", b"cancel")])

    await event.respond(
        "SOURCE CHANNEL select karo:",
        buttons=buttons
    )


async def show_destinations(event):
    owned = get_owned_channels()

    if not owned:
        await event.respond(
            "Koi creator/admin broadcast channel destination ke liye nahi mila."
        )
        return

    buttons = []
    for d in owned:
        mark = "✓ " if d.id in selected_dest_ids else ""
        buttons.append([
            Button.inline(
                (mark + d.name)[:50],
                f"dst:{d.id}".encode()
            )
        ])

    buttons.append([
        Button.inline("Confirm destinations", b"dest_confirm")
    ])
    buttons.append([Button.inline("Cancel", b"cancel")])

    await event.respond(
        "DESTINATION CHANNELS select karo. Multiple channels select kar sakte ho:",
        buttons=buttons
    )


async def refresh_destinations(event):
    owned = get_owned_channels()
    buttons = []

    for d in owned:
        mark = "✓ " if d.id in selected_dest_ids else ""
        buttons.append([
            Button.inline(
                (mark + d.name)[:50],
                f"dst:{d.id}".encode()
            )
        ])

    buttons.append([
        Button.inline("Confirm destinations", b"dest_confirm")
    ])
    buttons.append([Button.inline("Cancel", b"cancel")])

    await event.edit(
        "Destination select karo (✓ = selected):",
        buttons=buttons
    )


# ── PHASE 1: CACHE SOURCE MESSAGES ───────────────────────────────

async def cache_messages(source_dialog, cache_key, last_cached_id):
    cache = load_cache()
    if cache_key not in cache:
        cache[cache_key] = {}

    msg_store = cache[cache_key]
    new_count = 0

    await admin_message(
        f"📥 Cache started\nSource: {source_dialog.name}\n"
        f"Already cached: {len(msg_store)}"
    )

    try:
        async for msg in client.iter_messages(
            source_dialog.entity,
            reverse=True,
            min_id=last_cached_id,
            wait_time=2
        ):
            await pause_event.wait()
            if stop_event.is_set():
                break

            if not msg or msg.action:
                continue

            if getattr(msg, "noforwards", False):
                continue

            msg_store[msg.id] = msg
            new_count += 1

            if new_count % 25 == 0:
                save_cache(cache)
                await admin_message(
                    f"📥 Cached {len(msg_store)} messages..."
                )

    except ChannelPrivateError:
        await admin_message(
            "⚠️ Source channel access nahi hai. "
            "Jo messages cache ho chuke hain, woh save kar diye gaye hain."
        )
    except Exception as e:
        await admin_message(f"⚠️ Cache error: {type(e).__name__}: {e}")
    finally:
        save_cache(cache)

    return msg_store


# ── MEDIA DOWNLOAD WITH RETRIES ─────────────────────────────────

async def download_with_retry(msg):
    for attempt in range(1, RETRIES + 1):
        try:
            path = await client.download_media(
                msg,
                file=MEDIA_DIR + "/"
            )
            if path:
                return path
            raise RuntimeError("No file returned")

        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 2)

        except Exception as e:
            await admin_message(
                f"Download attempt {attempt}/{RETRIES} failed: {e}"
            )
            if attempt < RETRIES:
                await asyncio.sleep(attempt * 3)

    return None


# ── SEND ONE MESSAGE ─────────────────────────────────────────────

async def send_one(msg, destination_list):
    text = msg.raw_text or ""
    media = msg.media

    for dest in destination_list:
        try:
            if not media or isinstance(media, MessageMediaWebPage):
                if text:
                    await client.send_message(
                        dest.entity,
                        text,
                        formatting_entities=msg.entities,
                        link_preview=isinstance(media, MessageMediaWebPage)
                    )
            else:
                path = await download_with_retry(msg)

                if not path:
                    return False

                try:
                    await client.send_file(
                        dest.entity,
                        path,
                        caption=text or None,
                        formatting_entities=msg.entities
                    )
                finally:
                    try:
                        os.remove(path)
                    except OSError:
                        pass

        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 2)
            return False

        except Exception as e:
            await admin_message(
                f"Send error for message {msg.id}: {e}"
            )
            return False

    return True


# ── PHASE 2: TRANSFER FROM CACHE ─────────────────────────────────

async def transfer_from_cache(msg_store, destination_list, state, state_key):
    transferred_ids = set(state.get(state_key + "_done", []))
    sorted_ids = sorted(msg_store.keys())
    remaining = [i for i in sorted_ids if i not in transferred_ids]

    success = 0
    failed = 0

    await admin_message(
        f"🚀 Transfer started\nCached: {len(sorted_ids)}\n"
        f"Remaining: {len(remaining)}"
    )

    for msg_id in remaining:
        await pause_event.wait()

        if stop_event.is_set():
            break

        msg = msg_store.get(msg_id)
        if not msg:
            continue

        try:
            ok = await send_one(msg, destination_list)
        except Exception as e:
            await admin_message(f"Transfer error {msg_id}: {e}")
            ok = False

        if ok:
            success += 1
            transferred_ids.add(msg_id)
            state[state_key + "_done"] = list(transferred_ids)
            state[state_key + "_last_sent"] = msg_id
            save_state(state)
        else:
            failed += 1

        if (success + failed) % 10 == 0:
            await admin_message(
                f"📊 Progress\nSuccess: {success}\nFailed: {failed}\n"
                f"Remaining: {len(remaining) - success - failed}"
            )

        await asyncio.sleep(0.5)

    await admin_message(
        f"🏁 Transfer finished or stopped\n"
        f"Success: {success}\nFailed: {failed}\n"
        f"Progress saved."
    )

    return success, failed


# ── FULL JOB ─────────────────────────────────────────────────────

async def run_job():
    global source, destinations

    state = load_state()
    dest_ids = ",".join(str(d.id) for d in destinations)
    base_key = f"{source.id}:{dest_ids}"

    last_cached_id = int(state.get(base_key + "_last_cached", 0))

    await admin_message(
        f"Selected source: {source.name}\n"
        f"Destinations: {', '.join(d.name for d in destinations)}"
    )

    msg_store = await cache_messages(
        source,
        base_key,
        last_cached_id
    )

    if msg_store:
        state[base_key + "_last_cached"] = max(msg_store.keys())
        save_state(state)

    if stop_event.is_set():
        await admin_message("⏹️ Stopped after caching. Cache saved.")
        return

    if not msg_store:
        await admin_message("Cache empty. Transfer nahi kiya gaya.")
        return

    await transfer_from_cache(
        msg_store,
        destinations,
        state,
        base_key
    )


# ── BOT COMMANDS ─────────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r"^/start$"))
async def start_handler(event):
    global current_admin_chat

    if event.sender_id != ADMIN_USER_ID:
        return

    current_admin_chat = event.chat_id

    await event.respond(
        "Channel Copier Control Panel\n\n"
        "Source aur destination select karke transfer start karo.",
        buttons=[
            [Button.inline("Select source channel", b"choose_source")],
            [Button.inline("Status", b"status")],
            [Button.inline("Pause", b"pause"),
             Button.inline("Resume", b"resume")],
            [Button.inline("Stop job", b"stop")]
        ]
    )


@bot.on(events.NewMessage(pattern=r"^/status$"))
async def status_handler(event):
    if event.sender_id != ADMIN_USER_ID:
        return

    status = "RUNNING" if job_task and not job_task.done() else "IDLE"
    pause_status = "PAUSED" if not pause_event.is_set() else "NOT PAUSED"

    await event.respond(
        f"Status: {status}\nPause: {pause_status}"
    )


@bot.on(events.NewMessage(pattern=r"^/pause$"))
async def pause_handler(event):
    if event.sender_id != ADMIN_USER_ID:
        return

    pause_event.clear()
    await event.respond("⏸️ Pause requested.")


@bot.on(events.NewMessage(pattern=r"^/resume$"))
async def resume_handler(event):
    if event.sender_id != ADMIN_USER_ID:
        return

    pause_event.set()
    await event.respond("▶️ Resume requested.")


@bot.on(events.NewMessage(pattern=r"^/stop$"))
async def stop_handler(event):
    if event.sender_id != ADMIN_USER_ID:
        return

    stop_event.set()
    pause_event.set()
    await event.respond("⏹️ Stop requested. Current operation may finish first.")


@bot.on(events.CallbackQuery)
async def callback_handler(event):
    global source, destinations, selected_dest_ids
    global job_task, current_admin_chat

    if event.sender_id != ADMIN_USER_ID:
        await event.answer("Unauthorized", alert=True)
        return

    current_admin_chat = event.chat_id
    data = event.data.decode()

    if data == "cancel":
        await event.answer("Cancelled")
        await event.edit("Cancelled.")
        return

    if data == "choose_source":
        await event.answer()
        await show_sources(event)
        return

    if data.startswith("src:"):
        source_id = int(data.split(":", 1)[1])
        source = next((d for d in channels if d.id == source_id), None)

        if source is None:
            await event.answer("Source not found. Try /start again.", alert=True)
            return

        selected_dest_ids.clear()
        await event.answer(f"Source: {source.name}")
        await show_destinations(event)
        return

    if data.startswith("dst:"):
        dest_id = int(data.split(":", 1)[1])

        if dest_id in selected_dest_ids:
            selected_dest_ids.remove(dest_id)
        else:
            selected_dest_ids.add(dest_id)

        await event.answer()
        await refresh_destinations(event)
        return

    if data == "dest_confirm":
        if source is None:
            await event.answer("Select source first.", alert=True)
            return

        owned = get_owned_channels()
        destinations = [d for d in owned if d.id in selected_dest_ids]

        if not destinations:
            await event.answer("Select at least one destination.", alert=True)
            return

        if job_task and not job_task.done():
            await event.answer("A job is already running.", alert=True)
            return

        stop_event.clear()
        pause_event.set()

        await event.answer("Starting job")
        await event.edit(
            f"Ready to start\nSource: {source.name}\n"
            f"Destinations: {', '.join(d.name for d in destinations)}",
            buttons=[
                [Button.inline("START FULL RUN", b"run_job")],
                [Button.inline("Cancel", b"cancel")]
            ]
        )
        return

    if data == "run_job":
        if source is None or not destinations:
            await event.answer("Choose source and destinations first.", alert=True)
            return

        if job_task and not job_task.done():
            await event.answer("A job is already running.", alert=True)
            return

        stop_event.clear()
        pause_event.set()
        job_task = asyncio.create_task(run_job())

        await event.answer("Job started")
        await event.edit("🚀 Full run started. Use /status, /pause, /resume or /stop.")
        return

    if data == "status":
        status = "RUNNING" if job_task and not job_task.done() else "IDLE"
        pause_status = "PAUSED" if not pause_event.is_set() else "NOT PAUSED"
        await event.answer(f"{status} / {pause_status}", alert=True)
        return

    if data == "pause":
        pause_event.clear()
        await event.answer("Pause requested")
        return

    if data == "resume":
        pause_event.set()
        await event.answer("Resume requested")
        return

    if data == "stop":
        stop_event.set()
        pause_event.set()
        await event.answer("Stop requested")
        return


# ── STARTUP ──────────────────────────────────────────────────────

async def main():
    await client.connect()

    if not await client.is_user_authorized():
        raise RuntimeError(
            "SESSION_STRING is not authorized. Generate a valid user session first."
        )

    me = await client.get_me()
    print(f"User logged in: {me.first_name} (ID: {me.id})")

    await bot.start(bot_token=BOT_TOKEN)
    print("Control bot started.")

    await bot.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
        
