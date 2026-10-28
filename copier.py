import asyncio
import json
import os
from pathlib import Path

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, ChannelPrivateError, ChatWriteForbiddenError
from telethon.tl.functions.messages import CheckChatInviteRequest
from telethon.tl.types import ChatInviteAlready

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION_STRING = os.environ["SESSION_STRING"]
SOURCE = os.environ["SOURCE_CHANNEL"]
DESTINATION = os.environ["DESTINATION_CHANNEL"]
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "5"))

PROGRESS_FILE = Path(os.getenv("PROGRESS_FILE", "copy_progress.json"))
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "downloads"))
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)


def load_progress():
    try:
        return json.loads(PROGRESS_FILE.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {"last_id": 0}


def save_progress(last_id):
    temp = PROGRESS_FILE.with_suffix(".tmp")
    temp.write_text(json.dumps({"last_id": last_id}))
    temp.replace(PROGRESS_FILE)


async def resolve_source(client, source):
    if "t.me/+" in source or "joinchat/" in source:
        invite_hash = source.rstrip("/").split("/")[-1].replace("+", "")
        checked = await client(CheckChatInviteRequest(invite_hash))
        if isinstance(checked, ChatInviteAlready):
            return checked.chat
        raise RuntimeError(
            "This account has not joined the invite channel. Join it first; "
            "if approval is required, wait for approval and restart."
        )
    return await client.get_entity(source)


async def main():
    last_id = int(load_progress().get("last_id", 0))

    async with TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH) as client:
        source = await resolve_source(client, SOURCE)
        destination = await client.get_entity(DESTINATION)

        print(f"Source: {getattr(source, 'title', SOURCE)}")
        print(f"Destination: {getattr(destination, 'title', DESTINATION)}")
        print(f"Resuming after message ID {last_id}")

        while True:
            try:
                found_new = False
                async for message in client.iter_messages(source, min_id=last_id, reverse=True):
                    found_new = True
                    try:
                        if message.media:
                            path = await client.download_media(message, file=str(DOWNLOAD_DIR))
                            if path:
                                await client.send_file(
                                    destination, path,
                                    caption=message.message or "",
                                    force_document=False
                                )
                                Path(path).unlink(missing_ok=True)
                            else:
                                print(f"Download failed for message {message.id}; stopping.")
                                raise RuntimeError("Media download returned no file")
                        elif message.message:
                            await client.send_message(destination, message.message)

                        last_id = message.id
                        save_progress(last_id)
                        print(f"Copied source message ID {last_id}")

                    except FloodWaitError as e:
                        print(f"Telegram rate limit; waiting {e.seconds} seconds")
                        await asyncio.sleep(e.seconds)
                    except (ChannelPrivateError, ChatWriteForbiddenError):
                        print(f"Access/permission error at message {message.id}; stopping safely.")
                        raise

                if not found_new:
                    print(f"No new messages. Checking again in {POLL_SECONDS} seconds.")
                await asyncio.sleep(POLL_SECONDS)

            except (ChannelPrivateError, ChatWriteForbiddenError):
                print("Stopped: source access or destination posting permission is missing.")
                return


if __name__ == "__main__":
    asyncio.run(main())
