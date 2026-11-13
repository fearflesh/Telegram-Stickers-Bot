"""
Telegram Sticker & Emoji Studio Bot - high-traffic edition with a unified UI kit.

Files: telegram_sticker_bot.py + ui_kit.py (keep them in the same folder)

Settings are read from environment variables, falling back to the defaults in the
CONFIGURATION block. Local run:
  pip install -r requirements.txt
  python telegram_sticker_bot.py

Render (Web Service):
  Build command:  pip install -r requirements.txt
  Start command:  python telegram_sticker_bot.py
  Env vars:       BOT_TOKEN, MONGO_URI (required); the rest are optional.
  Render sets PORT and RENDER_EXTERNAL_URL automatically, which switches the bot to webhook mode.
"""

import asyncio
import io
import logging
import multiprocessing as mp
import os
import random
import re
import secrets
import sys
import tarfile
import time
import zipfile
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field as dc_field
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, TypeVar

from aiohttp import web
from bson import ObjectId
from bson.errors import InvalidId
from pymongo import ASCENDING, DESCENDING, MongoClient
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import MaskPositionPoint, ParseMode, StickerFormat, StickerType
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest, TelegramNetworkError, TelegramRetryAfter
from aiogram.filters import CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    ErrorEvent,
    InlineKeyboardMarkup,
    InputSticker,
    MaskPosition,
    Message,
)
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from PIL import Image, ImageChops, ImageDraw, ImageFilter, ImageOps

import ui_kit as ui
from ui_kit import Nav, Role, btn

step = ui.step  # "➤ Step n of t" line used across multi-step flows

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
def env(name: str, default: str = "") -> str:
    """Read a setting from the environment, falling back to the default."""
    return os.getenv(name, default).strip()


BOT_TOKEN = env("BOT_TOKEN", "PASTE_YOUR_BOT_TOKEN_HERE")   # from @BotFather
MONGO_URI = env("MONGO_URI", "mongodb://localhost:27017")  # Atlas connection string on Render
MONGO_DB = env("MONGO_DB", "sticker_studio")
REDIS_URL = env("REDIS_URL", "")                           # empty = in-memory FSM state
# Render sets RENDER_EXTERNAL_URL on web services, so webhook mode is picked up automatically.
WEBHOOK_URL = env("WEBHOOK_URL", env("RENDER_EXTERNAL_URL", "")).rstrip("/")
WEBHOOK_PATH = env("WEBHOOK_PATH", "/tg-webhook")
WEBHOOK_SECRET = env("WEBHOOK_SECRET", "")                 # optional, checked on every webhook call
WEB_HOST = env("HOST", "0.0.0.0")
WEB_PORT = int(env("PORT", "8080"))                        # Render injects PORT

MAX_DOWNLOAD_BYTES = 20 * 1024 * 1024       # Bot API getFile limit
MAX_FILE_BYTES = 20 * 1024 * 1024
MAX_ARCHIVE_ENTRIES = 120
MAX_ARCHIVE_TOTAL_BYTES = 100 * 1024 * 1024
MAX_IMAGE_PIXELS = 40_000_000               # decompression-bomb guard
MAX_QUEUE_ITEMS = 240
QUEUE_TTL = 1800
ALBUM_WAIT = 1.5
MAX_CLONE_STICKERS = 200
CROP_RATIO = 1.15                           # crop when the long side is >15% longer than the short side
TG_CONCURRENCY = int(env("MAX_PARALLEL_TG_CALLS", "24"))
IMAGE_WORKERS = int(env("IMAGE_WORKERS", str(max(2, os.cpu_count() or 2))))

IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tiff", ".gif"}
ARCHIVE_EXTS = (".zip", ".tar", ".tar.gz", ".tgz", ".gz")
POPULAR_EMOJIS = [
    "😀", "😂", "🤣", "😎", "😍", "🥳", "🤔", "🥺", "🔥", "✨", "🎉", "👍",
    "👀", "💯", "❤️", "🙌", "🐱", "🐶", "🌟", "⚡", "🚀", "💎", "🍀", "🍕",
]
SINGLE_EMOJI_RE = re.compile(
    r"[\U0001F1E6-\U0001F1FF]{2}"
    r"|[\U0001F000-\U0001FAFF\u2600-\u27BF]\ufe0f?[\U0001F3FB-\U0001F3FF]?"
)
CLONE_LINK_RE = re.compile(r"t\.me/(?:addstickers|addemoji)/([A-Za-z0-9_]{1,64})", re.IGNORECASE)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("StickerStudioBot")
Image.MAX_IMAGE_PIXELS = MAX_IMAGE_PIXELS

T = TypeVar("T")


def pick_random_emoji() -> str:
    return random.choice(POPULAR_EMOJIS)


def parse_emoji_list(text: Optional[str]) -> List[str]:
    return SINGLE_EMOJI_RE.findall(text)[:7] if text else []


def error_text(exc: Exception) -> str:
    return ui.e(getattr(exc, "message", None) or str(exc))


def titled(base: str, uname: str) -> str:
    """Telegram titles are 1-64 characters. Append the bot handle without overflowing."""
    if f"@{uname}" in base:
        return base[:64]
    suffix = f" (@{uname})"
    return base[: 64 - len(suffix)] + suffix


# ---------------------------------------------------------------------------
# Database (SQLite in WAL mode; calls run in worker threads)
# ---------------------------------------------------------------------------
PREF_VALUES = {
    "emoji_mode": {"ask", "random", "manual"},
    "frame_mode": {"fit", "pad", "rounded"},
    "auto_border": {"0", "1"},
    "auto_crop": {"0", "1"},
}


USER_DEFAULTS: Dict[str, Any] = {
    "username": None,
    "active_pack_name": None,
    "emoji_mode": "ask",
    "frame_mode": "fit",
    "auto_border": 0,
    "auto_crop": 0,
}


class Database:
    """
    MongoDB storage. Collections:
      users          {user_id, username, active_pack_name, emoji_mode, frame_mode, auto_border, auto_crop}
      sticker_packs  {_id, pack_name, user_id, pack_title, pack_kind, created_at}

    Constructing this object does no network I/O; call ensure_indexes() once at startup.
    Methods are blocking (pymongo) and are called through db_call().
    """

    def __init__(self, uri: str, db_name: str) -> None:
        self.client = MongoClient(uri, serverSelectionTimeoutMS=5000, maxPoolSize=100)
        self.db = self.client[db_name]
        self.users = self.db["users"]
        self.packs = self.db["sticker_packs"]

    def ensure_indexes(self) -> None:
        self.users.create_index([("user_id", ASCENDING)], unique=True)
        self.packs.create_index([("pack_name", ASCENDING)], unique=True)
        self.packs.create_index([("user_id", ASCENDING), ("created_at", DESCENDING)])

    @staticmethod
    def _pack(doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not doc:
            return None
        doc = dict(doc)
        doc["pid"] = str(doc.pop("_id"))  # short, safe string id for callback data
        return doc

    # --- users -------------------------------------------------------------
    def get_user(self, uid: int, username: Optional[str] = None) -> Dict[str, Any]:
        self.users.update_one({"user_id": uid}, {"$setOnInsert": dict(USER_DEFAULTS)}, upsert=True)
        if username:
            self.users.update_one({"user_id": uid, "username": {"$ne": username}}, {"$set": {"username": username}})
        doc = self.users.find_one({"user_id": uid}) or {}
        return {**USER_DEFAULTS, **doc}

    def set_pref(self, uid: int, col: str, val: str) -> None:
        allowed = PREF_VALUES.get(col)
        if not allowed or val not in allowed:
            raise ValueError(f"invalid preference {col}={val}")
        value: Any = int(val) if col in ("auto_border", "auto_crop") else val
        self.users.update_one({"user_id": uid}, {"$set": {col: value}}, upsert=True)

    def set_active(self, uid: int, name: Optional[str]) -> None:
        self.users.update_one({"user_id": uid}, {"$set": {"active_pack_name": name}}, upsert=True)

    # --- packs -------------------------------------------------------------
    def user_packs(self, uid: int) -> List[Dict[str, Any]]:
        cursor = self.packs.find({"user_id": uid}).sort([("created_at", DESCENDING), ("_id", DESCENDING)])
        return [self._pack(d) for d in cursor]

    def pack_by_name(self, name: Optional[str]) -> Optional[Dict[str, Any]]:
        if not name:
            return None
        return self._pack(self.packs.find_one({"pack_name": name}))

    def pack_by_id(self, uid: int, pid: str) -> Optional[Dict[str, Any]]:
        try:
            oid = ObjectId(pid)
        except (InvalidId, TypeError):
            return None
        return self._pack(self.packs.find_one({"_id": oid, "user_id": uid}))

    def save_pack(self, uid: int, name: str, title: str, kind: str = "static") -> None:
        self.packs.insert_one({
            "pack_name": name,
            "user_id": uid,
            "pack_title": title,
            "pack_kind": kind,
            "created_at": time.time(),
        })
        self.set_active(uid, name)

    def rename_pack(self, name: str, title: str) -> None:
        self.packs.update_one({"pack_name": name}, {"$set": {"pack_title": title}})

    def delete_pack(self, uid: int, name: str) -> None:
        self.packs.delete_one({"pack_name": name, "user_id": uid})
        nxt = self.packs.find_one({"user_id": uid}, sort=[("created_at", DESCENDING)])
        self.set_active(uid, nxt["pack_name"] if nxt else None)


db = Database(MONGO_URI, MONGO_DB)


async def db_call(fn: Callable[..., T], *args: Any) -> T:
    return await asyncio.to_thread(fn, *args)


# ---------------------------------------------------------------------------
# Image & archive processing (CPU work runs in a process pool)
# ---------------------------------------------------------------------------
IMG_POOL: Optional[ProcessPoolExecutor] = None
CPU_SEM = asyncio.Semaphore(max(2, IMAGE_WORKERS * 2))


async def run_cpu(fn: Callable[..., T], *args: Any) -> T:
    loop = asyncio.get_running_loop()
    async with CPU_SEM:
        return await loop.run_in_executor(IMG_POOL, fn, *args)


def center_square(img: Image.Image) -> Image.Image:
    """Keep only the middle, largest square of a non-square picture."""
    w, h = img.size
    if max(w, h) / max(1, min(w, h)) <= CROP_RATIO:
        return img
    side = min(w, h)
    left, top = (w - side) // 2, (h - side) // 2
    return img.crop((left, top, left + side, top + side))


def process_image(raw: bytes, frame: str = "fit", border: bool = False, kind: str = "static", auto_crop: bool = False) -> bytes:
    """Convert any image into a Telegram-compliant PNG. Pure function, safe in a worker process."""
    with Image.open(io.BytesIO(raw)) as src:
        if src.width * src.height > MAX_IMAGE_PIXELS:
            raise ValueError("Image resolution is too large.")
        img = ImageOps.exif_transpose(src).convert("RGBA")

    if auto_crop:
        img = center_square(img)

    is_small = kind in ("emoji", "icon")
    target = 100 if is_small else 512
    max_bytes = (128 if is_small else 512) * 1024
    pad = max(2, round(target * 0.04)) if border else 0
    inner = target - 2 * pad

    w, h = img.size
    if w == 0 or h == 0:
        raise ValueError("Invalid image dimensions.")
    scale = inner / max(w, h)
    img = img.resize((max(1, round(w * scale)), max(1, round(h * scale))), Image.Resampling.LANCZOS)

    if frame == "rounded":
        w, h = img.size
        mask = Image.new("L", (w, h), 0)
        ImageDraw.Draw(mask).rounded_rectangle([(0, 0), (w - 1, h - 1)], radius=int(min(w, h) * 0.14), fill=255)
        img.putalpha(ImageChops.multiply(img.getchannel("A"), mask))

    if border:
        exp = Image.new("RGBA", (img.width + pad * 2, img.height + pad * 2), (0, 0, 0, 0))
        exp.paste(img, (pad, pad), img)
        stroke = max(1, pad // 2)
        alpha = exp.getchannel("A").filter(ImageFilter.MaxFilter(2 * stroke + 1)).filter(ImageFilter.GaussianBlur(1.2))
        white = Image.new("RGBA", exp.size, (255, 255, 255, 255))
        white.putalpha(alpha)
        img = Image.alpha_composite(white, exp)

    if is_small or kind == "masks" or frame == "pad":
        canvas = Image.new("RGBA", (target, target), (0, 0, 0, 0))
        canvas.paste(img, ((target - img.width) // 2, (target - img.height) // 2), img)
        img = canvas

    for colors in (None, 256, 128, 64):
        out = img if colors is None else img.quantize(colors=colors, method=Image.Quantize.FASTOCTREE).convert("RGBA")
        buf = io.BytesIO()
        out.save(buf, format="PNG", compress_level=6)
        if buf.tell() <= max_bytes:
            return buf.getvalue()
    raise ValueError("Image is too detailed to fit Telegram's size limit.")


def extract_archive(filename: str, raw: bytes) -> List[Tuple[str, bytes]]:
    items: List[Tuple[str, bytes]] = []
    total = 0

    def valid(name: str) -> bool:
        p = Path(name)
        return not p.name.startswith(".") and "__MACOSX" not in p.parts and p.suffix.lower() in IMAGE_EXTS

    bio = io.BytesIO(raw)
    if filename.lower().endswith(".zip"):
        with zipfile.ZipFile(bio, "r") as zf:
            for info in zf.infolist():
                if info.is_dir() or not valid(info.filename) or info.file_size > MAX_FILE_BYTES:
                    continue
                if total + info.file_size > MAX_ARCHIVE_TOTAL_BYTES:
                    break
                data = zf.read(info.filename)
                total += len(data)
                items.append((Path(info.filename).name, data))
                if len(items) >= MAX_ARCHIVE_ENTRIES:
                    break
    else:
        with tarfile.open(fileobj=bio, mode="r:*") as tf:
            for m in tf.getmembers():
                if not m.isfile() or not valid(m.name) or m.size > MAX_FILE_BYTES:
                    continue
                if total + m.size > MAX_ARCHIVE_TOTAL_BYTES:
                    break
                f = tf.extractfile(m)
                if not f:
                    continue
                data = f.read()
                total += len(data)
                items.append((Path(m.name).name, data))
                if len(items) >= MAX_ARCHIVE_ENTRIES:
                    break
    return items


# ---------------------------------------------------------------------------
# States, queues, locks
# ---------------------------------------------------------------------------
class BotStages(StatesGroup):
    creating_title = State()
    creating_first_item = State()
    single_emoji_choice = State()
    single_custom_emoji = State()
    bulk_collecting = State()
    bulk_custom_emoji = State()
    edit_waiting_sticker = State()
    edit_waiting_input = State()
    replace_waiting_old = State()
    replace_waiting_new = State()
    order_waiting_sticker = State()
    order_waiting_pos = State()
    delete_waiting_sticker = State()
    icon_waiting_image = State()
    rename_waiting_title = State()
    clone_confirm = State()


@dataclass
class QueuedSticker:
    filename: str
    data: bytes
    fmt: StickerFormat = StickerFormat.STATIC
    emojis: List[str] = dc_field(default_factory=list)


@dataclass
class BatchResult:
    added: int = 0
    failed: int = 0
    name: str = ""
    title: str = ""
    created: bool = False
    errors: List[str] = dc_field(default_factory=list)


PENDING: Dict[int, Tuple[float, List[QueuedSticker]]] = {}
ALBUM_BUFFERS: Dict[str, List[Message]] = {}
USER_LOCKS: Dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)


def get_queue(uid: int) -> List[QueuedSticker]:
    entry = PENDING.get(uid)
    if not entry:
        return []
    if time.monotonic() - entry[0] > QUEUE_TTL:
        PENDING.pop(uid, None)
        return []
    return entry[1]


def set_queue(uid: int, items: List[QueuedSticker]) -> None:
    PENDING[uid] = (time.monotonic(), items[:MAX_QUEUE_ITEMS])


def extend_queue(uid: int, items: List[QueuedSticker]) -> Tuple[int, int]:
    current = get_queue(uid)
    accepted = items[: max(0, MAX_QUEUE_ITEMS - len(current))]
    set_queue(uid, current + accepted)
    return len(current) + len(accepted), len(items) - len(accepted)


def pop_queue(uid: int) -> List[QueuedSticker]:
    items = get_queue(uid)
    PENDING.pop(uid, None)
    return items


def clear_queue(uid: int) -> None:
    PENDING.pop(uid, None)


async def sweep_state() -> None:
    while True:
        await asyncio.sleep(300)
        now = time.monotonic()
        for uid, (ts, _) in list(PENDING.items()):
            if now - ts > QUEUE_TTL:
                PENDING.pop(uid, None)


# ---------------------------------------------------------------------------
# Telegram helpers
# ---------------------------------------------------------------------------
TG_SEM = asyncio.Semaphore(TG_CONCURRENCY)


async def tg(fn: Callable[..., Awaitable[Any]], **kwargs: Any) -> Any:
    """Bot API call with a global concurrency cap and retry on flood-wait or network errors."""
    async with TG_SEM:
        for _ in range(3):
            try:
                return await fn(**kwargs)
            except TelegramRetryAfter as exc:
                await asyncio.sleep(exc.retry_after + 1)
            except TelegramNetworkError:
                await asyncio.sleep(1)
        return await fn(**kwargs)


async def download_bytes(bot: Bot, file: Any) -> bytes:
    bio = io.BytesIO()
    await bot.download(file, destination=bio)
    return bio.getvalue()


async def safe_edit(msg: Message, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> Message:
    """Edit a message without letting a Telegram error abort a flow."""
    try:
        return await msg.edit_text(text, reply_markup=reply_markup, disable_web_page_preview=True)
    except TelegramBadRequest as exc:
        if "message is not modified" not in str(exc).lower():
            logger.warning("edit failed: %s", exc)
    except TelegramAPIError as exc:
        logger.warning("edit failed: %s", exc)
    return msg


def input_name(item: QueuedSticker) -> str:
    """Name uploads by their real format so Telegram never receives WEBP bytes named .png."""
    if item.fmt == StickerFormat.VIDEO:
        return "sticker.webm"
    if item.fmt == StickerFormat.ANIMATED:
        return "sticker.tgs"
    return "sticker.webp" if item.data[:4] == b"RIFF" else "sticker.png"


SPECS = {
    "emoji": (StickerType.CUSTOM_EMOJI, StickerFormat.STATIC),
    "masks": (StickerType.MASK, StickerFormat.STATIC),
    "video": (StickerType.REGULAR, StickerFormat.VIDEO),
    "animated": (StickerType.REGULAR, StickerFormat.ANIMATED),
}


def pack_specs(kind: str) -> Tuple[StickerType, StickerFormat]:
    return SPECS.get(kind, (StickerType.REGULAR, StickerFormat.STATIC))


def pack_limit(kind: str) -> int:
    return 200 if kind == "emoji" else 120


def item_kind(item: QueuedSticker) -> str:
    if item.fmt == StickerFormat.VIDEO:
        return "video"
    if item.fmt == StickerFormat.ANIMATED:
        return "animated"
    return "static"


# ---------------------------------------------------------------------------
# Sticker set service
# ---------------------------------------------------------------------------
class StickerService:
    def __init__(self, database: Database) -> None:
        self.db = database
        self._bot_user: Optional[str] = None

    async def bot_username(self, bot: Bot) -> str:
        if not self._bot_user:
            self._bot_user = (await tg(bot.get_me)).username
        return self._bot_user

    @staticmethod
    def new_pack_name(uid: int, kind: str, uname: str) -> str:
        code = {"emoji": "em", "masks": "ma", "video": "vi", "animated": "an"}.get(kind, "st")
        suffix = f"_by_{uname}"
        return f"s{uid}_{code}_{secrets.token_hex(3)}"[: 64 - len(suffix)] + suffix

    async def create_set(self, bot: Bot, uid: int, title: str, kind: str, item: QueuedSticker, emojis: List[str]) -> Tuple[str, str]:
        uname = await self.bot_username(bot)
        name = self.new_pack_name(uid, kind, uname)
        full_title = titled(title, uname)
        st_type, def_fmt = pack_specs(kind)
        inp = InputSticker(
            sticker=BufferedInputFile(item.data, filename=input_name(item)),
            format=item.fmt or def_fmt,
            emoji_list=emojis or [pick_random_emoji()],
        )
        await tg(bot.create_new_sticker_set, user_id=uid, name=name, title=full_title, stickers=[inp], sticker_type=st_type)
        await db_call(self.db.save_pack, uid, name, full_title, kind)
        return name, full_title

    async def add_items(
        self,
        bot: Bot,
        uid: int,
        items: List[QueuedSticker],
        fixed_emojis: Optional[List[str]] = None,
        progress: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
    ) -> BatchResult:
        """
        Adds items to the active pack one by one under a per-user lock, so concurrent uploads
        can't race past the pack limit. Full packs roll over to a new set automatically.
        """
        res = BatchResult()
        total = len(items)
        async with USER_LOCKS[uid]:
            u = await db_call(self.db.get_user, uid)
            cur = await db_call(self.db.pack_by_name, u["active_pack_name"])
            state: Optional[Dict[str, Any]] = None
            if cur and cur["user_id"] == uid:
                try:
                    sset = await tg(bot.get_sticker_set, name=cur["pack_name"])
                    state = {"name": cur["pack_name"], "title": sset.title, "kind": cur["pack_kind"], "count": len(sset.stickers)}
                except TelegramAPIError as exc:
                    logger.warning("Could not read active pack %s: %s", cur["pack_name"], exc)

            for idx, item in enumerate(items, 1):
                emojis = fixed_emojis or item.emojis or [pick_random_emoji()]
                try:
                    if state is None:
                        kind = cur["pack_kind"] if cur else item_kind(item)
                        name, title = await self.create_set(bot, uid, "Custom Pack", kind, item, emojis)
                        state = {"name": name, "title": title, "kind": kind, "count": 1}
                        res.created = True
                    elif state["count"] >= pack_limit(state["kind"]):
                        name, title = await self.create_set(bot, uid, "Custom Pack", state["kind"], item, emojis)
                        state = {"name": name, "title": title, "kind": state["kind"], "count": 1}
                        res.created = True
                    else:
                        _, def_fmt = pack_specs(state["kind"])
                        inp = InputSticker(
                            sticker=BufferedInputFile(item.data, filename=input_name(item)),
                            format=item.fmt or def_fmt,
                            emoji_list=emojis,
                        )
                        await tg(bot.add_sticker_to_set, user_id=uid, name=state["name"], sticker=inp)
                        state["count"] += 1
                    res.added += 1
                except Exception as exc:
                    res.failed += 1
                    res.errors.append(getattr(exc, "message", None) or str(exc))
                    logger.error("Sticker upload error (%s): %s", item.filename, exc)

                if progress:
                    await progress(idx, total, state["title"] if state else "")

        if state:
            res.name, res.title = state["name"], state["title"]
        return res


db_service = StickerService(db)
router = Router()


# ---------------------------------------------------------------------------
# Media extraction & ownership
# ---------------------------------------------------------------------------
async def active_kind(u: Dict[str, Any]) -> str:
    p = await db_call(db.pack_by_name, u["active_pack_name"])
    return p["pack_kind"] if p else "static"


async def owned_set_name(uid: int, set_name: Optional[str]) -> Optional[str]:
    """The bot can edit any set it created, so ownership must be enforced here."""
    if not set_name:
        return None
    p = await db_call(db.pack_by_name, set_name)
    return set_name if p and p["user_id"] == uid else None


async def check_own_sticker(msg: Message) -> bool:
    return bool(await owned_set_name(msg.from_user.id, msg.sticker.set_name))


async def extract_media_items(bot: Bot, msg: Message, u: Dict[str, Any], kind: str = "static") -> List[QueuedSticker]:
    frame, border = u["frame_mode"], bool(u["auto_border"])
    crop = bool(u["auto_crop"])
    cap_emojis = parse_emoji_list(msg.caption)

    if msg.photo:
        if kind in ("video", "animated"):
            raise ValueError(f"This pack requires .{'webm' if kind == 'video' else 'tgs'} files, not photos.")
        raw = await download_bytes(bot, msg.photo[-1])
        png = await run_cpu(process_image, raw, frame, border, kind, crop)
        return [QueuedSticker(f"photo_{msg.photo[-1].file_unique_id}.png", png, StickerFormat.STATIC, cap_emojis)]

    if msg.video:
        if kind != "video":
            raise ValueError("Video uploads are only accepted for WEBM video packs.")
        raw = await download_bytes(bot, msg.video)
        return [QueuedSticker("video.webm", raw, StickerFormat.VIDEO, cap_emojis)]

    if msg.document:
        doc = msg.document
        fname = doc.file_name or "file"
        lower = fname.lower()
        if doc.file_size and doc.file_size > MAX_DOWNLOAD_BYTES:
            raise ValueError("Bots can only download files up to 20 MB. Please send a smaller file.")

        if lower.endswith(".webm") or doc.mime_type == "video/webm":
            if kind != "video":
                raise ValueError("WEBM files are only accepted for video sticker packs.")
            return [QueuedSticker(fname, await download_bytes(bot, doc), StickerFormat.VIDEO, cap_emojis)]

        if lower.endswith(".tgs") or doc.mime_type == "application/x-tgsticker":
            if kind != "animated":
                raise ValueError("TGS files are only accepted for animated sticker packs.")
            return [QueuedSticker(fname, await download_bytes(bot, doc), StickerFormat.ANIMATED, cap_emojis)]

        if lower.endswith(ARCHIVE_EXTS):
            if kind in ("video", "animated"):
                raise ValueError("Archives are only supported for static, emoji and mask packs.")
            raw = await download_bytes(bot, doc)
            extracted = await run_cpu(extract_archive, fname, raw)

            async def convert(name: str, data: bytes) -> Optional[QueuedSticker]:
                try:
                    png = await run_cpu(process_image, data, frame, border, kind, crop)
                    return QueuedSticker(name, png, StickerFormat.STATIC, cap_emojis)
                except Exception as exc:
                    logger.warning("Skipping archive file %s: %s", name, exc)
                    return None

            converted = await asyncio.gather(*(convert(n, d) for n, d in extracted))
            return [q for q in converted if q]

        if Path(fname).suffix.lower() in IMAGE_EXTS or (doc.mime_type or "").startswith("image/"):
            if kind in ("video", "animated"):
                raise ValueError(f"This pack requires .{'webm' if kind == 'video' else 'tgs'} files, not images.")
            raw = await download_bytes(bot, doc)
            png = await run_cpu(process_image, raw, frame, border, kind, crop)
            return [QueuedSticker(fname, png, StickerFormat.STATIC, cap_emojis)]

    return []


async def collect_album_or_single(bot: Bot, msg: Message, u: Dict[str, Any], kind: str) -> Optional[List[QueuedSticker]]:
    if not msg.media_group_id:
        return await extract_media_items(bot, msg, u, kind)

    key = f"{msg.from_user.id}:{msg.media_group_id}"
    is_leader = key not in ALBUM_BUFFERS
    ALBUM_BUFFERS.setdefault(key, []).append(msg)
    if not is_leader:
        return None

    try:
        await asyncio.sleep(ALBUM_WAIT)
    finally:
        batch_msgs = ALBUM_BUFFERS.pop(key, [])

    sem = asyncio.Semaphore(6)

    async def one(m: Message) -> List[QueuedSticker]:
        async with sem:
            try:
                return await extract_media_items(bot, m, u, kind)
            except Exception as exc:
                logger.warning("Album item skipped: %s", exc)
                return []

    results = await asyncio.gather(*(one(m) for m in batch_msgs))
    return [q for part in results for q in part]


# ---------------------------------------------------------------------------
# Views: every screen is built here, so breadcrumbs and back buttons stay consistent
# ---------------------------------------------------------------------------
async def home_view(user: Any) -> Tuple[str, InlineKeyboardMarkup]:
    u = await db_call(db.get_user, user.id, user.username)
    packs = await db_call(db.user_packs, user.id)
    p = await db_call(db.pack_by_name, u["active_pack_name"])
    active = (
        f"<b>{ui.e(p['pack_title'])}</b>  {ui.PACK_BADGES.get(p['pack_kind'], '')}"
        if p else "<i>None: created automatically on first upload</i>"
    )
    body = "\n".join([
        ui.field("❏", "Active pack", active),
        ui.field("❐", "Saved packs", str(len(packs))),
        ui.field("☻", "Emoji mode", ui.EMOJI_MODE_LABELS.get(u["emoji_mode"], "")),
        ui.field(
            "▣", "Processing",
            f"{ui.FRAME_LABELS.get(u['frame_mode'], '')} · outline {ui.on_off(u['auto_border'])} · auto-crop {ui.on_off(u['auto_crop'])}",
        ),
        "",
        "Choose an action below, or send an image, sticker, archive, or <code>t.me</code> pack link.",
    ])
    open_url = f"https://t.me/addstickers/{p['pack_name']}" if p else None
    return ui.card("✿", "Sticker Studio", ("home",), body), ui.home_kb(open_url)


async def create_view(notice: Optional[str] = None) -> Tuple[str, InlineKeyboardMarkup]:
    body = (ui.warn(notice) + "\n\n") if notice else ""
    body += "Choose the type of pack to build. Each option explains its requirements before you send files."
    return ui.card("➕", "Create a pack", ("home", "create"), body), ui.create_kb()


async def edit_view(uid: int) -> Tuple[str, InlineKeyboardMarkup]:
    u = await db_call(db.get_user, uid)
    p = await db_call(db.pack_by_name, u["active_pack_name"])
    if not p:
        packs = await db_call(db.user_packs, uid)
        if not packs:
            return await create_view("You don't have a pack yet. Create one to start editing.")
        await db_call(db.set_active, uid, packs[0]["pack_name"])
        p = packs[0]
    body = "\n".join([
        ui.field("❏", "Pack", ui.e(p["pack_title"])),
        ui.field("❏", "Type", ui.PACK_BADGES.get(p["pack_kind"], "Static")),
        "",
        "Choose what you'd like to change.",
    ])
    return ui.card("✎", "Pack editor", ("home", "edit"), body), ui.edit_kb(f"https://t.me/addstickers/{p['pack_name']}")


async def add_view() -> Tuple[str, InlineKeyboardMarkup]:
    body = "Send a photo, image, album, or archive now, and it goes straight into your active pack.\n\nTo collect several files first, open the bulk queue."
    return ui.stage("add_sticker", body), ui.add_kb()


async def packs_view(uid: int) -> Tuple[str, InlineKeyboardMarkup]:
    u = await db_call(db.get_user, uid)
    packs = await db_call(db.user_packs, uid)
    if not packs:
        return ui.card("❏", "My packs", ("home", "packs"), ui.info("You have no saved packs yet.")), ui.markup(
            [btn("➕ Create pack", Nav.CREATE, role=Role.SUCCESS)],
            [ui.back(Nav.HOME)],
        )
    rows = [
        (
            f"{'✔ ' if p['pack_name'] == u['active_pack_name'] else ''}{p['pack_title'][:30]}",
            f"sel_pack:{p['pid']}",
            f"edit_pack:{p['pid']}",
            f"https://t.me/addstickers/{p['pack_name']}",
        )
        for p in packs[:15]
    ]
    body = (
        "Tap a pack to make it active. Use ✎ to open its editor. ✔ marks the active pack.\n\n"
        + ui.field("❐", "Showing", f"{min(len(packs), 15)} of {len(packs)}")
    )
    return ui.card("❏", "My packs", ("home", "packs"), body), ui.packs_kb(rows)


async def settings_view(uid: int) -> Tuple[str, InlineKeyboardMarkup]:
    u = await db_call(db.get_user, uid)
    body = "Changes apply to your next upload.\n\nPick an emoji mode and frame style, then toggle outline and auto-crop."
    return ui.card("⌘", "Settings", ("home", "settings"), body), ui.settings_kb(
        u["emoji_mode"], u["frame_mode"], bool(u["auto_border"]), bool(u["auto_crop"])
    )


async def build_view(name: str, user: Any) -> Tuple[str, InlineKeyboardMarkup]:
    if name == "create":
        return await create_view()
    if name == "edit":
        return await edit_view(user.id)
    if name == "add":
        return await add_view()
    if name == "packs":
        return await packs_view(user.id)
    if name == "settings":
        return await settings_view(user.id)
    return await home_view(user)


def bulk_text(count: int) -> str:
    body = ui.field("⤓", "Queued", f"{count} of {MAX_QUEUE_ITEMS}")
    body += "\n\nSend photos, albums, images, or archives. Use the buttons when you're ready to process them."
    return ui.stage("bulk", body)


# ---------------------------------------------------------------------------
# Entry points & navigation
# ---------------------------------------------------------------------------
@router.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext) -> None:
    await state.clear()
    clear_queue(msg.from_user.id)
    text, kb = await home_view(msg.from_user)
    await msg.answer(text, reply_markup=kb, disable_web_page_preview=True)


@router.callback_query(F.data.startswith("nav:"))
async def cb_nav(cb: CallbackQuery, state: FSMContext) -> None:
    was_active = bool(await state.get_state())
    await state.clear()
    name = cb.data.split(":", 1)[1]
    if name == "home":
        clear_queue(cb.from_user.id)
    text, kb = await build_view(name, cb.from_user)
    await safe_edit(cb.message, text, kb)
    await cb.answer("Cancelled" if was_active and name == "home" else None)


# ---------------------------------------------------------------------------
# Create flow: type -> title -> first item
# ---------------------------------------------------------------------------
CREATE_ACTION = {"emoji": "new_emoji", "static": "new_static", "video": "new_video", "animated": "new_animated", "masks": "new_masks"}


@router.callback_query(F.data.startswith("create_type:"))
async def cb_create_type(cb: CallbackQuery, state: FSMContext) -> None:
    kind = cb.data.split(":", 1)[1]
    await state.set_state(BotStages.creating_title)
    await state.update_data(new_pack_kind=kind)
    body = step(1, 2, "Send the title for your new pack (2–64 characters).")
    await safe_edit(cb.message, ui.stage(CREATE_ACTION[kind], body), reply_markup=ui.operation_kb(Nav.CREATE))
    await cb.answer()


@router.message(BotStages.creating_title, F.text)
async def on_new_pack_title(msg: Message, state: FSMContext) -> None:
    title = msg.text.strip()[:64]
    if len(title) < 2:
        await msg.reply(ui.warn("Please send a title with at least 2 characters."))
        return
    kind = (await state.get_data()).get("new_pack_kind", "static")
    await state.update_data(new_pack_title=title)
    await state.set_state(BotStages.creating_first_item)
    req = {
        "video": "Send the first <code>.webm</code> video file.",
        "animated": "Send the first <code>.tgs</code> animated sticker file.",
        "emoji": "Send the first image. It is resized to <code>100×100 px</code>.",
    }.get(kind, "Send the first picture, file, or <code>.zip</code> archive. It is resized to <code>512×512 px</code>.")
    body = ui.ok(f"Title: <b>{ui.e(title)}</b>") + "\n\n" + step(2, 2, req)
    await msg.answer(ui.stage(CREATE_ACTION[kind], body), reply_markup=ui.operation_kb(Nav.CREATE))


@router.message(BotStages.creating_first_item, F.photo | F.document | F.video)
async def on_new_pack_first_item(msg: Message, state: FSMContext, bot: Bot) -> None:
    uid = msg.from_user.id
    u = await db_call(db.get_user, uid, msg.from_user.username)
    data = await state.get_data()
    title, kind = data.get("new_pack_title", "Custom Pack"), data.get("new_pack_kind", "static")

    try:
        items = await extract_media_items(bot, msg, u, kind)
    except Exception as exc:
        await msg.reply(ui.warn(error_text(exc)))
        return
    if not items:
        await msg.reply(ui.warn("That file isn't supported for this pack type."))
        return

    first = items[0]
    emojis = first.emojis or [pick_random_emoji()]
    status = await msg.answer(ui.card("⏳", "Creating your pack", ("home", "create"), "Uploading to Telegram..."))
    try:
        name, full_title = await db_service.create_set(bot, uid, title, kind, first, emojis)
    except Exception as exc:
        await safe_edit(status, ui.card("✖", "Could not create pack", ("home", "create"), f"Telegram said: {error_text(exc)}"),
                        reply_markup=ui.operation_kb(Nav.CREATE))
        return

    await state.clear()
    if len(items) > 1:
        await run_batch(bot, status, uid, items[1:], edit_start=True)
        return

    body = "\n".join([
        ui.field("❏", "Title", f"<a href='https://t.me/addstickers/{name}'>{ui.e(full_title)}</a>"),
        ui.field("❏", "Format", ui.PACK_BADGES.get(kind, "Static")),
        ui.field("☻", "First emoji", "".join(emojis)),
    ])
    await safe_edit(status, ui.card("🎉", "Pack created", ("home", "create"), body), reply_markup=ui.done_kb(name))


# ---------------------------------------------------------------------------
# Editor actions (each one: step 1 -> step 2 -> result)
# ---------------------------------------------------------------------------
@router.callback_query(F.data == "act:edit_sticker")
async def cb_edit_sticker(cb: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(BotStages.edit_waiting_sticker)
    await safe_edit(cb.message, ui.stage("edit_sticker", step(1, 2, "Send the sticker from your pack that you want to edit.")),
                    reply_markup=ui.operation_kb(Nav.EDIT))
    await cb.answer()


@router.message(BotStages.edit_waiting_sticker, F.sticker)
async def on_edit_sticker_received(msg: Message, state: FSMContext) -> None:
    if not await check_own_sticker(msg):
        await msg.answer(ui.warn("That sticker isn't from one of your packs."), reply_markup=ui.operation_kb(Nav.EDIT))
        return
    await state.update_data(target_sticker=msg.sticker.file_id)
    await state.set_state(BotStages.edit_waiting_input)
    body = ui.ok("Sticker selected.") + "\n\n" + step(2, 2, "Send 1–7 emojis, tap <b>Random</b>, or choose a face-mask anchor.")
    await msg.answer(ui.stage("edit_sticker", body), reply_markup=ui.emoji_input_kb())


MASK_POINT_MAP = {
    "forehead": MaskPositionPoint.FOREHEAD,
    "eyes": MaskPositionPoint.EYES,
    "mouth": MaskPositionPoint.MOUTH,
    "chin": MaskPositionPoint.CHIN,
}


@router.callback_query(BotStages.edit_waiting_input, F.data.startswith("edit_st:"))
async def cb_edit_sticker_apply(cb: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    fid = (await state.get_data()).get("target_sticker")
    if not fid:
        await state.clear()
        await cb.answer("Session expired. Select the sticker again.", show_alert=True)
        return
    parts = cb.data.split(":")
    try:
        if parts[1] == "random":
            em = pick_random_emoji()
            await tg(bot.set_sticker_emoji_list, sticker=fid, emoji_list=[em])
            result = ui.ok(f"Emoji updated to {em}")
        else:
            await tg(bot.set_sticker_mask_position, sticker=fid,
                     mask_position=MaskPosition(point=MASK_POINT_MAP[parts[2]], x_shift=0.0, y_shift=0.0, scale=1.0))
            result = ui.ok(f"Mask anchor set to {parts[2].capitalize()}")
        await state.clear()
        text = ui.card("✔", "Sticker updated", ("home", "edit", "emoji"), result)
    except TelegramAPIError as exc:
        text = ui.card("✖", "Update failed", ("home", "edit", "emoji"), f"Telegram said: {error_text(exc)}")
    await safe_edit(cb.message, text, reply_markup=ui.result_kb())
    await cb.answer()


@router.message(BotStages.edit_waiting_input, F.text)
async def on_edit_sticker_emojis(msg: Message, state: FSMContext, bot: Bot) -> None:
    emojis = parse_emoji_list(msg.text)
    if not emojis:
        await msg.reply(ui.warn("Send at least one valid emoji."), reply_markup=ui.emoji_input_kb())
        return
    fid = (await state.get_data()).get("target_sticker")
    try:
        await tg(bot.set_sticker_emoji_list, sticker=fid, emoji_list=emojis)
        await state.clear()
        text = ui.card("✔", "Emoji updated", ("home", "edit", "emoji"), ui.ok(f"New emojis: {''.join(emojis)}"))
    except TelegramAPIError as exc:
        text = ui.card("✖", "Update failed", ("home", "edit", "emoji"), f"Telegram said: {error_text(exc)}")
    await msg.answer(text, reply_markup=ui.result_kb())


@router.callback_query(F.data == "act:replace_sticker")
async def cb_replace_sticker(cb: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(BotStages.replace_waiting_old)
    await safe_edit(cb.message, ui.stage("replace_sticker", step(1, 2, "Send the existing sticker from your pack that you want to replace.")),
                    reply_markup=ui.operation_kb(Nav.EDIT))
    await cb.answer()


@router.message(BotStages.replace_waiting_old, F.sticker)
async def on_replace_old(msg: Message, state: FSMContext) -> None:
    if not await check_own_sticker(msg):
        await msg.answer(ui.warn("That sticker isn't from one of your packs."), reply_markup=ui.operation_kb(Nav.EDIT))
        return
    await state.update_data(old_fid=msg.sticker.file_id, old_set=msg.sticker.set_name, old_emoji=msg.sticker.emoji or pick_random_emoji())
    await state.set_state(BotStages.replace_waiting_new)
    body = ui.ok("Sticker selected.") + "\n\n" + step(2, 2, "Upload the new image or file to replace it with.")
    await msg.answer(ui.stage("replace_sticker", body), reply_markup=ui.operation_kb(Nav.EDIT))


@router.message(BotStages.replace_waiting_new, F.photo | F.document | F.video)
async def on_replace_new(msg: Message, state: FSMContext, bot: Bot) -> None:
    uid = msg.from_user.id
    u = await db_call(db.get_user, uid, msg.from_user.username)
    data = await state.get_data()
    set_name = await owned_set_name(uid, data.get("old_set")) or await owned_set_name(uid, u["active_pack_name"])
    if not set_name:
        await msg.reply(ui.warn("Could not determine the target pack."), reply_markup=ui.operation_kb(Nav.EDIT))
        return
    p = await db_call(db.pack_by_name, set_name)
    kind = p["pack_kind"] if p else "static"
    try:
        items = await extract_media_items(bot, msg, u, kind)
    except Exception as exc:
        await msg.reply(ui.warn(error_text(exc)))
        return
    if not items:
        await msg.reply(ui.warn("Send a valid image or sticker file."))
        return

    item = items[0]
    emojis = item.emojis or [data.get("old_emoji", "✦")]
    _, def_fmt = pack_specs(kind)
    inp = InputSticker(sticker=BufferedInputFile(item.data, filename=input_name(item)), format=item.fmt or def_fmt, emoji_list=emojis)
    try:
        await tg(bot.replace_sticker_in_set, user_id=uid, name=set_name, old_sticker=data["old_fid"], sticker=inp)
        await state.clear()
        text = ui.card("✔", "Sticker replaced", ("home", "edit", "replace"), ui.ok("The new sticker kept the original position."))
    except TelegramAPIError as exc:
        text = ui.card("✖", "Replace failed", ("home", "edit", "replace"), f"Telegram said: {error_text(exc)}")
    await msg.answer(text, reply_markup=ui.result_kb())


@router.callback_query(F.data == "act:order_sticker")
async def cb_order_sticker(cb: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(BotStages.order_waiting_sticker)
    await safe_edit(cb.message, ui.stage("order_sticker", step(1, 2, "Send the sticker from your pack that you want to move.")),
                    reply_markup=ui.operation_kb(Nav.EDIT))
    await cb.answer()


@router.message(BotStages.order_waiting_sticker, F.sticker)
async def on_order_sticker(msg: Message, state: FSMContext) -> None:
    if not await check_own_sticker(msg):
        await msg.answer(ui.warn("That sticker isn't from one of your packs."), reply_markup=ui.operation_kb(Nav.EDIT))
        return
    await state.update_data(order_fid=msg.sticker.file_id)
    await state.set_state(BotStages.order_waiting_pos)
    body = ui.ok("Sticker selected.") + "\n\n" + step(2, 2, "Reply with the new position number. <code>1</code> is first.")
    await msg.answer(ui.stage("order_sticker", body), reply_markup=ui.operation_kb(Nav.EDIT))


@router.message(BotStages.order_waiting_pos, F.text)
async def on_order_pos(msg: Message, state: FSMContext, bot: Bot) -> None:
    if not msg.text.strip().isdigit():
        await msg.reply(ui.warn("Enter a whole number, for example <code>1</code> or <code>2</code>."))
        return
    pos = max(1, int(msg.text.strip()))
    fid = (await state.get_data()).get("order_fid")
    try:
        await tg(bot.set_sticker_position_in_set, sticker=fid, position=pos - 1)
        await state.clear()
        text = ui.card("✔", "Sticker moved", ("home", "edit", "order"), ui.ok(f"Now at position {pos}."))
    except TelegramAPIError as exc:
        text = ui.card("✖", "Move failed", ("home", "edit", "order"), f"Telegram said: {error_text(exc)}")
    await msg.answer(text, reply_markup=ui.result_kb())


@router.callback_query(F.data == "act:del_sticker")
async def cb_del_sticker(cb: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(BotStages.delete_waiting_sticker)
    await safe_edit(cb.message, ui.stage("del_sticker", step(1, 1, "Send the sticker you want to remove from the pack.")),
                    reply_markup=ui.operation_kb(Nav.EDIT))
    await cb.answer()


@router.message(BotStages.delete_waiting_sticker, F.sticker)
async def on_del_sticker(msg: Message, state: FSMContext, bot: Bot) -> None:
    if not await check_own_sticker(msg):
        await msg.answer(ui.warn("That sticker isn't from one of your packs."), reply_markup=ui.operation_kb(Nav.EDIT))
        return
    try:
        await tg(bot.delete_sticker_from_set, sticker=msg.sticker.file_id)
        await state.clear()
        text = ui.card("✔", "Sticker deleted", ("home", "edit", "delete"), ui.ok("The sticker was removed from the pack."))
    except TelegramAPIError as exc:
        text = ui.card("✖", "Delete failed", ("home", "edit", "delete"), f"Telegram said: {error_text(exc)}")
    await msg.answer(text, reply_markup=ui.result_kb())


@router.callback_query(F.data == "act:set_pack_icon")
async def cb_set_icon(cb: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(BotStages.icon_waiting_image)
    await safe_edit(cb.message, ui.stage("set_pack_icon", step(1, 1, "Send any picture. It will be resized to 100×100 px.")),
                    reply_markup=ui.operation_kb(Nav.EDIT))
    await cb.answer()


@router.message(BotStages.icon_waiting_image, F.photo | F.document)
async def on_set_icon(msg: Message, state: FSMContext, bot: Bot) -> None:
    uid = msg.from_user.id
    u = await db_call(db.get_user, uid)
    if not u["active_pack_name"]:
        await msg.answer(ui.warn("Select an active pack first."), reply_markup=ui.operation_kb(Nav.EDIT))
        return
    try:
        items = await extract_media_items(bot, msg, u, kind="icon")
        if not items:
            raise ValueError("Please send a valid image.")
        await tg(
            bot.set_sticker_set_thumbnail,
            name=u["active_pack_name"],
            user_id=uid,
            thumbnail=BufferedInputFile(items[0].data, filename="icon.png"),
            format=StickerFormat.STATIC,
        )
        await state.clear()
        text = ui.card("✔", "Pack icon updated", ("home", "edit", "icon"), ui.ok("Your new 100×100 icon is live."))
    except Exception as exc:
        text = ui.card("✖", "Icon update failed", ("home", "edit", "icon"), f"Reason: {error_text(exc)}")
    await msg.answer(text, reply_markup=ui.result_kb())


@router.callback_query(F.data == "act:rename_pack")
async def cb_rename_pack(cb: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(BotStages.rename_waiting_title)
    await safe_edit(cb.message, ui.stage("rename_pack", step(1, 1, "Reply with the new display title (2–64 characters).")),
                    reply_markup=ui.operation_kb(Nav.EDIT))
    await cb.answer()


@router.message(BotStages.rename_waiting_title, F.text)
async def on_rename_pack(msg: Message, state: FSMContext, bot: Bot) -> None:
    title = msg.text.strip()[:64]
    if len(title) < 2:
        await msg.reply(ui.warn("Enter at least 2 characters."))
        return
    uid = msg.from_user.id
    u = await db_call(db.get_user, uid)
    if not u["active_pack_name"]:
        await state.clear()
        await msg.answer(ui.warn("No active pack selected."), reply_markup=ui.result_kb())
        return
    full = titled(title, await db_service.bot_username(bot))
    try:
        await tg(bot.set_sticker_set_title, name=u["active_pack_name"], title=full)
        await db_call(db.rename_pack, u["active_pack_name"], full)
        await state.clear()
        text = ui.card("✔", "Pack renamed", ("home", "edit", "rename"), ui.field("❏", "New title", ui.e(full)))
    except TelegramAPIError as exc:
        text = ui.card("✖", "Rename failed", ("home", "edit", "rename"), f"Telegram said: {error_text(exc)}")
    await msg.answer(text, reply_markup=ui.result_kb())


@router.callback_query(F.data == "act:del_pack")
async def cb_del_pack(cb: CallbackQuery) -> None:
    u = await db_call(db.get_user, cb.from_user.id)
    p = await db_call(db.pack_by_name, u["active_pack_name"])
    if not p or p["user_id"] != cb.from_user.id:
        await cb.answer("No active pack selected.", show_alert=True)
        return
    body = ui.warn(f"You're about to delete <b>{ui.e(p['pack_title'])}</b>. This removes it from Telegram for good.")
    await safe_edit(cb.message, ui.stage("del_pack", body), reply_markup=ui.delete_confirm_kb())
    await cb.answer()


@router.callback_query(F.data == "confirm_del_pack")
async def cb_confirm_del_pack(cb: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    uid = cb.from_user.id
    u = await db_call(db.get_user, uid)
    p = await db_call(db.pack_by_name, u["active_pack_name"])
    if p and p["user_id"] == uid:
        try:
            await tg(bot.delete_sticker_set, name=p["pack_name"])
        except TelegramBadRequest as exc:
            logger.warning("Telegram set already gone or invalid: %s", exc)
        await db_call(db.delete_pack, uid, p["pack_name"])
    await state.clear()
    text, kb = await home_view(cb.from_user)
    await cb.answer("Pack deleted")
    await safe_edit(cb.message, ui.card("✘", "Pack deleted", ("home",), ui.ok("The pack was permanently removed.")) + "\n\n" + text, reply_markup=kb)


# ---------------------------------------------------------------------------
# Pack switcher & settings
# ---------------------------------------------------------------------------
async def pack_from_callback(uid: int, data: str) -> Optional[Dict[str, Any]]:
    try:
        pid = data.split(":", 1)[1]
    except IndexError:
        return None
    return await db_call(db.pack_by_id, uid, pid)


@router.callback_query(F.data.startswith("sel_pack:"))
async def cb_sel_pack(cb: CallbackQuery) -> None:
    p = await pack_from_callback(cb.from_user.id, cb.data)
    if not p:
        await cb.answer("Pack not found.", show_alert=True)
        return
    await db_call(db.set_active, cb.from_user.id, p["pack_name"])
    text, kb = await packs_view(cb.from_user.id)
    await safe_edit(cb.message, text, kb)
    await cb.answer(f"Active pack: {p['pack_title'][:30]}")


@router.callback_query(F.data.startswith("edit_pack:"))
async def cb_edit_selected_pack(cb: CallbackQuery) -> None:
    p = await pack_from_callback(cb.from_user.id, cb.data)
    if not p:
        await cb.answer("Pack not found.", show_alert=True)
        return
    await db_call(db.set_active, cb.from_user.id, p["pack_name"])
    text, kb = await edit_view(cb.from_user.id)
    await safe_edit(cb.message, text, kb)
    await cb.answer()


@router.callback_query(F.data.startswith("pref:"))
async def cb_update_pref(cb: CallbackQuery) -> None:
    parts = cb.data.split(":")
    if len(parts) != 3:
        await cb.answer()
        return
    _, col, val = parts
    uid = cb.from_user.id
    try:
        if val == "toggle":
            u = await db_call(db.get_user, uid)
            val = "0" if u[col] else "1"
        await db_call(db.set_pref, uid, col, val)
    except (ValueError, IndexError):
        await cb.answer("Invalid option.", show_alert=True)
        return
    text, kb = await settings_view(uid)
    await safe_edit(cb.message, text, kb)
    await cb.answer("Saved")


# ---------------------------------------------------------------------------
# Bulk queue & direct upload
# ---------------------------------------------------------------------------
@router.callback_query(F.data == "act:bulk")
async def cb_bulk_start(cb: CallbackQuery, state: FSMContext) -> None:
    clear_queue(cb.from_user.id)
    await state.set_state(BotStages.bulk_collecting)
    await safe_edit(cb.message, bulk_text(0), reply_markup=ui.bulk_kb(0))
    await cb.answer()


@router.message(BotStages.bulk_collecting, F.photo | F.document | F.video)
async def on_bulk_media(msg: Message, bot: Bot) -> None:
    uid = msg.from_user.id
    u = await db_call(db.get_user, uid, msg.from_user.username)
    kind = await active_kind(u)
    try:
        items = await collect_album_or_single(bot, msg, u, kind)
    except Exception as exc:
        await msg.reply(ui.warn(error_text(exc)))
        return
    if items is None:
        return
    if not items:
        await msg.reply(ui.warn("That file isn't supported in this pack type."))
        return

    total, dropped = extend_queue(uid, items)
    note = f"\n\n{ui.warn(f'The queue is full. {dropped} item(s) were skipped.')}" if dropped else ""
    await msg.reply(bulk_text(total) + note, reply_markup=ui.bulk_kb(total))


@router.callback_query(BotStages.bulk_collecting, F.data.startswith("bulk:"))
async def cb_bulk_action(cb: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    act = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    q = get_queue(uid)

    if act == "clear":
        clear_queue(uid)
        await safe_edit(cb.message, bulk_text(0), reply_markup=ui.bulk_kb(0))
        await cb.answer("Queue cleared")
        return
    if not q:
        await cb.answer("The queue is empty.", show_alert=True)
        return
    if act == "random":
        await state.clear()
        batch = pop_queue(uid)
        await cb.answer("Processing")
        await run_batch(bot, cb.message, uid, batch, edit_start=True)
    elif act == "custom":
        await state.set_state(BotStages.bulk_custom_emoji)
        body = step(1, 1, f"Reply with 1–7 emojis to assign to all {len(q)} queued stickers.")
        await safe_edit(cb.message, ui.stage("bulk", body), reply_markup=ui.operation_kb(Nav.ADD))
        await cb.answer()


@router.message(BotStages.bulk_custom_emoji, F.text)
async def on_bulk_custom_emoji(msg: Message, state: FSMContext, bot: Bot) -> None:
    emojis = parse_emoji_list(msg.text)
    if not emojis:
        await msg.reply(ui.warn("Send at least one valid emoji."))
        return
    uid = msg.from_user.id
    batch = pop_queue(uid)
    await state.clear()
    await run_batch(bot, msg, uid, batch, fixed_emojis=emojis)


async def run_batch(
    bot: Bot,
    target: Message,
    user_id: int,
    queue: List[QueuedSticker],
    fixed_emojis: Optional[List[str]] = None,
    edit_start: bool = False,
) -> None:
    total = len(queue)
    processing = ui.card("✲", "Processing", ("home", "edit", "add"), f"{ui.progress_bar(0, total)}\nPreparing {total} sticker(s)...")
    status = await (safe_edit(target, processing) if edit_start else target.answer(processing))

    started = time.monotonic()
    last_edit = [0.0]

    async def progress(idx: int, count: int, title: str) -> None:
        now = time.monotonic()
        if idx != count and now - last_edit[0] < 1.0:
            return
        last_edit[0] = now
        body = "\n".join([
            ui.progress_block(idx, count, now - started),
            ui.field("❏", "Pack", ui.e(title or "…")),
        ])
        await safe_edit(status, ui.card("✲", "Uploading to Telegram", ("home", "edit", "add"), body))

    result = await db_service.add_items(bot, user_id, queue, fixed_emojis, progress)
    lines = [ui.field("▣", "Added", f"{result.added} of {total}")]
    if result.failed:
        lines.append(ui.field("✖", "Failed", str(result.failed)))
    if result.name:
        lines.append(ui.field("❏", "Pack", f"<a href='https://t.me/addstickers/{result.name}'>{ui.e(result.title)}</a>"))
        markup = ui.done_kb(result.name)
    else:
        markup = ui.result_kb()
    await safe_edit(status, ui.card("✔", "Batch complete", ("home", "edit", "add"), "\n".join(lines)), reply_markup=markup)


async def finalize_single(bot: Bot, target: Message, user_id: int, item: QueuedSticker, emojis: List[str], edit_in_place: bool = False) -> None:
    wait = ui.card("⧗", "Saving sticker", ("home", "edit", "add"), f"Adding with {''.join(emojis)}...")
    status = await (safe_edit(target, wait) if edit_in_place else target.reply(wait))
    res = await db_service.add_items(bot, user_id, [item], fixed_emojis=emojis)
    if res.added == 0:
        reason = ui.e(res.errors[0]) if res.errors else "unknown error"
        await safe_edit(status, ui.card("✖", "Could not save sticker", ("home", "edit", "add"), f"Telegram said: {reason}"),
                        reply_markup=ui.result_kb())
        return
    label = "Pack created" if res.created else "Sticker added"
    body = "\n".join([
        ui.field("❏", "Pack", f"<a href='https://t.me/addstickers/{res.name}'>{ui.e(res.title)}</a>"),
        ui.field("☻", "Emoji", "".join(emojis)),
    ])
    await safe_edit(status, ui.card("🎉" if res.created else "✅", label, ("home", "edit", "add"), body), reply_markup=ui.done_kb(res.name))


@router.message(F.photo | F.document | F.video)
async def on_direct_upload(msg: Message, state: FSMContext, bot: Bot) -> None:
    uid = msg.from_user.id
    u = await db_call(db.get_user, uid, msg.from_user.username)
    kind = await active_kind(u)

    try:
        items = await collect_album_or_single(bot, msg, u, kind)
    except Exception as exc:
        await msg.reply(ui.warn(error_text(exc)))
        return
    if items is None:
        return
    if not items:
        await msg.reply(ui.warn("Send a photo, PNG / WEBP / WEBM / TGS file, or <code>.zip</code> archive."))
        return

    if len(items) > 1:
        if u["emoji_mode"] == "random":
            await run_batch(bot, msg, uid, items)
        else:
            set_queue(uid, items)
            await state.set_state(BotStages.bulk_collecting)
            body = f"{ui.field('⤓', 'Prepared', f'{len(items)} stickers')}\n\nChoose how to assign emojis."
            await msg.reply(ui.card("❒", "Bulk queue", ("home", "edit", "add", "bulk"), body), reply_markup=ui.bulk_kb(len(items)))
        return

    item = items[0]
    if item.emojis:
        await finalize_single(bot, msg, uid, item, item.emojis)
        return
    if u["emoji_mode"] == "random":
        await finalize_single(bot, msg, uid, item, [pick_random_emoji()])
        return

    set_queue(uid, [item])
    if u["emoji_mode"] == "manual":
        await state.set_state(BotStages.single_custom_emoji)
        body = ui.ok("Image processed.") + "\n\n" + step(1, 1, "Reply with 1–7 emojis for this sticker.")
        await msg.reply(ui.stage("add_sticker", body), reply_markup=ui.single_back_kb())
        return

    await state.set_state(BotStages.single_emoji_choice)
    body = ui.ok("Image processed.") + "\n\nChoose how to assign an emoji to this sticker."
    await msg.reply(ui.card("✦", "Sticker ready", ("home", "edit", "add"), body), reply_markup=ui.single_emoji_kb())


@router.callback_query(BotStages.single_emoji_choice, F.data.startswith("s_em:"))
async def cb_single_emoji(cb: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    uid = cb.from_user.id
    if not get_queue(uid):
        await state.clear()
        await cb.answer("Session expired. Send the image again.", show_alert=True)
        return

    parts = cb.data.split(":")
    if parts[1] == "custom":
        await state.set_state(BotStages.single_custom_emoji)
        body = step(1, 1, "Reply with 1–7 emojis for this sticker.")
        await safe_edit(cb.message, ui.stage("add_sticker", body), reply_markup=ui.single_back_kb())
        await cb.answer()
        return

    emoji = parts[2] if parts[1] == "pick" else pick_random_emoji()
    item = pop_queue(uid)[0]
    await state.clear()
    await cb.answer(f"Assigned {emoji}")
    await finalize_single(bot, cb.message, uid, item, [emoji], edit_in_place=True)


@router.message(BotStages.single_custom_emoji, F.text)
async def on_single_custom_emoji(msg: Message, state: FSMContext, bot: Bot) -> None:
    emojis = parse_emoji_list(msg.text)
    if not emojis:
        await msg.reply(ui.warn("Send at least one valid emoji."), reply_markup=ui.single_back_kb())
        return
    uid = msg.from_user.id
    q = pop_queue(uid)
    await state.clear()
    if not q:
        await msg.reply(ui.warn("Session expired. Send the image again."), reply_markup=ui.single_back_kb())
        return
    await finalize_single(bot, msg, uid, q[0], emojis)


# ---------------------------------------------------------------------------
# Clone a pack from a t.me link or a sticker, then edit the copy
# ---------------------------------------------------------------------------
def detect_source_kind(src: Any) -> str:
    raw_type = getattr(src, "sticker_type", None)
    st = getattr(raw_type, "value", raw_type)
    if st == "custom_emoji":
        return "emoji"
    if st == "mask":
        return "masks"
    first = src.stickers[0]
    if getattr(first, "is_video", False):
        return "video"
    if getattr(first, "is_animated", False):
        return "animated"
    return "static"


def source_format(s: Any) -> StickerFormat:
    if getattr(s, "is_video", False):
        return StickerFormat.VIDEO
    if getattr(s, "is_animated", False):
        return StickerFormat.ANIMATED
    return StickerFormat.STATIC


async def run_clone(bot: Bot, target: Message, uid: int, source_name: str) -> None:
    status = await target.answer(ui.stage("clone", step(1, 3, "Reading the source pack...")))
    try:
        src = await tg(bot.get_sticker_set, name=source_name)
    except TelegramAPIError as exc:
        await safe_edit(status, ui.card("✖", "Can't read that pack", ("home", "clone"), f"Telegram said: {error_text(exc)}"),
                        reply_markup=ui.single_back_kb())
        return
    if not src.stickers:
        await safe_edit(status, ui.card("✖", "Nothing to clone", ("home", "clone"), "That pack has no stickers."),
                        reply_markup=ui.single_back_kb())
        return

    srcs = list(src.stickers)[:MAX_CLONE_STICKERS]
    kind = detect_source_kind(src)
    await safe_edit(status, ui.stage("clone", step(2, 3, f"Downloading {len(srcs)} sticker(s) from <b>{ui.e(src.title)}</b>...")))

    download_sem = asyncio.Semaphore(16)

    async def fetch(s: Any) -> Optional[QueuedSticker]:
        async with download_sem:
            try:
                data = await download_bytes(bot, s.file_id)
            except Exception as exc:
                logger.warning("Clone: could not download %s: %s", s.file_id, exc)
                return None
        return QueuedSticker(
            filename="sticker",
            data=data,
            fmt=source_format(s),
            emojis=[s.emoji] if getattr(s, "emoji", None) else [],
        )

    total_src = len(srcs)
    clone_started = time.monotonic()
    fetched = [0]
    last_ui = [0.0]

    async def tracked(s: Any) -> Optional[QueuedSticker]:
        result = await fetch(s)
        fetched[0] += 1
        now = time.monotonic()
        if fetched[0] == total_src or now - last_ui[0] > 1.0:
            last_ui[0] = now
            body = ui.progress_block(fetched[0], total_src, now - clone_started)
            await safe_edit(status, ui.stage("clone", step(2, 3, f"Downloading from <b>{ui.e(src.title)}</b>\n{body}")))
        return result

    items = [i for i in await asyncio.gather(*(tracked(s) for s in srcs)) if i]
    if not items:
        await safe_edit(status, ui.card("✖", "Download failed", ("home", "clone"), "No stickers could be downloaded."),
                        reply_markup=ui.single_back_kb())
        return

    first, rest = items[0], items[1:]
    try:
        name, full_title = await db_service.create_set(bot, uid, src.title, kind, first, first.emojis)
    except Exception as exc:
        await safe_edit(status, ui.card("✖", "Clone failed", ("home", "clone"), f"Telegram said: {error_text(exc)}"),
                        reply_markup=ui.single_back_kb())
        return

    if not rest:
        body = ui.ok("The copy is ready and is now your active pack.") + "\n\n" + ui.field(
            "❏", "Pack", f"<a href='https://t.me/addstickers/{name}'>{ui.e(full_title)}</a>")
        await safe_edit(status, ui.card("∞", "Pack cloned", ("home", "clone"), body), reply_markup=ui.done_kb(name))
        return

    # The copy is now the active pack, so the normal batch path appends the remaining stickers to it.
    await run_batch(bot, status, uid, rest, edit_start=True)


async def show_clone_prompt(msg: Message, state: FSMContext, bot: Bot, source_name: str) -> None:
    try:
        src = await tg(bot.get_sticker_set, name=source_name)
    except TelegramAPIError as exc:
        await msg.reply(ui.card("✖", "Can't open that pack", ("home", "clone"), f"Telegram said: {error_text(exc)}"))
        return
    await state.set_state(BotStages.clone_confirm)
    await state.update_data(clone_source=source_name)
    body = "\n".join([
        ui.field("❏", "Pack", ui.e(src.title)),
        ui.field("▣", "Stickers", str(len(src.stickers))),
        "",
        "I'll copy it into a new pack under your account so you can edit it. The original stays unchanged.",
    ])
    await msg.reply(ui.card("∞", "Clone this pack?", ("home", "clone"), body), reply_markup=ui.clone_confirm_kb())


@router.message(StateFilter(None), F.text.regexp(CLONE_LINK_RE))
async def on_clone_link(msg: Message, state: FSMContext, bot: Bot) -> None:
    match = CLONE_LINK_RE.search(msg.text or "")
    if match:
        await show_clone_prompt(msg, state, bot, match.group(1))


@router.message(StateFilter(None), F.sticker)
async def on_clone_sticker(msg: Message, state: FSMContext, bot: Bot) -> None:
    if not msg.sticker.set_name:
        await msg.reply(ui.warn("That sticker isn't part of a pack, so there's nothing to clone."))
        return
    await show_clone_prompt(msg, state, bot, msg.sticker.set_name)


@router.callback_query(BotStages.clone_confirm, F.data == "clone:go")
async def cb_clone_go(cb: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    source = (await state.get_data()).get("clone_source")
    await state.clear()
    if not source:
        await cb.answer("Session expired. Send the link again.", show_alert=True)
        return
    await cb.answer("Cloning")
    await run_clone(bot, cb.message, cb.from_user.id, source)


# ---------------------------------------------------------------------------
# Errors & entrypoint
# ---------------------------------------------------------------------------
@router.errors()
async def on_error(event: ErrorEvent) -> bool:
    logger.exception("Unhandled error: %s", event.exception)
    return True


async def health(_: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "mode": "webhook" if WEBHOOK_URL else "polling"})


async def main() -> None:
    global IMG_POOL
    if not BOT_TOKEN or BOT_TOKEN.startswith("PASTE_"):
        logger.error("BOT_TOKEN is not set. Add it as an environment variable or in the config block.")
        sys.exit(1)

    IMG_POOL = ProcessPoolExecutor(max_workers=IMAGE_WORKERS, mp_context=mp.get_context("spawn"))
    await db_call(db.ensure_indexes)

    if REDIS_URL:
        from aiogram.fsm.storage.redis import RedisStorage
        storage = RedisStorage.from_url(REDIS_URL)
    else:
        storage = MemoryStorage()

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=storage)
    dp.include_router(router)
    sweeper = asyncio.create_task(sweep_state())

    # The HTTP server always runs: Render needs a listening port, and /health lets it check the service.
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)

    polling_task: Optional[asyncio.Task] = None
    if WEBHOOK_URL:
        SimpleRequestHandler(dispatcher=dp, bot=bot, secret_token=WEBHOOK_SECRET or None).register(app, path=WEBHOOK_PATH)
        setup_application(app, dp, bot=bot)
        await bot.set_webhook(
            url=f"{WEBHOOK_URL}{WEBHOOK_PATH}",
            secret_token=WEBHOOK_SECRET or None,
            allowed_updates=dp.resolve_used_update_types(),
            drop_pending_updates=False,
        )
        logger.info("Webhook mode: %s%s", WEBHOOK_URL, WEBHOOK_PATH)
    else:
        await bot.delete_webhook(drop_pending_updates=False)
        polling_task = asyncio.create_task(dp.start_polling(bot, handle_signals=False))
        logger.info("Polling mode")

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, WEB_HOST, WEB_PORT).start()
    logger.info("HTTP server on %s:%s", WEB_HOST, WEB_PORT)

    try:
        if polling_task:
            await polling_task
        else:
            await asyncio.Event().wait()
    finally:
        sweeper.cancel()
        if polling_task:
            polling_task.cancel()
        await runner.cleanup()
        IMG_POOL.shutdown(wait=False, cancel_futures=True)
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
