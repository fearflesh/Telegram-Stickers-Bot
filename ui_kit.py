"""
Sticker Studio UI kit: colour roles, buttons, keyboards and message layout.

Button colour roles (sent as the Telegram button `style`; clients that don't support
colours show the default grey button, and the icons still carry the meaning):

  PRIMARY  blue   - the main action on a screen (one per screen where possible)
  SUCCESS  green  - confirm, finish, go, or the currently selected option
  DANGER   red    - destructive or irreversible actions
  NAV      grey   - navigation, back, cancel, secondary options

Every stage has a back button to its parent, and every message shows a breadcrumb
trail so users always know where they are, e.g. "🏠 Home › ✏️ Editor › 🔄 Replace".
"""

import html
from typing import Optional, Sequence, Tuple

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

DIVIDER = "━━━━━━━━━━━━━━━━━━━━━━"


class Role:
    PRIMARY = "primary"
    SUCCESS = "success"
    DANGER = "danger"
    NAV = None


class Nav:
    """Screen navigation callbacks. Every back button points at one of these."""
    HOME = "nav:home"
    CREATE = "nav:create"
    EDIT = "nav:edit"
    ADD = "nav:add"
    PACKS = "nav:packs"
    SETTINGS = "nav:settings"


BACK_NAMES = {
    Nav.HOME: "Home",
    Nav.CREATE: "Create",
    Nav.EDIT: "Editor",
    Nav.ADD: "Add menu",
    Nav.PACKS: "My packs",
    Nav.SETTINGS: "Settings",
}

CRUMBS = {
    "home": "🏠 Home",
    "create": "➕ Create",
    "edit": "✏️ Editor",
    "add": "📥 Add",
    "bulk": "📚 Bulk queue",
    "packs": "📦 My packs",
    "settings": "🛠 Settings",
    "clone": "🔗 Clone",
    "emoji": "😀 Emoji & mask",
    "replace": "🔄 Replace",
    "order": "🔢 Reorder",
    "delete": "🗑 Delete",
    "icon": "🖼 Pack icon",
    "rename": "🏷 Rename",
}

# action key -> (icon, title, description, breadcrumb trail)
ACTIONS = {
    "new_static": ("🖼", "Static sticker set", "Standard image pack with up to 120 stickers. Photos and files are converted to 512 px PNG or WEBP automatically.", ("home", "create")),
    "new_emoji": ("😀", "Custom emoji pack", "Up to 200 custom emoji. Images are centred and scaled to 100×100 px.", ("home", "create")),
    "new_video": ("🎬", "Video sticker set", "WEBM video stickers: VP9, up to 3 seconds, 30 FPS or less, 256 KB, 512 px longest side, no audio.", ("home", "create")),
    "new_animated": ("✨", "Animated sticker set", "Vector TGS stickers: 512×512 px, 60 FPS or less, up to 64 KB.", ("home", "create")),
    "new_masks": ("🎭", "Face-mask set", "Masks for Telegram's photo editor. Images are padded to 512×512 px.", ("home", "create")),
    "add_sticker": ("📥", "Add stickers", "Send a photo, image, album, or archive to add it to your active pack.", ("home", "edit", "add")),
    "bulk": ("📚", "Bulk queue", "Collect several photos, albums, images, and archives, then process them together.", ("home", "edit", "add")),
    "clone": ("🔗", "Clone a pack", "Copies a pack into a new pack under your account. The original is unchanged, and the copy becomes your active pack.", ("home", "clone")),
    "edit_sticker": ("😀", "Emoji & mask", "Change a sticker's emoji list, or its face-mask anchor point.", ("home", "edit", "emoji")),
    "replace_sticker": ("🔄", "Replace sticker", "Swap an existing sticker for a new image while keeping its position.", ("home", "edit", "replace")),
    "order_sticker": ("🔢", "Reorder sticker", "Move a sticker to a new position. Position 1 is first.", ("home", "edit", "order")),
    "del_sticker": ("🗑", "Delete sticker", "Permanently remove one sticker from the active pack.", ("home", "edit", "delete")),
    "set_pack_icon": ("🖼", "Pack icon", "Upload any picture. It is resized to 100×100 px and used as the pack's tray icon.", ("home", "edit", "icon")),
    "rename_pack": ("🏷", "Rename pack", "Change the display title. The share link stays the same.", ("home", "edit", "rename")),
    "del_pack": ("💥", "Delete pack", "Permanently deletes the active pack from Telegram. This cannot be undone.", ("home", "edit", "delete")),
}

PACK_BADGES = {
    "static": "🖼 Static",
    "emoji": "😀 Custom emoji",
    "video": "🎬 Video",
    "animated": "✨ Animated",
    "masks": "🎭 Face masks",
}

EMOJI_MODE_OPTIONS = [("ask", "💬 Ask each time"), ("random", "🎲 Always random"), ("manual", "✍️ Always custom")]
FRAME_OPTIONS = [("fit", "📐 Aspect fit"), ("pad", "⬛ 512×512 square pad"), ("rounded", "🟢 Rounded corners")]
EMOJI_MODE_LABELS = dict(EMOJI_MODE_OPTIONS)
FRAME_LABELS = dict(FRAME_OPTIONS)

TYPE_OPTIONS = [
    ("emoji", "😀 Custom emoji pack (100×100)"),
    ("static", "🖼 Static sticker set (PNG / WEBP)"),
    ("video", "🎬 Video sticker set (WEBM)"),
    ("animated", "✨ Animated sticker set (TGS)"),
    ("masks", "🎭 Face-mask set"),
]
QUICK_EMOJIS = ("🔥", "😂", "❤️", "😎", "👍")
MASK_POINTS = (("forehead", "🎭 Forehead"), ("eyes", "👀 Eyes"), ("mouth", "👄 Mouth"), ("chin", "🧔 Chin"))


# ---------------------------------------------------------------------------
# Message layout
# ---------------------------------------------------------------------------
def e(value: object) -> str:
    return html.escape(str(value))


def crumb(*keys: str) -> str:
    return " › ".join(CRUMBS[k] for k in keys)


def card(icon: str, title: str, trail: Sequence[str], body: str, hint: Optional[str] = None) -> str:
    """Standard message: icon + title, breadcrumb, divider, body, optional hint."""
    lines = [f"{icon} <b>{title}</b>", f"<i>{crumb(*trail)}</i>", DIVIDER, body]
    if hint:
        lines += ["", f"💡 <i>{hint}</i>"]
    return "\n".join(lines)


def stage(action: str, body: str = "") -> str:
    """Card for a specific action, using its icon, title, description and trail."""
    icon, title, desc, trail = ACTIONS[action]
    parts = [desc] + ([body] if body else [])
    return card(icon, title, trail, "\n\n".join(parts))


def step(n: int, total: int, text: str) -> str:
    return f"🧭 <b>Step {n} of {total}</b>\n{text}"


def ok(text: str) -> str:
    return f"✅ {text}"


def warn(text: str) -> str:
    return f"⚠️ {text}"


def fail(text: str) -> str:
    return f"❌ {text}"


def info(text: str) -> str:
    return f"ℹ️ {text}"


def field(icon: str, label: str, value: str) -> str:
    return f"{icon} <b>{label}:</b> {value}"


def progress_bar(done: int, total: int, width: int = 10) -> str:
    pct = round(100 * done / total) if total else 0
    filled = round(width * done / total) if total else 0
    return f"[{'█' * filled}{'░' * (width - filled)}] {pct}% ({done}/{total})"


def on_off(flag: object) -> str:
    return "ON ✅" if flag else "OFF ⭕"


# ---------------------------------------------------------------------------
# Buttons & keyboards
# ---------------------------------------------------------------------------
def btn(
    text: str,
    cb: Optional[str] = None,
    *,
    url: Optional[str] = None,
    role: Optional[str] = Role.NAV,
) -> InlineKeyboardButton:
    kwargs = {"text": text, "callback_data": cb, "url": url}
    if role:
        try:
            return InlineKeyboardButton(**kwargs, style=role)
        except (TypeError, ValueError):  # aiogram version without button colours
            pass
    return InlineKeyboardButton(**kwargs)


def markup(*rows: Optional[Sequence[Optional[InlineKeyboardButton]]]) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for row in rows:
        if not row:
            continue
        buttons = [x for x in row if x is not None]
        if buttons:
            b.row(*buttons)
    return b.as_markup()


def back(to: str) -> InlineKeyboardButton:
    return btn(f"⬅️ Back to {BACK_NAMES[to]}", to)


def cancel() -> InlineKeyboardButton:
    return btn("✖️ Cancel", Nav.HOME)


def home_kb(open_url: Optional[str]) -> InlineKeyboardMarkup:
    return markup(
        [btn("➕ Create pack", Nav.CREATE, role=Role.PRIMARY), btn("✏️ Edit active pack", Nav.EDIT, role=Role.PRIMARY)],
        [btn("📥 Add stickers", Nav.ADD, role=Role.SUCCESS)],
        [btn("📦 My packs", Nav.PACKS), btn("🛠 Settings", Nav.SETTINGS)],
        [btn("🔗 Open active pack", url=open_url)] if open_url else None,
    )


def create_kb() -> InlineKeyboardMarkup:
    return markup(
        *[[btn(label, f"create_type:{key}", role=Role.PRIMARY)] for key, label in TYPE_OPTIONS],
        [back(Nav.HOME)],
    )


def operation_kb(back_to: str = Nav.EDIT) -> InlineKeyboardMarkup:
    """Used on every step of a multi-step flow: back to its parent, or cancel."""
    return markup([back(back_to)], [cancel()])


def result_kb() -> InlineKeyboardMarkup:
    return markup(
        [btn("✏️ Back to Editor", Nav.EDIT, role=Role.PRIMARY)],
        [btn("🏠 Home", Nav.HOME)],
    )


def done_kb(pack_name: str) -> InlineKeyboardMarkup:
    return markup(
        [btn("🎁 Open pack", url=f"https://t.me/addstickers/{pack_name}")] if pack_name else None,
        [btn("✏️ Edit pack", Nav.EDIT, role=Role.PRIMARY), btn("🏠 Home", Nav.HOME)],
    )


def edit_kb(open_url: Optional[str]) -> InlineKeyboardMarkup:
    return markup(
        [btn("📥 Add stickers", Nav.ADD, role=Role.SUCCESS)],
        [btn("😀 Emoji & mask", "act:edit_sticker", role=Role.PRIMARY), btn("🔄 Replace", "act:replace_sticker", role=Role.PRIMARY)],
        [btn("🔢 Reorder", "act:order_sticker", role=Role.PRIMARY), btn("🖼 Pack icon", "act:set_pack_icon", role=Role.PRIMARY)],
        [btn("🏷 Rename pack", "act:rename_pack", role=Role.PRIMARY)],
        [btn("🗑 Delete sticker", "act:del_sticker", role=Role.DANGER), btn("💥 Delete pack", "act:del_pack", role=Role.DANGER)],
        [btn("🔗 Open in Telegram", url=open_url)] if open_url else None,
        [btn("📦 Switch pack", Nav.PACKS), back(Nav.HOME)],
    )


def add_kb() -> InlineKeyboardMarkup:
    return markup(
        [btn("📚 Open bulk queue", "act:bulk", role=Role.PRIMARY)],
        [back(Nav.EDIT)],
    )


def emoji_input_kb() -> InlineKeyboardMarkup:
    return markup(
        [btn("🎲 Random emoji", "edit_st:random", role=Role.PRIMARY)],
        [btn(label, f"edit_st:mask:{key}") for key, label in MASK_POINTS],
        [back(Nav.EDIT)],
        [cancel()],
    )


def packs_kb(rows: Sequence[Tuple[str, str, str, str]]) -> InlineKeyboardMarkup:
    """rows: (label, select_callback, edit_callback, open_url)"""
    return markup(
        *[[btn(label, sel), btn("✏️ Edit", edit_cb, role=Role.PRIMARY), btn("🔗", url=url)] for label, sel, edit_cb, url in rows],
        [btn("➕ New pack", Nav.CREATE, role=Role.SUCCESS)],
        [back(Nav.HOME)],
    )


def settings_kb(emoji_mode: str, frame_mode: str, border: bool, crop: bool) -> InlineKeyboardMarkup:
    emoji_rows = [
        [btn(("✅ " if emoji_mode == key else "") + label, f"pref:emoji_mode:{key}",
             role=Role.SUCCESS if emoji_mode == key else Role.NAV)]
        for key, label in EMOJI_MODE_OPTIONS
    ]
    frame_rows = [
        [btn(("✅ " if frame_mode == key else "") + label, f"pref:frame_mode:{key}",
             role=Role.SUCCESS if frame_mode == key else Role.NAV)]
        for key, label in FRAME_OPTIONS
    ]
    return markup(
        *emoji_rows,
        *frame_rows,
        [btn(f"✨ Sticker outline · {on_off(border)}", "pref:auto_border:toggle",
             role=Role.SUCCESS if border else Role.NAV)],
        [btn(f"✂️ Auto-crop non-square photos · {on_off(crop)}", "pref:auto_crop:toggle",
             role=Role.SUCCESS if crop else Role.NAV)],
        [back(Nav.HOME)],
    )


def bulk_kb(count: int) -> InlineKeyboardMarkup:
    rows = []
    if count:
        rows = [
            [btn(f"🎲 Process {count} with random emoji", "bulk:random", role=Role.SUCCESS)],
            [btn(f"✍️ Process {count} with custom emoji", "bulk:custom", role=Role.PRIMARY)],
            [btn("🧹 Clear queue", "bulk:clear", role=Role.DANGER)],
        ]
    return markup(*rows, [back(Nav.ADD)], [cancel()])


def single_emoji_kb() -> InlineKeyboardMarkup:
    return markup(
        [btn("🎲 Random emoji", "s_em:random", role=Role.PRIMARY), btn("✍️ Custom emoji", "s_em:custom", role=Role.PRIMARY)],
        [btn(emo, f"s_em:pick:{emo}") for emo in QUICK_EMOJIS],
        [back(Nav.HOME)],
    )


def single_back_kb() -> InlineKeyboardMarkup:
    return markup([back(Nav.HOME)])


def clone_confirm_kb() -> InlineKeyboardMarkup:
    return markup(
        [btn("✅ Clone & edit", "clone:go", role=Role.SUCCESS)],
        [back(Nav.HOME)],
    )


def delete_confirm_kb() -> InlineKeyboardMarkup:
    return markup(
        [btn("⚠️ Delete permanently", "confirm_del_pack", role=Role.DANGER)],
        [back(Nav.EDIT)],
    )
