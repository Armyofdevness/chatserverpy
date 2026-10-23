"""
Argensoft Comic Chat — Flask-SocketIO backend
Run:  python server.py
      (or: flask --app server run --debug)

Dependencies:
    pip install flask flask-socketio eventlet
    (eventlet is optional: without it the server falls back to plain threads)

Environment:
    SECRET_KEY    Flask secret (change it in production)
    FRONTEND_URL  Allowed CORS origin of the deployed front-end
    BOT_SECRET    Optional. A client that sends this value as "botSecret" in its
                  "join" is treated as a trusted bot: exempt from rate limits and
                  allowed to use the reserved bot nicks. Unset = no such clients.
"""

try:
    import eventlet
    eventlet.monkey_patch()      # must run before the other imports below
    ASYNC_MODE = "eventlet"
except ImportError:              # eventlet not installed → plain threads
    ASYNC_MODE = "threading"

import collections
import hmac
import json
import os
import re
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

from flask import Flask, send_from_directory
from flask_socketio import SocketIO, emit, join_room, leave_room

# ── Config ────────────────────────────────────────────────────────────────────
HISTORY_FILE  = Path("history.json")
MAX_HISTORY   = 100          # rolling window size
HOST          = "0.0.0.0"
PORT          = 5000
SECRET_KEY    = os.environ.get("SECRET_KEY", "change-me-in-production")

# Avatar ids / display nicks of the built-in bots (BOT_CAST in data.js).
# They are used for exactly two things:
#   • panel routing — messages from these avatars/nicks are always Solo panels
#   • reserved nicks — a normal user cannot take these names
# They grant NO privileges. NB: "chatbot" is also a normal, user-selectable
# avatar, so it must never mean "trusted". Rate-limit exemption is reserved for
# sessions that authenticated with BOT_SECRET (see below).
BOT_AVATAR_IDS = frozenset({"cyberbot", "chatbot"})
BOT_NICKS      = frozenset({"chatbot", "cyberbot"})   # case-insensitive

# Shared secret for trusted bot clients (empty = feature off).
BOT_SECRET = os.environ.get("BOT_SECRET", "")

# Seconds within which a human 'say' from a DIFFERENT nick upgrades the
# in-progress panel (Solo→Duo, Duo→Trio) instead of starting a new Solo.
PANEL_UPGRADE_WINDOW_SECS = 3.0

# ── Rate-limiting — sliding window per sid ────────────────────────────────────
#
# Two independent limiters share the same sliding-window helper:
#
#   chat_message limiter
#     RATE_MSG_LIMIT   — max messages allowed in the window
#     RATE_MSG_WINDOW  — window duration in seconds
#     RATE_MSG_WARN    — warn the sender (private system_msg) when this
#                        many messages are sent inside the window; must be
#                        ≤ RATE_MSG_LIMIT.  Set equal to RATE_MSG_LIMIT to
#                        disable the warning (only the hard drop fires).
#
#   room_command limiter  (visual effects, /bsod, /duel …)
#     RATE_CMD_LIMIT / RATE_CMD_WINDOW / RATE_CMD_WARN
#     Commands are cheaper than chat (no history write, no panel upgrade),
#     so they get a slightly more generous limit.
#
# Only sessions that authenticated as a bot (BOT_SECRET) are exempt.
#
#   typing limiter
#     Typing indicators are broadcast to the room on every keystroke burst,
#     so they get their own generous per-sid cap (excess is dropped silently).
#
RATE_MSG_LIMIT  = 10    # hard cap: drop the message silently above this
RATE_MSG_WARN   = 7     # soft cap: send a private warning above this
RATE_MSG_WINDOW = 5.0   # sliding window in seconds

RATE_CMD_LIMIT  = 15    # hard cap for room_command events
RATE_CMD_WARN   = 10    # soft cap for room_command events
RATE_CMD_WINDOW = 5.0   # sliding window in seconds

RATE_TYPING_LIMIT  = 60   # typing events per window
RATE_TYPING_WINDOW = 5.0

# ── Image / GIF prop (actionType "image") ────────────────────────────────────
# Mirrored for friendly client-side errors in data.js (IMAGE_HOST_ALLOWLIST).
# EXACT hostnames only — never substring / wildcard matching.
IMAGE_HOST_ALLOWLIST = frozenset({
    # Giphy
    "media.giphy.com", "media0.giphy.com", "media1.giphy.com", "media2.giphy.com",
    "media3.giphy.com", "media4.giphy.com", "i.giphy.com",
    # Tenor (existing direct links only). media1.tenor.com is deliberately absent:
    # those URLs return an HTML page, not an image.
    "media.tenor.com", "c.tenor.com",
})
# NOT ENABLED — each needs the verification checklist from the feature handoff
# first (terms allow hotlinking, plain <img> loads cross-origin, https only, stable
# URLs, exact hostnames). Kept here only as a reminder; nothing reads this set.
IMAGE_HOSTS_PENDING = frozenset({
    # "klipy …"               — direct-media hostname NOT confirmed
    # "upload.wikimedia.org"  — check Wikimedia's current hotlinking policy first
})
IMAGE_EXTENSIONS    = (".gif", ".webp", ".png", ".jpg", ".jpeg")
IMAGE_URL_MAX_LEN   = 512

IMG_POST_LIMIT       = 1      # posts per user …
IMG_POST_WINDOW      = 30.0   # … per this many seconds
IMG_MIN_DISPLAY_SECS = 20     # nobody else may replace an image for this long

# Frame props (actionType "frame") — pictures hung IN the room art. Several per room,
# each independent. {room name: its frame prop ids}; keep in sync with data.js.
# They share the validator, the replace lock length and the per-user post limiter
# (_rate_img) with the overlay prop above, so nobody gets extra posts from having frames.
IMAGE_FRAME_PROPS = {
    "RainyCafe": frozenset({"prop-rc-3", "prop-rc-4"}),
}

# Per-sid deques: { sid: deque([timestamp, ...]) }
# Written / read only inside the rate-limit helper (protected by GIL for CPython;
# a dedicated lock is added for safety under eventlet green-threads).
_rate_msg: dict[str, collections.deque] = {}
_rate_cmd: dict[str, collections.deque] = {}
_rate_typing: dict[str, collections.deque] = {}
_rate_img: dict[str, collections.deque] = {}
_rate_lock = threading.Lock()

# ── App & Socket init ─────────────────────────────────────────────────────────
app = Flask(__name__, static_folder=".", static_url_path="")
app.config["SECRET_KEY"] = SECRET_KEY

# async_mode="eventlet" gives full WebSocket support; ASYNC_MODE falls back to
# "threading" (see the imports at the top) when eventlet isn't installed.

# Replace with your actual Netlify app URL once you have it
FRONTEND_URL = os.environ.get("FRONTEND_URL", "https://argensoftchat.netlify.app")

socketio = SocketIO(
    app,
    async_mode=ASYNC_MODE,
    cors_allowed_origins=[
        FRONTEND_URL,
        "http://localhost:5000", "http://127.0.0.1:5000",
        # local dev: pages served by VS Code Live Server / python -m http.server
        "http://localhost:5500", "http://127.0.0.1:5500",
        "http://localhost:8000", "http://127.0.0.1:8000",
        "http://localhost:8080", "http://127.0.0.1:8080",
    ],
    ping_timeout=60,
    ping_interval=25,
    logger=False,
    engineio_logger=False,
)

# ── Thread lock for history file I/O ─────────────────────────────────────────
_history_lock = threading.Lock()

# ── Online users: {sid: {nick, avatarId, room}} ───────────────────────────────
online_users: dict[str, dict] = {}
# Guards the check-then-register in on_join (nick uniqueness) under real threads.
_users_lock = threading.Lock()


# ─────────────────────────────────────────────────────────────────────────────
# Retroactive Panel Upgrade — tracks the last in-progress human panel per room
#
# Structure per room:
#   _last_human_panel[room] = {
#       "msgId":     str,        # ID of the ORIGINAL Solo — used as DOM key forever
#       "nicks":     list[str],  # ordered list of speaker nicks already in panel
#       "speakers":  list[dict], # _to_speaker() projections, same order as nicks
#       "timestamp": datetime,   # UTC time of the most recent addition
#   }
#
# Upgrade rules (evaluated on every new human 'say'):
#   Panel has 1 speaker  → upgrade to Duo   (emit upgrade_panel, keep same msgId)
#   Panel has 2 speakers → upgrade to Trio  (emit upgrade_panel, keep same msgId)
#   Panel has 3 speakers → start fresh Solo (new msgId)
#   New speaker is already IN the panel → start fresh Solo
#   Window expired                       → start fresh Solo
#
# Bot Solos never write to this dict.
# ─────────────────────────────────────────────────────────────────────────────
_last_human_panel: dict[str, dict] = {}
_panel_lock = threading.Lock()


# ─────────────────────────────────────────────────────────────────────────────
# Rate-limit helper
# ─────────────────────────────────────────────────────────────────────────────

def _check_rate_limit(
    sid: str,
    store: dict,
    limit: int,
    warn_at: int,
    window: float,
) -> str:
    """Sliding-window rate limiter.

    Maintains a deque of UTC timestamps (as floats) for each *sid* in *store*.
    On every call:
      1. Prune timestamps older than *window* seconds.
      2. Append now.
      3. Return a verdict string:

           "ok"    — count ≤ warn_at   → allow, no warning
           "warn"  — warn_at < count ≤ limit → allow, send a private warning
           "block" — count > limit     → drop the event entirely

    Thread-safe under both threading and eventlet via _rate_lock.

    Args:
        sid     — Socket.IO session id of the sender.
        store   — one of _rate_msg or _rate_cmd.
        limit   — hard maximum messages allowed in the window.
        warn_at — threshold above which a warning is sent but the message
                  is still delivered.
        window  — sliding window duration in seconds.

    Returns:
        "ok" | "warn" | "block"
    """
    now = datetime.now(timezone.utc).timestamp()

    with _rate_lock:
        if sid not in store:
            store[sid] = collections.deque()

        dq = store[sid]

        # Prune expired timestamps from the left (oldest)
        while dq and (now - dq[0]) > window:
            dq.popleft()

        # Count BEFORE appending so the current message is included in the
        # decision (appending first would shift the threshold by one).
        count = len(dq)

        if count >= limit:
            # Hard block — don't even record this attempt (avoids deque growth
            # under a flood; the window will naturally expire the old entries).
            return "block"

        # Record this message
        dq.append(now)
        count += 1   # now reflects the message we just admitted

        if count > warn_at:
            return "warn"
        return "ok"

# ─────────────────────────────────────────────────────────────────────────────
# Active event tracker — {room: initiator_nick}
#
# Populated when any user starts Event Mode; cleared when the initiator ends it.
# Used so on_event_mode can emit global_event_mode to the whole room (including
# the sender) and so late-joining clients could eventually be informed.
# ─────────────────────────────────────────────────────────────────────────────
_active_events: dict[str, str] = {}

# ─────────────────────────────────────────────────────────────────────────────
# Image prop — {room: {"by": nick, "ts": epoch seconds}} of the latest post.
# Drives the replace lock: a DIFFERENT user cannot replace an image within
# IMG_MIN_DISPLAY_SECS of it being posted. Clearing is never blocked.
# ─────────────────────────────────────────────────────────────────────────────
_room_images: dict[str, dict] = {}
# Frame props — {room: {prop_id: {"by": nick, "ts": epoch seconds}}}: the same replace
# lock, but per frame (two frames in a room never block each other).
_room_frames: dict[str, dict[str, dict]] = {}
_image_lock = threading.Lock()


def _validate_image_url(url) -> "str | None":
    """Return the NORMALISED image URL if it is acceptable, else None.

    Rules: https only; hostname must be IN the allowlist exactly; no user-info;
    port absent or 443; ASCII only, no whitespace / control characters / backslashes;
    ≤ 512 chars; a real path ending in an image extension. Query strings are fine
    (shared Giphy links carry them). This rejects "media.giphy.com.evil.net",
    "media.giphy.com@evil.net", page links (no image extension) and video files.

    What is returned is REBUILT from the validated parts, so the string that is
    relayed to the clients is exactly the one that was checked (no parser
    differences between Python and the browsers can smuggle another host through).
    """
    if not isinstance(url, str):
        return None
    url = url.strip()
    if not url or len(url) > IMAGE_URL_MAX_LEN or not url.isascii():
        return None
    if any(ch.isspace() or ord(ch) < 32 or ord(ch) == 127 for ch in url) or "\\" in url:
        return None
    try:
        parts = urlsplit(url)
        port  = parts.port
    except ValueError:
        return None
    if parts.scheme != "https" or "@" in parts.netloc:
        return None
    if parts.username is not None or parts.password is not None:
        return None
    if port not in (None, 443):
        return None
    host = parts.hostname                    # lower-cased by urlsplit
    if host not in IMAGE_HOST_ALLOWLIST:
        return None
    if not parts.path or parts.path == "/" or not parts.path.lower().endswith(IMAGE_EXTENSIONS):
        return None
    return f"https://{host}{parts.path}" + (f"?{parts.query}" if parts.query else "")


def _is_allowed_image_url(url) -> bool:
    return _validate_image_url(url) is not None
_event_lock = threading.Lock()

# ─────────────────────────────────────────────────────────────────────────────
# Active Pasacalle banner tracker — {room: banner_text}
#
# Populated when a user posts a /pasacalle banner; cleared when the text is
# empty (bare /pasacalle takes it down). Sent to late joiners in on_join so
# a room's active banner survives across page loads and new connections —
# same shape as _active_events above, just for a per-room decoration rather
# than a room-wide mode.
# ─────────────────────────────────────────────────────────────────────────────
_active_pasacalle: dict[str, str] = {}
_pasacalle_lock = threading.Lock()


def _try_panel_upgrade(msg: dict) -> bool:
    """
    Attempt to upgrade the most recent in-progress human panel in this room
    by adding *msg* as the next speaker.

    Returns True  → upgrade_panel emitted; caller must NOT emit a new Solo.
    Returns False → no upgrade; caller should emit a fresh Solo and call
                    _record_panel() to start a new in-progress entry.

    Upgrade fires when ALL hold:
      • An in-progress panel exists for this room.
      • The new speaker is NOT already in that panel.
      • The panel has fewer than 3 speakers (max = Trio).
      • The message arrived within PANEL_UPGRADE_WINDOW_SECS of the last
        speaker added to the in-progress panel.
    """
    room = msg["room"]
    nick = msg["nick"]
    now  = datetime.now(timezone.utc)

    with _panel_lock:
        prev = _last_human_panel.get(room)

        if prev is None:
            return False

        already_in  = nick in prev["nicks"]
        panel_full  = len(prev["nicks"]) >= 3
        age_secs    = (now - prev["timestamp"]).total_seconds()
        expired     = age_secs > PANEL_UPGRADE_WINDOW_SECS

        if already_in or panel_full or expired:
            # Can't upgrade → clear the tracker so a new Solo can start fresh
            del _last_human_panel[room]
            return False

        # Build the upgraded speaker list
        new_speakers = prev["speakers"] + [_to_speaker(msg)]
        new_nicks    = prev["nicks"]    + [nick]

        # Determine layout name for the client
        layout = "trio" if len(new_speakers) == 3 else "duo"

        # Update tracker in-place (same msgId preserved — this is the key)
        prev["nicks"]     = new_nicks
        prev["speakers"]  = new_speakers
        prev["timestamp"] = now

        # If we just filled the panel to a Trio, remove it from the tracker
        # so the NEXT message always starts fresh.
        if len(new_nicks) >= 3:
            del _last_human_panel[room]

    socketio.emit(
        "upgrade_panel",
        {
            "targetMsgId": prev["msgId"],   # always the original Solo's id
            "speakers":    new_speakers,     # complete ordered list
            "layout":      layout,
            "room":        room,
        },
        to=room,
    )
    return True


def _record_panel(msg: dict) -> None:
    """Store a freshly emitted Solo as the start of a new in-progress panel."""
    room = msg["room"]
    with _panel_lock:
        _last_human_panel[room] = {
            "msgId":     msg["msgId"],
            "nicks":     [msg["nick"]],
            "speakers":  [_to_speaker(msg)],
            "timestamp": datetime.now(timezone.utc),
        }


def _emit_panel(layout: str, msgs: list, room: str) -> None:
    """Emit a render_panel event to all clients in *room*.

    For Solo panels the top-level ``msgId`` field is set to msgs[0]'s id.
    This is the authoritative ID the client stamps onto panel.id and uses
    to match upgrade_panel's targetMsgId.  Duo/Trio panels carry no msgId
    because they are produced by upgrade_panel, not render_panel.
    """
    payload = {
        "layout":   layout,
        "speakers": [_to_speaker(m) for m in msgs],
        "room":     room,
    }
    if layout == "solo" and msgs:
        payload["msgId"] = msgs[0].get("msgId", "")
    socketio.emit("render_panel", payload, to=room)


def _to_speaker(msg: dict) -> dict:
    """Project a full message dict down to the fields the client panel renderer needs.

    msgId is included so the client's _toSpeaker() can copy it onto the speaker
    descriptor, which then flows into appendPanel() and becomes panel.id.

    All visual flags — original (bsod, negative, focus) and the 20 new v7 ones —
    are forwarded so every client's appendPanel() applies effects consistently,
    including clients that join mid-session.
    """
    return {
        "who":            msg["nick"],
        "avatarId":       msg.get("avatarId", "kid"),
        "text":           msg["text"],
        "emotion":        msg.get("emotion", "normal"),
        "type":           msg.get("type", "say"),
        "msgId":          msg.get("msgId", ""),
        # original flags
        "bsod":           bool(msg.get("bsod",           False)),
        "negative":       bool(msg.get("negative",       False)),
        "focus":          bool(msg.get("focus",          False)),
        # new v7 flags
        "vhs":            bool(msg.get("vhs",            False)),
        "sepia":          bool(msg.get("sepia",          False)),
        "wingdings":      bool(msg.get("wingdings",      False)),
        "zoomFocus":      bool(msg.get("zoomFocus",      False)),
        "silhouette":     bool(msg.get("silhouette",     False)),
        "fileteado":      bool(msg.get("fileteado",      False)),
        "lowRes":         bool(msg.get("lowRes",         False)),
        "void":           bool(msg.get("void",           False)),
        "mirror":         bool(msg.get("mirror",         False)),
        "weight":         bool(msg.get("weight",         False)),
        "heavyRain":      bool(msg.get("heavyRain",      False)),
        "inkBleed":       bool(msg.get("inkBleed",       False)),
        "pageTear":       bool(msg.get("pageTear",       False)),
        "ghostWriter":    bool(msg.get("ghostWriter",    False)),
        "innerMonologue": bool(msg.get("innerMonologue", False)),
        "zoomDramatic":   bool(msg.get("zoomDramatic",   False)),
        "screensaver":    bool(msg.get("screensaver",    False)),
        "pixelSort":      bool(msg.get("pixelSort",      False)),
        "directX":        bool(msg.get("directX",        False)),
        "gutters":        bool(msg.get("gutters",        False)),
        "blindShadows":   bool(msg.get("blindShadows",   False)),
        "desktopHoles":   bool(msg.get("desktopHoles",   False)),
        "avatarMissing":  bool(msg.get("avatarMissing",  False)),
        "roughCut":       bool(msg.get("roughCut",       False)),
        "negativeSpace":  bool(msg.get("negativeSpace",  False)),
        "granizo":        bool(msg.get("granizo",        False)),
        "streetDog":      bool(msg.get("streetDog",      False)),
        "graveEntrance":  bool(msg.get("graveEntrance",  False)),
        "title":          str(msg.get("title",  "")) or None,
        "titleRarity":    str(msg.get("titleRarity", "common")),
    }


# ─────────────────────────────────────────────────────────────────────────────
# History helpers
# ─────────────────────────────────────────────────────────────────────────────

def _load_history() -> list:
    """Read history.json from disk; return [] on any error."""
    with _history_lock:
        try:
            return json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            return []


def _save_history(history: list) -> None:
    """Persist history list to disk (rolling, max MAX_HISTORY entries)."""
    with _history_lock:
        trimmed = history[-MAX_HISTORY:]
        HISTORY_FILE.write_text(
            json.dumps(trimmed, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def _append_message(msg: dict) -> None:
    """Append one message to the rolling history and flush to disk."""
    history = _load_history()
    history.append(msg)
    _save_history(history)


# ─────────────────────────────────────────────────────────────────────────────
# HTTP routes
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    """Serve the single-page Comic Chat frontend."""
    return send_from_directory(".", "index.html")


@app.route("/history")
def get_history():
    """REST endpoint — returns last MAX_HISTORY messages as JSON."""
    return json.dumps(_load_history()), 200, {"Content-Type": "application/json"}


# ─────────────────────────────────────────────────────────────────────────────
# Socket.IO event handlers
# ─────────────────────────────────────────────────────────────────────────────

@socketio.on("connect")
def on_connect():
    """Client connected — nothing to broadcast yet; wait for 'join'."""
    print(f"[connect] sid={_sid()}")


@socketio.on("disconnect")
def on_disconnect():
    """Remove user from online roster and notify the room."""
    sid = _sid()
    with _users_lock:
        user = online_users.pop(sid, None)

    # Always clean up rate-limit state for this sid, even if the user
    # was not fully joined (e.g. connected then disconnected immediately).
    with _rate_lock:
        _rate_msg.pop(sid, None)
        _rate_cmd.pop(sid, None)
        _rate_typing.pop(sid, None)
        _rate_img.pop(sid, None)

    room = user.get("room") if user else None
    if user and room:                      # room is None after an explicit "leave"
        leave_room(room)
        _release_event(room, user["nick"])
        _broadcast_user_list(room)
        emit(
            "system_msg",
            {"text": f"*** {user['nick']} has left #{room}"},
            to=room,
        )
        print(f"[disconnect] {user['nick']} left #{room}")


@socketio.on("join")
def on_join(data: dict):
    """
    Client sends:
        { nick, avatarId, room, botSecret? }

    Server responds with:
        - "nick_assigned" → only if the nick had to change (taken / reserved)
        - "history"       → last N messages for this room only
        - "user_list"     → current online roster
        - broadcasts "system_msg" to the room

    The nick registered here is the user's identity for the rest of the
    connection: every later event is attributed to it, whatever the payload says.
    """
    data        = data if isinstance(data, dict) else {}
    sid         = _sid()
    desired     = _clean_nick(data.get("nick"))
    avatar_id   = _clean_id(data.get("avatarId"), "kid")
    room        = _clean_room(data.get("room"))
    trusted_bot = _is_trusted_bot(data.get("botSecret"))

    # Check-then-register must be atomic, or two simultaneous joins could both
    # win the same nick.
    with _users_lock:
        prev      = online_users.get(sid)
        prev_room = prev.get("room") if prev else None
        prev_nick = prev["nick"] if prev else None
        nick      = _unique_nick(room, desired, sid, trusted_bot)
        online_users[sid] = {"nick": nick, "avatarId": avatar_id, "room": room, "bot": trusted_bot}

    # Moving straight from another room (a client that did not send "leave"):
    # leave it cleanly. After an explicit "leave", prev_room is None and the
    # announcement was already made there.
    if prev_room and prev_room != room:
        leave_room(prev_room)
        _release_event(prev_room, prev_nick)
        socketio.emit(
            "system_msg",
            {"text": f"*** {prev_nick} has left #{prev_room}"},
            to=prev_room,
        )
        _broadcast_user_list(prev_room)

    join_room(room)

    if nick != desired:
        # Tell the client the name it really has, so it can update its own state.
        emit("nick_assigned", {"nick": nick, "requested": desired})

    # Send only this room's history to the joining client
    all_history  = _load_history()
    room_history = [m for m in all_history if m.get("room") == room]
    emit("history", {"messages": room_history})

    # Send this room's active /pasacalle banner (empty string if none) so
    # late joiners — and clients switching rooms, which re-runs this same
    # handler — see the current banner (or a clean slate) immediately.
    with _pasacalle_lock:
        banner_text = _active_pasacalle.get(room, "")
    emit("room_banner", {"text": banner_text})

    # Broadcast updated user list to everyone in the new room
    _broadcast_user_list(room)

    # Announce arrival
    emit(
        "system_msg",
        {"text": f"*** {nick} has joined #{room}"},
        to=room,
    )
    print(f"[join] {nick} (avatar={avatar_id}) joined #{room}"
          + (f"  [asked for {desired!r}]" if nick != desired else "")
          + ("  [trusted bot]" if trusted_bot else ""))


@socketio.on("leave")
def on_leave(data=None):
    """
    Emitted by websocket-patch.js when the user switches rooms via switchRoom().
    The payload is ignored: the user leaves the room the SERVER has them in,
    and the announcement uses their registered nick.
    """
    sid  = _sid()
    user = online_users.get(sid)
    if not user or not user.get("room"):
        return
    room, nick = user["room"], user["nick"]

    leave_room(room)
    _release_event(room, nick)

    # Stay registered (a "join" for the new room follows immediately) but with
    # no room: the old room's roster no longer lists them, messages sent in the
    # gap are dropped, and on_join will not announce the departure a second time.
    user["room"] = None

    emit(
        "system_msg",
        {"text": f"*** {nick} has left #{room}"},
        to=room,
    )
    _broadcast_user_list(room)
    print(f"[leave] {nick} left #{room}")


@socketio.on("chat_message")
def on_chat_message(data: dict):
    """
    Client sends:
        {
          nick:      str,
          avatarId:  str,
          text:      str,
          emotion:   str,   // e.g. 'happy', 'sad', …
          type:      str,   // 'say' | 'action' | 'think'
          room:      str,
        }

    Server:
        1. Validates / sanitises the payload.
        2. Appends to rolling history.
        3. Routes through the Retroactive Panel Upgrade pipeline (Solo→Duo→Trio):

              Bot avatars (cyberbot / chatbot)
                → immediate Solo render_panel, NOT tracked for upgrades.

              'action' / 'think' types
                → immediate Solo render_panel, NOT tracked for upgrades.

              Human 'say' messages
                → emit immediate Solo render_panel (msgId stamped).
                → if a panel is in-progress with a DIFFERENT nick (or nicks)
                  and fewer than 3 speakers, emit upgrade_panel instead so
                  every client replaces the existing panel in-place (same msgId).
                → panel full (3 speakers) or same nick → start fresh Solo.
    """
    sid, user = _joined_user()
    if user is None or not isinstance(data, dict):
        return   # not in a room (never joined, or between leave and join)

    # WHO and WHERE come from the session registered at join — the payload's
    # nick / room are ignored, so nobody can speak as someone else or post into
    # a room they are not in. avatarId is the one cosmetic field the client may
    # change mid-session (Change Character), so it is accepted but validated.
    nick      = user["nick"]
    room      = user["room"]
    avatar_id = _clean_id(data.get("avatarId"), user["avatarId"])
    user["avatarId"] = avatar_id          # keep the roster / fusion lookup current
    text      = _sanitise(data.get("text", ""), max_len=500)
    emotion   = _sanitise(data.get("emotion", "normal"), max_len=32)
    msg_type  = data.get("type", "say")

    if not text:
        return   # ignore empty messages

    # ── Rate limiting ─────────────────────────────────────────────────────────
    # Only sessions that authenticated as a bot with BOT_SECRET are exempt. The
    # old check trusted the payload's avatarId / nick — and "chatbot" is also a
    # normal avatar — so anyone could skip the limit just by claiming to be one.
    # The check runs before any history write or panel logic so blocked messages
    # cost essentially nothing server-side.
    if not user.get("bot"):
        _rl = _check_rate_limit(sid, _rate_msg, RATE_MSG_LIMIT, RATE_MSG_WARN, RATE_MSG_WINDOW)
        if _rl == "block":
            # Silently drop — send a private throttle notice to the sender only.
            emit(
                "system_msg",
                {"text": (
                    f"⚡ Slow down! You're sending too many messages too fast. "
                    f"Limit: {RATE_MSG_LIMIT} messages per {int(RATE_MSG_WINDOW)}s."
                )},
            )
            print(f"[rate-limit/block] chat from sid={sid} nick={nick!r} dropped")
            return
        if _rl == "warn":
            # Deliver the message but warn the sender privately.
            emit(
                "system_msg",
                {"text": (
                    f"⚠ You're sending messages quickly "
                    f"({RATE_MSG_WARN}+ in {int(RATE_MSG_WINDOW)}s). "
                    f"Slow down to avoid being muted."
                )},
            )

    if msg_type not in ("say", "action", "think", "sfx"):
        msg_type = "say"

    # msgId: accept the client's hint (it already has the value they used locally)
    # but always guarantee a non-empty, server-sanitised id.  Using the client's
    # own id means render_panel echoes the exact same string the client generated,
    # so panel.id on the sender's screen matches what every other client will stamp.
    client_msg_id = _sanitise(data.get("msgId", ""), max_len=64)
    msg_id        = client_msg_id if client_msg_id else str(uuid.uuid4())

    msg = {
        "nick":      nick,
        "avatarId":  avatar_id,
        "text":      text,
        "emotion":   emotion,
        "type":      msg_type,
        "room":      room,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "msgId":     msg_id,
        # Visual effect flags — passed through from the client, validated as bool.
        # These are only trusted when the sender is a known human (not a bot).
        # original flags
        "bsod":           bool(data.get("bsod",           False)),
        "negative":       bool(data.get("negative",       False)),
        "focus":          bool(data.get("focus",          False)),
        # new v7 flags
        "vhs":            bool(data.get("vhs",            False)),
        "sepia":          bool(data.get("sepia",          False)),
        "wingdings":      bool(data.get("wingdings",      False)),
        "zoomFocus":      bool(data.get("zoomFocus",      False)),
        "silhouette":     bool(data.get("silhouette",     False)),
        "fileteado":      bool(data.get("fileteado",      False)),
        "lowRes":         bool(data.get("lowRes",         False)),
        "void":           bool(data.get("void",           False)),
        "mirror":         bool(data.get("mirror",         False)),
        "weight":         bool(data.get("weight",         False)),
        "heavyRain":      bool(data.get("heavyRain",      False)),
        "inkBleed":       bool(data.get("inkBleed",       False)),
        "pageTear":       bool(data.get("pageTear",       False)),
        "ghostWriter":    bool(data.get("ghostWriter",    False)),
        "innerMonologue": bool(data.get("innerMonologue", False)),
        "zoomDramatic":   bool(data.get("zoomDramatic",   False)),
        "screensaver":    bool(data.get("screensaver",    False)),
        "pixelSort":      bool(data.get("pixelSort",      False)),
        "directX":        bool(data.get("directX",        False)),
        "gutters":        bool(data.get("gutters",        False)),
        "blindShadows":   bool(data.get("blindShadows",   False)),
        "desktopHoles":   bool(data.get("desktopHoles",   False)),
        "avatarMissing":  bool(data.get("avatarMissing",  False)),
        "roughCut":       bool(data.get("roughCut",       False)),
        "negativeSpace":  bool(data.get("negativeSpace",  False)),
        "granizo":        bool(data.get("granizo",        False)),
        "streetDog":      bool(data.get("streetDog",      False)),
        "graveEntrance":  bool(data.get("graveEntrance",  False)),
        "title":          _sanitise(data.get("title", ""), max_len=64) or None,
        "titleRarity":    (str(data.get("titleRarity", "common")).lower()
                           if str(data.get("titleRarity", "common")).lower()
                           in ("common", "uncommon", "rare", "legendary") else "common"),
    }

    _append_message(msg)

    # ── IRC log broadcast (all clients, all types) ────────────────────────────
    emit("chat_message", msg, to=room)

    # ── Panel routing ─────────────────────────────────────────────────────────
    # A sender is a bot if EITHER their avatarId OR their display nick matches
    # a known bot identity.  The nick check catches cases where a bot message
    # is sent with a non-canonical avatarId (e.g. during Event Mode).
    is_bot       = bool(user.get("bot")) or (avatar_id.lower() in BOT_AVATAR_IDS) \
                   or (nick.lower() in BOT_NICKS)
    # sfx panels are always Solo — they should never merge with dialogue panels
    is_solo_type = msg_type in ("action", "think", "sfx")

    if is_bot or is_solo_type:
        # Bots and non-'say' types: immediate Solo, never tracked or upgraded.
        _emit_panel("solo", [msg], room)

    else:
        # Human 'say': attempt retroactive upgrade first.
        # If no upgrade fires, emit a fresh Solo and record it for future use.
        upgraded = _try_panel_upgrade(msg)
        if not upgraded:
            _emit_panel("solo", [msg], room)
            _record_panel(msg)

    print(f"[msg] {nick} (avatar={avatar_id}) in #{room}: {text[:60]}")


@socketio.on("typing")
def on_typing(data=None):
    """
    Lightweight ephemeral event — not stored in history.
    Client sends: { nick, room, isTyping }
    Forwarded to everyone else in the room.
    """
    sid, user = _joined_user()
    if user is None:
        return
    # Per-sid cap; excess is dropped silently (typing is purely cosmetic).
    if _check_rate_limit(sid, _rate_typing, RATE_TYPING_LIMIT, RATE_TYPING_LIMIT,
                         RATE_TYPING_WINDOW) == "block":
        return
    is_typing = bool(data.get("isTyping")) if isinstance(data, dict) else False
    # Rebuilt from the session — the raw payload is never relayed.
    emit("typing", {"nick": user["nick"], "room": user["room"], "isTyping": is_typing},
         to=user["room"], include_self=False)


@socketio.on("event_mode")
def on_event_mode(data: dict):
    """
    Handle Event Mode start/end from a client.

    Two events are emitted:

    1. ``event_mode``  → to=room, include_self=False
       Received only by OTHER clients.  They render a Chatbot announcement
       comic panel and (for humans) show the join-prompt UI.
       Bots auto-join via this event.

    2. ``global_event_mode``  → to=room  (everyone, including sender)
       Drives the persistent red JOIN LIVE STAGE banner.
       active=True  → show banner on all screens.
       active=False → hide banner on all screens.
       Only the initiator's end signal clears the room's active-event slot,
       so a second user ending their own local stage cannot wipe the banner.

    Client sends:
        { nick, avatarId, room, active: bool }
    """
    sid, user = _joined_user()
    if user is None or not isinstance(data, dict):
        return
    # Identity + room come from the session. This matters here more than most:
    # the room's "initiator" slot is keyed by nick, so a spoofed nick could end
    # someone else's Event Mode.
    nick      = user["nick"]
    avatar_id = _clean_id(data.get("avatarId"), user["avatarId"])
    room      = user["room"]
    active    = bool(data.get("active", True))

    # ── Per-room initiator tracking ───────────────────────────────────────────
    with _event_lock:
        if active:
            # Any user can start; first one wins the initiator slot.
            _active_events.setdefault(room, nick)
            initiator = _active_events[room]
        else:
            initiator = _active_events.get(room)
            # Only the recorded initiator can clear the room-level event.
            if initiator == nick:
                _active_events.pop(room, None)
                initiator = None   # cleared — broadcast off to everyone

    # ── Event announcement (excludes sender — they handle their own UI) ────────
    emit(
        "event_mode",
        {"nick": nick, "avatarId": avatar_id, "room": room, "active": active},
        to=room,
        include_self=False,
    )

    # ── Global banner signal (includes sender) ────────────────────────────────
    # active=True  : always broadcast — a new user joining the stage should
    #               refresh the banner for everyone.
    # active=False : only broadcast when the initiator ended — other users
    #               leaving their own local stage must not remove the banner.
    if active or initiator is None:
        socketio.emit(
            "global_event_mode",
            {"nick": nick, "room": room, "active": active},
            to=room,
        )

    print(f"[event_mode] {nick} {'started' if active else 'ended'} Event Mode in #{room}")


# ─────────────────────────────────────────────────────────────────────────────
# TV synchronisation handlers
# ─────────────────────────────────────────────────────────────────────────────

@socketio.on("update_tv_video")
def on_update_tv_video(data: dict):
    """
    Client starts a YouTube video on the TV prop.

    Client sends:
        { videoId: str, propId: str, room: str }

    Server broadcasts ``sync_tv`` to everyone in the room (including sender,
    so their own startTV is also called via the listener — consistent path).
    """
    sid, user = _joined_user()
    if user is None or not isinstance(data, dict):
        return
    room     = user["room"]
    video_id = _sanitise(data.get("videoId", ""), max_len=16)
    prop_id  = _sanitise(data.get("propId",  ""), max_len=64)

    # The id is spliced into an embed URL by the clients — only plain YouTube-id
    # characters are allowed (no "?", "/", "&" … that could alter the URL).
    if not _VIDEO_RE.match(video_id) or not _PROP_RE.match(prop_id):
        return

    socketio.emit("sync_tv", {"videoId": video_id, "propId": prop_id}, to=room)
    print(f"[tv] sync_tv videoId={video_id} propId={prop_id} room=#{room}")


@socketio.on("end_tv_video")
def on_end_tv_video(data: dict):
    """
    Client stopped the TV.

    Client sends:
        { propId: str, room: str }

    Server broadcasts ``sync_tv_stop`` to everyone in the room.
    """
    sid, user = _joined_user()
    if user is None or not isinstance(data, dict):
        return
    room    = user["room"]
    prop_id = _sanitise(data.get("propId", ""), max_len=64)
    if not _PROP_RE.match(prop_id):
        return

    socketio.emit("sync_tv_stop", {"propId": prop_id}, to=room)
    print(f"[tv] sync_tv_stop propId={prop_id} room=#{room}")


@socketio.on("sync_tv_size")
def on_sync_tv_size(data: dict):
    """
    Client resized the TV overlay (mini ↔ big).

    Client sends:
        { size: 'mini'|'big', room: str }

    Server broadcasts ``sync_tv_size`` to everyone ELSE in the room.
    The sender already applied the size change locally.
    """
    sid, user = _joined_user()
    if user is None or not isinstance(data, dict):
        return
    room = user["room"]
    size = data.get("size", "mini")
    if size not in ("mini", "big"):
        size = "mini"

    emit("sync_tv_size", {"size": size}, to=room, include_self=False)
    print(f"[tv] sync_tv_size size={size} room=#{room}")


@socketio.on("room_command")
def on_room_command(data: dict):
    """
    Generic room-wide visual command dispatcher.

    Client sends:
        { type: str, room: str, ...payload }

    Server sanitises, attaches the sender's nick, and rebroadcasts
    ``room_command`` to the entire room (including sender).

    Supported types: all original commands plus the 23 new v7 commands.
    Unknown types are silently dropped.
    """
    # ── All allowed command types ─────────────────────────────────────────────
    ALLOWED_TYPES = {
        # original
        'bsod', 'negative', 'apagon', 'nudge', 'focus', 'dim',
        # v7
        'vhs', 'quiniela', 'sepia', 'font_wingdings', 'zoom_focus',
        'silhouette', 'fileteado', 'win_tab', 'low_res',
        'page_flip', 'subte', 'void', 'mirror', 'weight',
        'heavy_rain', 'ink_bleed', 'page_tear',
        'ghost_writer', 'alt_f4', 'inner_monologue', 'zoom_dramatic',
        # batch 2
        'screensaver', 'pixel_sort', 'direct_x', 'shred', 'spotlight',
        'gutters', 'blind_shadows', 'coinflip', 'desktop_holes',
        'avatar_missing', 'rough_cut', 'trial_expired', 'register',
        'negative_space', 'granizo', 'bondi_full',
        # batch 3 — props & cinematic
        'obelisco', 'bondi_exit', 'piquete', 'shun',
        # fusion
        'fusion',
        # batch 4 — new cinematic / environmental
        'kamehameha', 'zero_g', 'anti_gravity', 'petrify', 'statue',
        'reboot', 'ufo', 'hologram', 'cyber_grid', 'abduct',
        'matrix_rain', 'disco_ball', 'split_focus', 'manga_speed',
        'overlay_caption',
        # aura system
        'set_aura',
        # title system
        'set_title',
        # batch 5 — new commands
        'thought_cloud', 'duel', 'musica', 'avatar_404',
        # batch 6 — new rooms commands
        'shh',
        # batch 7 — new commands
        'gift', 'crown', 'fiaca',
        # batch 8 — new commands
        'perro_callejero', 'page_number',
        # batch 9 — new commands
        'pasacalle',
        # batch 10 — room-exclusive commands
        'highscore', 'fishing', 'fantasma', 'rise_from_grave', 'warp', 'alert', 'parada', 'pickpocket',
        # batch 11 — room-exclusive commands
        'possession', 'game_over',
        # batch 12 — room-exclusive commands
        'rush_hour',
        # batch 13 — any-room commands
        'beam',
        # batch 14 — La Pared
        'graffiti',
        # batch 15 — image / GIF prop
        'image_post', 'image_clear',
        # batch 16 — picture frames (independent, per prop)
        'frame_post', 'frame_clear',
    }

    sid, user = _joined_user()
    if user is None or not isinstance(data, dict):
        return
    # WHO (actor) and WHERE (room) are the session's — never the payload's.
    actor    = user["nick"]
    room     = user["room"]
    cmd_type = _sanitise(data.get("type", ""), max_len=32)

    if cmd_type not in ALLOWED_TYPES:
        return

    # ── Rate limiting ─────────────────────────────────────────────────────────
    # Commands are cheaper than chat messages (no history write) but still
    # represent a spam vector (e.g. /apagon, /disco-ball spam).
    # Only authenticated bot sessions are exempt (same rule as on_chat_message).
    if not user.get("bot"):
        _rl_cmd = _check_rate_limit(sid, _rate_cmd, RATE_CMD_LIMIT, RATE_CMD_WARN, RATE_CMD_WINDOW)
        if _rl_cmd == "block":
            emit(
                "system_msg",
                {"text": (
                    f"⚡ Command rate limit reached. "
                    f"Max {RATE_CMD_LIMIT} commands per {int(RATE_CMD_WINDOW)}s."
                )},
            )
            print(f"[rate-limit/block] room_command '{cmd_type}' from sid={sid} nick={actor!r} dropped")
            return
        if _rl_cmd == "warn":
            emit(
                "system_msg",
                {"text": (
                    f"⚠ You're firing commands quickly "
                    f"({RATE_CMD_WARN}+ in {int(RATE_CMD_WINDOW)}s). Ease up!"
                )},
            )

    # Build a clean, server-stamped payload. Every field that names the ACTOR is
    # overwritten with the session's nick, so the per-type branches below (which
    # read data["nick"] / data["from"] / data["initiator"]) cannot be spoofed:
    # nobody can fire /bsod, /petrify, /set_title … "as" another user.
    # Fields that name a TARGET (data["target"]) are the client's choice.
    data = dict(data)
    data["nick"] = data["from"] = data["initiator"] = actor
    payload = {
        "type":     cmd_type,
        "room":     room,
        "fromNick": actor,
    }

    # ── Type-specific field passthrough (sanitised individually) ─────────────

    # Original commands
    if cmd_type == "bsod":
        payload["nick"]   = _sanitise(data.get("nick", ""), max_len=32)
        payload["active"] = bool(data.get("active", True))
    elif cmd_type == "negative":
        payload["nick"]   = _sanitise(data.get("nick", ""), max_len=32)
        payload["active"] = bool(data.get("active", True))
    elif cmd_type == "apagon":
        payload["nick"]   = _sanitise(data.get("nick", ""), max_len=32)
    elif cmd_type == "nudge":
        payload["from"]   = _sanitise(data.get("from",   ""), max_len=32)
        payload["target"] = _sanitise(data.get("target", ""), max_len=32)
    elif cmd_type == "focus":
        payload["nick"]   = _sanitise(data.get("nick", ""), max_len=32)
        payload["count"]  = _to_int(data.get("count", 3), 3, 0, 10)
    elif cmd_type == "dim":
        payload["nick"]   = _sanitise(data.get("nick", ""), max_len=32)
        payload["active"] = bool(data.get("active", True))

    # New toggle commands — nick + active bool
    elif cmd_type in (
        'vhs', 'sepia', 'font_wingdings', 'zoom_focus', 'silhouette',
        'fileteado', 'low_res', 'void', 'mirror', 'weight',
        'heavy_rain', 'ink_bleed', 'page_tear',
        'ghost_writer', 'inner_monologue', 'zoom_dramatic',
    ):
        payload["nick"]   = _sanitise(data.get("nick", ""), max_len=32)
        payload["active"] = bool(data.get("active", True))

    # /quiniela — nick + lucky number string (validated 10–99)
    elif cmd_type == "quiniela":
        payload["nick"]   = _sanitise(data.get("nick", ""), max_len=32)
        raw_num           = str(data.get("number", "00"))
        # isascii(): str.isdigit() is also True for e.g. "²", which int() rejects
        payload["number"] = raw_num if (raw_num.isascii() and raw_num.isdigit()
                                        and 10 <= int(raw_num) <= 99) else "42"

    # /win_tab — nick + duration in ms (capped at 10 s)
    elif cmd_type == "win_tab":
        payload["nick"]       = _sanitise(data.get("nick", ""), max_len=32)
        payload["durationMs"] = _to_int(data.get("durationMs", 3000), 3000, 0, 10_000)

    # One-shot transition commands — nick only, no active flag
    elif cmd_type in ("page_flip", "subte", "alt_f4"):
        payload["nick"] = _sanitise(data.get("nick", ""), max_len=32)

    # Batch 2 — toggle commands (nick + active)
    elif cmd_type in (
        'screensaver', 'pixel_sort', 'direct_x', 'gutters',
        'blind_shadows', 'desktop_holes', 'avatar_missing', 'rough_cut',
        'negative_space', 'granizo',
    ):
        payload["nick"]   = _sanitise(data.get("nick", ""), max_len=32)
        payload["active"] = bool(data.get("active", True))

    # /coinflip — nick + result string (pre-resolved by sender)
    elif cmd_type == "coinflip":
        payload["nick"]   = _sanitise(data.get("nick", ""), max_len=32)
        raw_result        = str(data.get("result", "🪙 HEADS"))
        # Only allow the two valid values
        payload["result"] = raw_result if raw_result in ("🪙 HEADS", "🪙 TAILS") else "🪙 HEADS"

    # /shred, /spotlight, /bondi_full — one-shot, nick only
    elif cmd_type in ("shred", "spotlight", "bondi_full"):
        payload["nick"] = _sanitise(data.get("nick", ""), max_len=32)

    # /trial_expired, /register — one-shot, nick only
    elif cmd_type in ("trial_expired", "register"):
        payload["nick"] = _sanitise(data.get("nick", ""), max_len=32)

    # Batch 3 props — one-shot, nick only
    elif cmd_type in ("obelisco", "bondi_exit", "piquete"):
        payload["nick"] = _sanitise(data.get("nick", ""), max_len=32)

    # /fusion — DBZ fusion; initiator + target + avatarIds + duration
    elif cmd_type == 'fusion':
        initiator = _sanitise(data.get('initiator', ''), max_len=32)
        target    = _sanitise(data.get('target',    ''), max_len=32)
        if not initiator or not target or initiator.lower() == target.lower():
            return
        payload['initiator']         = initiator
        payload['target']            = target
        payload['initiatorAvatarId'] = _clean_id(
            data.get('initiatorAvatarId'), user.get('avatarId', 'kid'))
        # Resolve target's live avatarId from online_users so all clients
        # see the correct sprite even if the target never sent their avatarId
        target_avatar = 'kid'
        for u in online_users.values():
            if u.get('nick', '').lower() == target.lower():
                target_avatar = u.get('avatarId', 'kid')
                break
        payload['targetAvatarId'] = target_avatar
        payload['durationMs']     = _to_int(data.get('durationMs', 30000), 30000, 0, 60000)

    # /shun — cinematic special move; carries initiator + target nicks
    elif cmd_type == "shun":
        payload["initiator"] = _sanitise(data.get("initiator", ""), max_len=32)
        payload["target"]    = _sanitise(data.get("target",    ""), max_len=32)
        if not payload["initiator"] or not payload["target"]:
            return
        if payload["initiator"].lower() == payload["target"].lower():
            return

    # Batch 4 — targeted commands (initiator + target)
    elif cmd_type in ("kamehameha", "petrify", "abduct"):
        payload["initiator"] = _sanitise(data.get("initiator", ""), max_len=32)
        payload["target"]    = _sanitise(data.get("target",    ""), max_len=32)
        if not payload["initiator"] or not payload["target"]:
            return
        if payload["initiator"].lower() == payload["target"].lower():
            return

    # Batch 4 — one-shot / room-wide, nick only
    elif cmd_type in (
        "zero_g", "anti_gravity", "statue", "reboot", "ufo",
        "hologram", "cyber_grid", "matrix_rain", "disco_ball",
        "split_focus", "manga_speed",
    ):
        payload["nick"] = _sanitise(data.get("nick", ""), max_len=32)

    # /overlay_caption — nick + short text
    elif cmd_type == "overlay_caption":
        payload["nick"]    = _sanitise(data.get("nick", ""), max_len=32)
        # Canonical field is "caption". Older clients sent "text" (and the caption was
        # silently dropped), so accept both; fall back to the default only if empty.
        raw_caption = data.get("caption")
        if raw_caption is None or str(raw_caption).strip() == "":
            raw_caption = data.get("text")
        payload["caption"] = _sanitise(str(raw_caption or ""), max_len=48) or "MEANWHILE…"

    # /set_aura — nick + aura string ('gold' | 'blue' | 'dark' | '' = clear)
    elif cmd_type == "set_aura":
        VALID_AURAS = {"gold", "blue", "dark", ""}
        payload["nick"] = _sanitise(data.get("nick", ""), max_len=32)
        raw_aura        = str(data.get("aura", "")).lower()
        payload["aura"] = raw_aura if raw_aura in VALID_AURAS else ""

    # /set_title — nick + title string (empty = clear) + rarity
    elif cmd_type == "set_title":
        VALID_RARITIES = {"common", "uncommon", "rare", "legendary"}
        payload["nick"]   = _sanitise(data.get("nick", ""), max_len=32)
        # Allow empty string (= clear title); cap label length
        payload["title"]  = _sanitise(str(data.get("title",  "")), max_len=64)
        raw_rarity        = str(data.get("rarity", "common")).lower()
        payload["rarity"] = raw_rarity if raw_rarity in VALID_RARITIES else "common"

    # Batch 5 — new commands
    # /thought_cloud — toggle; nick + active
    elif cmd_type == "thought_cloud":
        payload["nick"]   = _sanitise(data.get("nick", ""), max_len=32)
        payload["active"] = bool(data.get("active", True))

    # /duel — initiator + target (two different nicks required)
    elif cmd_type == "duel":
        initiator = _sanitise(data.get("initiator", ""), max_len=32)
        target    = _sanitise(data.get("target",    ""), max_len=32)
        if not initiator or not target or initiator.lower() == target.lower():
            return
        payload["initiator"] = initiator
        payload["target"]    = target

    # /musica — one-shot room-wide; nick only
    elif cmd_type == "musica":
        payload["nick"] = _sanitise(data.get("nick", ""), max_len=32)

    # /404 — toggle; nick + active
    elif cmd_type == "avatar_404":
        payload["nick"]   = _sanitise(data.get("nick", ""), max_len=32)
        payload["active"] = bool(data.get("active", True))

    # /shh — initiator + target; mutes target's bubble for 20s
    elif cmd_type == "shh":
        initiator = _sanitise(data.get("initiator", ""), max_len=32)
        target    = _sanitise(data.get("target",    ""), max_len=32)
        if not initiator or not target or initiator.lower() == target.lower():
            return
        payload["initiator"] = initiator
        payload["target"]    = target

    # /gift — animated gift box from initiator to target
    elif cmd_type == "gift":
        initiator = _sanitise(data.get("initiator", ""), max_len=32)
        target    = _sanitise(data.get("target",    ""), max_len=32)
        if not initiator or not target or initiator.lower() == target.lower():
            return
        payload["initiator"] = initiator
        payload["target"]    = target

    # /crown — crown badge above target for 30s
    elif cmd_type == "crown":
        initiator = _sanitise(data.get("initiator", ""), max_len=32)
        target    = _sanitise(data.get("target",    ""), max_len=32)
        if not initiator or not target:
            return
        payload["initiator"] = initiator
        payload["target"]    = target

    # /fiaca — toggle; nick + active
    elif cmd_type == "fiaca":
        payload["nick"]   = _sanitise(data.get("nick", ""), max_len=32)
        payload["active"] = bool(data.get("active", True))

    # /perro_callejero — toggle; nick + active
    elif cmd_type == "perro_callejero":
        payload["nick"]   = _sanitise(data.get("nick", ""), max_len=32)
        payload["active"] = bool(data.get("active", True))

    # /page_number — one-shot stamp; nick only (count is client-local, not
    # sent by the server — see loadPanelsToday()/incrementPanelsToday() in
    # utils.js)
    elif cmd_type == "page_number":
        payload["nick"] = _sanitise(data.get("nick", ""), max_len=32)

    # /pasacalle [text] — sticky per-room banner. New text replaces any
    # existing banner; empty text (bare /pasacalle) takes it down. Persisted
    # server-side in _active_pasacalle so late joiners see the current
    # banner — see the room_banner emit in on_join.
    elif cmd_type == "pasacalle":
        payload["nick"] = _sanitise(data.get("nick", ""), max_len=32)
        text = _sanitise(data.get("text", ""), max_len=60)
        payload["text"] = text
        with _pasacalle_lock:
            if text:
                _active_pasacalle[room] = text
            else:
                _active_pasacalle.pop(room, None)

    # /highscore — GamersZone-exclusive (enforced client-side in chat.js);
    # one-shot, nick + a sanitised fake score.
    elif cmd_type == "highscore":
        payload["nick"] = _sanitise(data.get("nick", ""), max_len=32)
        payload["score"] = _to_int(data.get("score", 0), 0, 0, 9_999_999)

    # /fishing — TranquilBoat-exclusive (enforced client-side in chat.js);
    # one-shot, nick only.
    elif cmd_type == "fishing":
        payload["nick"] = _sanitise(data.get("nick", ""), max_len=32)

    # /fantasma — Cemetery-exclusive (enforced client-side in chat.js);
    # one-shot, nick only.
    elif cmd_type == "fantasma":
        payload["nick"] = _sanitise(data.get("nick", ""), max_len=32)

    # /rise_from_grave — Cemetery-exclusive (enforced client-side in
    # chat.js); one-shot, nick only.
    elif cmd_type == "rise_from_grave":
        payload["nick"] = _sanitise(data.get("nick", ""), max_len=32)

    # /warp — Starship-exclusive (enforced client-side in chat.js);
    # one-shot, nick only.
    elif cmd_type == "warp":
        payload["nick"] = _sanitise(data.get("nick", ""), max_len=32)

    # /alert — Starship-exclusive (enforced client-side in chat.js);
    # one-shot, nick only.
    elif cmd_type == "alert":
        payload["nick"] = _sanitise(data.get("nick", ""), max_len=32)

    # /parada — Subte-exclusive (enforced client-side in chat.js). The
    # station name + line icon are picked once by the sender and relayed
    # as-is (same pattern as /coinflip's result) — NOT re-picked inside
    # the shared room_command handler, or every client would roll its own
    # random station and the room would see different "next stop"
    # announcements instead of one shared PA message.
    elif cmd_type == "parada":
        payload["nick"]    = _sanitise(data.get("nick", ""), max_len=32)
        payload["station"] = _sanitise(data.get("station", "Constitución"), max_len=40)
        raw_line            = str(data.get("line", "🔵"))
        payload["line"]     = raw_line if raw_line in ("🔵", "🟡") else "🔵"

    # /pickpocket — Subte-exclusive (enforced client-side in chat.js);
    # nick (triggering player) + target (who gets pickpocketed).
    elif cmd_type == "pickpocket":
        nick_val   = _sanitise(data.get("nick", ""), max_len=32)
        target_val = _sanitise(data.get("target", ""), max_len=32)
        if not nick_val or not target_val:
            return
        if nick_val.lower() == target_val.lower():
            return
        payload["nick"]   = nick_val
        payload["target"] = target_val

    # /possession — Cemetery-exclusive (enforced client-side in chat.js);
    # initiator + target (two different nicks required).
    elif cmd_type == "possession":
        initiator = _sanitise(data.get("initiator", ""), max_len=32)
        target    = _sanitise(data.get("target",    ""), max_len=32)
        if not initiator or not target or initiator.lower() == target.lower():
            return
        payload["initiator"] = initiator
        payload["target"]    = target

    # /game_over — Arcades-exclusive (enforced client-side in chat.js);
    # one-shot, nick only.
    elif cmd_type == "game_over":
        payload["nick"] = _sanitise(data.get("nick", ""), max_len=32)

    # /rush_hour — Subte-exclusive (enforced client-side in chat.js);
    # one-shot, nick only.
    elif cmd_type == "rush_hour":
        payload["nick"] = _sanitise(data.get("nick", ""), max_len=32)

    # /beam — any room; one-shot transporter materialize effect on the
    # sender's avatar (reuses playTeleportIn() client-side); nick only.
    elif cmd_type == "beam":
        payload["nick"] = _sanitise(data.get("nick", ""), max_len=32)

    # /graffiti — La Pared; nick + capped text + colour from a fixed allow-list.
    # Empty text is dropped (nothing to spray).
    elif cmd_type == "graffiti":
        VALID_GRAFFITI_COLORS = {"white", "yellow", "red", "cyan", "pink", "lime", "orange"}
        payload["nick"] = _sanitise(data.get("nick", ""), max_len=32)
        payload["text"] = _sanitise(data.get("text", ""), max_len=48)
        if not payload["text"]:
            return
        raw_color = str(data.get("color", "white")).strip().lower()
        payload["color"] = raw_color if raw_color in VALID_GRAFFITI_COLORS else "white"

    # /image_post — a direct image / GIF link shown to the whole room.
    # Order matters: validation first (an invalid URL must not burn the user's slot),
    # then the room replace lock (also side-effect free), then the per-user limiter.
    elif cmd_type == "image_post":
        safe_url = _validate_image_url(data.get("url"))
        if safe_url is None:
            return                                   # invalid → dropped silently
        now = time.time()
        with _image_lock:
            current = _room_images.get(room)
            if current and current["by"] != actor and (now - current["ts"]) < IMG_MIN_DISPLAY_SECS:
                wait = int(IMG_MIN_DISPLAY_SECS - (now - current["ts"])) + 1
                emit("system_msg", {"text": (
                    f"🖼️ An image is already showing — it can be replaced in {wait}s."
                )})
                return
        if not user.get("bot"):
            if _check_rate_limit(sid, _rate_img, IMG_POST_LIMIT, IMG_POST_LIMIT,
                                 IMG_POST_WINDOW) == "block":
                emit("system_msg", {"text": (
                    f"🖼️ Slow down — one image every {int(IMG_POST_WINDOW)}s."
                )})
                return
        with _image_lock:
            _room_images[room] = {"by": actor, "ts": now}
        payload["nick"] = actor                      # attribution is the SESSION's nick
        payload["url"]  = safe_url                   # the rebuilt, validated URL

    # /image_clear — anyone may clear; the replace lock never blocks this.
    elif cmd_type == "image_clear":
        with _image_lock:
            _room_images.pop(room, None)
        payload["nick"] = actor

    # /frame_post, /frame_clear — one picture frame (propId) in a room that has frames.
    # propId must be one of THIS room's frame ids (session room, never the payload's).
    elif cmd_type in ("frame_post", "frame_clear"):
        prop_id = _sanitise(data.get("propId", ""), max_len=64)
        if prop_id not in IMAGE_FRAME_PROPS.get(room, frozenset()):
            return
        if cmd_type == "frame_post":
            safe_url = _validate_image_url(data.get("url"))
            if safe_url is None:
                return                               # invalid → dropped silently
            now = time.time()
            with _image_lock:
                current = _room_frames.get(room, {}).get(prop_id)
                if current and current["by"] != actor and (now - current["ts"]) < IMG_MIN_DISPLAY_SECS:
                    wait = int(IMG_MIN_DISPLAY_SECS - (now - current["ts"])) + 1
                    emit("system_msg", {"text": (
                        f"🖼️ That frame was just filled — it can be replaced in {wait}s."
                    )})
                    return
            if not user.get("bot"):
                if _check_rate_limit(sid, _rate_img, IMG_POST_LIMIT, IMG_POST_LIMIT,
                                     IMG_POST_WINDOW) == "block":
                    emit("system_msg", {"text": (
                        f"🖼️ Slow down — one image every {int(IMG_POST_WINDOW)}s."
                    )})
                    return
            with _image_lock:
                _room_frames.setdefault(room, {})[prop_id] = {"by": actor, "ts": now}
            payload["url"] = safe_url                # the rebuilt, validated URL
        else:
            with _image_lock:
                _room_frames.get(room, {}).pop(prop_id, None)
        payload["nick"]   = actor
        payload["propId"] = prop_id

    socketio.emit("room_command", payload, to=room)
    print(f"[room_command] {cmd_type} by {payload['fromNick']} in #{room}")   # (no URLs in the log)


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _sid() -> str:
    """Return the current request's Socket.IO session id."""
    from flask import request
    return request.sid           # type: ignore[attr-defined]


# ── Input hygiene ────────────────────────────────────────────────────────────
_CTRL_CHARS = re.compile(r"[\x00-\x1f\x7f]")
_ID_RE      = re.compile(r"^[A-Za-z0-9_-]{1,32}$")      # avatar ids: knight, dwarf_jump…
_VIDEO_RE   = re.compile(r"^[A-Za-z0-9_-]{1,16}$")      # YouTube ids (end up in an embed URL)
_PROP_RE    = re.compile(r"^[A-Za-z0-9_-]{1,64}$")      # prop ids: prop-cc-2…


def _sanitise(value, max_len: int = 256) -> str:
    """Replace control characters, strip surrounding whitespace, truncate."""
    return _CTRL_CHARS.sub(" ", str(value)).strip()[:max_len]


def _clean_nick(raw) -> str:
    """A display nick: no control chars, single spaces, ≤32 chars, never empty."""
    nick = " ".join(_sanitise("" if raw is None else raw, 64).split())[:32].strip()
    return nick or "Anonymous"


def _clean_room(raw) -> str:
    return _sanitise("" if raw is None else raw, 32) or "general"


def _clean_id(raw, fallback: str) -> str:
    """Accept only identifier-shaped values (avatar ids…); otherwise *fallback*."""
    value = "" if raw is None else str(raw).strip()
    return value if _ID_RE.match(value) else fallback


def _to_int(raw, default: int, lo: int, hi: int) -> int:
    """int(raw) clamped to [lo, hi]; garbage input gives *default* instead of an exception."""
    try:
        n = int(raw)
    except (TypeError, ValueError, OverflowError):
        n = default
    return max(lo, min(hi, n))


# ── Session identity ─────────────────────────────────────────────────────────
# The ONLY source of truth for "who is this" and "which room" is the session
# registered by on_join. Whatever nick / room / initiator a client puts in an
# event payload is ignored.

def _joined_user():
    """(sid, user) for the current connection; user is None until it has joined a room."""
    sid  = _sid()
    user = online_users.get(sid)
    if not user or not user.get("room"):
        return sid, None
    return sid, user


def _is_trusted_bot(secret) -> bool:
    return bool(BOT_SECRET) and isinstance(secret, str) \
        and hmac.compare_digest(secret.encode(), BOT_SECRET.encode())


def _unique_nick(room: str, desired: str, sid: str, trusted_bot: bool) -> str:
    """*desired*, or desired_2, desired_3… if another user in *room* already
    has it (case-insensitive) or it is a reserved bot nick. Caller holds _users_lock."""
    taken    = {u["nick"].lower() for other, u in online_users.items()
                if other != sid and u.get("room") == room}
    reserved = set() if trusted_bot else set(BOT_NICKS)
    candidate, n = desired, 1
    while candidate.lower() in taken or candidate.lower() in reserved:
        n += 1
        suffix    = f"_{n}"
        candidate = desired[:32 - len(suffix)] + suffix
    return candidate


def _release_event(room: str, nick: str) -> None:
    """If *nick* started the room's Event Mode, clear it and tell the room —
    otherwise the red banner would stay up forever after the initiator leaves."""
    with _event_lock:
        if _active_events.get(room) != nick:
            return
        _active_events.pop(room, None)
    socketio.emit("global_event_mode", {"nick": nick, "room": room, "active": False}, to=room)


def _broadcast_user_list(room: str) -> None:
    """Emit the current online roster to everyone in *room*."""
    users = [
        {"nick": u["nick"], "avatarId": u["avatarId"]}
        for u in online_users.values()
        if u.get("room") == room
    ]
    emit("user_list", {"users": users}, to=room)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"Argensoft Comic Chat server starting on http://{HOST}:{PORT}")
    print(f"Async mode : {ASYNC_MODE}")
    print(f"History    : {HISTORY_FILE.resolve()} (max {MAX_HISTORY} messages)")
    if SECRET_KEY == "change-me-in-production":
        print("WARNING    : SECRET_KEY is the default — set the SECRET_KEY environment variable.")
    print(f"Bot access : {'enabled (BOT_SECRET set)' if BOT_SECRET else 'off (BOT_SECRET not set)'}")
    run_kwargs = {"allow_unsafe_werkzeug": True} if ASYNC_MODE == "threading" else {}
    socketio.run(app, host=HOST, port=PORT, debug=False, use_reloader=False, **run_kwargs)
