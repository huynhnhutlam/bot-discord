import discord
from discord.ext import commands
from discord import app_commands
import yt_dlp
import asyncio
from collections import deque
import os
from dotenv import load_dotenv
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode
from typing import Optional, Tuple
import time
from itertools import islice

from flask import Flask
from threading import Thread
# Load Opus với đường dẫn đầy đủ (Apple Silicon Homebrew)
OPUS_PATH = '/opt/homebrew/lib/libopus.dylib'  # <-- Đây là fix chính!
app = Flask(__name__)

# Nếu Intel Mac: OPUS_PATH = '/usr/local/lib/libopus.dylib'

if not discord.opus.is_loaded():
    try:
        discord.opus.load_opus(OPUS_PATH)
        print(f"Opus loaded thành công từ: {OPUS_PATH}")
    except Exception as e:
        print(f"Lỗi load Opus: {e}")
        print("Kiểm tra file tồn tại bằng: ls /opt/homebrew/lib/libopus.dylib")
        # Nếu vẫn lỗi, thử tên đơn giản (nếu brew link đúng)
        discord.opus.load_opus('libopus.dylib')

print("Opus loaded status:", discord.opus.is_loaded())

load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')
GUILD_ID = os.getenv('DISCORD_GUILD_ID')
SYNC_SCOPE = (os.getenv("DISCORD_SYNC_SCOPE") or "").lower()  # "guild" | "global"
CLEAR_GLOBAL_COMMANDS = (os.getenv("DISCORD_CLEAR_GLOBAL_COMMANDS") == "1")
CLEAR_GUILD_COMMANDS = (os.getenv("DISCORD_CLEAR_GUILD_COMMANDS") == "1")
MAX_PLAYLIST_ITEMS = int(os.getenv("DISCORD_MAX_PLAYLIST_ITEMS") or "25")
QUEUE_PAGE_SIZE = int(os.getenv("DISCORD_QUEUE_PAGE_SIZE") or "10")

intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True
intents.members = True

bot = commands.Bot(command_prefix='!', intents=intents)

# Queue cho mỗi guild (server)
queues = {}
current_song = {}
current_song_url = {}
playlist_enqueue_locks = {}
repeat_mode = {}  # guild_id -> "off" | "one" | "all"
last_track_source = {}  # guild_id -> last played queue item (dict) for repeat-one/all
end_reasons = {}  # guild_id -> "skip" | "stop"
current_track_item = {}  # guild_id -> current playing queue item (dict)
history = {}  # guild_id -> deque of played items (dict), newest at end
next_override_item = {}  # guild_id -> queue item (dict) to play next (e.g., back)
requeue_front_item = {}  # guild_id -> queue item (dict) to push front before next play (e.g., back)

def _repeat_text(mode: str) -> str:
    return {"off": "Tắt", "one": "1 bài", "all": "Tất cả"}.get(mode, mode)


def _build_now_playing_text(guild_id: int, title: str, web_url: Optional[str]) -> str:
    mode = _get_repeat_mode(guild_id)
    mode_text = _repeat_text(mode)
    if web_url:
        return f"Đang phát: **{title}**\n{web_url}\nRepeat: **{mode_text}**"
    return f"Đang phát: **{title}**\nRepeat: **{mode_text}**"


class NowPlayingView(discord.ui.View):
    def __init__(self, guild_id: int):
        super().__init__(timeout=600)
        self.guild_id = guild_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        guild = interaction.guild
        if guild is None or guild.id != self.guild_id:
            return False
        vc = guild.voice_client
        if vc is None or not vc.is_connected():
            await interaction.response.send_message("Bot không ở trong voice channel.", ephemeral=True)
            return False
        # Only allow users who are in the same voice channel to control
        if isinstance(interaction.user, discord.Member):
            if interaction.user.voice is None or interaction.user.voice.channel != vc.channel:
                await interaction.response.send_message(
                    "Bạn phải ở cùng voice channel với bot để dùng nút này.",
                    ephemeral=True,
                )
                return False
        return True

    # Button order: back, pause/resume, skip, repeat (icon-only)
    @discord.ui.button(label="⏮", style=discord.ButtonStyle.secondary, custom_id="np_back")
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if guild is None:
            return
        vc = guild.voice_client
        if vc is None or not vc.is_connected():
            return await interaction.response.send_message("Bot không ở trong voice channel.", ephemeral=True)

        h = history.get(guild.id)
        if h is None or len(h) < 2:
            return await interaction.response.send_message("Chưa có bài trước đó để back.", ephemeral=True)

        prev_item = h[-2]
        cur_item = current_track_item.get(guild.id)
        if isinstance(cur_item, dict):
            requeue_front_item[guild.id] = cur_item
        next_override_item[guild.id] = prev_item

        end_reasons[guild.id] = "back"
        if vc.is_playing() or vc.is_paused():
            vc.stop()

        # Update UI immediately (next song message will be sent/edited by play_next)
        await interaction.response.edit_message(
            content="⏮ Đang quay lại bài trước...",
            view=self,
        )

    @discord.ui.button(label="⏸", style=discord.ButtonStyle.secondary, custom_id="np_pause")
    async def pause_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if guild is None:
            return
        vc = guild.voice_client
        if vc is None or not vc.is_connected():
            return await interaction.response.send_message("Bot không ở trong voice channel.", ephemeral=True)

        if vc.is_paused():
            vc.resume()
            button.label = "⏸"
            status = "▶ Resume"
        else:
            if not vc.is_playing():
                return await interaction.response.send_message("Không có bài nào đang phát.", ephemeral=True)
            vc.pause()
            button.label = "▶"
            status = "⏸ Pause"

        # keep content as now playing, only update view (and optionally a small status line)
        title = current_song.get(guild.id) or "Unknown title"
        url = current_song_url.get(guild.id)
        await interaction.response.edit_message(
            content=_build_now_playing_text(guild.id, title, url) + f"\nStatus: **{status}**",
            view=self,
        )

    @discord.ui.button(label="⏭", style=discord.ButtonStyle.secondary, custom_id="np_skip")
    async def skip_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if guild is None:
            return
        vc = guild.voice_client
        if vc is None or not vc.is_connected():
            return await interaction.response.send_message("Bot không ở trong voice channel.", ephemeral=True)
        if vc.is_playing() or vc.is_paused():
            end_reasons[guild.id] = "skip"
            vc.stop()
            # update message content quickly
            title = current_song.get(guild.id) or "Unknown"
            url = current_song_url.get(guild.id)
            await interaction.response.edit_message(
                content=f"Đã skip: **{title}**\n{url}" if url else f"Đã skip: **{title}**",
                view=self,
            )
        else:
            await interaction.response.send_message("Không có bài nào đang phát.", ephemeral=True)

    @discord.ui.button(emoji="🔁", style=discord.ButtonStyle.secondary, custom_id="np_repeat")
    async def repeat_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if guild is None:
            return
        cur = _get_repeat_mode(guild.id)
        nxt = "one" if cur == "off" else ("all" if cur == "one" else "off")
        repeat_mode[guild.id] = nxt

        # Refresh message to reflect new repeat mode
        title = current_song.get(guild.id) or "Unknown title"
        url = current_song_url.get(guild.id)
        await interaction.response.edit_message(
            content=_build_now_playing_text(guild.id, title, url),
            view=self,
        )


def _print_registered_slash_commands() -> None:
    try:
        cmds = bot.tree.get_commands()
        names = sorted([c.qualified_name for c in cmds])
        print(f"Local app_commands registered ({len(names)}): {', '.join(names)}")
    except Exception as e:
        print(f"Cannot list local app_commands: {e}")

YDL_OPTS = {
    "format": "bestaudio/best",
    "quiet": True,
    "no_warnings": True,
    "default_search": "ytsearch",
    "source_address": "0.0.0.0",
    "extractor_args": {"youtube": {"player_client": ["ios", "android", "tv", "web"]}},
    "force_ipv4": True,
}


def _normalize_youtube_query(query: str) -> str:
    q = query.strip()
    if q.startswith("<") and q.endswith(">"):
        q = q[1:-1].strip()

    try:
        parsed = urlparse(q)
    except Exception:
        return q

    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return q

    host = parsed.netloc.lower()
    if "youtube.com" in host and parsed.path == "/watch":
        qs = parse_qs(parsed.query)
        v = (qs.get("v") or [None])[0]
        if v:
            # Keep only the video id; removes list/start_radio/etc.
            new_query = urlencode({"v": v})
            return urlunparse(("https", "www.youtube.com", "/watch", "", new_query, ""))

    if host.endswith("youtu.be"):
        # Keep youtu.be/<id> as-is, strip query junk
        video_id = parsed.path.lstrip("/").split("/")[0]
        if video_id:
            return f"https://youtu.be/{video_id}"

    return q


def _make_queue_item(
    source: str,
    requester_id: int,
    title: Optional[str] = None,
    web_url: Optional[str] = None,
) -> dict:
    # title/web_url may be filled later by background resolution
    return {
        "source": source,  # original query / URL used to resolve
        "title": title,
        "web_url": web_url,
        "stream_url": None,  # resolved direct audio URL (may expire)
        "extracted_at": None,  # epoch seconds when stream_url was resolved
        "requester_id": requester_id,
    }


def _queue_item_display(item: dict) -> tuple[str, str, str]:
    title = item.get("title") or "Đang xử lý..."
    link = item.get("web_url") or (
        item.get("source")
        if isinstance(item.get("source"), str) and item.get("source", "").startswith("http")
        else "-"
    )
    requester_id = item.get("requester_id")
    requester = f"<@{requester_id}>" if isinstance(requester_id, int) and requester_id else "@unknown"
    return title, link, requester


def _is_youtube_playlist_or_mix(url: str) -> bool:
    """Detect YouTube playlist / mix URLs (e.g. list=PL..., list=RD..., /playlist?list=...)."""
    s = url.strip()
    if not (s.startswith("http://") or s.startswith("https://")):
        return False
    try:
        parsed = urlparse(s)
    except Exception:
        return False
    host = (parsed.netloc or "").lower()
    if "youtube.com" not in host and not host.endswith("youtu.be"):
        return False
    qs = parse_qs(parsed.query)
    if "list" in qs:
        return True
    # Some playlist links are /playlist?list=...
    if parsed.path == "/playlist" and "list" in qs:
        return True
    return False


async def _extract_playlist_entries(query: str, max_items: int) -> list[tuple[str, str]]:
    """Extract playlist/mix entries quickly (flat), returning watch URLs + titles."""

    def _work() -> list[tuple[str, str]]:
        q = query.strip()
        if q.startswith("<") and q.endswith(">"):
            q = q[1:-1].strip()

        opts = dict(YDL_OPTS)
        # Flat + lazy playlist extraction: fastest way to just get IDs.
        opts.update(
            {
                "extract_flat": True,  # do not resolve each entry
                "lazy_playlist": True,
                "skip_download": True,
                "noplaylist": False,
                "ignoreerrors": True,
                "playliststart": 1,
                "playlistend": max_items,
            }
        )

        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(q, download=False)
            if not isinstance(info, dict) or "entries" not in info:
                return []

            out: list[tuple[str, str]] = []
            for e in (info.get("entries") or []):
                if not e or not isinstance(e, dict):
                    continue
                title = e.get("title") or "Unknown title"
                vid = e.get("id")
                url = e.get("url")

                # For YouTube flat playlists, url/id are often the video id.
                if url and isinstance(url, str) and url.startswith("http"):
                    watch_url = url
                else:
                    watch_id = None
                    if isinstance(url, str) and url:
                        watch_id = url
                    elif isinstance(vid, str) and vid:
                        watch_id = vid
                    if not watch_id:
                        continue
                    watch_url = f"https://www.youtube.com/watch?v={watch_id}"

                out.append((watch_url, title))
                if len(out) >= max_items:
                    break

            return out

    return await asyncio.to_thread(_work)


async def _resolve_queue_item_metadata(item: dict) -> None:
    """Fill title/web_url for a queue item in the background (best-effort)."""
    try:
        stream_url, title, web_url = await _extract_yt_info_with_web(item.get("source", ""))
    except Exception:
        return
    if not item.get("title"):
        item["title"] = title
    if not item.get("web_url"):
        item["web_url"] = web_url
    # Cache stream url for faster immediate playback (best-effort; may expire)
    if not item.get("stream_url"):
        item["stream_url"] = stream_url
        item["extracted_at"] = time.time()


def _build_queue_embed(guild_id: int, page: int, page_size: int) -> discord.Embed:
    dq = queues.get(guild_id, deque())
    total = len(dq)
    if total == 0:
        return discord.Embed(title="Queue", description="Queue đang trống.")

    max_page = max(1, (total + page_size - 1) // page_size)
    page = max(0, min(page, max_page - 1))
    start = page * page_size
    end = min(start + page_size, total)

    lines = []
    for i, item in enumerate(islice(dq, start, end), start=start + 1):
        if isinstance(item, dict):
            title, link, requester = _queue_item_display(item)
            lines.append(f"**{i}** - {title} - {link} - {requester}")
        else:
            # Backward compatible if old queue items still exist
            lines.append(f"**{i}** - {item} - - - @unknown")

    cur = current_song.get(guild_id)
    cur_url = current_song_url.get(guild_id)
    mode = _get_repeat_mode(guild_id)
    mode_text = {"off": "Tắt", "one": "1 bài", "all": "Tất cả"}.get(mode, mode)

    desc = "\n".join(lines)
    # Discord embed description limit is 4096
    if len(desc) > 3900:
        desc = desc[:3900] + "\n...(cắt bớt do quá dài)"
    embed = discord.Embed(title="Queue", description=desc)
    if cur:
        if cur_url:
            embed.add_field(name="Đang phát", value=f"**{cur}**\n{cur_url}", inline=False)
        else:
            embed.add_field(name="Đang phát", value=f"**{cur}**", inline=False)
    embed.add_field(name="Repeat", value=f"**{mode_text}**", inline=True)
    embed.set_footer(text=f"Trang {page + 1}/{max_page} • {total} bài trong queue")
    return embed


class QueueView(discord.ui.View):
    def __init__(self, guild_id: int, author_id: int, page_size: int):
        super().__init__(timeout=180)
        self.guild_id = guild_id
        self.author_id = author_id
        self.page_size = page_size
        self.page = 0
        self._sync_buttons()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.guild is None or interaction.guild.id != self.guild_id:
            return False
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "Chỉ người gọi lệnh `/queue` mới điều khiển được nút.",
                ephemeral=True,
            )
            return False
        return True

    def _sync_buttons(self) -> None:
        total = len(queues.get(self.guild_id, deque()))
        max_page = max(1, (total + self.page_size - 1) // self.page_size)
        self.page = max(0, min(self.page, max_page - 1))
        for child in self.children:
            if not isinstance(child, discord.ui.Button):
                continue
            if child.custom_id == "queue_prev":
                child.disabled = (self.page <= 0)
            elif child.custom_id == "queue_next":
                child.disabled = (self.page >= max_page - 1)

    @discord.ui.button(label="◀", style=discord.ButtonStyle.secondary, custom_id="queue_prev")
    async def prev(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = max(0, self.page - 1)
        self._sync_buttons()
        await interaction.response.edit_message(
            embed=_build_queue_embed(self.guild_id, self.page, self.page_size),
            view=self,
        )

    @discord.ui.button(label="▶", style=discord.ButtonStyle.secondary, custom_id="queue_next")
    async def next(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = self.page + 1
        self._sync_buttons()
        await interaction.response.edit_message(
            embed=_build_queue_embed(self.guild_id, self.page, self.page_size),
            view=self,
        )

    @discord.ui.button(label="🔄 Refresh", style=discord.ButtonStyle.primary, custom_id="queue_refresh")
    async def refresh(self, interaction: discord.Interaction, button: discord.ui.Button):
        self._sync_buttons()
        await interaction.response.edit_message(
            embed=_build_queue_embed(self.guild_id, self.page, self.page_size),
            view=self,
        )


async def _extract_yt_info(query: str) -> tuple[str, str]:
    """Run yt-dlp in a thread so slash commands don't time out."""

    def _work() -> tuple[str, str]:
        normalized = _normalize_youtube_query(query)
        opts = dict(YDL_OPTS)
        # If it's a direct watch URL (v=...), avoid accidental playlist extraction.
        if "youtube.com/watch?v=" in normalized or "youtu.be/" in normalized:
            opts["noplaylist"] = True

        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(normalized, download=False)

            # Playlist handling: pick first playable entry
            if isinstance(info, dict) and "entries" in info:
                entries = info.get("entries") or []
                picked = None
                for e in entries:
                    if not e:
                        continue
                    if isinstance(e, dict) and (e.get("url") or e.get("id")):
                        picked = e
                        break
                if picked is None:
                    raise RuntimeError("Playlist không có entry playable.")
                info = picked

            # yt-dlp may return 'id' for some extractors; prefer direct 'url' when available
            song_url = info.get("url") or info.get("webpage_url")  # type: ignore[union-attr]
            title = info.get("title") or "Unknown title"  # type: ignore[union-attr]
            if not song_url:
                raise RuntimeError("Không lấy được stream URL.")
            return song_url, title

    return await asyncio.to_thread(_work)


async def _extract_yt_info_with_web(query: str) -> Tuple[str, str, Optional[str]]:
    """Like _extract_yt_info, but also returns a shareable webpage URL when possible."""

    def _work() -> Tuple[str, str, Optional[str]]:
        normalized = _normalize_youtube_query(query)
        opts = dict(YDL_OPTS)
        if "youtube.com/watch?v=" in normalized or "youtu.be/" in normalized:
            opts["noplaylist"] = True

        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(normalized, download=False)

            if isinstance(info, dict) and "entries" in info:
                entries = info.get("entries") or []
                picked = None
                for e in entries:
                    if not e:
                        continue
                    if isinstance(e, dict) and (e.get("url") or e.get("id")):
                        picked = e
                        break
                if picked is None:
                    raise RuntimeError("Playlist không có entry playable.")
                info = picked

            stream_url = info.get("url") or info.get("webpage_url")  # type: ignore[union-attr]
            title = info.get("title") or "Unknown title"  # type: ignore[union-attr]
            web_url = (
                info.get("webpage_url")  # type: ignore[union-attr]
                or info.get("original_url")  # type: ignore[union-attr]
                or (normalized if normalized.startswith("http") else None)
            )
            if not stream_url:
                raise RuntimeError("Không lấy được stream URL.")
            return stream_url, title, web_url

    return await asyncio.to_thread(_work)


@bot.event
async def on_ready():
    print(f'Đã login với tên {bot.user}')
    try:
        guild_obj: Optional[discord.Object] = None
        if GUILD_ID:
            guild_obj = discord.Object(id=int(GUILD_ID))  # right-click server > Copy Server ID

        # ---- One-time cleanup to remove duplicated scope ----
        # Duplicates usually happen when you previously synced BOTH:
        # - global commands (available everywhere)
        # - guild commands (synced to a specific server)
        #
        # Use these env flags once, then remove them:
        # - DISCORD_CLEAR_GLOBAL_COMMANDS=1  -> delete GLOBAL remote commands
        # - DISCORD_CLEAR_GUILD_COMMANDS=1   -> delete GUILD remote commands (requires DISCORD_GUILD_ID)
        saved_cmds = bot.tree.get_commands()

        if CLEAR_GLOBAL_COMMANDS:
            bot.tree.clear_commands(guild=None)
            await bot.tree.sync()
            for cmd in saved_cmds:
                bot.tree.add_command(cmd)
            print("Đã xóa GLOBAL slash commands (remote).")

        if CLEAR_GUILD_COMMANDS:
            if guild_obj is None:
                print("DISCORD_CLEAR_GUILD_COMMANDS=1 nhưng chưa set DISCORD_GUILD_ID.")
            else:
                bot.tree.clear_commands(guild=None)
                await bot.tree.sync(guild=guild_obj)
                for cmd in saved_cmds:
                    bot.tree.add_command(cmd)
                print(f"Đã xóa GUILD slash commands (remote) cho guild {guild_obj.id}.")

        # ---- Sync mode (pick ONE to avoid duplicates) ----
        scope = SYNC_SCOPE or ("guild" if guild_obj is not None else "global")

        if scope == "guild":
            if guild_obj is None:
                print("DISCORD_SYNC_SCOPE=guild nhưng chưa set DISCORD_GUILD_ID -> fallback global sync.")
                synced = await bot.tree.sync()
                print(f'Đã sync {len(synced)} lệnh slash (global)!')
            else:
                synced = await bot.tree.sync(guild=guild_obj)
                print(f'Đã sync {len(synced)} lệnh slash cho guild {guild_obj.id}!')
        else:
            synced = await bot.tree.sync()
            print(f'Đã sync {len(synced)} lệnh slash (global)!')

        _print_registered_slash_commands()
    except Exception as e:
        print(f"Slash sync error: {e}")

# Hàm chơi nhạc
def _get_repeat_mode(guild_id: int) -> str:
    return repeat_mode.get(guild_id, "off")


async def play_next(ctx):
    guild_id = ctx.guild.id
    voice_client = ctx.voice_client
    if voice_client is None or not voice_client.is_connected():
        return

    mode = _get_repeat_mode(guild_id)

    # Decide next track source (supports repeat-one even when queue is empty)
    if guild_id in requeue_front_item:
        if guild_id not in queues:
            queues[guild_id] = deque()
        queues[guild_id].appendleft(requeue_front_item.pop(guild_id))

    if guild_id in next_override_item:
        item = next_override_item.pop(guild_id)
    elif mode == "one" and end_reasons.get(guild_id) is None and last_track_source.get(guild_id):
        item = last_track_source[guild_id]
    else:
        if guild_id not in queues or not queues[guild_id]:
            current_song[guild_id] = None
            current_song_url[guild_id] = None
            return
        item = queues[guild_id].popleft()

    # Backward compatible if queue contains raw strings
    if isinstance(item, str):
        item = _make_queue_item(source=item, requester_id=0)

    # If we already have a fresh stream URL cached, reuse it to start faster.
    # YouTube signed URLs can expire; keep the cache window conservative.
    CACHE_TTL_SECONDS = 8 * 60
    extracted_at = item.get("extracted_at")
    cached_stream = item.get("stream_url")
    cached_title = item.get("title")
    cached_web = item.get("web_url")

    info_msg = None
    if not cached_stream or not isinstance(extracted_at, (int, float)) or (time.time() - extracted_at) > CACHE_TTL_SECONDS:
        # Give immediate feedback while extracting
        try:
            info_msg = await ctx.send(f"Đang lấy thông tin: **{item.get('source', '')}** ...")
        except Exception:
            info_msg = None
        try:
            song_url, title, web_url = await _extract_yt_info_with_web(item.get("source", ""))
        except Exception as e:
            await ctx.send(f"Không lấy được info bài: **{item.get('source', '')}**\n```{e}```")
            await play_next(ctx)
            return

        item["stream_url"] = song_url
        item["extracted_at"] = time.time()
        item["title"] = title
        item["web_url"] = web_url
    else:
        song_url = cached_stream
        title = cached_title or "Unknown title"
        web_url = cached_web

    source = discord.FFmpegPCMAudio(
        song_url,
        before_options='-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5',
        options='-vn'
    )

    current_song[guild_id] = title
    current_song_url[guild_id] = web_url
    last_track_source[guild_id] = item
    current_track_item[guild_id] = item
    if guild_id not in history:
        history[guild_id] = deque(maxlen=100)
    history[guild_id].append(item)

    now_playing_text = _build_now_playing_text(guild_id, title, web_url)
    view = NowPlayingView(guild_id=guild_id)
    if info_msg is not None:
        try:
            await info_msg.edit(content=now_playing_text, view=view)
        except Exception:
            await ctx.send(now_playing_text, view=view)
    else:
        await ctx.send(now_playing_text, view=view)

    async def _after_track(played_item: dict):
        # Determine why the track ended (natural vs skip/stop)
        reason = end_reasons.pop(guild_id, None)
        mode_after = _get_repeat_mode(guild_id)

        # Repeat-all: push played track to the end on NATURAL end only
        if mode_after == "all" and reason is None:
            if guild_id not in queues:
                queues[guild_id] = deque()
            queues[guild_id].append(played_item)

        # If bot got disconnected, stop chain
        vc = ctx.voice_client
        if vc is None or not vc.is_connected():
            return

        await play_next(ctx)

    def after_play(error):
        if error:
            print(error)
        asyncio.run_coroutine_threadsafe(_after_track(item), bot.loop)

    voice_client.play(source, after=after_play)


@bot.hybrid_command(name='play', aliases=['p'])
@app_commands.describe(query="Link YouTube hoặc từ khóa tìm kiếm")
async def play(ctx, *, query: str):
    if not ctx.author.voice:
        return await ctx.send("Bạn phải vào voice channel trước đã!")

    # Slash interactions need a quick ACK to avoid "interaction failed"
    if getattr(ctx, "interaction", None) is not None:
        try:
            await ctx.defer()
        except Exception:
            pass

    channel = ctx.author.voice.channel

    if not ctx.voice_client:
        await channel.connect()
    elif ctx.voice_client.channel != channel:
        await ctx.voice_client.move_to(channel)

    guild_id = ctx.guild.id
    if guild_id not in queues:
        queues[guild_id] = deque()

    async def _ensure_lock(gid: int):
        lock = playlist_enqueue_locks.get(gid)
        if lock is None:
            lock = asyncio.Lock()
            playlist_enqueue_locks[gid] = lock
        return lock

    async def _enqueue_playlist_in_background():
        lock = await _ensure_lock(guild_id)
        async with lock:
            try:
                entries = await _extract_playlist_entries(query, MAX_PLAYLIST_ITEMS)
            except Exception as e:
                await ctx.send(f"Không lấy được playlist/mix.\n```{e}```")
                return

            if not entries:
                it = _make_queue_item(source=query, requester_id=ctx.author.id)
                queues[guild_id].append(it)
                bot.loop.create_task(_resolve_queue_item_metadata(it))
                await ctx.send("Playlist/mix không có entry playable, đã thêm link gốc vào queue.")
            else:
                for watch_url, title in entries:
                    queues[guild_id].append(
                        _make_queue_item(
                            source=watch_url,
                            requester_id=ctx.author.id,
                            title=title,
                            web_url=watch_url,
                        )
                    )
                await ctx.send(f"Đã thêm **{len(entries)}** bài từ playlist/mix vào queue.")

            # If bot is idle, start playing after the playlist is enqueued.
            if ctx.voice_client and not ctx.voice_client.is_playing():
                await play_next(ctx)

    # Playlist/mix extraction can be slow; do it in the background for faster response.
    if _is_youtube_playlist_or_mix(query):
        await ctx.send(f"Đang lấy danh sách playlist/mix (tối đa {MAX_PLAYLIST_ITEMS} bài)...")
        bot.loop.create_task(_enqueue_playlist_in_background())
        return

    it = _make_queue_item(
        source=query,
        requester_id=ctx.author.id,
        title=None,
        web_url=query if query.startswith("http") else None,
    )
    queues[guild_id].append(it)
    # Prefetch metadata in background (helps /queue and may speed up immediate playback)
    bot.loop.create_task(_resolve_queue_item_metadata(it))

    if not ctx.voice_client.is_playing():
        await play_next(ctx)
    else:
        await ctx.send(f'Đã thêm vào queue: **{query}**')


@bot.hybrid_command(name='skip')
async def skip(ctx):
    if not ctx.voice_client:
        return await ctx.send("Bot không ở trong voice channel.")
    if ctx.voice_client.is_playing():
        guild_id = ctx.guild.id
        skipped_title = current_song.get(guild_id)
        skipped_url = current_song_url.get(guild_id)
        end_reasons[guild_id] = "skip"
        ctx.voice_client.stop()
        if skipped_title and skipped_url:
            await ctx.send(f"Đã skip: **{skipped_title}**\n{skipped_url}")
        elif skipped_title:
            await ctx.send(f"Đã skip: **{skipped_title}**")
        else:
            await ctx.send("Đã skip bài hiện tại!")
    else:
        await ctx.send("Không có bài nào đang phát.")


@bot.hybrid_command(name='stop')
async def stop(ctx):
    if ctx.voice_client:
        end_reasons[ctx.guild.id] = "stop"
        await ctx.voice_client.disconnect()
        queues[ctx.guild.id].clear()
        current_song[ctx.guild.id] = None
        current_song_url[ctx.guild.id] = None
        await ctx.send("Đã dừng và disconnect.")
    else:
        await ctx.send("Bot không ở trong voice channel.")


@bot.hybrid_command(name='queue', aliases=['q'])
async def show_queue(ctx):
    guild_id = ctx.guild.id
    if guild_id not in queues or not queues[guild_id]:
        return await ctx.send("Queue đang trống.")
    
    view = QueueView(guild_id=guild_id, author_id=ctx.author.id, page_size=QUEUE_PAGE_SIZE)
    embed = _build_queue_embed(guild_id, page=0, page_size=QUEUE_PAGE_SIZE)
    await ctx.send(embed=embed, view=view)


@bot.hybrid_command(name='nowplaying', aliases=['np'])
async def now_playing(ctx):
    guild_id = ctx.guild.id
    if guild_id in current_song and current_song[guild_id]:
        title = current_song[guild_id]
        web_url = current_song_url.get(guild_id)
        mode = _get_repeat_mode(guild_id)
        mode_text = {"off": "Tắt", "one": "1 bài", "all": "Tất cả"}.get(mode, mode)
        if web_url:
            await ctx.send(f"Đang phát: **{title}**\n{web_url}\nRepeat: **{mode_text}**")
        else:
            await ctx.send(f"Đang phát: **{title}**\nRepeat: **{mode_text}**")
    else:
        await ctx.send("Không có bài nào đang phát.")


@bot.hybrid_command(name="repeat", aliases=["loopmode"])
@app_commands.describe(mode="off / one / all")
async def repeat_cmd(ctx, mode: Optional[str] = None):
    """Set repeat mode: off / one (repeat current) / all (loop queue)."""
    guild_id = ctx.guild.id
    if mode is None:
        m = _get_repeat_mode(guild_id)
        mode_text = {"off": "Tắt", "one": "1 bài", "all": "Tất cả"}.get(m, m)
        return await ctx.send(f"Repeat hiện tại: **{mode_text}**")

    mode_value = (mode or "").strip().lower()
    if mode_value not in ("off", "one", "all"):
        return await ctx.send("Mode không hợp lệ. Dùng: `off` / `one` / `all`.")

    repeat_mode[guild_id] = mode_value
    mode_text = {"off": "Tắt", "one": "1 bài", "all": "Tất cả"}.get(mode_value, mode_value)
    await ctx.send(f"Đã set repeat: **{mode_text}**")


@bot.hybrid_command(name="loop")
async def loop_cmd(ctx):
    """Toggle loop queue (repeat all)."""
    guild_id = ctx.guild.id
    cur = _get_repeat_mode(guild_id)
    repeat_mode[guild_id] = "off" if cur == "all" else "all"
    mode_text = "Tắt" if repeat_mode[guild_id] == "off" else "Tất cả"
    await ctx.send(f"Loop queue: **{mode_text}**")

@app.route('/')
def home():
    return "Bot is alive! 🎶"

def run_flask():
    port = int(os.environ.get("PORT", 10000))  # Render dùng PORT env var
    app.run(host='0.0.0.0', port=port)

# Chạy Flask trong thread riêng
flask_thread = Thread(target=run_flask)
flask_thread.start()


bot.run(TOKEN)