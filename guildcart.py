import os
import json
import asyncio
import logging
import traceback
from datetime import datetime, timezone, timedelta, time
from pathlib import Path

import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiosqlite
from dotenv import load_dotenv

# ================= CONFIG =================

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
DB = "public_cart.db"
BACKUP_FOLDER = "public_backups"

logging.basicConfig(level=logging.INFO)

intents = discord.Intents.default()
intents.members = True
intents.message_content = False

bot = commands.Bot(command_prefix="!", intents=intents)

CART_HOURS = [f"{hour:02d}:00" for hour in range(24)]
BACKUP_KEEP_LAST = 30

MAINTENANCE_TIMES = [
    time(hour=hour, minute=minute, tzinfo=timezone.utc)
    for hour in range(24)
    for minute in range(0, 60, 5)
]

NIGHTLY_BACKUP_TIME = time(hour=0, minute=1, tzinfo=timezone.utc)

# ================= DATE HELPERS =================

def today_utc():
    return datetime.now(timezone.utc).date()


def default_cart_date(position: int):
    return (today_utc() + timedelta(days=position - 1)).isoformat()


def valid_date(value: str):
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def valid_hour(value: str):
    return value in CART_HOURS


def utc_badge(cart_date: str):
    try:
        date_obj = datetime.strptime(str(cart_date), "%Y-%m-%d").date()
    except ValueError:
        return ""

    today = today_utc()
    tomorrow = today + timedelta(days=1)

    if date_obj == today:
        return "🔥 TODAY "
    if date_obj == tomorrow:
        return "🟡 TOMORROW "
    return ""


# ================= DATABASE =================

async def init_db():
    async with aiosqlite.connect(DB) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS guild_settings(
            guild_id INTEGER PRIMARY KEY,
            cart_channel_id INTEGER,
            cart_role_id INTEGER,
            officer_role_id INTEGER,
            guildmaster_role_id INTEGER,
            log_channel_id INTEGER,
            utc_channel_id INTEGER,
            panel_message_id INTEGER,
            backup_message_id INTEGER,
            officer_message_id INTEGER
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS carts(
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            position INTEGER NOT NULL,
            hour TEXT NOT NULL,
            cart_date TEXT NOT NULL,
            reminded INTEGER DEFAULT 0,
            manual_name TEXT,
            PRIMARY KEY(guild_id, user_id)
        )
        """)

        for column in ("backup_message_id", "officer_message_id"):
            try:
                await db.execute(f"ALTER TABLE guild_settings ADD COLUMN {column} INTEGER")
            except Exception:
                pass

        await db.commit()


async def get_settings(guild_id: int):
    async with aiosqlite.connect(DB) as db:
        cursor = await db.execute(
            """
            SELECT guild_id, cart_channel_id, cart_role_id, officer_role_id,
                   guildmaster_role_id, log_channel_id, utc_channel_id,
                   panel_message_id, backup_message_id, officer_message_id
            FROM guild_settings
            WHERE guild_id=?
            """,
            (guild_id,)
        )
        row = await cursor.fetchone()

    if not row:
        return None

    keys = [
        "guild_id", "cart_channel_id", "cart_role_id", "officer_role_id",
        "guildmaster_role_id", "log_channel_id", "utc_channel_id",
        "panel_message_id", "backup_message_id", "officer_message_id"
    ]
    return dict(zip(keys, row))


async def save_settings(guild_id: int, cart_channel_id: int, cart_role_id: int,
                        officer_role_id: int, guildmaster_role_id: int,
                        log_channel_id: int | None = None,
                        utc_channel_id: int | None = None):
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            """
            INSERT INTO guild_settings(
                guild_id, cart_channel_id, cart_role_id, officer_role_id,
                guildmaster_role_id, log_channel_id, utc_channel_id
            ) VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(guild_id) DO UPDATE SET
                cart_channel_id=excluded.cart_channel_id,
                cart_role_id=excluded.cart_role_id,
                officer_role_id=excluded.officer_role_id,
                guildmaster_role_id=excluded.guildmaster_role_id,
                log_channel_id=excluded.log_channel_id,
                utc_channel_id=excluded.utc_channel_id
            """,
            (
                guild_id, cart_channel_id, cart_role_id, officer_role_id,
                guildmaster_role_id, log_channel_id, utc_channel_id
            )
        )
        await db.commit()


async def update_message_id(guild_id: int, column: str, message_id: int):
    if column not in {"panel_message_id", "backup_message_id", "officer_message_id"}:
        raise ValueError("Invalid message id column")

    async with aiosqlite.connect(DB) as db:
        await db.execute(
            f"UPDATE guild_settings SET {column}=? WHERE guild_id=?",
            (message_id, guild_id)
        )
        await db.commit()


async def get_queue(guild_id: int):
    async with aiosqlite.connect(DB) as db:
        cursor = await db.execute(
            """
            SELECT user_id, position, hour, manual_name, cart_date
            FROM carts
            WHERE guild_id=?
            ORDER BY cart_date, hour, position
            """,
            (guild_id,)
        )
        return await cursor.fetchall()


async def get_user(guild_id: int, user_id: int):
    async with aiosqlite.connect(DB) as db:
        cursor = await db.execute(
            """
            SELECT position, hour, cart_date
            FROM carts
            WHERE guild_id=? AND user_id=?
            """,
            (guild_id, user_id)
        )
        return await cursor.fetchone()


async def compress_queue(guild_id: int):
    rows = await get_queue(guild_id)
    async with aiosqlite.connect(DB) as db:
        for index, row in enumerate(rows, start=1):
            uid = row[0]
            await db.execute(
                "UPDATE carts SET position=? WHERE guild_id=? AND user_id=?",
                (index, guild_id, uid)
            )
        await db.commit()


async def move_member(guild_id: int, user_id: int, direction: str):
    rows = await get_queue(guild_id)
    ids = [row[0] for row in rows]

    if user_id not in ids:
        return False

    index = ids.index(user_id)

    if direction == "up":
        if index == 0:
            return False
        new_index = index - 1
    elif direction == "down":
        if index == len(ids) - 1:
            return False
        new_index = index + 1
    else:
        return False

    ids[index], ids[new_index] = ids[new_index], ids[index]

    async with aiosqlite.connect(DB) as db:
        for pos, uid in enumerate(ids, start=1):
            await db.execute(
                "UPDATE carts SET position=? WHERE guild_id=? AND user_id=?",
                (pos, guild_id, uid)
            )
        await db.commit()

    return True


async def cleanup_expired_carts(guild_id: int | None = None):
    """Remove carts that are already in the past using UTC date + hour."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")

    async with aiosqlite.connect(DB) as db:
        if guild_id is None:
            cursor = await db.execute(
                """
                DELETE FROM carts
                WHERE datetime(cart_date || ' ' || hour) < datetime(?)
                """,
                (now,)
            )
        else:
            cursor = await db.execute(
                """
                DELETE FROM carts
                WHERE guild_id=?
                  AND datetime(cart_date || ' ' || hour) < datetime(?)
                """,
                (guild_id, now)
            )

        deleted = cursor.rowcount
        await db.commit()

    if deleted:
        if guild_id is None:
            async with aiosqlite.connect(DB) as db:
                cursor = await db.execute("SELECT DISTINCT guild_id FROM carts")
                guild_ids = [row[0] for row in await cursor.fetchall()]
            for gid in guild_ids:
                await compress_queue(gid)
        else:
            await compress_queue(guild_id)

    return deleted

# ================= PERMISSIONS / LOG =================

def has_admin_access(member: discord.Member, settings: dict | None):
    if member.guild_permissions.administrator:
        return True

    if not settings:
        return False

    role_ids = {role.id for role in member.roles}
    return (
        settings.get("officer_role_id") in role_ids
        or settings.get("guildmaster_role_id") in role_ids
    )


async def log_action(guild: discord.Guild, action: str):
    settings = await get_settings(guild.id)
    if not settings or not settings.get("log_channel_id"):
        return

    channel = guild.get_channel(settings["log_channel_id"])
    if not channel:
        return

    try:
        await channel.send(f"⚜️ **GuildCart Log**\n📌 {action}")
    except Exception:
        traceback.print_exc()

# ================= BACKUPS =================

def create_backup_folder():
    Path(BACKUP_FOLDER).mkdir(parents=True, exist_ok=True)


def prune_old_guild_backups(guild_id: int):
    create_backup_folder()
    files = sorted(
        Path(BACKUP_FOLDER).glob(f"guild_{guild_id}_*.json"),
        key=lambda file: file.stat().st_mtime,
        reverse=True,
    )

    for old_file in files[BACKUP_KEEP_LAST:]:
        try:
            old_file.unlink()
        except Exception:
            traceback.print_exc()


async def create_guild_backup(guild_id: int):
    create_backup_folder()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup_file = Path(BACKUP_FOLDER) / f"guild_{guild_id}_{timestamp}.json"

    async with aiosqlite.connect(DB) as db:
        cursor = await db.execute(
            """
            SELECT user_id, position, hour, cart_date, reminded, manual_name
            FROM carts
            WHERE guild_id=?
            ORDER BY cart_date, hour, position
            """,
            (guild_id,)
        )
        rows = await cursor.fetchall()

    data = {
        "guild_id": guild_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "carts": [
            {
                "user_id": row[0],
                "position": row[1],
                "hour": row[2],
                "cart_date": row[3],
                "reminded": row[4],
                "manual_name": row[5],
            }
            for row in rows
        ]
    }

    backup_file.write_text(json.dumps(data, indent=2), encoding="utf-8")
    prune_old_guild_backups(guild_id)
    return backup_file


async def restore_latest_guild_backup(guild_id: int):
    create_backup_folder()
    files = sorted(Path(BACKUP_FOLDER).glob(f"guild_{guild_id}_*.json"), reverse=True)

    if not files:
        return None

    latest = files[0]
    data = json.loads(latest.read_text(encoding="utf-8"))

    async with aiosqlite.connect(DB) as db:
        await db.execute("DELETE FROM carts WHERE guild_id=?", (guild_id,))

        for row in data.get("carts", []):
            await db.execute(
                """
                INSERT INTO carts(guild_id, user_id, position, hour, cart_date, reminded, manual_name)
                VALUES(?,?,?,?,?,?,?)
                """,
                (
                    guild_id,
                    int(row["user_id"]),
                    int(row["position"]),
                    str(row["hour"]),
                    str(row["cart_date"]),
                    int(row.get("reminded") or 0),
                    row.get("manual_name"),
                )
            )

        await db.commit()

    await compress_queue(guild_id)
    return latest

# ================= EMBED / PANELS =================

async def build_queue_embed(guild: discord.Guild):
    await cleanup_expired_carts(guild.id)
    rows = await get_queue(guild.id)

    embed = discord.Embed(
        title="🚚 Guild Cart Queue (UTC)",
        colour=discord.Colour.green()
    )

    if not rows:
        embed.description = "No carts are currently scheduled.\n\nUse ➕ **Join Queue** to claim the next available cart."
        return embed

    lines = []

    for uid, pos, hour, manual_name, cart_date in rows:
        if manual_name:
            owner = f"**{manual_name}**"
        else:
            member = guild.get_member(uid)
            owner = member.mention if member else f"<@{uid}>"

        lines.append(
            f"{utc_badge(cart_date)}📅 {cart_date} 🕒 {hour} UTC - {owner}"
        )

    embed.description = "\n".join(lines)
    embed.set_footer(text=f"Total scheduled carts: {len(rows)}")
    return embed


async def get_saved_message(channel: discord.TextChannel, message_id: int | None):
    if not message_id:
        return None
    try:
        return await channel.fetch_message(int(message_id))
    except (discord.NotFound, discord.Forbidden):
        return None
    except Exception:
        traceback.print_exc()
        return None


async def upsert_message(channel: discord.TextChannel, settings: dict, column: str, embed: discord.Embed, view: discord.ui.View):
    message = await get_saved_message(channel, settings.get(column))

    if message:
        await message.edit(embed=embed, view=view)
        return message

    message = await channel.send(embed=embed, view=view)
    await update_message_id(channel.guild.id, column, message.id)
    return message


async def refresh_queue_panel(guild: discord.Guild):
    settings = await get_settings(guild.id)
    if not settings or not settings.get("cart_channel_id"):
        return

    channel = guild.get_channel(settings["cart_channel_id"])
    if not channel:
        return

    await upsert_message(
        channel,
        settings,
        "panel_message_id",
        await build_queue_embed(guild),
        CartView(),
    )


async def refresh_backup_panel(guild: discord.Guild):
    # Backup controls are now included in the Officer Panel.
    # This keeps the public bot cleaner by avoiding a separate Backup Panel message.
    return


async def get_all_members(guild: discord.Guild):
    result = []
    seen_ids = set()

    members = sorted(
        [member for member in guild.members if not member.bot],
        key=lambda member: member.display_name.lower()
    )

    for member in members:
        result.append({"id": member.id, "name": f"👤 {member.display_name}"})
        seen_ids.add(member.id)

    async with aiosqlite.connect(DB) as db:
        cursor = await db.execute(
            """
            SELECT user_id, manual_name
            FROM carts
            WHERE guild_id=?
            ORDER BY cart_date, hour, position
            """,
            (guild.id,)
        )
        rows = await cursor.fetchall()

    for uid, manual_name in rows:
        if manual_name:
            result.append({"id": uid, "name": f"📝 {manual_name}"})
        elif uid not in seen_ids:
            member = guild.get_member(uid)
            if member:
                result.append({"id": uid, "name": f"👤 {member.display_name}"})
            else:
                result.append({"id": uid, "name": f"👤 <@{uid}>"})
            seen_ids.add(uid)

    return result


async def refresh_officer_panel(guild: discord.Guild):
    settings = await get_settings(guild.id)
    if not settings or not settings.get("cart_channel_id"):
        return

    channel = guild.get_channel(settings["cart_channel_id"])
    if not channel:
        return

    members = await get_all_members(guild)

    embed = discord.Embed(
        title="⚜️ Officer Panel",
        description=(
            "Manage the queue using the buttons below.\n\n"
            "➕ Add member to queue\n"
            "📝 Add manual entry\n"
            "➖ Remove member\n"
            "⬆️ Move member up\n"
            "⬇️ Move member down\n"
            "🗓 Edit date and hour\n"
            "💾 Create backup\n"
            "♻️ Restore backup"
        ),
        colour=discord.Colour.gold(),
    )

    await upsert_message(
        channel,
        settings,
        "officer_message_id",
        embed,
        OfficerPanelView(members),
    )


async def refresh_all_panels(guild: discord.Guild):
    await refresh_queue_panel(guild)
    await refresh_backup_panel(guild)
    await refresh_officer_panel(guild)

# ================= USER VIEWS =================

class JoinHourSelect(discord.ui.Select):
    def __init__(self):
        options = [discord.SelectOption(label=f"{hour} UTC", value=hour) for hour in CART_HOURS]
        super().__init__(placeholder="Choose a cart hour", options=options)

    async def callback(self, interaction: discord.Interaction):
        guild = interaction.guild
        settings = await get_settings(guild.id)

        if not settings:
            return await interaction.response.send_message(
                "This server is not set up yet. An admin must use `/cart setup` first.",
                ephemeral=True
            )

        existing = await get_user(guild.id, interaction.user.id)
        if existing:
            return await interaction.response.send_message(
                "⚠️ You are already in the queue.",
                ephemeral=True
            )

        rows = await get_queue(guild.id)
        position = len(rows) + 1
        cart_date = default_cart_date(position)
        hour = self.values[0]

        async with aiosqlite.connect(DB) as db:
            await db.execute(
                """
                INSERT INTO carts(guild_id, user_id, position, hour, cart_date)
                VALUES(?,?,?,?,?)
                """,
                (guild.id, interaction.user.id, position, hour, cart_date)
            )
            await db.commit()

        await refresh_queue_panel(guild)
        await refresh_officer_panel(guild)
        await log_action(guild, f"{interaction.user.mention} joined the queue at `{cart_date} {hour} UTC`")

        await interaction.response.send_message(
            f"✅ Added to queue.\n\n📅 {cart_date}\n🕒 {hour} UTC",
            ephemeral=True
        )


class JoinHourView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=60)
        self.add_item(JoinHourSelect())


class EditHourSelect(discord.ui.Select):
    def __init__(self):
        options = [discord.SelectOption(label=f"{hour} UTC", value=hour) for hour in CART_HOURS]
        super().__init__(placeholder="Choose a new hour", options=options)

    async def callback(self, interaction: discord.Interaction):
        guild = interaction.guild
        existing = await get_user(guild.id, interaction.user.id)
        if not existing:
            return await interaction.response.send_message("You are not in the queue.", ephemeral=True)

        hour = self.values[0]
        async with aiosqlite.connect(DB) as db:
            await db.execute(
                "UPDATE carts SET hour=?, reminded=0 WHERE guild_id=? AND user_id=?",
                (hour, guild.id, interaction.user.id)
            )
            await db.commit()

        await refresh_queue_panel(guild)
        await refresh_officer_panel(guild)
        await log_action(guild, f"{interaction.user.mention} changed their cart hour to `{hour} UTC`")
        await interaction.response.send_message(f"✅ Hour changed to {hour} UTC.", ephemeral=True)


class EditHourView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=60)
        self.add_item(EditHourSelect())


class CartView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Join Queue", emoji="➕", style=discord.ButtonStyle.green, custom_id="public_join_queue")
    async def join_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("Choose a cart hour:", view=JoinHourView(), ephemeral=True)

    @discord.ui.button(label="Edit Hour", emoji="✏️", style=discord.ButtonStyle.blurple, custom_id="public_edit_hour")
    async def edit_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await get_user(interaction.guild.id, interaction.user.id):
            return await interaction.response.send_message("You are not in the queue.", ephemeral=True)
        await interaction.response.send_message("Choose a new hour:", view=EditHourView(), ephemeral=True)

    @discord.ui.button(label="View Queue", emoji="📋", style=discord.ButtonStyle.secondary, custom_id="public_view_queue")
    async def view_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(embed=await build_queue_embed(interaction.guild), ephemeral=True)

    @discord.ui.button(label="Leave Queue", emoji="❌", style=discord.ButtonStyle.red, custom_id="public_leave_queue")
    async def leave_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        async with aiosqlite.connect(DB) as db:
            await db.execute(
                "DELETE FROM carts WHERE guild_id=? AND user_id=?",
                (interaction.guild.id, interaction.user.id)
            )
            await db.commit()

        await compress_queue(interaction.guild.id)
        await refresh_queue_panel(interaction.guild)
        await refresh_officer_panel(interaction.guild)
        await log_action(interaction.guild, f"{interaction.user.mention} left the queue")
        await interaction.response.send_message("❌ Removed from queue.", ephemeral=True)

# ================= BACKUP VIEW =================

class BackupView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Backup Queue", emoji="💾", style=discord.ButtonStyle.green, custom_id="public_backup_queue")
    async def backup_queue(self, interaction: discord.Interaction, button: discord.ui.Button):
        settings = await get_settings(interaction.guild.id)
        if not has_admin_access(interaction.user, settings):
            return await interaction.response.send_message("No permission.", ephemeral=True)

        backup_file = await create_guild_backup(interaction.guild.id)
        await log_action(interaction.guild, f"{interaction.user.mention} created backup `{backup_file.name}`")
        await interaction.response.send_message(f"✅ Backup created: `{backup_file.name}`", ephemeral=True)

    @discord.ui.button(label="Restore Backup", emoji="♻️", style=discord.ButtonStyle.blurple, custom_id="public_restore_backup")
    async def restore_backup(self, interaction: discord.Interaction, button: discord.ui.Button):
        settings = await get_settings(interaction.guild.id)
        if not has_admin_access(interaction.user, settings):
            return await interaction.response.send_message("No permission.", ephemeral=True)

        await create_guild_backup(interaction.guild.id)
        latest = await restore_latest_guild_backup(interaction.guild.id)

        if not latest:
            return await interaction.response.send_message("No backups found.", ephemeral=True)

        await refresh_queue_panel(interaction.guild)
        await refresh_officer_panel(interaction.guild)
        await log_action(interaction.guild, f"{interaction.user.mention} restored backup `{latest.name}`")
        await interaction.response.send_message(f"✅ Restored `{latest.name}`", ephemeral=True)

# ================= OFFICER PANEL =================

async def find_member_matches(guild: discord.Guild, query: str, queued_only: bool = False):
    query = query.strip().lower()
    if not query:
        return []

    matches = []
    seen_ids = set()

    queue_rows = await get_queue(guild.id)
    queued_ids = {row[0] for row in queue_rows}
    manual_rows = [(row[0], row[3]) for row in queue_rows if row[3]]

    def add_match(user_id: int, name: str, mention: str | None = None):
        if user_id in seen_ids:
            return
        seen_ids.add(user_id)
        matches.append({"id": user_id, "name": name, "mention": mention or name})

    if query.lstrip("<@!&>").isdigit():
        wanted_id = int(query.lstrip("<@!&>"))
        if not queued_only or wanted_id in queued_ids:
            member = guild.get_member(wanted_id)
            if member:
                add_match(member.id, member.display_name, member.mention)

    for member in guild.members:
        if member.bot:
            continue
        if queued_only and member.id not in queued_ids:
            continue

        names = [
            member.display_name.lower(),
            member.name.lower(),
            str(member.id),
        ]

        if any(query in name for name in names):
            add_match(member.id, member.display_name, member.mention)

    if queued_only:
        for manual_id, manual_name in manual_rows:
            if manual_name and query in manual_name.lower():
                add_match(manual_id, manual_name, f"**{manual_name}**")

    return matches


def format_match_list(matches):
    lines = []
    for index, match in enumerate(matches[:10], start=1):
        lines.append(f"{index}. {match['mention']}")
    return "\n".join(lines)


class ManualAddModal(discord.ui.Modal, title="Add Name Manually"):
    name = discord.ui.TextInput(label="Name", placeholder="Type the name here", required=True, max_length=50)
    cart_date = discord.ui.TextInput(label="Date", placeholder="YYYY-MM-DD", required=True, max_length=10)
    hour = discord.ui.TextInput(label="Hour UTC", placeholder="Example: 18:00", required=True, max_length=5)

    async def on_submit(self, interaction: discord.Interaction):
        date_value = str(self.cart_date).strip()
        hour_value = str(self.hour).strip()

        if not valid_date(date_value):
            return await interaction.response.send_message("Invalid date. Use YYYY-MM-DD.", ephemeral=True)
        if not valid_hour(hour_value):
            return await interaction.response.send_message("Invalid hour. Use format like 18:00.", ephemeral=True)

        rows = await get_queue(interaction.guild.id)
        position = len(rows) + 1
        manual_id = -int(datetime.now(timezone.utc).timestamp() * 1000)

        async with aiosqlite.connect(DB) as db:
            await db.execute(
                """
                INSERT INTO carts(guild_id, user_id, position, hour, cart_date, manual_name)
                VALUES(?,?,?,?,?,?)
                """,
                (interaction.guild.id, manual_id, position, hour_value, date_value, str(self.name)[:50])
            )
            await db.commit()

        await refresh_queue_panel(interaction.guild)
        await refresh_officer_panel(interaction.guild)
        await log_action(interaction.guild, f"{interaction.user.mention} added manual name `{self.name}` at `{date_value} {hour_value} UTC`")
        await interaction.response.send_message(f"✅ Added `{self.name}` at {date_value} {hour_value} UTC.", ephemeral=True)


class OfficerSearchModal(discord.ui.Modal):
    def __init__(self, action: str):
        titles = {
            "add": "Add Member",
            "remove": "Remove Member",
            "up": "Move Member Up",
            "down": "Move Member Down",
            "edit_datetime": "Edit Member Date + Hour",
        }
        super().__init__(title=titles.get(action, "Member Search"))
        self.action = action

        self.member_query = discord.ui.TextInput(
            label="Member name or ID",
            placeholder="Type first letters, full name, mention, or ID",
            required=True,
            max_length=100,
        )
        self.add_item(self.member_query)

        if action in {"add", "edit_datetime"}:
            self.cart_date = discord.ui.TextInput(
                label="Date",
                placeholder="YYYY-MM-DD",
                required=True,
                max_length=10,
            )
            self.hour = discord.ui.TextInput(
                label="Hour UTC",
                placeholder="Example: 18:00",
                required=True,
                max_length=5,
            )
            self.add_item(self.cart_date)
            self.add_item(self.hour)

    async def on_submit(self, interaction: discord.Interaction):
        settings = await get_settings(interaction.guild.id)
        if not has_admin_access(interaction.user, settings):
            return await interaction.response.send_message("No permission.", ephemeral=True)

        queued_only = self.action in {"remove", "up", "down", "edit_datetime"}
        matches = await find_member_matches(
            interaction.guild,
            str(self.member_query),
            queued_only=queued_only,
        )

        if not matches:
            where = "in the queue" if queued_only else "on this server"
            return await interaction.response.send_message(
                f"No member found {where}. Try a longer or different name.",
                ephemeral=True,
            )

        if len(matches) > 1:
            return await interaction.response.send_message(
                "Multiple matches found. Type more letters or use the exact ID:\n\n" + format_match_list(matches),
                ephemeral=True,
            )

        member_id = matches[0]["id"]
        member_label = matches[0]["mention"]

        if self.action in {"add", "edit_datetime"}:
            date_value = str(self.cart_date).strip()
            hour_value = str(self.hour).strip()

            if not valid_date(date_value):
                return await interaction.response.send_message("Invalid date. Use YYYY-MM-DD.", ephemeral=True)
            if not valid_hour(hour_value):
                return await interaction.response.send_message("Invalid hour. Use HH:00, example 18:00.", ephemeral=True)

        if self.action == "add":
            rows = await get_queue(interaction.guild.id)
            position = len(rows) + 1

            async with aiosqlite.connect(DB) as db:
                await db.execute(
                    """
                    INSERT OR REPLACE INTO carts(guild_id, user_id, position, hour, cart_date, manual_name, reminded)
                    VALUES(?,?,?,?,?,?,0)
                    """,
                    (interaction.guild.id, member_id, position, hour_value, date_value, None)
                )
                await db.commit()

            await compress_queue(interaction.guild.id)
            await refresh_queue_panel(interaction.guild)
            await refresh_officer_panel(interaction.guild)
            await log_action(interaction.guild, f"{interaction.user.mention} added {member_label} at `{date_value} {hour_value} UTC`")
            return await interaction.response.send_message(f"✅ Added {member_label} at {date_value} {hour_value} UTC.", ephemeral=True)

        if self.action == "edit_datetime":
            async with aiosqlite.connect(DB) as db:
                cursor = await db.execute(
                    "UPDATE carts SET cart_date=?, hour=?, reminded=0 WHERE guild_id=? AND user_id=?",
                    (date_value, hour_value, interaction.guild.id, member_id)
                )
                await db.commit()

            await refresh_queue_panel(interaction.guild)
            await refresh_officer_panel(interaction.guild)
            await log_action(interaction.guild, f"{interaction.user.mention} changed {member_label} to `{date_value} {hour_value} UTC`")
            return await interaction.response.send_message(f"✅ Updated {cursor.rowcount} member(s).", ephemeral=True)

        if self.action == "remove":
            async with aiosqlite.connect(DB) as db:
                cursor = await db.execute(
                    "DELETE FROM carts WHERE guild_id=? AND user_id=?",
                    (interaction.guild.id, member_id)
                )
                await db.commit()

            await compress_queue(interaction.guild.id)
            await refresh_queue_panel(interaction.guild)
            await refresh_officer_panel(interaction.guild)
            await log_action(interaction.guild, f"{interaction.user.mention} removed {member_label} from the queue")
            return await interaction.response.send_message(f"✅ Removed {cursor.rowcount} member(s).", ephemeral=True)

        if self.action in {"up", "down"}:
            moved = await move_member(interaction.guild.id, member_id, self.action)
            await refresh_queue_panel(interaction.guild)
            await refresh_officer_panel(interaction.guild)
            await log_action(interaction.guild, f"{interaction.user.mention} moved {member_label} {self.action}")
            direction_text = "up" if self.action == "up" else "down"
            return await interaction.response.send_message(
                f"✅ Moved {member_label} {direction_text}." if moved else "Could not move this member.",
                ephemeral=True,
            )


class OfficerActionButton(discord.ui.Button):
    def __init__(self, label, action, style):
        super().__init__(label=label, style=style, custom_id=f"public_officer_action_{action}")
        self.action = action

    async def callback(self, interaction: discord.Interaction):
        settings = await get_settings(interaction.guild.id)
        if not has_admin_access(interaction.user, settings):
            return await interaction.response.send_message("No permission.", ephemeral=True)

        if self.action == "add_manual":
            return await interaction.response.send_modal(ManualAddModal())

        if self.action == "backup":
            backup_file = await create_guild_backup(interaction.guild.id)
            await log_action(interaction.guild, f"{interaction.user.mention} created backup `{backup_file.name}`")
            return await interaction.response.send_message(f"✅ Backup created: `{backup_file.name}`", ephemeral=True)

        if self.action == "restore":
            await create_guild_backup(interaction.guild.id)
            latest = await restore_latest_guild_backup(interaction.guild.id)

            if not latest:
                return await interaction.response.send_message("No backups found.", ephemeral=True)

            await refresh_queue_panel(interaction.guild)
            await refresh_officer_panel(interaction.guild)
            await log_action(interaction.guild, f"{interaction.user.mention} restored backup `{latest.name}`")
            return await interaction.response.send_message(f"✅ Restored `{latest.name}`", ephemeral=True)

        return await interaction.response.send_modal(OfficerSearchModal(self.action))


class OfficerPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

        # Row 1: queue management
        self.add_item(OfficerActionButton("➕ Add", "add", discord.ButtonStyle.green))
        self.add_item(OfficerActionButton("📝 Manual", "add_manual", discord.ButtonStyle.green))
        self.add_item(OfficerActionButton("➖ Remove", "remove", discord.ButtonStyle.red))
        self.add_item(OfficerActionButton("⬆️ Up", "up", discord.ButtonStyle.secondary))
        self.add_item(OfficerActionButton("⬇️ Down", "down", discord.ButtonStyle.secondary))

        # Row 2: edit + backups
        self.add_item(OfficerActionButton("🗓 Edit Date/Hour", "edit_datetime", discord.ButtonStyle.blurple))
        self.add_item(OfficerActionButton("💾 Backup", "backup", discord.ButtonStyle.green))
        self.add_item(OfficerActionButton("♻️ Restore", "restore", discord.ButtonStyle.blurple))
# ================= COMMAND GROUP =================

cart = app_commands.Group(name="cart", description="Guild Cart commands")


@cart.command(name="setup", description="Set up the public Guild Cart bot for this server")
@app_commands.checks.has_permissions(administrator=True)
async def setup_command(
    interaction: discord.Interaction,
    cart_channel: discord.TextChannel,
    cart_role: discord.Role,
    officer_role: discord.Role,
    guildmaster_role: discord.Role,
    log_channel: discord.TextChannel | None = None,
    utc_channel: discord.VoiceChannel | None = None,
):
    await save_settings(
        interaction.guild.id,
        cart_channel.id,
        cart_role.id,
        officer_role.id,
        guildmaster_role.id,
        log_channel.id if log_channel else None,
        utc_channel.id if utc_channel else None,
    )

    await interaction.response.send_message(
        "✅ Guild Cart bot setup saved. Posting/updating the panels now...",
        ephemeral=True
    )
    await refresh_all_panels(interaction.guild)


@cart.command(name="panel", description="Post or refresh all Guild Cart panels")
async def panel_command(interaction: discord.Interaction):
    settings = await get_settings(interaction.guild.id)
    if not has_admin_access(interaction.user, settings):
        return await interaction.response.send_message("No permission.", ephemeral=True)

    if not settings:
        return await interaction.response.send_message("Run `/cart setup` first.", ephemeral=True)

    await refresh_all_panels(interaction.guild)
    await interaction.response.send_message("✅ Panels refreshed.", ephemeral=True)


@cart.command(name="join", description="Join the cart queue")
async def join_command(interaction: discord.Interaction):
    await interaction.response.send_message("Choose a cart hour:", view=JoinHourView(), ephemeral=True)


@cart.command(name="edit", description="Change your cart hour")
async def edit_command(interaction: discord.Interaction):
    if not await get_user(interaction.guild.id, interaction.user.id):
        return await interaction.response.send_message("You are not in the queue.", ephemeral=True)
    await interaction.response.send_message("Choose a new hour:", view=EditHourView(), ephemeral=True)


@cart.command(name="leave", description="Leave the cart queue")
async def leave_command(interaction: discord.Interaction):
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "DELETE FROM carts WHERE guild_id=? AND user_id=?",
            (interaction.guild.id, interaction.user.id)
        )
        await db.commit()

    await compress_queue(interaction.guild.id)
    await refresh_queue_panel(interaction.guild)
    await refresh_officer_panel(interaction.guild)
    await interaction.response.send_message("❌ Removed from queue.", ephemeral=True)


@cart.command(name="list", description="Show the cart queue")
async def list_command(interaction: discord.Interaction):
    await interaction.response.send_message(embed=await build_queue_embed(interaction.guild), ephemeral=True)


@cart.command(name="officer_add", description="Officer: add a member to the queue")
async def officer_add_command(interaction: discord.Interaction, member: discord.Member, date: str, hour: str):
    settings = await get_settings(interaction.guild.id)
    if not has_admin_access(interaction.user, settings):
        return await interaction.response.send_message("No permission.", ephemeral=True)

    if not valid_date(date):
        return await interaction.response.send_message("Invalid date. Use YYYY-MM-DD.", ephemeral=True)
    if not valid_hour(hour):
        return await interaction.response.send_message("Invalid hour. Use HH:00, example 18:00.", ephemeral=True)

    rows = await get_queue(interaction.guild.id)
    position = len(rows) + 1

    async with aiosqlite.connect(DB) as db:
        await db.execute(
            """
            INSERT OR REPLACE INTO carts(guild_id, user_id, position, hour, cart_date, manual_name, reminded)
            VALUES(?,?,?,?,?,?,0)
            """,
            (interaction.guild.id, member.id, position, hour, date, None)
        )
        await db.commit()

    await compress_queue(interaction.guild.id)
    await refresh_queue_panel(interaction.guild)
    await refresh_officer_panel(interaction.guild)
    await log_action(interaction.guild, f"{interaction.user.mention} added {member.mention} at `{date} {hour} UTC`")
    await interaction.response.send_message(f"✅ Added {member.mention} at {date} {hour} UTC.", ephemeral=True)


@cart.command(name="manual_add", description="Officer: add a manual name to the queue")
async def manual_add_command(interaction: discord.Interaction, name: str, date: str, hour: str):
    settings = await get_settings(interaction.guild.id)
    if not has_admin_access(interaction.user, settings):
        return await interaction.response.send_message("No permission.", ephemeral=True)

    if not valid_date(date):
        return await interaction.response.send_message("Invalid date. Use YYYY-MM-DD.", ephemeral=True)
    if not valid_hour(hour):
        return await interaction.response.send_message("Invalid hour. Use HH:00, example 18:00.", ephemeral=True)

    rows = await get_queue(interaction.guild.id)
    position = len(rows) + 1
    manual_id = -int(datetime.now(timezone.utc).timestamp() * 1000)

    async with aiosqlite.connect(DB) as db:
        await db.execute(
            """
            INSERT INTO carts(guild_id, user_id, position, hour, cart_date, manual_name)
            VALUES(?,?,?,?,?,?)
            """,
            (interaction.guild.id, manual_id, position, hour, date, name[:50])
        )
        await db.commit()

    await refresh_queue_panel(interaction.guild)
    await refresh_officer_panel(interaction.guild)
    await log_action(interaction.guild, f"{interaction.user.mention} added manual name `{name}` at `{date} {hour} UTC`")
    await interaction.response.send_message(f"✅ Added `{name}` at {date} {hour} UTC.", ephemeral=True)


@cart.command(name="officer_edit", description="Officer: edit a member date and hour")
async def officer_edit_command(interaction: discord.Interaction, member: discord.Member, date: str, hour: str):
    settings = await get_settings(interaction.guild.id)
    if not has_admin_access(interaction.user, settings):
        return await interaction.response.send_message("No permission.", ephemeral=True)

    if not valid_date(date):
        return await interaction.response.send_message("Invalid date. Use YYYY-MM-DD.", ephemeral=True)
    if not valid_hour(hour):
        return await interaction.response.send_message("Invalid hour. Use HH:00, example 18:00.", ephemeral=True)

    async with aiosqlite.connect(DB) as db:
        cursor = await db.execute(
            "UPDATE carts SET cart_date=?, hour=?, reminded=0 WHERE guild_id=? AND user_id=?",
            (date, hour, interaction.guild.id, member.id)
        )
        await db.commit()

    await refresh_queue_panel(interaction.guild)
    await refresh_officer_panel(interaction.guild)
    await log_action(interaction.guild, f"{interaction.user.mention} changed {member.mention} to `{date} {hour} UTC`")
    await interaction.response.send_message(f"✅ Updated {cursor.rowcount} member(s).", ephemeral=True)


@cart.command(name="officer_remove", description="Officer: remove a member from the queue")
async def officer_remove_command(interaction: discord.Interaction, member: discord.Member):
    settings = await get_settings(interaction.guild.id)
    if not has_admin_access(interaction.user, settings):
        return await interaction.response.send_message("No permission.", ephemeral=True)

    async with aiosqlite.connect(DB) as db:
        cursor = await db.execute(
            "DELETE FROM carts WHERE guild_id=? AND user_id=?",
            (interaction.guild.id, member.id)
        )
        await db.commit()

    await compress_queue(interaction.guild.id)
    await refresh_queue_panel(interaction.guild)
    await refresh_officer_panel(interaction.guild)
    await log_action(interaction.guild, f"{interaction.user.mention} removed {member.mention} from the queue")
    await interaction.response.send_message(f"✅ Removed {cursor.rowcount} member(s).", ephemeral=True)


@cart.command(name="settings", description="Show this server's Guild Cart settings")
async def settings_command(interaction: discord.Interaction):
    settings = await get_settings(interaction.guild.id)
    if not settings:
        return await interaction.response.send_message("This server is not set up yet.", ephemeral=True)

    await interaction.response.send_message(
        f"**Guild Cart Settings**\n"
        f"Cart channel: <#{settings['cart_channel_id']}>\n"
        f"Cart role: <@&{settings['cart_role_id']}>\n"
        f"Officer role: <@&{settings['officer_role_id']}>\n"
        f"Guild Master role: <@&{settings['guildmaster_role_id']}>\n"
        f"Log channel: {('<#' + str(settings['log_channel_id']) + '>') if settings['log_channel_id'] else 'Not set'}\n"
        f"UTC channel: {('<#' + str(settings['utc_channel_id']) + '>') if settings['utc_channel_id'] else 'Not set'}",
        ephemeral=True
    )

bot.tree.add_command(cart)

# ================= TASKS =================

@tasks.loop(time=MAINTENANCE_TIMES)
async def cleanup_task():
    try:
        deleted = await cleanup_expired_carts()

        async with aiosqlite.connect(DB) as db:
            cursor = await db.execute("SELECT guild_id FROM guild_settings")
            guild_ids = [row[0] for row in await cursor.fetchall()]

        for guild_id in guild_ids:
            guild = bot.get_guild(guild_id)
            if guild:
                await refresh_queue_panel(guild)
                await refresh_officer_panel(guild)

        if deleted:
            print(f"Cleaned up {deleted} expired cart(s).")
        else:
            print("Queue maintenance refresh completed.")

    except Exception:
        traceback.print_exc()


@tasks.loop(time=NIGHTLY_BACKUP_TIME)
async def nightly_backup_task():
    try:
        async with aiosqlite.connect(DB) as db:
            cursor = await db.execute("SELECT guild_id FROM guild_settings")
            guild_ids = [row[0] for row in await cursor.fetchall()]

        for guild_id in guild_ids:
            backup_file = await create_guild_backup(guild_id)
            print(f"Nightly backup created: {backup_file.name}")

    except Exception:
        traceback.print_exc()


@tasks.loop(minutes=1)
async def reminder_task():
    await cleanup_expired_carts()
    now = datetime.now(timezone.utc)
    current_time = now.strftime("%H:%M")
    today = today_utc().isoformat()

    async with aiosqlite.connect(DB) as db:
        settings_cursor = await db.execute(
            "SELECT guild_id, cart_channel_id, cart_role_id FROM guild_settings"
        )
        all_settings = await settings_cursor.fetchall()

        if current_time == "00:00":
            await db.execute("UPDATE carts SET reminded=0")
            await db.commit()

        for guild_id, cart_channel_id, cart_role_id in all_settings:
            guild = bot.get_guild(guild_id)
            if not guild:
                continue

            channel = guild.get_channel(cart_channel_id)
            if not channel:
                continue

            role = guild.get_role(cart_role_id) if cart_role_id else None

            cursor = await db.execute(
                """
                SELECT user_id, hour, manual_name, cart_date, reminded
                FROM carts
                WHERE guild_id=? AND cart_date=?
                """,
                (guild_id, today)
            )
            rows = await cursor.fetchall()

            for uid, hour, manual_name, cart_date, reminded in rows:
                hour_dt = datetime.strptime(hour, "%H:%M")
                reminder_time = (hour_dt - timedelta(minutes=15)).strftime("%H:%M")

                if reminder_time != current_time or reminded:
                    continue

                owner = f"**{manual_name}**" if manual_name else f"<@{uid}>"
                role_ping = role.mention if role else ""

                try:
                    await channel.send(
                        f"{role_ping}\n\n"
                        f"🔔 **Guild Cart Reminder**\n\n"
                        f"📅 {cart_date}\n"
                        f"🕒 {hour} UTC\n\n"
                        f"Current owner: {owner}\n\n"
                        f"Today's cart starts in 15 minutes!"
                    )

                    await db.execute(
                        "UPDATE carts SET reminded=1 WHERE guild_id=? AND user_id=?",
                        (guild_id, uid)
                    )
                    await db.commit()

                except Exception:
                    traceback.print_exc()


@tasks.loop(minutes=1)
async def update_utc_channels():
    now = datetime.now(timezone.utc)
    minute = (now.minute // 15) * 15
    utc_time = f"{now.hour:02d}:{minute:02d}"
    new_name = f"🕒 UTC {utc_time}"

    async with aiosqlite.connect(DB) as db:
        cursor = await db.execute(
            "SELECT guild_id, utc_channel_id FROM guild_settings WHERE utc_channel_id IS NOT NULL"
        )
        rows = await cursor.fetchall()

    for guild_id, utc_channel_id in rows:
        guild = bot.get_guild(guild_id)
        if not guild:
            continue
        channel = guild.get_channel(utc_channel_id)
        if channel and channel.name != new_name:
            try:
                await channel.edit(name=new_name)
            except discord.Forbidden:
                # Bot has no access or lacks Manage Channels for this UTC channel.
                # Ignore to avoid log spam; fix by giving the bot Manage Channels or leaving utc_channel empty in /cart setup.
                pass
            except Exception:
                traceback.print_exc()

# ================= READY / ERRORS =================

@bot.event
async def on_ready():
    print("=" * 50)
    print(f"Logged in as {bot.user}")
    print("=" * 50)

    try:
        bot.add_view(CartView())
        bot.add_view(BackupView())
        bot.add_view(OfficerPanelView())
        print("Persistent views loaded.")
    except Exception:
        traceback.print_exc()

    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} commands.")
    except Exception:
        traceback.print_exc()

    async with aiosqlite.connect(DB) as db:
        cursor = await db.execute("SELECT guild_id FROM guild_settings")
        guild_ids = [row[0] for row in await cursor.fetchall()]

    for guild_id in guild_ids:
        guild = bot.get_guild(guild_id)
        if guild:
            try:
                await refresh_all_panels(guild)
            except Exception:
                traceback.print_exc()

    if not reminder_task.is_running():
        reminder_task.start()

    if not update_utc_channels.is_running():
        update_utc_channels.start()

    if not cleanup_task.is_running():
        cleanup_task.start()

    if not nightly_backup_task.is_running():
        nightly_backup_task.start()


@bot.event
async def on_error(event, *args, **kwargs):
    traceback.print_exc()


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error):
    traceback.print_exception(type(error), error, error.__traceback__)

    try:
        if interaction.response.is_done():
            await interaction.followup.send("Something went wrong.", ephemeral=True)
        else:
            await interaction.response.send_message("Something went wrong.", ephemeral=True)
    except Exception:
        pass

# ================= START =================

async def main():
    if not TOKEN:
        raise RuntimeError("DISCORD_TOKEN is missing in .env")

    await init_db()

    async with bot:
        await bot.start(TOKEN)


if __name__ == "__main__":
    asyncio.run(main())
