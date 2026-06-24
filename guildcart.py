import os
import json
import asyncio
import logging
import traceback
from datetime import datetime, timezone, timedelta
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
PAGE_SIZE = 25

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


def paginate(items, page: int):
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    return items[start:end]

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
    rows = await get_queue(guild.id)

    embed = discord.Embed(
        title="🚚 Guild Cart Queue (UTC)",
        colour=discord.Colour.green()
    )

    if not rows:
        embed.description = "Queue is empty."
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
    settings = await get_settings(guild.id)
    if not settings or not settings.get("cart_channel_id"):
        return

    channel = guild.get_channel(settings["cart_channel_id"])
    if not channel:
        return

    embed = discord.Embed(
        title="💾 Backup Panel",
        description="💾 Backup this server queue\n♻️ Restore latest server backup",
        colour=discord.Colour.blurple(),
    )

    await upsert_message(channel, settings, "backup_message_id", embed, BackupView())


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
            "Select one or more members.\n"
            "Then choose Add, Remove, Move Up, Move Down, or Edit Date + Hour."
        ),
        colour=discord.Colour.red(),
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

class MemberSelect(discord.ui.Select):
    def __init__(self, members, page: int = 0):
        self.members = members
        self.page = page
        page_members = paginate(members, page)

        options = []
        for member_data in page_members:
            label = member_data.get("name", "Unknown")
            value = str(member_data.get("id"))
            options.append(discord.SelectOption(label=label[:100], value=value))

        if not options:
            options.append(discord.SelectOption(label="No members found", value="none"))

        super().__init__(
            placeholder=f"Select members (page {page + 1})",
            min_values=1,
            max_values=len(options),
            options=options,
            custom_id=f"public_member_select_page_{page}"
        )

    async def callback(self, interaction: discord.Interaction):
        if self.values[0] == "none":
            return await interaction.response.send_message("No members available.", ephemeral=True)

        self.view.selected_members = [int(value) for value in self.values]

        name_lookup = {int(item["id"]): item["name"] for item in self.view.members}
        selected_names = [name_lookup.get(int(value), f"<@{value}>") for value in self.values]

        await interaction.response.send_message("Selected: " + ", ".join(selected_names), ephemeral=True)


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


class EditDateTimeModal(discord.ui.Modal, title="Edit Cart Date + Hour"):
    new_date = discord.ui.TextInput(label="New date", placeholder="Example: 2026-07-01", required=True, max_length=10)
    new_hour = discord.ui.TextInput(label="New hour UTC", placeholder="Example: 18:00", required=True, max_length=5)

    def __init__(self, member_ids):
        super().__init__()
        self.member_ids = member_ids

    async def on_submit(self, interaction: discord.Interaction):
        date_value = str(self.new_date).strip()
        hour_value = str(self.new_hour).strip()

        if not valid_date(date_value):
            return await interaction.response.send_message("Invalid date. Use YYYY-MM-DD.", ephemeral=True)
        if not valid_hour(hour_value):
            return await interaction.response.send_message("Invalid hour. Use format like 18:00.", ephemeral=True)

        updated = 0
        async with aiosqlite.connect(DB) as db:
            for member_id in self.member_ids:
                cursor = await db.execute(
                    "UPDATE carts SET cart_date=?, hour=?, reminded=0 WHERE guild_id=? AND user_id=?",
                    (date_value, hour_value, interaction.guild.id, member_id)
                )
                updated += cursor.rowcount
            await db.commit()

        await refresh_queue_panel(interaction.guild)
        await refresh_officer_panel(interaction.guild)
        await log_action(interaction.guild, f"{interaction.user.mention} changed date/hour for {updated} member(s) to `{date_value} {hour_value} UTC`")
        await interaction.response.send_message(f"✅ Updated {updated} member(s).", ephemeral=True)


class AddMemberDateHourModal(discord.ui.Modal, title="Add Member Date/Hour"):
    cart_date = discord.ui.TextInput(label="Date", placeholder="YYYY-MM-DD", required=True, max_length=10)
    hour = discord.ui.TextInput(label="Hour UTC", placeholder="Example: 18:00", required=True, max_length=5)

    def __init__(self, member_ids):
        super().__init__()
        self.member_ids = member_ids

    async def on_submit(self, interaction: discord.Interaction):
        date_value = str(self.cart_date).strip()
        hour_value = str(self.hour).strip()

        if not valid_date(date_value):
            return await interaction.response.send_message("Invalid date. Use YYYY-MM-DD.", ephemeral=True)
        if not valid_hour(hour_value):
            return await interaction.response.send_message("Invalid hour. Use format like 18:00.", ephemeral=True)

        added = 0
        rows = await get_queue(interaction.guild.id)
        existing_ids = {row[0] for row in rows}
        position = len(rows)

        async with aiosqlite.connect(DB) as db:
            for member_id in self.member_ids:
                if member_id in existing_ids:
                    continue
                position += 1
                await db.execute(
                    """
                    INSERT OR IGNORE INTO carts(guild_id, user_id, position, hour, cart_date)
                    VALUES(?,?,?,?,?)
                    """,
                    (interaction.guild.id, member_id, position, hour_value, date_value)
                )
                added += 1
            await db.commit()

        await compress_queue(interaction.guild.id)
        await refresh_queue_panel(interaction.guild)
        await refresh_officer_panel(interaction.guild)
        await log_action(interaction.guild, f"{interaction.user.mention} added {added} member(s) at `{date_value} {hour_value} UTC`")
        await interaction.response.send_message(f"✅ Added {added} member(s).", ephemeral=True)


class ActionSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label="Add Member", value="add"),
            discord.SelectOption(label="Add Name Manually", value="add_manual"),
            discord.SelectOption(label="Remove Member", value="remove"),
            discord.SelectOption(label="Move Up", value="up"),
            discord.SelectOption(label="Move Down", value="down"),
            discord.SelectOption(label="Edit Date + Hour", value="edit_datetime"),
        ]
        super().__init__(placeholder="Select action...", options=options, custom_id="public_action_select")

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        action = self.values[0]

        if action == "add_manual":
            return await view.handle_action(interaction, action, [])

        if not view.selected_members:
            return await interaction.response.send_message("Select at least one member first.", ephemeral=True)

        await view.handle_action(interaction, action, view.selected_members)


class OfficerPanelView(discord.ui.View):
    def __init__(self, members, page: int = 0):
        super().__init__(timeout=None)
        self.members = members
        self.page = page
        self.selected_members = []
        self.refresh_ui()

    def refresh_ui(self):
        self.clear_items()
        self.add_item(MemberSelect(self.members, self.page))
        self.add_item(ActionSelect())
        self.add_item(PrevButton(self))
        self.add_item(NextButton(self))

    async def handle_action(self, interaction: discord.Interaction, action: str, member_ids: list[int]):
        settings = await get_settings(interaction.guild.id)
        if not has_admin_access(interaction.user, settings):
            return await interaction.response.send_message("No permission.", ephemeral=True)

        if action == "add_manual":
            return await interaction.response.send_modal(ManualAddModal())

        if action == "edit_datetime":
            return await interaction.response.send_modal(EditDateTimeModal(member_ids))

        if action == "add":
            return await interaction.response.send_modal(AddMemberDateHourModal(member_ids))

        if action == "remove":
            removed = 0
            async with aiosqlite.connect(DB) as db:
                for member_id in member_ids:
                    cursor = await db.execute(
                        "DELETE FROM carts WHERE guild_id=? AND user_id=?",
                        (interaction.guild.id, member_id)
                    )
                    removed += cursor.rowcount
                await db.commit()

            await compress_queue(interaction.guild.id)
            await refresh_queue_panel(interaction.guild)
            await refresh_officer_panel(interaction.guild)
            await log_action(interaction.guild, f"{interaction.user.mention} removed {removed} member(s) from the queue")
            return await interaction.response.send_message(f"✅ Removed {removed} member(s).", ephemeral=True)

        if action == "up":
            moved = 0
            for member_id in member_ids:
                if await move_member(interaction.guild.id, member_id, "up"):
                    moved += 1
            await refresh_queue_panel(interaction.guild)
            await refresh_officer_panel(interaction.guild)
            await log_action(interaction.guild, f"{interaction.user.mention} moved {moved} member(s) up")
            return await interaction.response.send_message(f"✅ Moved {moved} member(s) up.", ephemeral=True)

        if action == "down":
            moved = 0
            for member_id in reversed(member_ids):
                if await move_member(interaction.guild.id, member_id, "down"):
                    moved += 1
            await refresh_queue_panel(interaction.guild)
            await refresh_officer_panel(interaction.guild)
            await log_action(interaction.guild, f"{interaction.user.mention} moved {moved} member(s) down")
            return await interaction.response.send_message(f"✅ Moved {moved} member(s) down.", ephemeral=True)


class PrevButton(discord.ui.Button):
    def __init__(self, panel):
        super().__init__(label="⬅️ Prev", style=discord.ButtonStyle.gray, custom_id="public_officer_prev")
        self.panel = panel

    async def callback(self, interaction: discord.Interaction):
        settings = await get_settings(interaction.guild.id)
        if not has_admin_access(interaction.user, settings):
            return await interaction.response.send_message("No permission.", ephemeral=True)

        if self.panel.page > 0:
            self.panel.page -= 1
        self.panel.selected_members = []
        self.panel.refresh_ui()
        await interaction.response.edit_message(view=self.panel)


class NextButton(discord.ui.Button):
    def __init__(self, panel):
        super().__init__(label="➡️ Next", style=discord.ButtonStyle.gray, custom_id="public_officer_next")
        self.panel = panel

    async def callback(self, interaction: discord.Interaction):
        settings = await get_settings(interaction.guild.id)
        if not has_admin_access(interaction.user, settings):
            return await interaction.response.send_message("No permission.", ephemeral=True)

        max_page = max((len(self.panel.members) - 1) // PAGE_SIZE, 0)
        if self.panel.page < max_page:
            self.panel.page += 1
        self.panel.selected_members = []
        self.panel.refresh_ui()
        await interaction.response.edit_message(view=self.panel)

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

@tasks.loop(minutes=1)
async def reminder_task():
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
