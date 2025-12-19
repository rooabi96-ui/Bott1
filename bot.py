import os
import asyncio
import logging
import random
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)

import psycopg
from psycopg.rows import dict_row

# ========= ENV / CONFIG =========
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}

TASKS_PER_DAY = int(os.getenv("TASKS_PER_DAY", "3"))
TZ = ZoneInfo("America/Argentina/Ushuaia")

LEVEL_STEP_DAYS = 4
MAX_LEVEL = 10

MIN_WITHDRAW_USD_CENTS = 500  # $5.00
MIN_PAYOUT_USD_CENTS = 1      # piso pago campaña (1 centavo)

if not BOT_TOKEN:
    raise RuntimeError("Falta BOT_TOKEN en variables de entorno")
if not DATABASE_URL:
    raise RuntimeError("Falta DATABASE_URL en variables de entorno")


# ========= HELPERS =========
def now_local() -> datetime:
    return datetime.now(TZ)

def today_local() -> date:
    return now_local().date()

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def format_usd_from_cents(cents: int) -> str:
    return f"${cents/100:.2f}"

def compute_level(streak_completed_days: int) -> int:
    lvl = 1 + (max(0, streak_completed_days) // LEVEL_STEP_DAYS)
    return min(max(lvl, 1), MAX_LEVEL)

def make_daily_code(day: date, idx: int, seed: int) -> str:
    return f"{day.strftime('%d%m')}{day.weekday()}{idx}{seed % 10}X"

def inline_menu(user_is_admin: bool) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton("📅 Tareas", callback_data="menu:tareas"),
         InlineKeyboardButton("💰 Saldo", callback_data="menu:saldo")],
        [InlineKeyboardButton("🏅 Nivel", callback_data="menu:nivel"),
         InlineKeyboardButton("🎁 Ingresar código", callback_data="menu:codigo")],
        [InlineKeyboardButton("💸 Retirar", callback_data="menu:retirar"),
         InlineKeyboardButton("📜 Historial", callback_data="menu:historial")],
        [InlineKeyboardButton("ℹ️ Ayuda", callback_data="menu:ayuda")],
    ]

    if user_is_admin:
        buttons += [
            [InlineKeyboardButton("🧩 Catálogo", callback_data="admin:catalogo"),
             InlineKeyboardButton("🎯 Campañas", callback_data="admin:campanias")],
            [InlineKeyboardButton("➕ Crear tarea", callback_data="admin:help_addtask"),
             InlineKeyboardButton("➕ Crear campaña", callback_data="admin:help_campaign")],
            [InlineKeyboardButton("💳 Retiros", callback_data="admin:retiros"),
             InlineKeyboardButton("📊 Stats", callback_data="admin:stats")],
        ]

    return InlineKeyboardMarkup(buttons)

def back_to_menu_btn(user_is_admin: bool) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Volver al menú", callback_data="menu:home")]
    ])

async def safe_edit_or_send(q, text: str, reply_markup: InlineKeyboardMarkup | None = None):
    """
    Intenta editar el mismo mensaje (UX pro).
    Si falla (mensaje viejo, etc), manda uno nuevo.
    """
    try:
        await q.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
    except Exception:
        await q.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)


# ========= DB INIT / MIGRATIONS =========
async def init_db(app: Application):
    def _setup():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                # 1) USERS (si no existe)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    last_active_date DATE,
                    last_completed_date DATE,
                    streak_days INT NOT NULL DEFAULT 0,
                    level INT NOT NULL DEFAULT 1,
                    balance_usd_cents INT NOT NULL DEFAULT 0,
                    held_usd_cents INT NOT NULL DEFAULT 0,
                    withdraw_notify_date DATE,
                    pending_withdraw_id BIGINT
                );
                """)

                # 2) MIGRACIONES (por si la tabla ya existía sin columnas nuevas)
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT;")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name TEXT;")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_active_date DATE;")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_completed_date DATE;")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS streak_days INT NOT NULL DEFAULT 0;")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS level INT NOT NULL DEFAULT 1;")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS balance_usd_cents INT NOT NULL DEFAULT 0;")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS held_usd_cents INT NOT NULL DEFAULT 0;")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS withdraw_notify_date DATE;")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS pending_withdraw_id BIGINT;")

                # TASK CATALOG
                cur.execute("""
                CREATE TABLE IF NOT EXISTS task_catalog (
                    id BIGSERIAL PRIMARY KEY,
                    emoji TEXT NOT NULL,
                    title TEXT NOT NULL,
                    type TEXT NOT NULL,
                    content TEXT,
                    weight INT NOT NULL DEFAULT 10,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """)

                # CAMPAIGNS
                cur.execute("""
                CREATE TABLE IF NOT EXISTS campaigns (
                    id BIGSERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    link_url TEXT NOT NULL,
                    budget_usd_cents INT NOT NULL,
                    goal_completions INT NOT NULL,
                    completed_count INT NOT NULL DEFAULT 0,
                    spent_usd_cents INT NOT NULL DEFAULT 0,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """)

                cur.execute("""
                CREATE TABLE IF NOT EXISTS campaign_payouts (
                    campaign_id BIGINT NOT NULL REFERENCES campaigns(id),
                    user_id BIGINT NOT NULL REFERENCES users(user_id),
                    paid_usd_cents INT NOT NULL,
                    paid_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (campaign_id, user_id)
                );
                """)

                # DAILY TASKS
                cur.execute("""
                CREATE TABLE IF NOT EXISTS daily_tasks (
                    id BIGSERIAL PRIMARY KEY,
                    day DATE NOT NULL,
                    idx INT NOT NULL,
                    kind TEXT NOT NULL,
                    catalog_id BIGINT REFERENCES task_catalog(id),
                    campaign_id BIGINT REFERENCES campaigns(id),
                    emoji TEXT NOT NULL,
                    title TEXT NOT NULL,
                    type TEXT NOT NULL,
                    payload TEXT,
                    link_url TEXT,
                    UNIQUE(day, idx)
                );
                """)

                # COMPLETIONS
                cur.execute("""
                CREATE TABLE IF NOT EXISTS task_completions (
                    user_id BIGINT NOT NULL REFERENCES users(user_id),
                    task_id BIGINT NOT NULL REFERENCES daily_tasks(id),
                    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, task_id)
                );
                """)

                # WITHDRAWALS
                cur.execute("""
                CREATE TABLE IF NOT EXISTS withdrawals (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(user_id),
                    amount_usd_cents INT NOT NULL,
                    payout_details TEXT,
                    status TEXT NOT NULL DEFAULT 'awaiting_details',
                    admin_note TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """)

            conn.commit()

    await asyncio.to_thread(_setup)


# ========= DB OPS =========
def ensure_user(conn, user):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO users (user_id, username, first_name, last_active_date)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_active_date = EXCLUDED.last_active_date;
        """, (user.id, user.username, user.first_name, today_local()))
    conn.commit()

def get_user(conn, user_id: int):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM users WHERE user_id=%s", (user_id,))
        return cur.fetchone()

def set_last_active(conn, user_id: int):
    with conn.cursor() as cur:
        cur.execute("UPDATE users SET last_active_date=%s WHERE user_id=%s", (today_local(), user_id))
    conn.commit()

def list_catalog(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM task_catalog ORDER BY id DESC")
        return cur.fetchall()

def add_catalog_task(conn, emoji: str, title: str, ttype: str, content: str, weight: int):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO task_catalog (emoji, title, type, content, weight, is_active)
            VALUES (%s, %s, %s, %s, %s, TRUE)
            RETURNING id
        """, (emoji, title, ttype, content, weight))
        new_id = cur.fetchone()["id"]
    conn.commit()
    return new_id

def del_catalog_task(conn, task_id: int):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM task_catalog WHERE id=%s", (task_id,))
        deleted = cur.rowcount
    conn.commit()
    return deleted

def toggle_catalog_task(conn, task_id: int):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE task_catalog
            SET is_active = NOT is_active
            WHERE id=%s
            RETURNING is_active
        """, (task_id,))
        row = cur.fetchone()
    conn.commit()
    return row["is_active"] if row else None

def create_campaign(conn, name: str, link_url: str, budget_cents: int, goal: int):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO campaigns (name, link_url, budget_usd_cents, goal_completions, is_active)
            VALUES (%s, %s, %s, %s, TRUE)
            RETURNING id
        """, (name, link_url, budget_cents, goal))
        cid = cur.fetchone()["id"]
    conn.commit()
    return cid

def list_campaigns(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM campaigns ORDER BY id DESC")
        return cur.fetchall()

def end_campaign(conn, campaign_id: int):
    with conn.cursor() as cur:
        cur.execute("UPDATE campaigns SET is_active=FALSE WHERE id=%s", (campaign_id,))
        changed = cur.rowcount
    conn.commit()
    return changed

def get_active_campaign(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT * FROM campaigns
            WHERE is_active=TRUE
            ORDER BY created_at DESC
            LIMIT 1
        """)
        return cur.fetchone()

def daily_tasks_exist(conn, day: date) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM daily_tasks WHERE day=%s LIMIT 1", (day,))
        return cur.fetchone() is not None

def complete_task(conn, user_id: int, task_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO task_completions (user_id, task_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """, (user_id, task_id))
        inserted = (cur.rowcount == 1)
    conn.commit()
    return inserted

def list_daily_tasks(conn, user_id: int, day: date):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT t.*,
                   EXISTS(
                      SELECT 1 FROM task_completions c
                      WHERE c.user_id=%s AND c.task_id=t.id
                   ) AS done
            FROM daily_tasks t
            WHERE t.day=%s
            ORDER BY t.idx ASC
        """, (user_id, day))
        return cur.fetchall()

def all_tasks_done(conn, user_id: int, day: date) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS total FROM daily_tasks WHERE day=%s", (day,))
        total = cur.fetchone()["total"]
        cur.execute("""
            SELECT COUNT(*) AS done
            FROM task_completions c
            JOIN daily_tasks t ON t.id=c.task_id
            WHERE c.user_id=%s AND t.day=%s
        """, (user_id, day))
        done = cur.fetchone()["done"]
    return total > 0 and done == total

def apply_streak_if_day_completed(conn, user_id: int, day: date) -> bool:
    if not all_tasks_done(conn, user_id, day):
        return False

    with conn.cursor() as cur:
        cur.execute("SELECT last_completed_date, streak_days FROM users WHERE user_id=%s", (user_id,))
        row = cur.fetchone()
        last_done = row["last_completed_date"]
        streak = row["streak_days"]

        if last_done == day:
            return False

        if last_done == (day - timedelta(days=1)):
            streak += 1
        else:
            streak = 1

        level = compute_level(streak)

        cur.execute("""
            UPDATE users
            SET last_completed_date=%s, streak_days=%s, level=%s
            WHERE user_id=%s
        """, (day, streak, level, user_id))

    conn.commit()
    return True

def create_daily_tasks(conn, day: date, tasks_per_day: int):
    campaign = get_active_campaign(conn)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, emoji, title, type, content, weight
            FROM task_catalog
            WHERE is_active = TRUE
        """)
        catalog = cur.fetchall()

    rng = random.Random(day.toordinal())
    selected = []

    if campaign:
        selected.append({
            "kind": "campaign",
            "campaign_id": campaign["id"],
            "emoji": "🎯",
            "title": f"Campaña: {campaign['name']}",
            "type": "campaign_link",
            "link_url": campaign["link_url"],
        })

    remaining_slots = max(0, tasks_per_day - len(selected))
    if catalog and remaining_slots:
        pool = catalog[:]
        for _ in range(min(remaining_slots, len(pool))):
            weights = [max(1, int(r["weight"])) for r in pool]
            choice = rng.choices(pool, weights=weights, k=1)[0]
            selected.append({
                "kind": "catalog",
                "catalog_id": choice["id"],
                "emoji": choice["emoji"],
                "title": choice["title"],
                "type": choice["type"].lower(),
                "content": (choice["content"] or "").strip(),
            })
            pool = [x for x in pool if x["id"] != choice["id"]]

    with conn.cursor() as cur:
        for idx, t in enumerate(selected, start=1):
            payload = None
            link_url = None

            if t["type"] == "quiz":
                payload = f"answer={t.get('content','').lower()}"
            elif t["type"] in ("link", "campaign_link"):
                seed = int(t.get("catalog_id") or t.get("campaign_id") or 0)
                code = make_daily_code(day, idx, seed)
                payload = f"code={code}"
                link_url = t.get("link_url") or t.get("content")

            cur.execute("""
                INSERT INTO daily_tasks (day, idx, kind, catalog_id, campaign_id, emoji, title, type, payload, link_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (day, idx) DO NOTHING
            """, (
                day, idx, t["kind"],
                t.get("catalog_id"), t.get("campaign_id"),
                t["emoji"], t["title"], t["type"],
                payload, link_url
            ))

    conn.commit()


# ========= CAMPAIGN PAYOUT =========
def compute_campaign_payout(conn, campaign_id: int, user_level: int) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM campaigns WHERE id=%s", (campaign_id,))
        c = cur.fetchone()
        if not c or not c["is_active"]:
            return 0

    remaining_budget = max(0, c["budget_usd_cents"] - c["spent_usd_cents"])
    remaining_needed = max(0, c["goal_completions"] - c["completed_count"])
    if remaining_budget <= 0 or remaining_needed <= 0:
        return 0

    base = max(MIN_PAYOUT_USD_CENTS, remaining_budget // remaining_needed)
    bonus = min(max(user_level - 1, 0), 5)
    desired = base + bonus

    max_allowed = remaining_budget - (remaining_needed - 1) * MIN_PAYOUT_USD_CENTS
    payout = min(desired, max_allowed)
    return max(MIN_PAYOUT_USD_CENTS, int(payout))

def try_pay_campaign(conn, campaign_id: int, user_id: int, user_level: int) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM campaign_payouts WHERE campaign_id=%s AND user_id=%s", (campaign_id, user_id))
        if cur.fetchone():
            return 0

    with conn.cursor() as cur:
        cur.execute("SELECT * FROM campaigns WHERE id=%s", (campaign_id,))
        c = cur.fetchone()
        if not c or not c["is_active"]:
            return 0

    payout = compute_campaign_payout(conn, campaign_id, user_level)
    if payout <= 0:
        return 0

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO campaign_payouts (campaign_id, user_id, paid_usd_cents)
            VALUES (%s, %s, %s)
        """, (campaign_id, user_id, payout))

        cur.execute("""
            UPDATE users
            SET balance_usd_cents = balance_usd_cents + %s
            WHERE user_id=%s
        """, (payout, user_id))

        cur.execute("""
            UPDATE campaigns
            SET completed_count = completed_count + 1,
                spent_usd_cents = spent_usd_cents + %s
            WHERE id=%s
        """, (payout, campaign_id))

        cur.execute("SELECT * FROM campaigns WHERE id=%s", (campaign_id,))
        c2 = cur.fetchone()
        if c2["completed_count"] >= c2["goal_completions"] or c2["spent_usd_cents"] >= c2["budget_usd_cents"]:
            cur.execute("UPDATE campaigns SET is_active=FALSE WHERE id=%s", (campaign_id,))

    conn.commit()
    return payout


# ========= WITHDRAWALS =========
def create_withdrawal_request(conn, user_id: int, amount_cents: int) -> int:
    """
    Al crear retiro: mueve disponible -> retenido y crea withdrawal.
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE users
            SET balance_usd_cents = balance_usd_cents - %s,
                held_usd_cents = held_usd_cents + %s
            WHERE user_id=%s AND balance_usd_cents >= %s
        """, (amount_cents, amount_cents, user_id, amount_cents))

        if cur.rowcount != 1:
            conn.rollback()
            raise RuntimeError("Saldo insuficiente para retener el monto.")

        cur.execute("""
            INSERT INTO withdrawals (user_id, amount_usd_cents, status)
            VALUES (%s, %s, 'awaiting_details')
            RETURNING id
        """, (user_id, amount_cents))
        wid = cur.fetchone()["id"]

        cur.execute("""
            UPDATE users
            SET pending_withdraw_id=%s
            WHERE user_id=%s
        """, (wid, user_id))

    conn.commit()
    return wid

def attach_withdrawal_details(conn, user_id: int, details: str):
    with conn.cursor() as cur:
        cur.execute("SELECT pending_withdraw_id FROM users WHERE user_id=%s", (user_id,))
        row = cur.fetchone()
        wid = row["pending_withdraw_id"] if row else None
        if not wid:
            return None

        cur.execute("""
            UPDATE withdrawals
            SET payout_details=%s,
                status='pending',
                updated_at=NOW()
            WHERE id=%s
            RETURNING id, amount_usd_cents
        """, (details, wid))
        wrow = cur.fetchone()

        cur.execute("""
            UPDATE users
            SET pending_withdraw_id=NULL
            WHERE user_id=%s
        """, (user_id,))

    conn.commit()
    return wrow

def list_withdrawals_user(conn, user_id: int, limit: int = 10):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, amount_usd_cents, status, created_at, updated_at, admin_note
            FROM withdrawals
            WHERE user_id=%s
            ORDER BY id DESC
            LIMIT %s
        """, (user_id, limit))
        return cur.fetchall()

def list_withdrawals_pending(conn, limit: int = 30):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT w.id, w.user_id, w.amount_usd_cents, w.payout_details, w.status, w.created_at
            FROM withdrawals w
            WHERE w.status='pending'
            ORDER BY w.id ASC
            LIMIT %s
        """, (limit,))
        return cur.fetchall()


# ========= TEXT BUILDERS =========
WELCOME_TEXT = (
    "✨ **Bienvenid@ {name}!**\n\n"
    "Espero que estés teniendo un hermoso día 🌷\n\n"
    "🎯 Hacé tus tareas diariamente.\n"
    "📌 Tu **racha** solo sube si completás **TODAS** las tareas del día.\n"
    f"⏳ El nivel sube **cada {LEVEL_STEP_DAYS} días** de racha.\n\n"
    "👇 Tocá un botón del menú para empezar."
)

def tasks_extra_line(t) -> str:
    ttype = (t["type"] or "").lower()
    if ttype == "checkin":
        return " → Completá con **/checkin**"
    if ttype == "quiz":
        return " → Respondé con **/quiz <respuesta>**"
    if ttype in ("link", "campaign_link"):
        code = (t["payload"] or "").split("code=")[-1].strip()
        link = t["link_url"] or ""
        return f"\n   🔗 Link: {link}\n   🔐 Código: **{code}** → tocá **🎁 Ingresar código**"
    return ""

async def build_saldo_text(user_id: int) -> str:
    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            return get_user(conn, user_id)
    user = await asyncio.to_thread(_work)
    available = user["balance_usd_cents"]
    held = user.get("held_usd_cents", 0)
    return (
        f"💰 **Saldo disponible:** {format_usd_from_cents(available)}\n"
        f"🔒 **Saldo retenido:** {format_usd_from_cents(held)}\n\n"
        f"💸 Retiro mínimo: **{format_usd_from_cents(MIN_WITHDRAW_USD_CENTS)}**\n"
        f"🏅 Nivel: {user['level']} | 🔥 Racha: {user['streak_days']}"
    )

async def build_nivel_text(user_id: int) -> str:
    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            return get_user(conn, user_id)
    user = await asyncio.to_thread(_work)
    next_level_at = (user["level"]) * LEVEL_STEP_DAYS
    return (
        f"🏅 **Tu nivel:** {user['level']}\n"
        f"🔥 **Racha:** {user['streak_days']} días\n\n"
        f"⏳ Subís de nivel cada **{LEVEL_STEP_DAYS} días** de racha completa.\n"
        f"👉 Próximo objetivo: **{next_level_at}** días."
    )

async def build_ayuda_text() -> str:
    return (
        "ℹ️ **Cómo funciona**\n\n"
        "1) Tocá **📅 Tareas**\n"
        "2) Completá todo lo del día\n"
        "3) Si terminás todo, tu **racha** suma 1\n"
        f"4) Cada **{LEVEL_STEP_DAYS} días** de racha subís de nivel\n\n"
        "🔗 Si hay links, siempre hay **código** para validar.\n"
        "Tocá **🎁 Ingresar código** para ver cómo.\n\n"
        f"💸 Retiro mínimo: **{format_usd_from_cents(MIN_WITHDRAW_USD_CENTS)}**"
    )

async def build_tareas_text(user_id: int) -> str:
    day = today_local()

    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            if not daily_tasks_exist(conn, day):
                create_daily_tasks(conn, day, TASKS_PER_DAY)
            user = get_user(conn, user_id)
            tasks = list_daily_tasks(conn, user_id, day)
            camp = get_active_campaign(conn)
            return user, tasks, camp

    user, tasks, camp = await asyncio.to_thread(_work)
    if not tasks:
        return "⚠️ Hoy no hay tareas publicadas.\n\nSi sos admin: cargá tareas con ➕ Crear tarea."

    lines = [
        f"📅 **Tareas de hoy ({day.strftime('%d/%m/%Y')})**",
        f"🔥 Racha: **{user['streak_days']}** | 🏅 Nivel: **{user['level']}**",
        "",
    ]

    if camp:
        def _p():
            with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                return compute_campaign_payout(conn, camp["id"], user["level"])
        preview = await asyncio.to_thread(_p)

        lines.append(
            f"🎯 **Campaña activa:** {camp['name']}\n"
            f"📌 Progreso: {camp['completed_count']}/{camp['goal_completions']}\n"
            f"💵 Pago estimado actual: **{format_usd_from_cents(preview)}**\n"
        )

    for t in tasks:
        status = "✅ Hecha" if t["done"] else "⏳ Pendiente"
        lines.append(f"{t['emoji']} **{t['idx']}. {t['title']}** — {status}{tasks_extra_line(t)}")

    lines.append("\n⭐ Tu racha sube cuando completás **todas** las tareas del día.")
    return "\n".join(lines)

async def build_historial_text(user_id: int) -> str:
    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            return list_withdrawals_user(conn, user_id, limit=10)
    items = await asyncio.to_thread(_work)
    if not items:
        return "📜 Todavía no tenés retiros."

    lines = ["📜 **Historial de retiros**\n"]
    for w in items:
        st = w["status"]
        note = (w.get("admin_note") or "").strip()
        extra = f" — _{note}_" if note else ""
        lines.append(f"ID {w['id']} — **{format_usd_from_cents(w['amount_usd_cents'])}** — `{st}`{extra}")

    return "\n".join(lines)

async def build_catalogo_text() -> str:
    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            return list_catalog(conn)
    items = await asyncio.to_thread(_work)
    if not items:
        return "🧩 Catálogo vacío. Usá ➕ Crear tarea."

    lines = ["🧩 **Catálogo (admin)**\n"]
    for t in items[:40]:
        status = "✅" if t["is_active"] else "⛔"
        lines.append(f"ID {t['id']} {status} — {t['emoji']} **{t['title']}** | `{t['type']}`")
    lines.append("\n/toggle <id> | /deltask <id>")
    return "\n".join(lines)

async def build_campanias_text() -> str:
    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            return list_campaigns(conn)
    items = await asyncio.to_thread(_work)
    if not items:
        return "🎯 No hay campañas."

    lines = ["🎯 **Campañas (admin)**\n"]
    for c in items[:20]:
        st = "✅" if c["is_active"] else "⛔"
        lines.append(
            f"ID {c['id']} {st} — **{c['name']}**\n"
            f"{c['completed_count']}/{c['goal_completions']} | "
            f"{format_usd_from_cents(c['spent_usd_cents'])}/{format_usd_from_cents(c['budget_usd_cents'])}\n"
        )
    lines.append("Cerrar: /endcampaign <id>")
    return "\n".join(lines)

async def build_retiros_pendientes_text() -> str:
    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            return list_withdrawals_pending(conn, limit=25)
    items = await asyncio.to_thread(_work)
    if not items:
        return "✅ No hay retiros pendientes."

    lines = ["💳 **Retiros pendientes**\n"]
    for w in items:
        lines.append(
            f"ID {w['id']} | Usuario `{w['user_id']}` | **{format_usd_from_cents(w['amount_usd_cents'])}**\n"
            f"Datos: `{(w['payout_details'] or '')[:120]}...`"
        )
    lines.append("\nPagar: /paywithdraw <id>\nRechazar: /rejectwithdraw <id> <motivo>")
    return "\n".join(lines)

async def build_stats_text() -> str:
    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS n FROM users"); users_n = cur.fetchone()["n"]
                cur.execute("SELECT COUNT(*) AS n FROM task_catalog"); catalog_n = cur.fetchone()["n"]
                cur.execute("SELECT COUNT(*) AS n FROM campaigns"); camp_n = cur.fetchone()["n"]
                cur.execute("SELECT COUNT(*) AS n FROM withdrawals WHERE status='pending'"); pending_w = cur.fetchone()["n"]
            return users_n, catalog_n, camp_n, pending_w
    users_n, catalog_n, camp_n, pending_w = await asyncio.to_thread(_work)
    return f"📊 Users: {users_n}\n🧩 Catálogo: {catalog_n}\n🎯 Campañas: {camp_n}\n💳 Retiros pendientes: {pending_w}"


# ========= HANDLERS =========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    day = today_local()

    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            ensure_user(conn, u)
            set_last_active(conn, u.id)
            if not daily_tasks_exist(conn, day):
                create_daily_tasks(conn, day, TASKS_PER_DAY)
            user = get_user(conn, u.id)
            return user

    user = await asyncio.to_thread(_work)

    msg = WELCOME_TEXT.format(name=(u.first_name or ""))
    msg += f"\n\n🔥 **Racha:** {user['streak_days']} días\n🏅 **Nivel:** {user['level']}"
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=inline_menu(is_admin(u.id)))

async def menu_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    await update.message.reply_text(
        "📌 **Menú**\nElegí una opción 👇",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=inline_menu(is_admin(u.id))
    )

async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    await update.message.reply_text(
        f"✅ Bot activo.\n"
        f"👤 user_id: {u.id}\n"
        f"👤 username: @{u.username}\n"
        f"🔧 admin: {is_admin(u.id)}",
        reply_markup=inline_menu(is_admin(u.id))
    )

async def ayuda(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    await update.message.reply_text(
        await build_ayuda_text(),
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=inline_menu(is_admin(u.id))
    )

async def ask_code_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    await update.message.reply_text(
        "🎁 Pegá tu código así:\n\n`/go CODIGO`\n\n(El código aparece en 📅 Tareas)",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=inline_menu(is_admin(u.id))
    )

async def tareas_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    await update.message.reply_text(
        await build_tareas_text(u.id),
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=inline_menu(is_admin(u.id))
    )

async def saldo_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    await update.message.reply_text(
        await build_saldo_text(u.id),
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=inline_menu(is_admin(u.id))
    )

async def nivel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    await update.message.reply_text(
        await build_nivel_text(u.id),
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=inline_menu(is_admin(u.id))
    )

async def historial_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    await update.message.reply_text(
        await build_historial_text(u.id),
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=inline_menu(is_admin(u.id))
    )


# ========= TASK ACTION COMMANDS =========
async def checkin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    day = today_local()

    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            ensure_user(conn, u)
            set_last_active(conn, u.id)
            if not daily_tasks_exist(conn, day):
                create_daily_tasks(conn, day, TASKS_PER_DAY)

            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id FROM daily_tasks
                    WHERE day=%s AND type='checkin'
                    ORDER BY idx
                    LIMIT 1
                """, (day,))
                row = cur.fetchone()
                if not row:
                    return "no_task", None

            inserted = complete_task(conn, u.id, row["id"])
            counted = apply_streak_if_day_completed(conn, u.id, day)
            done_all = all_tasks_done(conn, u.id, day)
            user = get_user(conn, u.id)
            return "ok", (inserted, counted, done_all, user)

    status, extra = await asyncio.to_thread(_work)
    if status == "no_task":
        await update.message.reply_text("Hoy no hay check-in 🤍", reply_markup=inline_menu(is_admin(u.id)))
        return

    inserted, counted, done_all, user = extra
    msg = "✅ Check-in completado." if inserted else "✅ Ese check-in ya estaba hecho hoy."
    if done_all:
        msg += "\n🎉 ¡Completaste TODO hoy!"
        if counted:
            msg += f"\n🔥 Racha: **{user['streak_days']}** | 🏅 Nivel: **{user['level']}**"
    else:
        msg += "\nSeguimos 💪 Tocá 📅 Tareas"
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=inline_menu(is_admin(u.id)))

async def quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    day = today_local()
    ans = " ".join(context.args).strip().lower()

    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            ensure_user(conn, u)
            set_last_active(conn, u.id)
            if not daily_tasks_exist(conn, day):
                create_daily_tasks(conn, day, TASKS_PER_DAY)

            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, payload FROM daily_tasks
                    WHERE day=%s AND type='quiz'
                    ORDER BY idx
                    LIMIT 1
                """, (day,))
                row = cur.fetchone()
                if not row:
                    return "no_task", None

            expected = (row["payload"] or "").split("answer=")[-1].strip().lower()
            if not ans:
                return "need", expected
            if ans != expected:
                return "wrong", expected

            inserted = complete_task(conn, u.id, row["id"])
            counted = apply_streak_if_day_completed(conn, u.id, day)
            done_all = all_tasks_done(conn, u.id, day)
            user = get_user(conn, u.id)
            return "ok", (inserted, counted, done_all, user)

    status, extra = await asyncio.to_thread(_work)
    if status == "no_task":
        await update.message.reply_text("Hoy no hay quiz 🤍", reply_markup=inline_menu(is_admin(u.id)))
        return
    if status == "need":
        await update.message.reply_text(f"Escribí: /quiz {extra}", reply_markup=inline_menu(is_admin(u.id)))
        return
    if status == "wrong":
        await update.message.reply_text(f"❌ Incorrecto. Probá: /quiz {extra}", reply_markup=inline_menu(is_admin(u.id)))
        return

    inserted, counted, done_all, user = extra
    msg = "🧠 Quiz completado." if inserted else "🧠 Ese quiz ya estaba hecho hoy."
    if done_all:
        msg += "\n🎉 ¡Completaste TODO hoy!"
        if counted:
            msg += f"\n🔥 Racha: **{user['streak_days']}** | 🏅 Nivel: **{user['level']}**"
    else:
        msg += "\nSeguimos 💪 Tocá 📅 Tareas"
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=inline_menu(is_admin(u.id)))

async def go(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    day = today_local()
    code_in = " ".join(context.args).strip()

    async def maybe_notify_withdrawable(user_id: int):
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            user = get_user(conn, user_id)
            if not user:
                return

            if user["balance_usd_cents"] < MIN_WITHDRAW_USD_CENTS:
                return
            if user["withdraw_notify_date"] == today_local():
                return

            with conn.cursor() as cur:
                cur.execute("UPDATE users SET withdraw_notify_date=%s WHERE user_id=%s", (today_local(), user_id))
            conn.commit()

        await context.bot.send_message(
            chat_id=user_id,
            text=(
                f"🎉 ¡Llegaste al mínimo de retiro!\n\n"
                f"💰 Saldo disponible: **{format_usd_from_cents(user['balance_usd_cents'])}**\n"
                f"💸 Mínimo: **{format_usd_from_cents(MIN_WITHDRAW_USD_CENTS)}**\n\n"
                "Si querés, tocá **💸 Retirar** en el menú."
            ),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=inline_menu(is_admin(user_id))
        )

        for aid in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    chat_id=aid,
                    text=(
                        "🔔 **Usuario alcanzó mínimo de retiro**\n"
                        f"ID: `{user_id}`\n"
                        f"Disponible: **{format_usd_from_cents(user['balance_usd_cents'])}**"
                    ),
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception:
                pass

    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            ensure_user(conn, u)
            set_last_active(conn, u.id)
            if not daily_tasks_exist(conn, day):
                create_daily_tasks(conn, day, TASKS_PER_DAY)

            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, type, payload, campaign_id
                    FROM daily_tasks
                    WHERE day=%s AND type IN ('link','campaign_link')
                    ORDER BY idx
                """, (day,))
                rows = cur.fetchall()
                if not rows:
                    return ("no_task", None)

            if not code_in:
                expected_first = (rows[0]["payload"] or "").split("code=")[-1].strip()
                return ("need", expected_first)

            match = None
            for r in rows:
                expected = (r["payload"] or "").split("code=")[-1].strip()
                if code_in == expected:
                    match = r
                    break
            if not match:
                return ("wrong", None)

            inserted = complete_task(conn, u.id, match["id"])

            paid = 0
            if inserted and match["type"] == "campaign_link" and match["campaign_id"]:
                user = get_user(conn, u.id)
                paid = try_pay_campaign(conn, int(match["campaign_id"]), u.id, int(user["level"]))

            counted = apply_streak_if_day_completed(conn, u.id, day)
            done_all = all_tasks_done(conn, u.id, day)
            user = get_user(conn, u.id)
            return ("ok", (inserted, paid, counted, done_all, user))

    status, extra = await asyncio.to_thread(_work)
    if status == "no_task":
        await update.message.reply_text("Hoy no hay códigos 🤍", reply_markup=inline_menu(is_admin(u.id)))
        return
    if status == "need":
        await update.message.reply_text(f"Ingresá: /go {extra}", reply_markup=inline_menu(is_admin(u.id)))
        return
    if status == "wrong":
        await update.message.reply_text("❌ Código incorrecto. Mirá el correcto en 📅 Tareas.", reply_markup=inline_menu(is_admin(u.id)))
        return

    inserted, paid, counted, done_all, user = extra
    msg = "🔐 Código validado." if inserted else "🔐 Ese código ya estaba validado hoy."
    if paid > 0:
        msg += f"\n💸 ¡Sumaste **{format_usd_from_cents(paid)}**!"
    if done_all:
        msg += "\n🎉 ¡Completaste TODO hoy!"
        if counted:
            msg += f"\n🔥 Racha: **{user['streak_days']}** | 🏅 Nivel: **{user['level']}**"
    else:
        msg += "\nSeguimos 💪 Tocá 📅 Tareas"

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=inline_menu(is_admin(u.id)))

    if paid > 0:
        await maybe_notify_withdrawable(u.id)


# ========= RETIRO =========
async def retirar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user

    def _work_create():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            ensure_user(conn, u)
            user = get_user(conn, u.id)

            if user["pending_withdraw_id"]:
                return ("pending_exists", user["pending_withdraw_id"], user["balance_usd_cents"], user.get("held_usd_cents", 0))

            if user["balance_usd_cents"] < MIN_WITHDRAW_USD_CENTS:
                return ("not_enough", None, user["balance_usd_cents"], user.get("held_usd_cents", 0))

            wid = create_withdrawal_request(conn, u.id, user["balance_usd_cents"])
            user2 = get_user(conn, u.id)
            return ("created", wid, user2["balance_usd_cents"], user2.get("held_usd_cents", 0))

    status, wid, available, held = await asyncio.to_thread(_work_create)

    if status == "not_enough":
        await update.message.reply_text(
            f"💸 Retiro mínimo: **{format_usd_from_cents(MIN_WITHDRAW_USD_CENTS)}**\n"
            f"Disponible: **{format_usd_from_cents(available)}**\n"
            f"Retenido: **{format_usd_from_cents(held)}**\n\n"
            "Seguí completando tareas y cuando llegues, te aviso 👇",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=inline_menu(is_admin(u.id))
        )
        return

    if status == "pending_exists":
        await update.message.reply_text(
            f"⏳ Ya tenés un retiro en curso (ID {wid}).\n\n"
            "Enviame ahora tus datos de cobro en un solo mensaje:\n"
            "**Alias / CBU / Banco / Titular / DNI**",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=inline_menu(is_admin(u.id))
        )
        return

    await update.message.reply_text(
        f"✅ Solicitud de retiro creada (ID **{wid}**)\n"
        f"🔒 Tu saldo pasó a **RETENIDO** hasta que se pague.\n\n"
        f"💰 Disponible: **{format_usd_from_cents(available)}**\n"
        f"🔒 Retenido: **{format_usd_from_cents(held)}**\n\n"
        "Ahora enviame tus datos para transferirte (en un solo mensaje):\n"
        "**Alias / CBU / Banco / Titular / DNI**",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=inline_menu(is_admin(u.id))
    )

async def capture_withdraw_details_if_pending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Si el usuario tiene pending_withdraw_id, cualquier texto NO comando
    se toma como datos de cobro.
    """
    u = update.effective_user
    text = (update.message.text or "").strip()
    if not text:
        return

    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            user = get_user(conn, u.id)
            if not user or not user["pending_withdraw_id"]:
                return None
            return attach_withdrawal_details(conn, u.id, text)

    wrow = await asyncio.to_thread(_work)
    if not wrow:
        return  # No estaba pendiente, ignoramos

    wid = wrow["id"]
    amount = wrow["amount_usd_cents"]

    await update.message.reply_text(
        f"✅ Recibí tus datos.\n"
        f"🧾 Retiro ID **{wid}** por **{format_usd_from_cents(amount)}** quedó en estado **pendiente**.\n\n"
        "Te aviso cuando esté pagado 💖",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=inline_menu(is_admin(u.id))
    )

    for aid in ADMIN_IDS:
        try:
            await context.bot.send_message(
                chat_id=aid,
                text=(
                    "💳 **Nuevo retiro pendiente**\n"
                    f"Retiro ID: `{wid}`\n"
                    f"Usuario ID: `{u.id}`\n"
                    f"Monto: **{format_usd_from_cents(amount)}**\n\n"
                    f"Datos:\n`{text[:3500]}`\n\n"
                    "Pagar: /paywithdraw <id>\n"
                    "Rechazar: /rejectwithdraw <id> <motivo>"
                ),
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            pass


# ========= ADMIN COMMANDS =========
def parse_usd_amount_to_cents(s: str) -> int:
    s = s.strip().replace("$", "").replace(",", ".")
    if "." in s:
        a, b = s.split(".", 1)
        b = (b + "00")[:2]
    else:
        a, b = s, "00"
    return int(a) * 100 + int(b)

async def addtask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not is_admin(u.id):
        return
    text = update.message.text.replace("/addtask", "", 1).strip()
    if "|" not in text:
        await update.message.reply_text(
            "➕ **Crear tarea (admin)**\n\n"
            "`/addtask <emoji> <titulo> | <tipo> | <contenido> | <peso>`\n\n"
            "Tipos: `checkin` | `quiz` | `link`\n"
            "Ej:\n"
            "`/addtask ✅ Check-in diario | checkin | - | 50`\n"
            "`/addtask 🧠 Quiz rápido | quiz | mate | 30`\n"
            "`/addtask 🔗 Ver tutorial | link | https://tulink.com | 20`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=inline_menu(True)
        )
        return

    try:
        left, ttype, content, weight = [p.strip() for p in text.split("|", 3)]
        emoji, title = left.split(" ", 1)
        ttype = ttype.lower()

        if ttype not in ("checkin", "quiz", "link"):
            raise ValueError("Tipo inválido")
        if content in ("-", "none", "None"):
            content = ""
        w = max(1, int(weight))
        if ttype == "quiz" and not content:
            raise ValueError("Quiz necesita respuesta")
        if ttype == "link" and not content.startswith(("http://", "https://")):
            raise ValueError("Link necesita URL https://")

        def _work():
            with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                return add_catalog_task(conn, emoji, title.strip(), ttype, content, w)

        new_id = await asyncio.to_thread(_work)
        await update.message.reply_text(f"✅ Tarea agregada. ID: {new_id}", reply_markup=inline_menu(True))
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}", reply_markup=inline_menu(True))

async def deltask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not is_admin(u.id):
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usá: /deltask <id>", reply_markup=inline_menu(True))
        return
    task_id = int(context.args[0])

    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            return del_catalog_task(conn, task_id)

    deleted = await asyncio.to_thread(_work)
    await update.message.reply_text("✅ Eliminada." if deleted else "⚠️ No encontré ese ID.", reply_markup=inline_menu(True))

async def toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not is_admin(u.id):
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usá: /toggle <id>", reply_markup=inline_menu(True))
        return
    task_id = int(context.args[0])

    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            return toggle_catalog_task(conn, task_id)

    state = await asyncio.to_thread(_work)
    await update.message.reply_text(("✅ Activada." if state else "⛔ Pausada.") if state is not None else "⚠️ No encontré ese ID.", reply_markup=inline_menu(True))

async def newcampaign(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not is_admin(u.id):
        return
    text = update.message.text.replace("/newcampaign", "", 1).strip()
    if "|" not in text:
        await update.message.reply_text(
            "➕ **Crear campaña (admin)**\n\n"
            "`/newcampaign Nombre | https://link | 10.00 | 20`\n\n"
            "El bot ajusta el pago automáticamente para llegar al objetivo con ese presupuesto.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=inline_menu(True)
        )
        return

    try:
        name, link, budget_str, goal_str = [p.strip() for p in text.split("|", 3)]
        if not link.startswith(("http://", "https://")):
            raise ValueError("Link inválido")
        budget = parse_usd_amount_to_cents(budget_str)
        goal = int(goal_str)
        if budget <= 0 or goal <= 0:
            raise ValueError("Presupuesto/objetivo inválidos")

        def _work():
            with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                return create_campaign(conn, name, link, budget, goal)

        cid = await asyncio.to_thread(_work)
        await update.message.reply_text(f"✅ Campaña creada. ID: {cid}", reply_markup=inline_menu(True))
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}", reply_markup=inline_menu(True))

async def endcampaign_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not is_admin(u.id):
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usá: /endcampaign <id>", reply_markup=inline_menu(True))
        return
    cid = int(context.args[0])

    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            return end_campaign(conn, cid)

    changed = await asyncio.to_thread(_work)
    await update.message.reply_text("✅ Cerrada." if changed else "⚠️ No existe.", reply_markup=inline_menu(True))

async def paywithdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not is_admin(u.id):
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usá: /paywithdraw <id> (opcional: nota)", reply_markup=inline_menu(True))
        return

    wid = int(context.args[0])
    note = " ".join(context.args[1:]).strip()

    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, user_id, amount_usd_cents, status FROM withdrawals WHERE id=%s", (wid,))
                w = cur.fetchone()
                if not w:
                    return ("not_found", None, None)
                if w["status"] != "pending":
                    return ("not_pending", w["user_id"], w["amount_usd_cents"])

                cur.execute("SELECT held_usd_cents FROM users WHERE user_id=%s", (w["user_id"],))
                ur = cur.fetchone()
                if not ur:
                    return ("user_missing", w["user_id"], w["amount_usd_cents"])
                if ur["held_usd_cents"] < w["amount_usd_cents"]:
                    return ("insufficient_held", w["user_id"], w["amount_usd_cents"])

                cur.execute("""
                    UPDATE withdrawals
                    SET status='paid', admin_note=%s, updated_at=NOW()
                    WHERE id=%s AND status='pending'
                """, (note, wid))
                if cur.rowcount != 1:
                    conn.rollback()
                    return ("failed", w["user_id"], w["amount_usd_cents"])

                cur.execute("""
                    UPDATE users
                    SET held_usd_cents = held_usd_cents - %s
                    WHERE user_id=%s
                """, (w["amount_usd_cents"], w["user_id"]))

            conn.commit()
            return ("ok", w["user_id"], w["amount_usd_cents"])

    status, user_id, amount = await asyncio.to_thread(_work)
    if status == "not_found":
        await update.message.reply_text("⚠️ No existe ese retiro.", reply_markup=inline_menu(True)); return
    if status == "not_pending":
        await update.message.reply_text("⚠️ Ese retiro no está pendiente.", reply_markup=inline_menu(True)); return
    if status == "insufficient_held":
        await update.message.reply_text("⚠️ No hay retenido suficiente.", reply_markup=inline_menu(True)); return
    if status != "ok":
        await update.message.reply_text("⚠️ No pude completar.", reply_markup=inline_menu(True)); return

    await update.message.reply_text("✅ Marcado como pagado y retenido descontado.", reply_markup=inline_menu(True))
    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=f"✅ Tu retiro (ID {wid}) por **{format_usd_from_cents(amount)}** fue pagado. 💖",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=inline_menu(is_admin(user_id))
        )
    except Exception:
        pass

async def rejectwithdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not is_admin(u.id):
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usá: /rejectwithdraw <id> <motivo>", reply_markup=inline_menu(True))
        return

    wid = int(context.args[0])
    reason = " ".join(context.args[1:]).strip() or "Sin motivo informado."

    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id, amount_usd_cents, status FROM withdrawals WHERE id=%s", (wid,))
                w = cur.fetchone()
                if not w:
                    return ("not_found", None, None)
                if w["status"] not in ("pending", "awaiting_details"):
                    return ("not_rejectable", w["user_id"], w["amount_usd_cents"])

                cur.execute("""
                    UPDATE withdrawals
                    SET status='rejected', admin_note=%s, updated_at=NOW()
                    WHERE id=%s AND status IN ('pending','awaiting_details')
                """, (reason, wid))
                if cur.rowcount != 1:
                    conn.rollback()
                    return ("failed", w["user_id"], w["amount_usd_cents"])

                cur.execute("""
                    UPDATE users
                    SET held_usd_cents = GREATEST(0, held_usd_cents - %s),
                        balance_usd_cents = balance_usd_cents + %s
                    WHERE user_id=%s
                """, (w["amount_usd_cents"], w["amount_usd_cents"], w["user_id"]))

            conn.commit()
            return ("ok", w["user_id"], w["amount_usd_cents"])

    status, user_id, amount = await asyncio.to_thread(_work)
    if status == "not_found":
        await update.message.reply_text("⚠️ No existe.", reply_markup=inline_menu(True)); return
    if status == "not_rejectable":
        await update.message.reply_text("⚠️ No se puede rechazar en este estado.", reply_markup=inline_menu(True)); return
    if status != "ok":
        await update.message.reply_text("⚠️ No pude completar.", reply_markup=inline_menu(True)); return

    await update.message.reply_text("✅ Rechazado y devuelto a disponible.", reply_markup=inline_menu(True))
    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=f"⚠️ Tu retiro (ID {wid}) por **{format_usd_from_cents(amount)}** fue rechazado.\nMotivo: {reason}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=inline_menu(is_admin(user_id))
        )
    except Exception:
        pass


# ========= INLINE MENU CALLBACKS =========
async def on_menu_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    u = q.from_user
    user_is_admin = is_admin(u.id)
    data = q.data or ""

    if data == "menu:home":
        await safe_edit_or_send(q, "📌 **Menú**\nElegí una opción 👇", inline_menu(user_is_admin))
        return

    # USER
    if data == "menu:tareas":
        await safe_edit_or_send(q, await build_tareas_text(u.id), back_to_menu_btn(user_is_admin))
        return
    if data == "menu:saldo":
        await safe_edit_or_send(q, await build_saldo_text(u.id), back_to_menu_btn(user_is_admin))
        return
    if data == "menu:nivel":
        await safe_edit_or_send(q, await build_nivel_text(u.id), back_to_menu_btn(user_is_admin))
        return
    if data == "menu:ayuda":
        await safe_edit_or_send(q, await build_ayuda_text(), back_to_menu_btn(user_is_admin))
        return
    if data == "menu:codigo":
        await safe_edit_or_send(
            q,
            "🎁 Pegá tu código así:\n\n`/go CODIGO`\n\n(El código aparece en 📅 Tareas)",
            back_to_menu_btn(user_is_admin)
        )
        return
    if data == "menu:retirar":
        await safe_edit_or_send(
            q,
            "💸 Para solicitar retiro usá **/retirar**.\n\n"
            "Después mandás tus datos de cobro (alias/CBU/banco/titular/DNI) en un solo mensaje.\n\n"
            f"📌 Retiro mínimo: **{format_usd_from_cents(MIN_WITHDRAW_USD_CENTS)}**",
            back_to_menu_btn(user_is_admin)
        )
        return
    if data == "menu:historial":
        await safe_edit_or_send(q, await build_historial_text(u.id), back_to_menu_btn(user_is_admin))
        return

    # ADMIN (solo admin)
    if data.startswith("admin:") and not user_is_admin:
        await safe_edit_or_send(q, "⛔ No autorizado.", inline_menu(False))
        return

    if data == "admin:catalogo":
        await safe_edit_or_send(q, await build_catalogo_text(), back_to_menu_btn(True))
        return
    if data == "admin:campanias":
        await safe_edit_or_send(q, await build_campanias_text(), back_to_menu_btn(True))
        return
    if data == "admin:retiros":
        await safe_edit_or_send(q, await build_retiros_pendientes_text(), back_to_menu_btn(True))
        return
    if data == "admin:stats":
        await safe_edit_or_send(q, await build_stats_text(), back_to_menu_btn(True))
        return
    if data == "admin:help_addtask":
        await safe_edit_or_send(
            q,
            "➕ **Crear tarea (admin)**\n\n"
            "`/addtask <emoji> <titulo> | <tipo> | <contenido> | <peso>`\n\n"
            "Tipos: `checkin` | `quiz` | `link`\n"
            "Ej:\n"
            "`/addtask ✅ Check-in diario | checkin | - | 50`\n"
            "`/addtask 🧠 Quiz rápido | quiz | mate | 30`\n"
            "`/addtask 🔗 Ver tutorial | link | https://tulink.com | 20`",
            back_to_menu_btn(True)
        )
        return
    if data == "admin:help_campaign":
        await safe_edit_or_send(
            q,
            "➕ **Crear campaña (admin)**\n\n"
            "`/newcampaign Nombre | https://link | 10.00 | 20`\n\n"
            "El bot ajusta el pago automáticamente para llegar al objetivo con ese presupuesto.",
            back_to_menu_btn(True)
        )
        return


# ========= FALLBACK: si el usuario escribe cualquier cosa =========
async def fallback_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Si NO tiene retiro pendiente, mostramos el menú inline (no hacemos nada raro).
    """
    u = update.effective_user

    def _pending():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            user = get_user(conn, u.id)
            return bool(user and user["pending_withdraw_id"])

    if await asyncio.to_thread(_pending):
        await capture_withdraw_details_if_pending(update, context)
        return

    await update.message.reply_text(
        "📌 Tocá el menú 👇",
        reply_markup=inline_menu(is_admin(u.id))
    )


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logging.exception("Unhandled exception", exc_info=context.error)


def main():
    logging.basicConfig(level=logging.INFO)

    app = Application.builder().token(BOT_TOKEN).post_init(init_db).build()

    # Basic
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("menu", menu_cmd))
    app.add_handler(CommandHandler("whoami", whoami))
    app.add_handler(CommandHandler("ayuda", ayuda))

    # Views
    app.add_handler(CommandHandler("tareas", tareas_cmd))
    app.add_handler(CommandHandler("saldo", saldo_cmd))
    app.add_handler(CommandHandler("nivel", nivel_cmd))
    app.add_handler(CommandHandler("historial", historial_cmd))
    app.add_handler(CommandHandler("codigo", ask_code_cmd))

    # Task actions
    app.add_handler(CommandHandler("checkin", checkin))
    app.add_handler(CommandHandler("quiz", quiz))
    app.add_handler(CommandHandler("go", go))

    # Withdraw
    app.add_handler(CommandHandler("retirar", retirar))

    # Admin actions
    app.add_handler(CommandHandler("addtask", addtask))
    app.add_handler(CommandHandler("deltask", deltask))
    app.add_handler(CommandHandler("toggle", toggle))
    app.add_handler(CommandHandler("newcampaign", newcampaign))
    app.add_handler(CommandHandler("endcampaign", endcampaign_cmd))
    app.add_handler(CommandHandler("paywithdraw", paywithdraw))
    app.add_handler(CommandHandler("rejectwithdraw", rejectwithdraw))

    # Inline menu callbacks
    app.add_handler(CallbackQueryHandler(on_menu_click))

    # Any text: if pending withdraw -> capture details, else show menu
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, fallback_text))

    app.add_error_handler(error_handler)
    app.run_polling()


if __name__ == "__main__":
    main()
