import os
import asyncio
import logging
import random
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ======================
# ENV / CONFIG
# ======================
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}

TZ = ZoneInfo("America/Argentina/Ushuaia")

TASKS_PER_DAY = int(os.getenv("TASKS_PER_DAY", "3"))
LEVEL_STEP_DAYS = 4
MAX_LEVEL = 4

MIN_WITHDRAW_USD_CENTS = 500  # $5.00

if not BOT_TOKEN:
    raise RuntimeError("Falta BOT_TOKEN en variables de entorno")
if not DATABASE_URL:
    raise RuntimeError("Falta DATABASE_URL en variables de entorno")


# ======================
# HELPERS
# ======================
def now_local() -> datetime:
    return datetime.now(TZ)

def today_local() -> date:
    return now_local().date()

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def format_usd_from_cents(cents: int) -> str:
    return f"${cents/100:.2f}"

def compute_level(streak_days: int) -> int:
    lvl = 1 + (max(0, streak_days) // LEVEL_STEP_DAYS)
    return min(max(lvl, 1), MAX_LEVEL)


# ======================
# UI MENUS
# ======================
def inline_menu(user_id: int) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton("📅 Tareas", callback_data="menu:tareas"),
         InlineKeyboardButton("💰 Saldo", callback_data="menu:saldo")],
        [InlineKeyboardButton("🏅 Nivel", callback_data="menu:nivel"),
         InlineKeyboardButton("💸 Retirar", callback_data="menu:retirar")],
        [InlineKeyboardButton("📜 Historial", callback_data="menu:historial"),
         InlineKeyboardButton("ℹ️ Ayuda", callback_data="menu:ayuda")],
    ]
    if is_admin(user_id):
        buttons.append([InlineKeyboardButton("⚙️ Admin", callback_data="admin:panel")])
    return InlineKeyboardMarkup(buttons)

def back_to_menu(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Menú", callback_data="menu:home")]])

def next_after_task_kb(after_idx: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➡️ Siguiente tarea", callback_data=f"task:next:{after_idx}")],
        [InlineKeyboardButton("📅 Ver tareas", callback_data="menu:tareas"),
         InlineKeyboardButton("🏠 Menú", callback_data="menu:home")],
    ])


# ======================
# DB INIT
# ======================
async def init_db(app: Application):
    def _setup():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
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
                    pending_withdraw_id BIGINT
                );
                """)

                cur.execute("""
                CREATE TABLE IF NOT EXISTS task_catalog (
                    id BIGSERIAL PRIMARY KEY,
                    emoji TEXT NOT NULL,
                    title TEXT NOT NULL,
                    type TEXT NOT NULL,      -- checkin | quiz | link
                    content TEXT,            -- quiz: answer ; link: url
                    weight INT NOT NULL DEFAULT 10,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """)

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
                CREATE TABLE IF NOT EXISTS daily_tasks (
                    id BIGSERIAL PRIMARY KEY,
                    day DATE NOT NULL,
                    idx INT NOT NULL,
                    kind TEXT NOT NULL,          -- catalog | campaign
                    catalog_id BIGINT REFERENCES task_catalog(id),
                    campaign_id BIGINT REFERENCES campaigns(id),
                    emoji TEXT NOT NULL,
                    title TEXT NOT NULL,
                    type TEXT NOT NULL,          -- checkin | quiz | link | campaign_link
                    payload TEXT,                -- quiz: answer=xxx
                    link_url TEXT,
                    UNIQUE(day, idx)
                );
                """)

                cur.execute("""
                CREATE TABLE IF NOT EXISTS task_completions (
                    user_id BIGINT NOT NULL REFERENCES users(user_id),
                    task_id BIGINT NOT NULL REFERENCES daily_tasks(id),
                    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, task_id)
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

                cur.execute("""
                CREATE TABLE IF NOT EXISTS withdrawals (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(user_id),
                    amount_usd_cents INT NOT NULL,
                    payout_details TEXT,
                    status TEXT NOT NULL DEFAULT 'awaiting_details', -- awaiting_details | pending | paid | rejected
                    admin_note TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """)

                cur.execute("""
                CREATE TABLE IF NOT EXISTS activity_log (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(user_id),
                    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    kind TEXT NOT NULL,
                    title TEXT NOT NULL,
                    meta TEXT
                );
                """)

            conn.commit()

    await asyncio.to_thread(_setup)


# ======================
# DB OPS
# ======================
def log_activity(conn, user_id: int, kind: str, title: str, meta: str = ""):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO activity_log (user_id, kind, title, meta) VALUES (%s,%s,%s,%s)",
            (user_id, kind, title, meta),
        )
    conn.commit()

def ensure_user(conn, user):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO users (user_id, username, first_name, last_active_date)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_active_date = EXCLUDED.last_active_date
        """, (user.id, user.username, user.first_name, today_local()))
    conn.commit()

def get_user(conn, user_id: int):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM users WHERE user_id=%s", (user_id,))
        return cur.fetchone()

def daily_tasks_exist(conn, day: date) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM daily_tasks WHERE day=%s LIMIT 1", (day,))
        return cur.fetchone() is not None

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

def apply_streak_if_day_completed(conn, user_id: int, day: date):
    if not all_tasks_done(conn, user_id, day):
        u = get_user(conn, user_id)
        return (False, u["streak_days"], u["level"])

    with conn.cursor() as cur:
        cur.execute("SELECT last_completed_date, streak_days, level FROM users WHERE user_id=%s", (user_id,))
        row = cur.fetchone()
        last_done = row["last_completed_date"]
        streak = row["streak_days"]
        old_level = row["level"]

        if last_done == day:
            return (False, streak, old_level)

        if last_done == (day - timedelta(days=1)):
            streak += 1
        else:
            streak = 1

        new_level = compute_level(streak)

        cur.execute("""
            UPDATE users
            SET last_completed_date=%s, streak_days=%s, level=%s
            WHERE user_id=%s
        """, (day, streak, new_level, user_id))

    conn.commit()
    log_activity(conn, user_id, "streak", f"🔥 Racha: {streak} días", "")
    if new_level != old_level:
        log_activity(conn, user_id, "level", f"🏅 Subió a nivel {new_level}", "")
    return (True, streak, new_level)


# ======================
# CAMPAIGNS PAYOUT (simple)
# ======================
def get_active_campaign(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT * FROM campaigns
            WHERE is_active=TRUE
            ORDER BY created_at DESC
            LIMIT 1
        """)
        return cur.fetchone()

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

    base = max(1, remaining_budget // remaining_needed)
    bonus = max(0, min(user_level - 1, 3))
    desired = base + bonus

    max_allowed = remaining_budget - (remaining_needed - 1) * 1
    return max(1, min(desired, max_allowed))

def try_pay_campaign(conn, campaign_id: int, user_id: int, user_level: int) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM campaign_payouts WHERE campaign_id=%s AND user_id=%s", (campaign_id, user_id))
        if cur.fetchone():
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
    log_activity(conn, user_id, "earn", f"💵 Ganó {format_usd_from_cents(payout)}", f"campaign={campaign_id}")
    return payout


# ======================
# DAILY TASK GENERATION
# ======================
def create_daily_tasks(conn, day: date):
    campaign = get_active_campaign(conn)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, emoji, title, type, content, weight
            FROM task_catalog
            WHERE is_active=TRUE
        """)
        catalog = cur.fetchall()

    rng = random.Random(day.toordinal())
    selected = []

    if campaign:
        selected.append({
            "kind": "campaign",
            "campaign_id": campaign["id"],
            "emoji": "🎯",
            "title": campaign["name"],
            "type": "campaign_link",
            "content": campaign["link_url"],
        })

    remaining = max(0, TASKS_PER_DAY - len(selected))
    pool = list(catalog)

    for _ in range(min(remaining, len(pool))):
        weights = [max(1, int(r["weight"])) for r in pool]
        choice = rng.choices(pool, weights=weights, k=1)[0]
        selected.append({
            "kind": "catalog",
            "catalog_id": choice["id"],
            "emoji": choice["emoji"],
            "title": choice["title"],
            "type": (choice["type"] or "").lower(),
            "content": (choice["content"] or "").strip(),
        })
        pool = [x for x in pool if x["id"] != choice["id"]]

    with conn.cursor() as cur:
        for idx, t in enumerate(selected, start=1):
            payload = None
            link_url = None
            ttype = (t["type"] or "").lower()

            if ttype == "quiz":
                payload = f"answer={t.get('content','').strip().lower()}"
            elif ttype in ("link", "campaign_link"):
                link_url = t.get("content")

            cur.execute("""
                INSERT INTO daily_tasks (day, idx, kind, catalog_id, campaign_id, emoji, title, type, payload, link_url)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (day, idx) DO NOTHING
            """, (
                day, idx, t["kind"],
                t.get("catalog_id"), t.get("campaign_id"),
                t["emoji"], t["title"], ttype,
                payload, link_url
            ))
    conn.commit()


# ======================
# TASK UI
# ======================
def task_list_kb(tasks: list[dict]) -> InlineKeyboardMarkup:
    rows = []
    for t in tasks:
        label = f"{t['emoji']} {t['idx']}. {t['title']}"
        if t["done"]:
            rows.append([InlineKeyboardButton(f"✅ {label}", callback_data="noop")])
            continue

        ttype = (t["type"] or "").lower()
        if ttype in ("link", "campaign_link"):
            rows.append([
                InlineKeyboardButton("🔗 Abrir", url=(t.get("link_url") or "")),
                InlineKeyboardButton("✅ Confirmar", callback_data=f"task:confirm:{t['id']}")
            ])
        elif ttype == "checkin":
            rows.append([InlineKeyboardButton(f"✅ {label}", callback_data=f"task:do:{t['id']}")])
        elif ttype == "quiz":
            rows.append([InlineKeyboardButton(f"🧠 {label}", callback_data=f"task:quiz:{t['id']}")])
        else:
            rows.append([InlineKeyboardButton(label, callback_data="noop")])

    rows.append([InlineKeyboardButton("⬅️ Menú", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)

def next_pending_task(conn, user_id: int, day: date, after_idx: int):
    tasks = list_daily_tasks(conn, user_id, day)
    pending = [t for t in tasks if not t["done"]]
    if not pending:
        return None
    after = [t for t in pending if t["idx"] > after_idx]
    return after[0] if after else pending[0]


# ======================
# WITHDRAWALS
# ======================
def create_withdrawal(conn, user_id: int, amount_cents: int) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE users
            SET balance_usd_cents = balance_usd_cents - %s,
                held_usd_cents = held_usd_cents + %s
            WHERE user_id=%s AND balance_usd_cents >= %s
        """, (amount_cents, amount_cents, user_id, amount_cents))
        if cur.rowcount != 1:
            conn.rollback()
            raise RuntimeError("Saldo insuficiente.")

        cur.execute("""
            INSERT INTO withdrawals (user_id, amount_usd_cents, status)
            VALUES (%s,%s,'awaiting_details')
            RETURNING id
        """, (user_id, amount_cents))
        wid = cur.fetchone()["id"]

        cur.execute("UPDATE users SET pending_withdraw_id=%s WHERE user_id=%s", (wid, user_id))

    conn.commit()
    log_activity(conn, user_id, "withdraw", f"💸 Solicitó retiro {format_usd_from_cents(amount_cents)}", f"id={wid}")
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
            SET payout_details=%s, status='pending', updated_at=NOW()
            WHERE id=%s
            RETURNING id, amount_usd_cents
        """, (details, wid))
        wrow = cur.fetchone()

        cur.execute("UPDATE users SET pending_withdraw_id=NULL WHERE user_id=%s", (user_id,))

    conn.commit()
    log_activity(conn, user_id, "withdraw", f"✅ Envió datos retiro #{wrow['id']}", "")
    return wrow


# ======================
# TEXTS
# ======================
WELCOME = (
    "✨ **Bienvenid@ {name}!**\n\n"
    "Espero que estés teniendo un hermoso día 🌷\n\n"
    "📌 Te invito a realizar tus tareas diariamente.\n"
    f"🏅 Subís de nivel cada **{LEVEL_STEP_DAYS} días** consecutivos.\n"
    "💸 Retiro mínimo **$5**.\n\n"
    "👇 Elegí una opción:"
)

def help_text():
    return (
        "ℹ️ **Ayuda**\n\n"
        "• **📅 Tareas:** Abrí y confirmá.\n"
        "• Si completás el día → suma racha.\n"
        f"• Cada **{LEVEL_STEP_DAYS} días** consecutivos subís de nivel.\n\n"
        f"💸 Retiro mínimo: **{format_usd_from_cents(MIN_WITHDRAW_USD_CENTS)}**\n"
        "Cuando retires, te voy a pedir: alias/CBU/banco/titular/DNI."
    )


# ======================
# VIEWS
# ======================
async def view_tasks(user_id: int):
    day = today_local()

    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            if not daily_tasks_exist(conn, day):
                create_daily_tasks(conn, day)
            user = get_user(conn, user_id)
            tasks = list_daily_tasks(conn, user_id, day)
            return user, tasks

    user, tasks = await asyncio.to_thread(_work)

    done = sum(1 for t in tasks if t["done"])
    total = len(tasks)

    text = (
        f"📅 **Tareas de hoy ({day.strftime('%d/%m/%Y')})**\n"
        f"🔥 Racha: **{user['streak_days']}** | 🏅 Nivel: **{user['level']}**\n"
        f"✅ Completadas: **{done}/{total}**\n\n"
        "Abrí y confirmá 👇"
    )
    return text, task_list_kb(tasks)

async def view_saldo(user_id: int):
    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            u = get_user(conn, user_id)
            return u
    u = await asyncio.to_thread(_work)
    return (
        f"💰 **Disponible:** {format_usd_from_cents(u['balance_usd_cents'])}\n"
        f"🔒 **Retenido:** {format_usd_from_cents(u.get('held_usd_cents', 0))}\n\n"
        f"💸 Mínimo retiro: **{format_usd_from_cents(MIN_WITHDRAW_USD_CENTS)}**"
    )

async def view_nivel(user_id: int):
    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            u = get_user(conn, user_id)
            return u
    u = await asyncio.to_thread(_work)
    next_target = u["level"] * LEVEL_STEP_DAYS
    return (
        f"🏅 **Nivel:** {u['level']} / {MAX_LEVEL}\n"
        f"🔥 **Racha:** {u['streak_days']} días\n\n"
        f"🎯 Próximo objetivo: **{next_target}** días consecutivos."
    )

async def view_historial(user_id: int):
    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT ts, title FROM activity_log
                    WHERE user_id=%s
                    ORDER BY id DESC
                    LIMIT 25
                """, (user_id,))
                return cur.fetchall()

    rows = await asyncio.to_thread(_work)
    if not rows:
        return "📜 Todavía no hay actividad registrada."

    lines = ["📜 **Historial (últimos 25)**\n"]
    for r in rows:
        ts = r["ts"].astimezone(TZ).strftime("%d/%m %H:%M")
        lines.append(f"• `{ts}` — {r['title']}")
    return "\n".join(lines)


# ======================
# ADMIN
# ======================
def admin_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Crear tarea", callback_data="admin:add_task"),
         InlineKeyboardButton("🎯 Crear campaña", callback_data="admin:add_campaign")],
        [InlineKeyboardButton("⬅️ Menú", callback_data="menu:home")]
    ])


# ======================
# COMMANDS
# ======================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user

    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            ensure_user(conn, u)
            log_activity(conn, u.id, "user", "👋 /start", "")
            if not daily_tasks_exist(conn, today_local()):
                create_daily_tasks(conn, today_local())
            return get_user(conn, u.id)

    user_row = await asyncio.to_thread(_work)

    await update.message.reply_text(
        WELCOME.format(name=(u.first_name or "")) +
        f"\n\n🔥 **Racha:** {user_row['streak_days']} | 🏅 **Nivel:** {user_row['level']}/{MAX_LEVEL}",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=inline_menu(u.id)
    )

async def cmd_whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    await update.message.reply_text(
        f"✅ Bot activo\nuser_id: {u.id}\nadmin: {is_admin(u.id)}",
        reply_markup=inline_menu(u.id)
    )

async def cmd_retirar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user

    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            ensure_user(conn, u)
            user_row = get_user(conn, u.id)

            if user_row["pending_withdraw_id"]:
                return ("pending", user_row["pending_withdraw_id"], user_row)

            if user_row["balance_usd_cents"] < MIN_WITHDRAW_USD_CENTS:
                return ("min", None, user_row)

            wid = create_withdrawal(conn, u.id, user_row["balance_usd_cents"])
            user_row2 = get_user(conn, u.id)
            return ("created", wid, user_row2)

    status, wid, user_row = await asyncio.to_thread(_work)

    if status == "min":
        await update.message.reply_text(
            f"💸 Mínimo retiro: **{format_usd_from_cents(MIN_WITHDRAW_USD_CENTS)}**\n"
            f"Disponible: **{format_usd_from_cents(user_row['balance_usd_cents'])}**",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=inline_menu(u.id)
        )
        return

    if status == "pending":
        await update.message.reply_text(
            f"⏳ Ya tenés un retiro en curso (ID #{wid}).\n\n"
            "Enviá tus datos en **un solo mensaje**:\n"
            "**Alias/CBU | Banco | Titular | DNI**",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=inline_menu(u.id)
        )
        return

    await update.message.reply_text(
        f"✅ Retiro creado (ID #{wid}).\n"
        f"🔒 El saldo pasó a **RETENIDO**.\n\n"
        "Ahora enviá tus datos en **un solo mensaje**:\n"
        "**Alias/CBU | Banco | Titular | DNI**",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=inline_menu(u.id)
    )


# ======================
# CALLBACKS
# ======================
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    u = q.from_user
    data = q.data or ""

    if data == "noop":
        return

    if data == "menu:home":
        await q.edit_message_text("📌 **Menú**", parse_mode=ParseMode.MARKDOWN, reply_markup=inline_menu(u.id))
        return

    if data == "menu:tareas":
        text, kb = await view_tasks(u.id)
        await q.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
        return

    if data == "menu:saldo":
        await q.edit_message_text(await view_saldo(u.id), parse_mode=ParseMode.MARKDOWN, reply_markup=back_to_menu(u.id))
        return

    if data == "menu:nivel":
        await q.edit_message_text(await view_nivel(u.id), parse_mode=ParseMode.MARKDOWN, reply_markup=back_to_menu(u.id))
        return

    if data == "menu:historial":
        await q.edit_message_text(await view_historial(u.id), parse_mode=ParseMode.MARKDOWN, reply_markup=back_to_menu(u.id))
        return

    if data == "menu:ayuda":
        await q.edit_message_text(help_text(), parse_mode=ParseMode.MARKDOWN, reply_markup=back_to_menu(u.id))
        return

    if data == "menu:retirar":
        await q.edit_message_text(
            f"💸 Para retirar usá **/retirar**\n\nMínimo: **{format_usd_from_cents(MIN_WITHDRAW_USD_CENTS)}**",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=back_to_menu(u.id)
        )
        return

    # ADMIN
    if data == "admin:panel":
        if not is_admin(u.id):
            await q.answer("⛔ No autorizado", show_alert=True)
            return
        await q.edit_message_text("⚙️ **Admin**", parse_mode=ParseMode.MARKDOWN, reply_markup=admin_kb())
        return

    if data == "admin:add_task":
        if not is_admin(u.id):
            await q.answer("⛔ No autorizado", show_alert=True)
            return
        context.user_data["admin_flow"] = {"type": "add_task"}
        await q.edit_message_text(
            "➕ **Crear tarea**\n\nPegá así:\n"
            "`emoji | titulo | tipo | contenido`\n\n"
            "Tipos: `checkin`, `quiz`, `link`\n"
            "Ej:\n`📌 | Visitar enlace | link | https://ejemplo.com`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=admin_kb()
        )
        return

    if data == "admin:add_campaign":
        if not is_admin(u.id):
            await q.answer("⛔ No autorizado", show_alert=True)
            return
        context.user_data["admin_flow"] = {"type": "add_campaign"}
        await q.edit_message_text(
            "🎯 **Crear campaña**\n\nPegá así:\n"
            "`nombre | link | presupuesto_usd | objetivo`\n\n"
            "Ej:\n`Campaña 1 | https://ejemplo.com | 10 | 200`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=admin_kb()
        )
        return

    # TASK ACTIONS
    if data.startswith("task:"):
        parts = data.split(":")
        action = parts[1]

        if action == "next":
            after_idx = int(parts[2])
            day = today_local()

            def _work():
                with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                    ensure_user(conn, u)
                    return next_pending_task(conn, u.id, day, after_idx)

            nxt = await asyncio.to_thread(_work)
            if not nxt:
                await q.edit_message_text(
                    "🎉 **Listo!** Completaste todas las tareas de hoy.",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=inline_menu(u.id)
                )
                return

            ttype = (nxt["type"] or "").lower()
            if ttype in ("link", "campaign_link"):
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔗 Abrir", url=(nxt.get("link_url") or "")),
                     InlineKeyboardButton("✅ Confirmar", callback_data=f"task:confirm:{nxt['id']}")],
                    [InlineKeyboardButton("📅 Ver tareas", callback_data="menu:tareas"),
                     InlineKeyboardButton("🏠 Menú", callback_data="menu:home")]
                ])
            elif ttype == "checkin":
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Completar", callback_data=f"task:do:{nxt['id']}")],
                    [InlineKeyboardButton("📅 Ver tareas", callback_data="menu:tareas"),
                     InlineKeyboardButton("🏠 Menú", callback_data="menu:home")]
                ])
            else:
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("📅 Ver tareas", callback_data="menu:tareas")]])

            await q.edit_message_text(
                f"➡️ **Siguiente tarea**\n\n{nxt['emoji']} **{nxt['idx']}. {nxt['title']}**",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=kb
            )
            return

        # actions with task_id
        task_id = int(parts[2])
        day = today_local()

        def _load():
            with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                ensure_user(conn, u)
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM daily_tasks WHERE id=%s", (task_id,))
                    t = cur.fetchone()
                user_row = get_user(conn, u.id)
                return t, user_row

        t, user_row = await asyncio.to_thread(_load)
        if not t or t["day"] != day:
            await q.answer("⚠️ Esa tarea no es de hoy.", show_alert=True)
            return

        ttype = (t["type"] or "").lower()

        if action == "do" and ttype == "checkin":
            def _do():
                with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                    inserted = complete_task(conn, u.id, task_id)
                    if inserted:
                        log_activity(conn, u.id, "task", f"✅ Tarea confirmada: {t['title']}", "")
                    streaked, streak, lvl = apply_streak_if_day_completed(conn, u.id, day)
                    return inserted, streaked, streak, lvl

            inserted, streaked, streak, lvl = await asyncio.to_thread(_do)
            msg = "✅ **Tarea confirmada.**" if inserted else "⚠️ Ya estaba confirmada."
            if streaked:
                msg += f"\n🔥 Racha: **{streak}** | 🏅 Nivel: **{lvl}**"
            await q.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=next_after_task_kb(t["idx"]))
            return

        if action == "quiz" and ttype == "quiz":
            context.user_data["pending_quiz"] = {"task_id": task_id}
            await q.edit_message_text(
                f"🧠 **{t['title']}**\n\nEscribí tu respuesta ahora.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=back_to_menu(u.id)
            )
            return

        if action == "confirm" and ttype in ("link", "campaign_link"):
            def _confirm():
                with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                    inserted = complete_task(conn, u.id, task_id)
                    paid = 0

                    if inserted:
                        log_activity(conn, u.id, "task", f"✅ Tarea confirmada: {t['title']}", "")
                        user2 = get_user(conn, u.id)
                        if ttype == "campaign_link" and t["campaign_id"]:
                            paid = try_pay_campaign(conn, int(t["campaign_id"]), u.id, int(user2["level"]))

                    streaked, streak, lvl = apply_streak_if_day_completed(conn, u.id, day)
                    return inserted, paid, streaked, streak, lvl

            inserted, paid, streaked, streak, lvl = await asyncio.to_thread(_confirm)

            msg = "✅ **Tarea confirmada.**" if inserted else "⚠️ Ya estaba confirmada."
            if paid > 0:
                msg += f"\n💵 Ganaste **{format_usd_from_cents(paid)}**"
            if streaked:
                msg += f"\n🔥 Racha: **{streak}** | 🏅 Nivel: **{lvl}**"

            await q.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=next_after_task_kb(t["idx"]))
            return

        await q.answer("⚠️ Acción inválida.", show_alert=True)
        return


# ======================
# TEXT INPUT
# ======================
async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    msg = (update.message.text or "").strip()
    if not msg:
        return

    # QUIZ ANSWER
    pending = context.user_data.get("pending_quiz")
    if isinstance(pending, dict):
        task_id = int(pending.get("task_id", 0))
        day = today_local()

        def _work():
            with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                ensure_user(conn, u)
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM daily_tasks WHERE id=%s", (task_id,))
                    t = cur.fetchone()
                return t

        t = await asyncio.to_thread(_work)
        if not t or t["day"] != day:
            context.user_data.pop("pending_quiz", None)
            await update.message.reply_text("⚠️ Ese quiz ya no es válido.", reply_markup=inline_menu(u.id))
            return

        expected = (t["payload"] or "").split("answer=")[-1].strip().lower()
        if msg.lower() != expected:
            await update.message.reply_text("❌ Incorrecto. Probá otra vez.")
            return

        def _complete():
            with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                inserted = complete_task(conn, u.id, task_id)
                if inserted:
                    log_activity(conn, u.id, "task", f"✅ Tarea confirmada: {t['title']}", "")
                streaked, streak, lvl = apply_streak_if_day_completed(conn, u.id, day)
                return inserted, streaked, streak, lvl, t["idx"]

        inserted, streaked, streak, lvl, idx = await asyncio.to_thread(_complete)
        context.user_data.pop("pending_quiz", None)

        out = "✅ **Tarea confirmada.**" if inserted else "⚠️ Ya estaba confirmada."
        if streaked:
            out += f"\n🔥 Racha: **{streak}** | 🏅 Nivel: **{lvl}**"

        await update.message.reply_text(out, parse_mode=ParseMode.MARKDOWN, reply_markup=next_after_task_kb(idx))
        return

    # ADMIN FLOWS
    admin_flow = context.user_data.get("admin_flow")
    if isinstance(admin_flow, dict) and is_admin(u.id):
        flow_type = admin_flow.get("type")

        if flow_type == "add_task":
            parts = [p.strip() for p in msg.split("|")]
            if len(parts) < 3:
                await update.message.reply_text("⚠️ Usá: `emoji | titulo | tipo | contenido`", parse_mode=ParseMode.MARKDOWN)
                return

            emoji = parts[0]
            title = parts[1]
            ttype = parts[2].lower()
            content = parts[3] if len(parts) >= 4 else ""

            if ttype not in ("checkin", "quiz", "link"):
                await update.message.reply_text("⚠️ Tipo inválido: checkin/quiz/link")
                return

            def _insert():
                with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO task_catalog (emoji, title, type, content, weight, is_active)
                            VALUES (%s,%s,%s,%s,10,TRUE)
                            RETURNING id
                        """, (emoji, title, ttype, content))
                        tid = cur.fetchone()["id"]
                    conn.commit()
                    log_activity(conn, u.id, "admin", f"⚙️ Creó tarea #{tid}: {title}", "")
                    return tid

            tid = await asyncio.to_thread(_insert)
            context.user_data.pop("admin_flow", None)
            await update.message.reply_text(f"✅ Tarea creada #{tid}", reply_markup=inline_menu(u.id))
            return

        if flow_type == "add_campaign":
            parts = [p.strip() for p in msg.split("|")]
            if len(parts) < 4:
                await update.message.reply_text("⚠️ Usá: `nombre | link | presupuesto_usd | objetivo`", parse_mode=ParseMode.MARKDOWN)
                return

            name = parts[0]
            link = parts[1]
            try:
                budget_usd = float(parts[2].replace(",", "."))
                goal = int(parts[3])
            except Exception:
                await update.message.reply_text("⚠️ Presupuesto u objetivo inválidos.")
                return

            budget_cents = int(round(budget_usd * 100))
            if budget_cents <= 0 or goal <= 0:
                await update.message.reply_text("⚠️ presupuesto y objetivo deben ser > 0.")
                return

            def _insert():
                with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE campaigns SET is_active=FALSE WHERE is_active=TRUE;")
                        cur.execute("""
                            INSERT INTO campaigns (name, link_url, budget_usd_cents, goal_completions, is_active)
                            VALUES (%s,%s,%s,%s,TRUE)
                            RETURNING id
                        """, (name, link, budget_cents, goal))
                        cid = cur.fetchone()["id"]
                    conn.commit()
                    log_activity(conn, u.id, "admin", f"⚙️ Creó campaña #{cid}: {name}", "")
                    return cid

            cid = await asyncio.to_thread(_insert)
            context.user_data.pop("admin_flow", None)
            await update.message.reply_text(f"✅ Campaña creada #{cid}", reply_markup=inline_menu(u.id))
            return

    # WITHDRAW DETAILS (si tiene retiro pendiente)
    def _attach():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            ensure_user(conn, u)
            wrow = attach_withdrawal_details(conn, u.id, msg)
            return wrow

    wrow = await asyncio.to_thread(_attach)
    if wrow:
        # notificar admin
        if ADMIN_IDS:
            admin_id = next(iter(ADMIN_IDS))
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=(
                        f"🔔 **Nuevo retiro pendiente**\n"
                        f"Usuario: `{u.id}` (@{u.username})\n"
                        f"Monto: **{format_usd_from_cents(wrow['amount_usd_cents'])}**\n"
                        f"ID retiro: **#{wrow['id']}**\n\n"
                        f"Datos:\n{msg}"
                    ),
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception:
                pass

        await update.message.reply_text(
            f"✅ Datos recibidos.\nTu retiro **#{wrow['id']}** quedó **pendiente**.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=inline_menu(u.id)
        )
        return

    await update.message.reply_text("📌 Tocá una opción del menú 👇", reply_markup=inline_menu(u.id))


# ======================
# ERROR HANDLER
# ======================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logging.exception("Unhandled exception", exc_info=context.error)


# ======================
# MAIN
# ======================
def main():
    logging.basicConfig(level=logging.INFO)

    app = Application.builder().token(BOT_TOKEN).post_init(init_db).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("whoami", cmd_whoami))
    app.add_handler(CommandHandler("retirar", cmd_retirar))

    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    app.add_error_handler(error_handler)

    app.run_polling()

if __name__ == "__main__":
    main()