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
# CONFIG / ENV
# ======================
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}

TZ = ZoneInfo("America/Argentina/Ushuaia")

TASKS_PER_DAY = int(os.getenv("TASKS_PER_DAY", "3"))  # total tareas diarias (incluye campaña si hay)
LEVEL_STEP_DAYS = 4  # cada 4 días consecutivos sube nivel
MAX_LEVEL = 4        # nivel máximo (como pediste)

MIN_WITHDRAW_USD_CENTS = 500  # $5.00 mínimo
MIN_PAYOUT_USD_CENTS = 1      # mínimo 1 centavo por tarea de campaña
MAX_DAILY_LOG_LINES = 25

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

def make_daily_code(day: date, idx: int, seed: int) -> str:
    # code para validar links/campaña (simple)
    return f"{day.strftime('%d%m')}{day.weekday()}{idx}{seed % 10}X"


# ======================
# MENUS (INLINE)
# ======================
def inline_menu(user_id: int) -> InlineKeyboardMarkup:
    # Menú para TODOS
    buttons = [
        [InlineKeyboardButton("📅 Tareas", callback_data="menu:tareas"),
         InlineKeyboardButton("💰 Saldo", callback_data="menu:saldo")],
        [InlineKeyboardButton("🏅 Nivel", callback_data="menu:nivel"),
         InlineKeyboardButton("💸 Retirar", callback_data="menu:retirar")],
        [InlineKeyboardButton("📜 Historial", callback_data="menu:historial"),
         InlineKeyboardButton("ℹ️ Ayuda", callback_data="menu:ayuda")],
    ]

    # Solo admin ve panel admin (solo vos)
    if is_admin(user_id):
        buttons.append([InlineKeyboardButton("⚙️ Admin", callback_data="admin:panel")])

    return InlineKeyboardMarkup(buttons)

def back_to_menu_btn(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Volver al menú", callback_data="menu:home")]
    ])


# ======================
# DB INIT / MIGRATIONS
# ======================
async def init_db(app: Application):
    def _setup():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                # USERS
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
                # Ensure columns (safe migrations)
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT;")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name TEXT;")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_active_date DATE;")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_completed_date DATE;")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS streak_days INT NOT NULL DEFAULT 0;")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS level INT NOT NULL DEFAULT 1;")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS balance_usd_cents INT NOT NULL DEFAULT 0;")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS held_usd_cents INT NOT NULL DEFAULT 0;")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS pending_withdraw_id BIGINT;")

                # TASK CATALOG (admin carga tareas)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS task_catalog (
                    id BIGSERIAL PRIMARY KEY,
                    emoji TEXT NOT NULL,
                    title TEXT NOT NULL,
                    type TEXT NOT NULL,      -- checkin | quiz | link
                    content TEXT,            -- quiz: respuesta, link: url
                    weight INT NOT NULL DEFAULT 10,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """)

                # CAMPAIGNS (admin carga campañas con presupuesto/objetivo)
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

                # DAILY TASKS (generadas por día)
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
                    payload TEXT,                -- quiz: answer=... | link: code=...
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

                # CAMPAIGN PAYOUTS (evita pagar 2 veces mismo usuario/campaña)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS campaign_payouts (
                    campaign_id BIGINT NOT NULL REFERENCES campaigns(id),
                    user_id BIGINT NOT NULL REFERENCES users(user_id),
                    paid_usd_cents INT NOT NULL,
                    paid_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (campaign_id, user_id)
                );
                """)

                # WITHDRAWALS
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

                # ACTIVITY LOG (historial)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS activity_log (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(user_id),
                    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    kind TEXT NOT NULL,          -- task | earn | streak | level | withdraw | admin
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
        cur.execute("""
            INSERT INTO activity_log (user_id, kind, title, meta)
            VALUES (%s, %s, %s, %s)
        """, (user_id, kind, title, meta))
    conn.commit()

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

def apply_streak_if_day_completed(conn, user_id: int, day: date) -> tuple[bool, int, int]:
    """
    Si el usuario completó TODAS las tareas del día, actualiza racha y nivel.
    """
    if not all_tasks_done(conn, user_id, day):
        return (False, 0, 0)

    with conn.cursor() as cur:
        cur.execute("SELECT last_completed_date, streak_days, level FROM users WHERE user_id=%s", (user_id,))
        row = cur.fetchone()
        last_done = row["last_completed_date"]
        streak = row["streak_days"]
        old_level = row["level"]

        # ya aplicado hoy
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
    log_activity(conn, user_id, "streak", f"🔥 Racha actual: {streak} días", "")
    if new_level != old_level:
        log_activity(conn, user_id, "level", f"🏅 Subiste a nivel {new_level}", "")

    return (True, streak, new_level)


# ======================
# CAMPAIGNS / PAYOUT
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
    """
    Paga por tarea de campaña según presupuesto restante y objetivo restante.
    Ajusta para poder llegar al objetivo (sube/baja).
    """
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM campaigns WHERE id=%s", (campaign_id,))
        c = cur.fetchone()
        if not c or not c["is_active"]:
            return 0

    remaining_budget = max(0, c["budget_usd_cents"] - c["spent_usd_cents"])
    remaining_needed = max(0, c["goal_completions"] - c["completed_count"])
    if remaining_budget <= 0 or remaining_needed <= 0:
        return 0

    # base mínimo para llegar al objetivo
    base = max(MIN_PAYOUT_USD_CENTS, remaining_budget // remaining_needed)

    # bonus por nivel (suave)
    # nivel 1: +0, nivel 2:+1, nivel3:+2, nivel4:+3
    bonus = max(0, min(user_level - 1, 3))
    desired = base + bonus

    # para no romper el objetivo: deja mínimo 1 centavo para cada completado restante
    max_allowed = remaining_budget - (remaining_needed - 1) * MIN_PAYOUT_USD_CENTS
    payout = min(desired, max_allowed)
    return max(MIN_PAYOUT_USD_CENTS, int(payout))

def try_pay_campaign(conn, campaign_id: int, user_id: int, user_level: int) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM campaign_payouts WHERE campaign_id=%s AND user_id=%s", (campaign_id, user_id))
        if cur.fetchone():
            return 0

    payout = compute_campaign_payout(conn, campaign_id, user_level)
    if payout <= 0:
        return 0

    with conn.cursor() as cur:
        # marca pago único
        cur.execute("""
            INSERT INTO campaign_payouts (campaign_id, user_id, paid_usd_cents)
            VALUES (%s, %s, %s)
        """, (campaign_id, user_id, payout))

        # suma al balance
        cur.execute("""
            UPDATE users
            SET balance_usd_cents = balance_usd_cents + %s
            WHERE user_id=%s
        """, (payout, user_id))

        # actualiza campaña
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
    log_activity(conn, user_id, "earn", f"💵 Ganaste {format_usd_from_cents(payout)} (campaña)", "")
    return payout


# ======================
# DAILY TASK GENERATION
# ======================
def create_daily_tasks(conn, day: date):
    """
    Genera tareas del día:
    - 1 campaña activa si existe (🎯)
    - resto se toma del catálogo activo (random ponderado por weight)
    """
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
            "title": f"{campaign['name']}",
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
                seed = int(t.get("catalog_id") or t.get("campaign_id") or 0)
                code = make_daily_code(day, idx, seed)
                payload = f"code={code}"
                link_url = t.get("content")

            cur.execute("""
                INSERT INTO daily_tasks (day, idx, kind, catalog_id, campaign_id, emoji, title, type, payload, link_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (day, idx) DO NOTHING
            """, (
                day, idx, t["kind"],
                t.get("catalog_id"), t.get("campaign_id"),
                t["emoji"], t["title"], ttype,
                payload, link_url
            ))

    conn.commit()


# ======================
# UI: TASK BUTTONS
# ======================
def task_buttons(tasks: list[dict], user_id: int) -> InlineKeyboardMarkup:
    rows = []
    for t in tasks:
        tid = t["id"]
        done = t["done"]
        label = f"{t['emoji']} {t['idx']}. {t['title']}"

        if done:
            rows.append([InlineKeyboardButton(f"✅ {label} (hecha)", callback_data="noop")])
            continue

        ttype = (t["type"] or "").lower()

        if ttype == "checkin":
            rows.append([InlineKeyboardButton(f"✅ {label}", callback_data=f"task:do:{tid}")])

        elif ttype == "quiz":
            rows.append([InlineKeyboardButton(f"🧠 {label}", callback_data=f"task:quiz:{tid}")])

        elif ttype in ("link", "campaign_link"):
            url = t.get("link_url") or ""
            rows.append([
                InlineKeyboardButton(f"🔗 Abrir {t['idx']}", url=url),
                InlineKeyboardButton(f"🔐 Validar {t['idx']}", callback_data=f"task:code:{tid}")
            ])
        else:
            rows.append([InlineKeyboardButton(label, callback_data="noop")])

    rows.append([InlineKeyboardButton("⬅️ Volver al menú", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


# ======================
# WITHDRAWALS
# ======================
def create_withdrawal_request(conn, user_id: int, amount_cents: int) -> int:
    with conn.cursor() as cur:
        # mueve disponible->retenido
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
            VALUES (%s, %s, 'awaiting_details')
            RETURNING id
        """, (user_id, amount_cents))
        wid = cur.fetchone()["id"]

        cur.execute("UPDATE users SET pending_withdraw_id=%s WHERE user_id=%s", (wid, user_id))

    conn.commit()
    log_activity(conn, user_id, "withdraw", f"💸 Retiro solicitado {format_usd_from_cents(amount_cents)}", f"id={wid}")
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

        cur.execute("UPDATE users SET pending_withdraw_id=NULL WHERE user_id=%s", (user_id,))

    conn.commit()
    log_activity(conn, user_id, "withdraw", f"✅ Datos enviados (retiro {wid})", "")
    return wrow


# ======================
# ACTIVITY LOG VIEW
# ======================
def list_activity(conn, user_id: int, limit: int = MAX_DAILY_LOG_LINES):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ts, title
            FROM activity_log
            WHERE user_id=%s
            ORDER BY id DESC
            LIMIT %s
        """, (user_id, limit))
        return cur.fetchall()


# ======================
# TEXT BUILDERS
# ======================
WELCOME_TEXT = (
    "✨ **Bienvenid@ {name}!**\n\n"
    "Espero que estés teniendo un hermoso día 🌷\n\n"
    "📌 Te invito a realizar las tareas diariamente.\n"
    f"🔥 Si completás **todas** las tareas del día, sumás racha.\n"
    f"🏅 Subís de nivel cada **{LEVEL_STEP_DAYS} días** consecutivos.\n\n"
    "👇 Elegí una opción:"
)

def help_text() -> str:
    return (
        "ℹ️ **Ayuda rápida**\n\n"
        "• Entrá en **📅 Tareas** y completá todo.\n"
        "• Si completás el día entero → sube tu **racha**.\n"
        f"• Cada **{LEVEL_STEP_DAYS} días** de racha → sube tu **nivel** (máx {MAX_LEVEL}).\n\n"
        f"💸 Retiro mínimo: **{format_usd_from_cents(MIN_WITHDRAW_USD_CENTS)}**\n"
        "🧾 Al retirar, te pedimos tus datos (alias/CBU/banco/titular/DNI)."
    )


# ======================
# VIEWS (ASYNC)
# ======================
async def build_tareas_view(user_id: int):
    day = today_local()

    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            user = get_user(conn, user_id)
            if not daily_tasks_exist(conn, day):
                create_daily_tasks(conn, day)
            tasks = list_daily_tasks(conn, user_id, day)
            return user, tasks

    user, tasks = await asyncio.to_thread(_work)
    done_count = sum(1 for t in tasks if t["done"])
    total = len(tasks)

    text = (
        f"📅 **Tareas de hoy ({day.strftime('%d/%m/%Y')})**\n"
        f"🔥 Racha: **{user['streak_days']}** | 🏅 Nivel: **{user['level']}**\n"
        f"✅ Completadas: **{done_count}/{total}**\n\n"
        "Elegí una tarea 👇"
    )
    kb = task_buttons(tasks, user_id)
    return text, kb

async def build_saldo_text(user_id: int) -> str:
    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            return get_user(conn, user_id)
    user = await asyncio.to_thread(_work)

    return (
        f"💰 **Saldo disponible:** {format_usd_from_cents(user['balance_usd_cents'])}\n"
        f"🔒 **Saldo retenido:** {format_usd_from_cents(user.get('held_usd_cents', 0))}\n\n"
        f"💸 Retiro mínimo: **{format_usd_from_cents(MIN_WITHDRAW_USD_CENTS)}**"
    )

async def build_nivel_text(user_id: int) -> str:
    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            return get_user(conn, user_id)
    user = await asyncio.to_thread(_work)

    next_target = (user["level"]) * LEVEL_STEP_DAYS
    return (
        f"🏅 **Tu nivel:** {user['level']} / {MAX_LEVEL}\n"
        f"🔥 **Racha:** {user['streak_days']} días\n\n"
        f"⏳ Subís de nivel cada **{LEVEL_STEP_DAYS} días** de racha completa.\n"
        f"🎯 Próximo objetivo: **{next_target}** días consecutivos."
    )

async def build_historial_text(user_id: int) -> str:
    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            rows = list_activity(conn, user_id, limit=MAX_DAILY_LOG_LINES)
            return rows
    rows = await asyncio.to_thread(_work)

    if not rows:
        return "📜 Todavía no hay actividad registrada."

    lines = ["📜 **Tu historial (últimos eventos)**\n"]
    for r in rows:
        ts = r["ts"].astimezone(TZ).strftime("%d/%m %H:%M")
        lines.append(f"• `{ts}` — {r['title']}")
    return "\n".join(lines)


# ======================
# ADMIN PANEL (ONLY ADMIN)
# ======================
def admin_panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Crear tarea", callback_data="admin:add_task"),
         InlineKeyboardButton("🎯 Crear campaña", callback_data="admin:add_campaign")],
        [InlineKeyboardButton("📋 Ver catálogo", callback_data="admin:list_catalog"),
         InlineKeyboardButton("🎯 Ver campañas", callback_data="admin:list_campaigns")],
        [InlineKeyboardButton("💳 Retiros pendientes", callback_data="admin:list_withdrawals")],
        [InlineKeyboardButton("⬅️ Volver", callback_data="menu:home")]
    ])

async def show_admin_panel(q):
    if not is_admin(q.from_user.id):
        await q.answer("⛔ No autorizado", show_alert=True)
        return
    await q.edit_message_text(
        "⚙️ **Panel de Administradora**\n\n"
        "Desde acá podés cargar tareas, campañas y ver retiros.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=admin_panel_kb()
    )


# ======================
# HANDLERS: COMMANDS
# ======================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user

    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            ensure_user(conn, u)
            log_activity(conn, u.id, "admin", "👋 Inició el bot (/start)", "")
            # crea tareas del día si no existen
            if not daily_tasks_exist(conn, today_local()):
                create_daily_tasks(conn, today_local())
            return get_user(conn, u.id)

    user = await asyncio.to_thread(_work)

    text = WELCOME_TEXT.format(name=(u.first_name or ""))
    text += f"\n\n🔥 **Racha:** {user['streak_days']} días\n🏅 **Nivel:** {user['level']} / {MAX_LEVEL}"

    await update.message.reply_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=inline_menu(u.id)
    )

async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    await update.message.reply_text(
        f"✅ Bot activo.\n"
        f"👤 user_id: {u.id}\n"
        f"👤 username: @{u.username}\n"
        f"🔧 admin: {is_admin(u.id)}",
        reply_markup=inline_menu(u.id)
    )

async def retirar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user

    def _work():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            ensure_user(conn, u)
            user = get_user(conn, u.id)

            if user["pending_withdraw_id"]:
                return ("pending_exists", user["pending_withdraw_id"], user)

            if user["balance_usd_cents"] < MIN_WITHDRAW_USD_CENTS:
                return ("not_enough", None, user)

            # retira TODO el disponible (simple y automático)
            wid = create_withdrawal_request(conn, u.id, user["balance_usd_cents"])
            user2 = get_user(conn, u.id)
            return ("created", wid, user2)

    status, wid, user = await asyncio.to_thread(_work)

    if status == "not_enough":
        await update.message.reply_text(
            f"💸 Retiro mínimo: **{format_usd_from_cents(MIN_WITHDRAW_USD_CENTS)}**\n"
            f"Disponible: **{format_usd_from_cents(user['balance_usd_cents'])}**\n"
            f"Retenido: **{format_usd_from_cents(user.get('held_usd_cents', 0))}**\n\n"
            "Seguí completando tareas 💖",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=inline_menu(u.id)
        )
        return

    if status == "pending_exists":
        await update.message.reply_text(
            f"⏳ Ya tenés un retiro en curso (ID {wid}).\n\n"
            "Enviame ahora tus datos (un solo mensaje):\n"
            "**Alias / CBU / Banco / Titular / DNI**",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=inline_menu(u.id)
        )
        return

    await update.message.reply_text(
        f"✅ Solicitud de retiro creada (ID **{wid}**)\n"
        f"🔒 Tu saldo pasó a **RETENIDO** hasta que se pague.\n\n"
        f"💰 Disponible: **{format_usd_from_cents(user['balance_usd_cents'])}**\n"
        f"🔒 Retenido: **{format_usd_from_cents(user.get('held_usd_cents', 0))}**\n\n"
        "Ahora enviame tus datos para transferirte (un solo mensaje):\n"
        "**Alias / CBU / Banco / Titular / DNI**",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=inline_menu(u.id)
    )


# ======================
# HANDLERS: CALLBACKS
# ======================
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    u = q.from_user
    data = q.data or ""

    # HOME
    if data == "menu:home":
        await q.edit_message_text("📌 **Menú**\nElegí una opción 👇", parse_mode=ParseMode.MARKDOWN, reply_markup=inline_menu(u.id))
        return

    # USER MENUS
    if data == "menu:tareas":
        text, kb = await build_tareas_view(u.id)
        await q.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
        return

    if data == "menu:saldo":
        await q.edit_message_text(await build_saldo_text(u.id), parse_mode=ParseMode.MARKDOWN, reply_markup=back_to_menu_btn(u.id))
        return

    if data == "menu:nivel":
        await q.edit_message_text(await build_nivel_text(u.id), parse_mode=ParseMode.MARKDOWN, reply_markup=back_to_menu_btn(u.id))
        return

    if data == "menu:historial":
        await q.edit_message_text(await build_historial_text(u.id), parse_mode=ParseMode.MARKDOWN, reply_markup=back_to_menu_btn(u.id))
        return

    if data == "menu:ayuda":
        await q.edit_message_text(help_text(), parse_mode=ParseMode.MARKDOWN, reply_markup=back_to_menu_btn(u.id))
        return

    if data == "menu:retirar":
        await q.edit_message_text(
            "💸 **Retirar**\n\n"
            "Para solicitar retiro tocá:\n"
            "👉 **/retirar**\n\n"
            f"Mínimo: **{format_usd_from_cents(MIN_WITHDRAW_USD_CENTS)}**\n"
            "Luego te pedirá tus datos (alias/CBU/banco/titular/DNI).",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=back_to_menu_btn(u.id)
        )
        return

    # TASK ACTIONS
    if data.startswith("task:"):
        await handle_task_click(update, context)
        return

    # ADMIN PANEL
    if data == "admin:panel":
        await show_admin_panel(q)
        return

    if data.startswith("admin:"):
        await handle_admin_click(update, context)
        return

    # noop
    return


async def handle_task_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    u = q.from_user
    day = today_local()

    parts = (q.data or "").split(":")
    if len(parts) < 3:
        return

    action = parts[1]
    task_id = int(parts[2])

    def _load_task_user():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            ensure_user(conn, u)
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM daily_tasks WHERE id=%s", (task_id,))
                t = cur.fetchone()
            user = get_user(conn, u.id)
            return t, user

    t, user = await asyncio.to_thread(_load_task_user)
    if not t or t["day"] != day:
        await q.answer("⚠️ Esa tarea no es de hoy.", show_alert=True)
        return

    ttype = (t["type"] or "").lower()

    # CHECKIN
    if action == "do" and ttype == "checkin":
        def _do():
            with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                inserted = complete_task(conn, u.id, task_id)
                if inserted:
                    log_activity(conn, u.id, "task", f"✅ Tarea completada: {t['title']}", "")
                streaked, streak, lvl = apply_streak_if_day_completed(conn, u.id, day)
                return inserted, streaked, streak, lvl

        inserted, streaked, streak, lvl = await asyncio.to_thread(_do)

        msg = "✅ Check-in completado." if inserted else "✅ Ya estaba completado."
        if streaked:
            msg += f"\n🔥 Racha: **{streak}** | 🏅 Nivel: **{lvl}**"

        text, kb = await build_tareas_view(u.id)
        await q.edit_message_text(text + "\n\n" + msg, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
        return

    # QUIZ -> captura por chat
    if action == "quiz" and ttype == "quiz":
        context.user_data["pending_action"] = {"type": "quiz", "task_id": task_id}
        await q.edit_message_text(
            f"🧠 **{t['title']}**\n\nEscribí tu respuesta ahora (un mensaje).",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=back_to_menu_btn(u.id)
        )
        return

    # CODE -> captura por chat
    if action == "code" and ttype in ("link", "campaign_link"):
        context.user_data["pending_action"] = {"type": "code", "task_id": task_id}
        await q.edit_message_text(
            f"🔐 **Validación**\n\nTarea: **{t['title']}**\n\nEnviá el **código** en un mensaje.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=back_to_menu_btn(u.id)
        )
        return

    await q.answer("⚠️ Acción inválida.", show_alert=True)


# ======================
# ADMIN CLICK HANDLER
# ======================
async def handle_admin_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    u = q.from_user
    data = q.data or ""

    if not is_admin(u.id):
        await q.answer("⛔ No autorizado", show_alert=True)
        return

    # Create task flow
    if data == "admin:add_task":
        context.user_data["admin_flow"] = {"type": "add_task", "step": "start"}
        await q.edit_message_text(
            "➕ **Crear tarea**\n\n"
            "Enviame un mensaje con este formato:\n"
            "`emoji | titulo | tipo | contenido`\n\n"
            "Tipos: `checkin`, `quiz`, `link`\n"
            "• checkin: contenido vacío\n"
            "• quiz: contenido = respuesta correcta\n"
            "• link: contenido = URL\n\n"
            "Ejemplo:\n"
            "`📌 | Seguinos en IG | link | https://instagram.com/tucuenta`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=admin_panel_kb()
        )
        return

    # Create campaign flow
    if data == "admin:add_campaign":
        context.user_data["admin_flow"] = {"type": "add_campaign", "step": "start"}
        await q.edit_message_text(
            "🎯 **Crear campaña**\n\n"
            "Enviame un mensaje con este formato:\n"
            "`nombre | link | presupuesto_usd | objetivo`\n\n"
            "Ejemplo:\n"
            "`Encuesta #1 | https://tuencuesta.com | 10 | 200`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=admin_panel_kb()
        )
        return

    # List catalog
    if data == "admin:list_catalog":
        def _work():
            with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT id, emoji, title, type, is_active
                        FROM task_catalog
                        ORDER BY id DESC
                        LIMIT 20
                    """)
                    return cur.fetchall()
        rows = await asyncio.to_thread(_work)
        if not rows:
            await q.edit_message_text("🧩 Catálogo vacío.", reply_markup=admin_panel_kb())
            return
        lines = ["🧩 **Últimas 20 tareas del catálogo**\n"]
        for r in rows:
            lines.append(f"• #{r['id']} {r['emoji']} {r['title']} ({r['type']}) {'✅' if r['is_active'] else '⛔'}")
        await q.edit_message_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN, reply_markup=admin_panel_kb())
        return

    # List campaigns
    if data == "admin:list_campaigns":
        def _work():
            with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT id, name, budget_usd_cents, goal_completions, completed_count, spent_usd_cents, is_active
                        FROM campaigns
                        ORDER BY id DESC
                        LIMIT 10
                    """)
                    return cur.fetchall()
        rows = await asyncio.to_thread(_work)
        if not rows:
            await q.edit_message_text("🎯 No hay campañas.", reply_markup=admin_panel_kb())
            return
        lines = ["🎯 **Últimas campañas**\n"]
        for c in rows:
            lines.append(
                f"• #{c['id']} {c['name']} | "
                f"Pres: {format_usd_from_cents(c['budget_usd_cents'])} | "
                f"Obj: {c['goal_completions']} | "
                f"Done: {c['completed_count']} | "
                f"Gasto: {format_usd_from_cents(c['spent_usd_cents'])} | "
                f"{'✅' if c['is_active'] else '⛔'}"
            )
        await q.edit_message_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN, reply_markup=admin_panel_kb())
        return

    # List withdrawals pending
    if data == "admin:list_withdrawals":
        def _work():
            with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT w.id, w.user_id, w.amount_usd_cents, w.status, w.payout_details, w.created_at
                        FROM withdrawals w
                        WHERE w.status IN ('awaiting_details','pending')
                        ORDER BY w.id DESC
                        LIMIT 20
                    """)
                    return cur.fetchall()
        rows = await asyncio.to_thread(_work)
        if not rows:
            await q.edit_message_text("💳 No hay retiros pendientes.", reply_markup=admin_panel_kb())
            return
        lines = ["💳 **Retiros pendientes (últimos 20)**\n"]
        for w in rows:
            lines.append(
                f"• ID {w['id']} | user {w['user_id']} | "
                f"{format_usd_from_cents(w['amount_usd_cents'])} | {w['status']}"
            )
        lines.append("\n📌 Para marcar pagado manualmente, podés hacerlo luego (si querés te agrego ese botón).")
        await q.edit_message_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN, reply_markup=admin_panel_kb())
        return

    # default
    await show_admin_panel(q)


# ======================
# TEXT INPUT HANDLER (quiz / code / admin flows / withdraw details)
# ======================
async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    msg = (update.message.text or "").strip()
    if not msg:
        return

    # 1) Quiz / Code flow for users
    pending = context.user_data.get("pending_action")
    if isinstance(pending, dict):
        ptype = pending.get("type")
        task_id = int(pending.get("task_id", 0))
        day = today_local()

        def _load():
            with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                ensure_user(conn, u)
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM daily_tasks WHERE id=%s", (task_id,))
                    t = cur.fetchone()
                user = get_user(conn, u.id)
                return t, user

        t, user = await asyncio.to_thread(_load)
        if not t or t["day"] != day:
            context.user_data.pop("pending_action", None)
            await update.message.reply_text("⚠️ Esa tarea ya no es válida. Tocá 📅 Tareas.", reply_markup=inline_menu(u.id))
            return

        ttype = (t["type"] or "").lower()

        # Quiz answer
        if ptype == "quiz" and ttype == "quiz":
            expected = (t["payload"] or "").split("answer=")[-1].strip().lower()
            if msg.lower() != expected:
                await update.message.reply_text("❌ Incorrecto. Probá de nuevo.")
                return

            def _complete():
                with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                    inserted = complete_task(conn, u.id, task_id)
                    if inserted:
                        log_activity(conn, u.id, "task", f"✅ Quiz completado: {t['title']}", "")
                    streaked, streak, lvl = apply_streak_if_day_completed(conn, u.id, day)
                    return inserted, streaked, streak, lvl

            inserted, streaked, streak, lvl = await asyncio.to_thread(_complete)
            context.user_data.pop("pending_action", None)

            msg_out = "✅ Quiz completado." if inserted else "✅ Ya estaba completado."
            if streaked:
                msg_out += f"\n🔥 Racha: **{streak}** | 🏅 Nivel: **{lvl}**"

            text_view, kb = await build_tareas_view(u.id)
            await update.message.reply_text(text_view + "\n\n" + msg_out, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
            return

        # Code validation
        if ptype == "code" and ttype in ("link", "campaign_link"):
            expected = (t["payload"] or "").split("code=")[-1].strip()
            if msg != expected:
                await update.message.reply_text("❌ Código incorrecto. Probá de nuevo.")
                return

            def _complete_code():
                with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                    inserted = complete_task(conn, u.id, task_id)
                    paid = 0
                    user2 = get_user(conn, u.id)

                    if inserted:
                        log_activity(conn, u.id, "task", f"✅ Link validado: {t['title']}", "")
                        if ttype == "campaign_link" and t["campaign_id"]:
                            paid = try_pay_campaign(conn, int(t["campaign_id"]), u.id, int(user2["level"]))

                    streaked, streak, lvl = apply_streak_if_day_completed(conn, u.id, day)
                    return inserted, paid, streaked, streak, lvl

            inserted, paid, streaked, streak, lvl = await asyncio.to_thread(_complete_code)
            context.user_data.pop("pending_action", None)

            out = "✅ Código validado." if inserted else "✅ Ya estaba validado."
            if paid > 0:
                out += f"\n💵 Ganaste **{format_usd_from_cents(paid)}**"
            if streaked:
                out += f"\n🔥 Racha: **{streak}** | 🏅 Nivel: **{lvl}**"

            text_view, kb = await build_tareas_view(u.id)
            await update.message.reply_text(text_view + "\n\n" + out, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
            return

    # 2) Admin flow for adding task/campaign
    admin_flow = context.user_data.get("admin_flow")
    if isinstance(admin_flow, dict) and is_admin(u.id):
        flow_type = admin_flow.get("type")

        # Add task format: emoji | title | type | content
        if flow_type == "add_task":
            parts = [p.strip() for p in msg.split("|")]
            if len(parts) < 3:
                await update.message.reply_text("⚠️ Formato inválido. Usá: `emoji | titulo | tipo | contenido`", parse_mode=ParseMode.MARKDOWN)
                return

            emoji = parts[0]
            title = parts[1]
            ttype = parts[2].lower()
            content = parts[3] if len(parts) >= 4 else ""

            if ttype not in ("checkin", "quiz", "link"):
                await update.message.reply_text("⚠️ Tipo inválido. Usá: checkin, quiz, link")
                return

            def _insert():
                with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO task_catalog (emoji, title, type, content, weight, is_active)
                            VALUES (%s, %s, %s, %s, 10, TRUE)
                            RETURNING id
                        """, (emoji, title, ttype, content))
                        tid = cur.fetchone()["id"]
                    conn.commit()
                    log_activity(conn, u.id, "admin", f"⚙️ Admin creó tarea #{tid}: {title}", "")
                    return tid

            tid = await asyncio.to_thread(_insert)
            context.user_data.pop("admin_flow", None)
            await update.message.reply_text(f"✅ Tarea creada #{tid}.", reply_markup=inline_menu(u.id))
            return

        # Add campaign format: name | link | budget_usd | goal
        if flow_type == "add_campaign":
            parts = [p.strip() for p in msg.split("|")]
            if len(parts) < 4:
                await update.message.reply_text("⚠️ Formato inválido. Usá: `nombre | link | presupuesto_usd | objetivo`", parse_mode=ParseMode.MARKDOWN)
                return

            name = parts[0]
            link = parts[1]
            try:
                budget_usd = float(parts[2].replace(",", "."))
                goal = int(parts[3])
            except Exception:
                await update.message.reply_text("⚠️ Presupuesto u objetivo inválidos.")
                return

            budget_cents = max(0, int(round(budget_usd * 100)))
            if budget_cents <= 0 or goal <= 0:
                await update.message.reply_text("⚠️ Presupuesto y objetivo deben ser mayores a 0.")
                return

            def _insert():
                with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                    with conn.cursor() as cur:
                        # desactiva otras campañas activas (opcional, para que haya 1 activa)
                        cur.execute("UPDATE campaigns SET is_active=FALSE WHERE is_active=TRUE;")
                        cur.execute("""
                            INSERT INTO campaigns (name, link_url, budget_usd_cents, goal_completions, is_active)
                            VALUES (%s, %s, %s, %s, TRUE)
                            RETURNING id
                        """, (name, link, budget_cents, goal))
                        cid = cur.fetchone()["id"]
                    conn.commit()
                    log_activity(conn, u.id, "admin", f"⚙️ Admin creó campaña #{cid}: {name}", "")
                    return cid

            cid = await asyncio.to_thread(_insert)
            context.user_data.pop("admin_flow", None)
            await update.message.reply_text(f"✅ Campaña creada #{cid} y activada.", reply_markup=inline_menu(u.id))
            return

    # 3) Withdrawal details capture (si hay retiro pendiente)
    def _maybe_attach_withdraw():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            ensure_user(conn, u)
            user = get_user(conn, u.id)
            if not user or not user["pending_withdraw_id"]:
                return None
            return attach_withdrawal_details(conn, u.id, msg)

    wrow = await asyncio.to_thread(_maybe_attach_withdraw)
    if wrow:
        await update.message.reply_text(
            f"✅ Recibí tus datos.\n"
            f"🧾 Retiro ID **{wrow['id']}** por **{format_usd_from_cents(wrow['amount_usd_cents'])}** quedó **pendiente**.\n\n"
            "Te aviso cuando esté pagado 💖",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=inline_menu(u.id)
        )
        return

    # 4) default
    await update.message.reply_text("📌 Tocá un botón 👇", reply_markup=inline_menu(u.id))


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

    # Commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("whoami", whoami))
    app.add_handler(CommandHandler("retirar", retirar))

    # Callbacks
    app.add_handler(CallbackQueryHandler(on_callback))

    # Text input
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    # Errors
    app.add_error_handler(error_handler)

    app.run_polling()

if __name__ == "__main__":
    main()
