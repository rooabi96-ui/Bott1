import os
import asyncio

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

import psycopg
from psycopg.rows import dict_row

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}

if not BOT_TOKEN:
    raise RuntimeError("Falta BOT_TOKEN en variables de entorno")
if not DATABASE_URL:
    raise RuntimeError("Falta DATABASE_URL en variables de entorno")


async def init_db(app: Application):
    # corre al iniciar el bot (dentro del loop del propio PTB)
    def _setup():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """)
            conn.commit()

    await asyncio.to_thread(_setup)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user

    def _insert_user():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO users (user_id, username, first_name)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE SET
                        username = EXCLUDED.username,
                        first_name = EXCLUDED.first_name;
                """, (u.id, u.username, u.first_name))
            conn.commit()

    await asyncio.to_thread(_insert_user)
    await update.message.reply_text("✅ Bot activo. Probá /whoami")


async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    await update.message.reply_text(f"ID: {u.id}\nUsername: @{u.username}\nNombre: {u.first_name}")


def main():
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(init_db)   # <-- acá inicializa la DB
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("whoami", whoami))

    # IMPORTANTE: sin await y sin asyncio.run
    app.run_polling()


if __name__ == "__main__":
    main()