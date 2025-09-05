import os
import logging
from typing import Any, Dict

from fastapi import FastAPI, HTTPException, Path
from pydantic import BaseModel, EmailStr
from dotenv import load_dotenv

import asyncmy
from asyncmy.errors import IntegrityError, MySQLError
from asyncmy.cursors import DictCursor   # <<-- ใช้จาก asyncmy.cursors

load_dotenv()

# basic log
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app")

app = FastAPI(title="FastAPI + MySQL (async)", version="1.0.1")

DB_HOST = os.getenv("DB_HOST", "container_mysql")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "testuser")
DB_PASS = os.getenv("DB_PASSWORD", "testpass")
DB_NAME = os.getenv("DB_NAME", "testdb")
POOL_MIN = int(os.getenv("DB_POOL_MIN", "1"))
POOL_MAX = int(os.getenv("DB_POOL_MAX", "10"))

class CreateUserBody(BaseModel):
    username: str
    email: EmailStr

@app.on_event("startup")
async def on_startup():
    app.state.pool = await asyncmy.create_pool(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME,
        minsize=POOL_MIN,
        maxsize=POOL_MAX,
        autocommit=True,
        charset="utf8mb4",
    )

@app.on_event("shutdown")
async def on_shutdown():
    pool = app.state.pool
    pool.close()
    await pool.wait_closed()

@app.get("/")
async def root() -> Dict[str, Any]:
    return {"message": "Hello World from Python FastAPI MySQL (async)"}

@app.post("/users", status_code=201)
async def create_user(body: CreateUserBody):
    try:
        pool = app.state.pool
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO users (username, email) VALUES (%s, %s)",
                    (body.username, body.email),
                )
                user_id = cur.lastrowid
        return {"message": "User created successfully", "user_id": user_id}

    except IntegrityError:
        raise HTTPException(status_code=409, detail="Email already exists")
    except MySQLError as e:
        logger.exception("DB error on POST /users")
        raise HTTPException(status_code=500, detail="Database error") from e

@app.get("/users/{user_id}")
async def get_user(
    user_id: int = Path(..., ge=1, description="User ID must be a positive integer")
):
    try:
        pool = app.state.pool
        async with pool.acquire() as conn:
            # ใช้ DictCursor ที่ถูกต้อง
            async with conn.cursor(DictCursor) as cur:
                await cur.execute(
                    "SELECT user_id, username, email FROM users WHERE user_id = %s",
                    (user_id,),
                )
                row = await cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="User not found")
        return row

    except MySQLError as e:
        logger.exception("DB error on GET /users/%s", user_id)
        raise HTTPException(status_code=500, detail="Database error") from e
