# -*- coding: utf-8 -*-
"""
store.py —— SQLite 持久化层
会话 / 消息 / 产物索引存取；产物文件实际写在 artifacts\\ 目录。
单进程单写入者即可（验收标准：勿过度设计并发）。
"""
import json
import sqlite3
import time
from pathlib import Path

BASE = Path(__file__).resolve().parent
DB_PATH = BASE / "studio.db"
ART_DIR = BASE / "artifacts"


def _now():
    return time.time()


def init_db():
    ART_DIR.mkdir(exist_ok=True)
    con = sqlite3.connect(str(DB_PATH))
    con.execute("PRAGMA journal_mode=WAL;")
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS sessions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'running',
            cost REAL NOT NULL DEFAULT 0,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            meta TEXT DEFAULT '{}'
        );
        CREATE TABLE IF NOT EXISTS messages(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            seq INTEGER NOT NULL,
            payload TEXT NOT NULL,
            created_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS artifacts(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            filename TEXT NOT NULL,
            title TEXT NOT NULL,
            created_at REAL NOT NULL
        );
        """
    )
    con.commit()
    con.close()


def create_session(request: str) -> int:
    con = sqlite3.connect(str(DB_PATH))
    cur = con.execute(
        "INSERT INTO sessions(request,status,created_at,updated_at,meta) VALUES(?,?,?,?,?)",
        (request, "running", _now(), _now(), "{}"),
    )
    con.commit()
    sid = cur.lastrowid
    con.close()
    return sid


def add_message(sid: int, seq: int, payload: dict):
    con = sqlite3.connect(str(DB_PATH))
    con.execute(
        "INSERT INTO messages(session_id,seq,payload,created_at) VALUES(?,?,?,?)",
        (sid, seq, json.dumps(payload, ensure_ascii=False), _now()),
    )
    con.commit()
    con.close()


def update_session(sid: int, *, status: str = None, cost: float = None, meta: dict = None):
    con = sqlite3.connect(str(DB_PATH))
    cur = con.execute("SELECT status,cost,meta FROM sessions WHERE id=?", (sid,))
    row = cur.fetchone()
    if not row:
        con.close()
        return
    st, cst, mta = row
    if status is not None:
        st = status
    if cost is not None:
        cst = cost
    if meta is not None:
        mta = json.dumps(meta, ensure_ascii=False)
    con.execute(
        "UPDATE sessions SET status=?,cost=?,meta=?,updated_at=? WHERE id=?",
        (st, cst, mta, _now(), sid),
    )
    con.commit()
    con.close()


def list_sessions(limit: int = 50):
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT id,request,status,cost,created_at,updated_at,meta FROM sessions "
        "ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    con.close()
    return [dict(r) for r in rows]


def get_messages(sid: int):
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT seq,payload FROM messages WHERE session_id=? ORDER BY seq ASC", (sid,)
    ).fetchall()
    con.close()
    return [json.loads(r["payload"]) for r in rows]


def add_artifact(sid: int, filename: str, title: str):
    con = sqlite3.connect(str(DB_PATH))
    cur = con.execute(
        "INSERT INTO artifacts(session_id,filename,title,created_at) VALUES(?,?,?,?)",
        (sid, filename, title, _now()),
    )
    con.commit()
    aid = cur.lastrowid
    con.close()
    return aid


def list_artifacts(sid: int):
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT id,filename,title FROM artifacts WHERE session_id=? ORDER BY id ASC",
        (sid,),
    ).fetchall()
    con.close()
    return [dict(r) for r in rows]


def get_session_cost(sid: int) -> float:
    con = sqlite3.connect(str(DB_PATH))
    row = con.execute("SELECT cost FROM sessions WHERE id=?", (sid,)).fetchone()
    con.close()
    return float(row[0]) if row else 0.0


def update_session_cost(sid: int, cost: float):
    con = sqlite3.connect(str(DB_PATH))
    con.execute(
        "UPDATE sessions SET cost=?,updated_at=? WHERE id=?",
        (cost, time.time(), sid),
    )
    con.commit()
    con.close()