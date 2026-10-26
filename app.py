# -*- coding: utf-8 -*-
"""
app.py —— FastAPI 主程序
- GET  /              页面
- POST /api/task      启动流水线（返回 session_id）
- GET  /stream/{sid}  SSE 事件流
- GET  /api/sessions  历史会话列表
- GET  /api/session/{sid}  会话详情（消息 + 产物）
- GET  /artifacts/{name}   下载产物
- uvicorn 启动；start.bat 打开浏览器。
"""
import asyncio
import json
import sys
import webbrowser
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import store
from orchestrator import Studio

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

BASE = Path(__file__).resolve().parent
CFG = json.loads((BASE / "config.json").read_text(encoding="utf-8"))

app = FastAPI(title="AI 数字工作室")
studio = Studio()


@app.on_event("startup")
def _startup():
    store.init_db()
    # 首次启动自动打开浏览器（可通过 config 关）
    if CFG.get("open_browser", True):
        webbrowser.open(f"http://{CFG['host']}:{CFG['port']}")


class TaskRequest(BaseModel):
    request: str


@app.post("/api/task")
async def start_task(req: TaskRequest):
    text = (req.request or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="需求不能为空")
    sid = store.create_session(text)
    asyncio.create_task(studio.run(sid, text))
    return {"session_id": sid, "status": "started"}


@app.get("/stream/{sid}")
async def stream(sid: int):
    q = studio.subscribe(sid)
    hist = store.get_messages(sid)

    async def gen():
        # 先回放历史（刷新页面恢复）
        for m in hist:
            yield f"event: replay\ndata: {json.dumps(m, ensure_ascii=False)}\n\n"
        yield f"event: replay_done\ndata: {json.dumps({'ok': True})}\n\n"
        try:
            while True:
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=15)
                except asyncio.TimeoutError:
                    yield "event: ping\ndata: {}\n\n"
                    continue
                yield f"event: {msg['type']}\ndata: {json.dumps(msg, ensure_ascii=False)}\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            studio.unsubscribe(sid, q)

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})


@app.get("/api/sessions")
def sessions():
    return store.list_sessions(limit=50)


@app.get("/api/session/{sid}")
def session_detail(sid: int):
    msgs = store.get_messages(sid)
    arts = store.list_artifacts(sid)
    sessions = {s["id"]: s for s in store.list_sessions(limit=1000)}
    meta = sessions.get(sid)
    if meta is None:
        raise HTTPException(status_code=404, detail="会话不存在")
    return {"session": meta, "messages": msgs, "artifacts": arts}


@app.get("/artifacts/{name}")
def artifact_file(name: str):
    # 防目录穿越
    safe = Path(name).name
    path = store.ART_DIR / safe
    if not path.exists():
        raise HTTPException(status_code=404, detail="产物不存在")
    return FileResponse(str(path), filename=safe, media_type="text/markdown; charset=utf-8")


app.mount("/static", StaticFiles(directory=str(BASE / "static")), name="static")


@app.get("/", response_class=HTMLResponse)
def index():
    return (BASE / "static" / "index.html").read_text(encoding="utf-8")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=CFG["host"], port=int(CFG["port"]))