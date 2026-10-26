# -*- coding: utf-8 -*-
"""
orchestrator.py —— 编排器
状态机 draft → review → revise(≤max_revise) → final；
预算检查（超 max_cost_cny 立即熔断）；SSE 消息广播；产物落盘。
"""
import asyncio
import json
import re
import time
from pathlib import Path

import store
from agents import (build_agents, estimate_cost, ProviderUnavailable,
                    check_available)

BASE = Path(__file__).resolve().parent


class Studio:
    def __init__(self):
        self.cfg = json.loads((BASE / "config.json").read_text(encoding="utf-8"))
        self.role_cfgs = json.loads((BASE / "agents.json").read_text(encoding="utf-8"))["roles"]
        self.agents = build_agents(self.role_cfgs)
        self.max_revise = int(self.cfg.get("max_revise", 2))
        self.pass_score = int(self.cfg.get("review_pass_score", 80))
        self.max_cost = float(self.cfg.get("max_cost_cny", 0.5))
        self._subscribers = {}  # session_id -> set(asyncio.Queue)

    # ---------------- SSE 订阅 ----------------
    def subscribe(self, sid: int):
        q = asyncio.Queue()
        self._subscribers.setdefault(sid, set()).add(q)
        return q

    def unsubscribe(self, sid: int, q):
        s = self._subscribers.get(sid)
        if s:
            s.discard(q)
            if not s:
                self._subscribers.pop(sid, None)

    async def broadcast(self, sid: int, event: str, data):
        payload = {"type": event}
        if isinstance(data, dict):
            payload.update(data)
        else:
            payload["data"] = data
        store.add_message(sid, payload.get("seq", 0), payload)
        for q in list(self._subscribers.get(sid, ())):
            try:
                q.put_nowait(payload)
            except Exception:
                pass

    # ---------------- 成本 ----------------
    def add_cost(self, sid, agent, usage):
        used = getattr(agent, "_last_used", None)
        model = (used or {}).get("model") or self.cfg.get("model", "deepseek-chat")
        c = estimate_cost(model, usage.get("prompt_tokens", 0),
                          usage.get("completion_tokens", 0))
        if c <= 0:
            return 0.0
        new_cost = store.get_session_cost(sid) + c
        store.update_session_cost(sid, new_cost)
        return new_cost

    # ---------------- 流程 ----------------
    async def run(self, sid: int, request: str):
        try:
            await self._process(sid, request)
        except BudgetExceeded:
            store.update_session(sid, status="budget",
                                 cost=store.get_session_cost(sid))
        except ProviderUnavailable:
            store.update_session(sid, status="error",
                                 cost=store.get_session_cost(sid))
        except Exception as e:  # noqa: BLE001
            store.update_session(sid, status="error",
                                 cost=store.get_session_cost(sid))

    async def _process(self, sid: int, request: str):
        seq = [0]

        def nxt():
            seq[0] += 1
            return seq[0]

        try:
            check_available()
        except ProviderUnavailable as e:
            await self.broadcast(sid, "system",
                                 {"content": f"❌ {e}（未检测到可用模型 key，无法开工，后台未假装在跑）"})
            await self.broadcast(sid, "end",
                                 {"status": "error", "reason": str(e)})
            store.update_session(sid, status="error")
            return

        manager = self.agents["manager"]
        writer = self.agents["writer"]
        reviewer = self.agents["reviewer"]

        # 阶段 1：主管派活（不拆任务，原样转发 + 一句话验收标准）
        await self.broadcast(sid, "stage", {"stage": "assign", "label": "主管派活"})
        await self.set_worker_status(sid, "manager", "busy")
        manager_prompt = (
            f"老板需求：{request}\n\n请把需求原样转发给资深撰稿人（文案），"
            f"并附上一句验收标准：符合需求、无错别字、字数达标、表达自然。"
            f"不要拆解任务。直接输出交给文案的话。"
        )
        manager_msg = ""
        streamed = manager.stream_chat(
            [{"role": "system", "content": manager.system_prompt},
             {"role": "user", "content": manager_prompt}], temperature=0.7)
        async for piece in self.guarded_stream(sid, streamed, "manager"):
            manager_msg += piece
        await self.broadcast(sid, "assign",
                             {"seq": nxt(), "from": "manager", "to": "writer",
                              "content": manager_msg.strip(), "turn": 0})
        await self.set_worker_status(sid, "manager", "idle")

        # 验收标准也存起来供后续使用
        accept_criteria = "符合需求、无错别字、字数达标、表达自然"

        draft = ""
        turn = 1
        while True:
            # 文案写稿（初稿或重写）
            await self.broadcast(sid, "stage", {"stage": "draft",
                                                "label": f"文案撰写 · 第{turn}稿"})
            await self.set_worker_status(sid, "writer", "busy")
            writer_prompt = self._writer_prompt(request, accept_criteria, draft, turn)
            writer_msg = ""
            wstream = writer.stream_chat(
                [{"role": "system", "content": writer.system_prompt},
                 {"role": "user", "content": writer_prompt}], temperature=0.85)
            async for piece in self.guarded_stream(sid, wstream, "writer"):
                writer_msg += piece
                # 边流式边转发给前端（用于逐字渲染）
                await self.broadcast(sid, "draft_token",
                                     {"seq": nxt(), "from": "writer",
                                      "token": piece, "turn": turn, "content": writer_msg})
            if not writer_msg.strip():
                writer_msg = "（文案未能产出内容）"
            await self.set_worker_status(sid, "writer", "idle")

            # 审核
            await self.broadcast(sid, "stage", {"stage": "review",
                                                "label": f"审核 · 第{turn}轮"})
            await self.set_worker_status(sid, "reviewer", "busy")
            reviewer_msg = ""
            rstream = reviewer.stream_chat(
                [{"role": "system", "content": reviewer.system_prompt},
                 {"role": "user", "content": f"请评审下面这份招新文案（{turn}稿）：\n\n{writer_msg}"}],
                temperature=0.4)
            async for piece in self.guarded_stream(sid, rstream, "reviewer"):
                reviewer_msg += piece
            score = self._parse_score(reviewer_msg)
            opinion = self._parse_opinion(reviewer_msg)
            await self.set_worker_status(sid, "reviewer", "idle")

            if score < self.pass_score and turn <= self.max_revise:
                # 打回
                await self.broadcast(sid, "reject",
                                     {"seq": nxt(), "from": "reviewer", "to": "writer",
                                      "score": score, "content": opinion, "turn": turn})
                await self.set_worker_status(sid, "reviewer", "rejected")
                await self.set_worker_status(sid, "writer", "busy")
                draft = writer_msg  # 保存上一稿供重写参考
                turn += 1
                continue

            # 通过（或 3 次打回后强制通过）
            wounded = (score < self.pass_score and turn > self.max_revise) \
                or turn > self.max_revise + 1
            note = "带伤交付" if wounded else ""
            await self.broadcast(sid, "pass",
                                 {"seq": nxt(), "from": "reviewer",
                                  "score": score, "content": opinion,
                                  "turn": turn, "note": note})
            if wounded:
                await self.broadcast(sid, "note",
                                     {"content": "⚠️ 已达驳回上限，强制通过，标注「带伤交付」"})
            break

        # 主管总结 + 产物落架
        final_text = self._final_text(writer_msg)
        await self.broadcast(sid, "stage", {"stage": "final", "label": "主管总结"})
        await self.set_worker_status(sid, "manager", "busy")
        summary_prompt = (
            f"需求：{request}；最终成品如下：\n\n{final_text}\n\n"
            f"请用一到两句作项目主管式总结，直接输出总结文字。"
        )
        summary = ""
        sstream = manager.stream_chat(
            [{"role": "system", "content": manager.system_prompt},
             {"role": "user", "content": summary_prompt}], temperature=0.7)
        async for piece in self.guarded_stream(sid, sstream, "manager"):
            summary += piece
        await self.broadcast(sid, "summary",
                             {"seq": nxt(), "from": "manager", "content": summary.strip()})
        await self.set_worker_status(sid, "manager", "idle")

        # 产物落架
        title = self._derive_title(request)
        filename = self._write_artifact(sid, final_text, title)
        await self.broadcast(sid, "artifact",
                             {"seq": nxt(), "filename": filename, "title": title,
                              "url": f"/artifacts/{filename}"})
        store.update_session(sid, status="done")
        await self.broadcast(sid, "end", {"status": "done", "cost": store.get_session_cost(sid)})

    # ---------------- 工具 ----------------
    async def guarded_stream(self, sid, stream, worker_role):
        """消费 agent 流，抛 ProviderUnavailable 时广播错误并中止。"""
        try:
            async for piece, usage in stream:
                if usage:
                    agent = self.agents[worker_role]
                    used = getattr(agent, "_last_used", {}) or {}
                    new_cost = self.add_cost(sid, agent, usage)
                    await self.broadcast(sid, "cost",
                                         {"cost": round(new_cost, 4),
                                          "model": used.get("model", "")})
                    if new_cost > self.max_cost:
                        await self.broadcast(sid, "budget_break",
                                             {"content": f"💥 预算熔断：累计 {new_cost:.4f} 元 已超过上限 {self.max_cost} 元"})
                        await self.broadcast(sid, "end", {"status": "budget",
                                                          "reason": f"预算超限 {new_cost:.4f} 元"})
                        raise BudgetExceeded(new_cost)
                    continue
                if piece:
                    yield piece
        except ProviderUnavailable as e:
            await self.broadcast(sid, "error",
                                 {"content": f"❌ {e}。流水线中止，已完成部分已保留。"})
            await self.broadcast(sid, "end", {"status": "error", "reason": str(e)})
            raise
        except BudgetExceeded:
            raise

    def _writer_prompt(self, request, criteria, last_draft, turn):
        if turn == 1:
            return (f"需求：{request}\n验收标准：{criteria}\n\n"
                    f"请撰写这份文案的初稿，直接输出正文。")
        return (f"需求：{request}\n验收标准：{criteria}\n"
                f"上一稿（第{turn-1}稿）：\n{last_draft}\n\n"
                f"请根据审核意见重写（这是第{turn}稿），修正问题，直接输出正文。")

    def _parse_score(self, text):
        m = re.search(r"(?:分数|评分|score)\s*[:：]?\s*(\d{1,3})", text, re.I)
        if not m:
            return 70  # 解析失败默认不给通过
        return max(0, min(100, int(m.group(1))))

    def _parse_opinion(self, text):
        # 去掉第一行评分，保留意见部分
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        out = []
        for i, l in enumerate(lines):
            if i == 0 and re.match(r"(?:分数|评分|score)", l, re.I):
                continue
            out.append(l)
        return "\n".join(out) if out else text

    async def set_worker_status(self, sid, role, status):
        emoji = self.agents[role].emoji
        await self.broadcast(sid, "worker_status",
                             {"role": role, "status": status, "emoji": emoji})

    def _final_text(self, text):
        return text.strip()

    def _derive_title(self, request):
        # 从需求提取标题关键词
        m = re.search(r"(为|给)?(.{1,12}?)(写|做|写一条|写一份|生成)", request)
        if m:
            return m.group(2).strip() or "AI创作"
        return "AI创作"

    def _write_artifact(self, sid, text, title):
        store.ART_DIR.mkdir(exist_ok=True)
        safe = re.sub(r"[\\/:*?\"<>|]", "_", title)
        filename = f"产出_{safe}_终稿.md"
        path = store.ART_DIR / filename
        n = 1
        while path.exists():
            filename = f"产出_{safe}_终稿_{n}.md"
            path = store.ART_DIR / filename
            n += 1
        path.write_text(text, encoding="utf-8")
        store.add_artifact(sid, filename, title)
        return filename


class BudgetExceeded(RuntimeError):
    pass