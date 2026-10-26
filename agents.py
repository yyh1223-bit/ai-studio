# -*- coding: utf-8 -*-
"""
agents.py —— Agent 基类 + 三个角色。
DeepSeek / 备选模型的 key/base/model 运行时只读加载自外部 config
（默认 D:\\deepseek ha\\config.json），绝不写入本仓库任何文件。
参考 D:\\deepseek ha\\bot.py 的 aiohttp 流式调用方式。
"""
import asyncio
import json
import sys
from pathlib import Path

import aiohttp

BASE = Path(__file__).resolve().parent
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")


class ProviderUnavailable(RuntimeError):
    pass


def load_providers():
    """运行时只读加载外部 config 中的模型通道。任何情况都只读，不写回。"""
    cfg_path = BASE / "config.json"
    local = json.loads(cfg_path.read_text(encoding="utf-8"))
    ext_path = Path(local.get("deepseek_config_path"))
    ext = {}
    if ext_path and ext_path.is_file():
        ext = json.loads(ext_path.read_text(encoding="utf-8"))
    return {
        "primary": {
            "name": "deepseek",
            "base": ext.get("deepseek_base", "https://api.deepseek.com"),
            "key": ext.get("deepseek_key", ""),
            "model": local.get("model", "deepseek-chat"),
            "ua": None,
        },
        "fallback": {
            "name": "nachoneko",
            "base": ext.get("nachoneko_base", "https://api.nachoneko.chat/v1"),
            "key": ext.get("nachoneko_key", ""),
            "model": local.get("fallback_model", "deepseek-v4.1-flash"),
            "ua": UA,
        },
    }


def check_available():
    """作为验收兜底：无 key 时明确报错，不允许假装在跑。"""
    p = load_providers()
    if not p["primary"]["key"] and not p["fallback"]["key"]:
        raise ProviderUnavailable("未检测到可用模型 key（主/备通道均无 key）")
    return p


def estimate_cost(model: str, prompt_tokens: int, completion_tokens: int) -> float:
    """按 pricing.json 官方估算单价算成本（元）。"""
    pricing = json.loads((BASE / "pricing.json").read_text(encoding="utf-8"))
    row = pricing.get(model)
    if not row:
        return 0.0
    return (prompt_tokens * row["input"] + completion_tokens * row["output"]) / 1000.0


AGENT_CLASSES = {}


class Agent:
    """Base。channel 决定本 Agent 优先用哪个模型通道。"""

    channel = "primary"  # 子类可改为 "fallback"（审核用便宜模型打杂）

    def __init__(self, role: str, role_cfg: dict):
        self.role = role
        self.name = role_cfg["name"]
        self.emoji = role_cfg["emoji"]
        self.role_label = role_cfg["role"]
        self.system_prompt = role_cfg["system_prompt"]

    # ---- 调用入口 ----------------
    async def stream_chat(self, messages, temperature=0.8, max_retries=2):
        providers = load_providers()
        order = self._provider_order(providers)
        last_err = None
        for attempt in range(max_retries + 1):
            for p in order:
                if not p.get("key"):
                    continue
                try:
                    async for piece in self._stream_one(p, messages, temperature):
                        yield piece
                    return
                except Exception as e:  # noqa: BLE001
                    last_err = e
            if attempt < max_retries:
                await asyncio.sleep(1 + attempt * 2)
        raise ProviderUnavailable(f"[{self.name}] 模型调用失败已达上限: {last_err}")

    # ---- 子类可覆写通道顺序 ----------------
    def _provider_order(self, providers):
        if self.channel == "fallback":
            return [providers["fallback"], providers["primary"]]
        return [providers["primary"], providers["fallback"]]

    async def _stream_one(self, p, messages, temperature):
        # 记录本次实际使用的通道（供成本估算）
        self._last_used = {"model": p["model"], "name": p["name"]}
        headers = {
            "Authorization": f"Bearer {p['key']}",
            "Content-Type": "application/json",
        }
        if p.get("ua"):
            headers["User-Agent"] = p["ua"]
            headers["Accept"] = "application/json"
            headers["HTTP-Referer"] = "https://api.nachoneko.chat"
            headers["X-Title"] = "nachoneko"
        body = {
            "model": p["model"],
            "messages": messages,
            "temperature": temperature,
            "stream": True,
        }
        url = f"{p['base'].rstrip('/')}/chat/completions"
        text_parts = []
        usage = None
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, headers=headers, json=body,
                timeout=aiohttp.ClientTimeout(total=90),
            ) as resp:
                if resp.status != 200:
                    msg = await resp.text()
                    raise ProviderUnavailable(f"{p['name']} HTTP {resp.status}: {msg[:120]}")
                async for raw in resp.content:
                    line = raw.decode("utf-8", errors="replace").strip()
                    if not line.startswith("data:"):
                        continue
                    data = line[5:].strip()
                    if data == "[DONE]":
                        break
                    try:
                        obj = json.loads(data)
                    except json.JSONDecodeError:
                        continue
                    choices = obj.get("choices") or []
                    if choices:
                        delta = (choices[0].get("delta") or {})
                        content = delta.get("content")
                        if content:
                            text_parts.append(content)
                            yield content, None
                    if obj.get("usage"):
                        usage = obj["usage"]
        if usage:
            yield "", {
                "prompt_tokens": usage.get("prompt_tokens", 0),
                "completion_tokens": usage.get("completion_tokens", 0),
            }
        else:
            pt = max(1, sum(len(m.get("content", "")) for m in messages) // 2)
            ct = max(1, sum(len(t) for t in text_parts) // 2)
            yield "", {"prompt_tokens": pt, "completion_tokens": ct}


class ManagerAgent(Agent):
    channel = "primary"


class WriterAgent(Agent):
    channel = "primary"


class ReviewerAgent(Agent):
    channel = "fallback"  # 审核用便宜模型打杂


AGENT_CLASSES = {
    "manager": ManagerAgent,
    "writer": WriterAgent,
    "reviewer": ReviewerAgent,
}


def build_agents(role_cfgs):
    return {role: cls(role, cfg) for role, cfg in role_cfgs.items()
            if (cls := AGENT_CLASSES.get(role))}