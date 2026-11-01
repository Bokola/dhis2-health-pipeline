"""
run the assistant on open-weight models through any openai-compatible endpoint:
ollama, vllm, llama.cpp server, lm studio, or hosted providers that speak the same api

the agent is written against anthropic's messages api. this adapter exposes the same
client.messages.create(...) call and translates requests and responses both ways,
so agent.py, the guard and the eval need no changes

    client = OpenAICompatClient(base_url="http://localhost:11434/v1")
    Assistant(con, ctx, client=client, model="qwen2.5:14b")
"""
from __future__ import annotations

import json
import os
import re
import time
import uuid
from types import SimpleNamespace
from typing import Any

import httpx


class QuotaExhausted(RuntimeError):
    """the endpoint's daily (or other long) limit is used up: retrying now is pointless"""


class OpenAICompatClient:
    def __init__(self, base_url: str | None = None, api_key: str | None = None,
                 timeout: float | None = None, transport: httpx.BaseTransport | None = None,
                 min_interval: float | None = None, max_retries: int = 5, max_wait: float = 120.0):
        # cpu inference (e.g. ollama on a ci runner) can take minutes per call
        timeout = timeout or float(os.environ.get("ASSISTANT_HTTP_TIMEOUT", "600"))
        # seconds between requests, e.g. 4.5 to stay under 15 requests/minute
        self.min_interval = min_interval if min_interval is not None else float(os.environ.get("ASSISTANT_MIN_INTERVAL", "0"))
        self.max_retries, self.max_wait = max_retries, max_wait
        self._last_call = 0.0
        self.base_url = (base_url or os.environ.get("OPENAI_BASE_URL", "http://localhost:11434/v1")).rstrip("/")
        # local servers ignore the key but some clients insist on sending one
        key = api_key or os.environ.get("OPENAI_API_KEY", "not-needed")
        self._http = httpx.Client(timeout=timeout, transport=transport,
                                  headers={"Authorization": f"Bearer {key}"})
        self.messages = SimpleNamespace(create=self._create)

    def preflight(self, model: str) -> str | None:
        """returns a human-readable problem, or None if the server is up and serves the model"""
        try:
            r = self._http.get(f"{self.base_url}/models", timeout=10)
            r.raise_for_status()
        except httpx.TransportError as e:
            return (f"can't reach an openai-compatible server at {self.base_url} ({type(e).__name__}). "
                    "for ollama: install it, run `ollama serve`, then `ollama pull <model>`")
        except httpx.HTTPStatusError:
            return None  # server is up but has no /models listing (e.g. github models): skip the check
        try:
            ids = [m.get("id") for m in (r.json().get("data") or [])]
        except (ValueError, AttributeError):
            return None
        if ids and model not in ids:
            return f"model '{model}' is not available on {self.base_url}. available: {', '.join(ids)}"
        return None

    # ---------- request: anthropic shape -> openai shape ----------

    def _create(self, *, model: str, max_tokens: int, messages: list[dict], system: Any = None,
                tools: list[dict] | None = None, tool_choice: dict | None = None, **_ignored) -> SimpleNamespace:
        body: dict[str, Any] = {
            "model": model,
            "max_tokens": max_tokens,
            "temperature": 0,
            "messages": _convert_messages(system, messages),
        }
        if tools:
            body["tools"] = [{"type": "function", "function": {
                "name": t["name"], "description": t.get("description", ""),
                "parameters": t["input_schema"]}} for t in tools]
            # anthropic "any" means "must call some tool"; openai calls that "required"
            if tool_choice:
                body["tool_choice"] = "required" if tool_choice.get("type") == "any" else "auto"

        r = self._post(body)
        if r.status_code == 400 and body.get("tool_choice") == "required":
            # some servers reject "required"; retry and rely on the agent's nudge
            body["tool_choice"] = "auto"
            r = self._post(body)
        if r.status_code >= 400:
            raise httpx.HTTPStatusError(f"{r.status_code} from {self.base_url}: {r.text[:500]}",
                                        request=r.request, response=r)
        return _convert_response(r.json(), tool_names={t["name"] for t in tools or []})

    def _post(self, body: dict) -> httpx.Response:
        for attempt in range(self.max_retries + 1):
            wait = self.min_interval - (time.monotonic() - self._last_call)
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.monotonic()
            r = self._http.post(f"{self.base_url}/chat/completions", json=body)
            if r.status_code != 429 and r.status_code < 500:
                return r
            if attempt == self.max_retries:
                return r
            delay = _retry_after(r) or min(2 ** attempt * 5, 60)
            if r.status_code == 429 and delay > self.max_wait:
                raise QuotaExhausted(f"rate limit at {self.base_url}, retry in {delay:.0f}s: {r.text[:300]}")
            time.sleep(delay)
        return r


def _retry_after(r: httpx.Response) -> float | None:
    h = r.headers.get("retry-after")
    if h:
        try:
            return float(h)
        except ValueError:
            pass
    m = re.search(r"wait (\d+) seconds", r.text or "")
    return float(m.group(1)) if m else None


def _system_text(system: Any) -> str:
    if not system:
        return ""
    if isinstance(system, str):
        return system
    return "\n".join(b.get("text", "") for b in system if isinstance(b, dict))


def _convert_messages(system: Any, messages: list[dict]) -> list[dict]:
    out: list[dict] = []
    sys_text = _system_text(system)
    if sys_text:
        out.append({"role": "system", "content": sys_text})

    for m in messages:
        role, content = m["role"], m["content"]
        if isinstance(content, str):
            out.append({"role": role, "content": content})
            continue

        if role == "assistant":
            text = "".join(b.get("text", "") for b in content if b.get("type") == "text")
            calls = [{"id": b["id"], "type": "function",
                      "function": {"name": b["name"], "arguments": json.dumps(b.get("input", {}))}}
                     for b in content if b.get("type") == "tool_use"]
            msg: dict[str, Any] = {"role": "assistant", "content": text or None}
            if calls:
                msg["tool_calls"] = calls
            out.append(msg)
            continue

        # user turn: tool results become role=tool messages, any text stays a user message
        texts = []
        for b in content:
            if b.get("type") == "tool_result":
                c = b.get("content", "")
                if isinstance(c, list):
                    c = "".join(x.get("text", "") for x in c if isinstance(x, dict))
                if b.get("is_error"):
                    c = f"ERROR: {c}"
                out.append({"role": "tool", "tool_call_id": b["tool_use_id"], "content": c})
            elif b.get("type") == "text":
                texts.append(b["text"])
        if texts:
            out.append({"role": "user", "content": "\n".join(texts)})
    return out


# ---------- response: openai shape -> anthropic shape ----------

def _parse_args(raw: Any) -> dict:
    if isinstance(raw, dict):
        return raw
    try:
        val = json.loads(raw or "{}")
        return val if isinstance(val, dict) else {}
    except json.JSONDecodeError:
        return {}


_JSON_OBJ = re.compile(r"\{.*\}", re.DOTALL)


def _tool_call_from_text(text: str, tool_names: set[str]) -> dict | None:
    """small local models sometimes write the tool call as json text instead of using tool_calls"""
    m = _JSON_OBJ.search(text or "")
    if not m:
        return None
    try:
        obj = json.loads(m.group(0))
    except json.JSONDecodeError:
        return None
    name = obj.get("name") or obj.get("tool") or obj.get("function")
    args = obj.get("arguments", obj.get("parameters", obj.get("input")))
    if name in tool_names and args is not None:
        return {"type": "tool_use", "id": f"call_{uuid.uuid4().hex[:12]}", "name": name, "input": _parse_args(args)}
    return None


def _convert_response(data: dict, tool_names: set[str]) -> SimpleNamespace:
    msg = (data.get("choices") or [{}])[0].get("message", {}) or {}
    blocks: list[dict] = []
    text = msg.get("content") or ""
    calls = msg.get("tool_calls") or []

    for c in calls:
        fn = c.get("function", {})
        blocks.append({"type": "tool_use", "id": c.get("id") or f"call_{uuid.uuid4().hex[:12]}",
                       "name": fn.get("name", ""), "input": _parse_args(fn.get("arguments"))})
    if not calls and text:
        recovered = _tool_call_from_text(text, tool_names)
        if recovered:
            blocks.append(recovered)
            text = ""
    if text:
        blocks.insert(0, {"type": "text", "text": text})

    u = data.get("usage") or {}
    usage = SimpleNamespace(input_tokens=u.get("prompt_tokens", 0), output_tokens=u.get("completion_tokens", 0))
    return SimpleNamespace(content=blocks, usage=usage)
