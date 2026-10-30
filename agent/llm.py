"""Ollama chat client with native tool-calling when the model supports it."""

from __future__ import annotations

import json
import urllib.error
import urllib.request

import config


class LLMUnavailable(Exception):
    pass


class OllamaClient:
    def __init__(
        self,
        base_url: str | None = None,
        model: str | None = None,
        timeout: int = 120,
    ):
        self.base_url = (base_url or config.OLLAMA_BASE_URL).rstrip("/")
        self.model = model or config.OLLAMA_MODEL
        self.timeout = timeout

    def is_available(self) -> bool:
        try:
            with urllib.request.urlopen(
                f"{self.base_url}/api/tags", timeout=3
            ) as response:
                return 200 <= response.status < 300
        except (urllib.error.URLError, TimeoutError, OSError):
            return False

    def chat(self, messages: list[dict], tools: list[dict] | None = None) -> dict:
        payload = {
            "model": self.model,
            "messages": messages,
            "stream": False,
        }
        if tools:
            payload["tools"] = tools
        data = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            f"{self.base_url}/api/chat",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                body = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            raise LLMUnavailable(
                f"Ollama HTTP {error.code} for model {self.model}: {detail}"
            ) from error
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            raise LLMUnavailable(f"Ollama is unavailable: {error}") from error
        return body.get("message") or {}
