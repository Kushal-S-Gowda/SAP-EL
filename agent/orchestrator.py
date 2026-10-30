"""Lightweight tool-using investigation loop. Spark remains the detector."""

from __future__ import annotations

import json
import logging
import re

from agent.llm import LLMUnavailable, OllamaClient
from agent.severity import assess_event_severity
from agent.tools import TOOL_SCHEMAS, execute_tool

logger = logging.getLogger("rtsed.agent")

SYSTEM_PROMPT = """
You are the Stock Event Investigation Agent for an academic stream-processing project.
Spark Structured Streaming already detected a PRICE_SPIKE or VOLUME_SURGE using fixed rules.
Your job is to investigate that detection with tools, then explain the numerical evidence.

Rules:
- Use tools to gather evidence. Do not invent prices, volumes, news, or causes.
- Never claim a news story, rumour, or company announcement caused the event.
- If evidence is missing, say so and lower confidence.
- Distinguish Spark's rule-based detection from your interpretation.
- When finished, reply with JSON only:
  {"investigation_summary": string, "confidence": "LOW"|"MEDIUM"|"HIGH", "uncertainty": string}
- Do not change numerical calculations produced by tools.
""".strip()

JSON_FALLBACK_HINT = (
    "If you cannot use native tool calling, reply with exactly one JSON object: "
    '{"tool": "<tool_name>", "arguments": {..}} or '
    '{"investigation_summary": "...", "confidence": "LOW|MEDIUM|HIGH", "uncertainty": "..."}'
)


class InvestigationAgent:
    def __init__(self, llm: OllamaClient | None = None, max_steps: int | None = None):
        self.llm = llm or OllamaClient()
        from config import AGENT_MAX_TOOL_STEPS

        self.max_steps = max_steps or AGENT_MAX_TOOL_STEPS

    def investigate(self, event: dict) -> dict:
        if not self.llm.is_available():
            raise LLMUnavailable(
                f"Ollama is not reachable at {self.llm.base_url}. "
                "The Kafka/Spark pipeline is unaffected; this investigation stays pending/failed."
            )

        tool_trace = []
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT + "\n" + JSON_FALLBACK_HINT},
            {
                "role": "user",
                "content": (
                    "Investigate this Spark-detected event. "
                    "Call relevant tools before writing the final JSON.\n"
                    + json.dumps(event, default=str)
                ),
            },
        ]

        native_tools = True
        final_report = None
        ran_default_tools = False
        for step in range(self.max_steps):
            try:
                message = self.llm.chat(
                    messages, tools=TOOL_SCHEMAS if native_tools else None
                )
            except LLMUnavailable:
                if native_tools:
                    native_tools = False
                    continue
                raise

            tool_calls = _extract_tool_calls(message)
            content = (message.get("content") or "").strip()

            if tool_calls:
                messages.append(
                    {
                        "role": "assistant",
                        "content": content,
                        "tool_calls": message.get("tool_calls") or tool_calls,
                    }
                )
                for call in tool_calls:
                    result = execute_tool(call["name"], call["arguments"], event)
                    tool_trace.append(
                        {
                            "step": step + 1,
                            "tool": call["name"],
                            "arguments": call["arguments"],
                            "result": result,
                        }
                    )
                    messages.append(
                        {
                            "role": "tool",
                            "tool_name": call["name"],
                            "content": json.dumps(result, default=str),
                        }
                    )
                continue

            parsed = _parse_final_report(content)
            if parsed and tool_trace:
                final_report = parsed
                break
            if parsed and not tool_trace and not ran_default_tools:
                ran_default_tools = True
                tool_trace.extend(_run_default_tools(event))
                messages.append({"role": "assistant", "content": content})
                messages.append(
                    {
                        "role": "user",
                        "content": (
                            "Tool evidence has been collected in Python. "
                            "Using only that evidence, return the final JSON report. "
                            "Do not invent news or causes.\n"
                            + json.dumps(tool_trace, default=str)
                        ),
                    }
                )
                continue
            if not tool_trace and not ran_default_tools:
                ran_default_tools = True
                tool_trace.extend(_run_default_tools(event))
                messages.append(
                    {
                        "role": "user",
                        "content": (
                            "No tool call was returned. The investigation tools were "
                            "executed. Using only this evidence, return the final JSON "
                            "report. Do not invent news or causes.\n"
                            + json.dumps(tool_trace, default=str)
                        ),
                    }
                )
                continue
            if parsed:
                final_report = parsed
                break
            messages.append({"role": "assistant", "content": content})
            messages.append(
                {
                    "role": "user",
                    "content": (
                        "No tool call or final JSON was detected. "
                        "Return the final JSON object now."
                    ),
                }
            )

        if final_report is None:
            raise LLMUnavailable(
                "The model did not return a structured investigation after "
                f"{self.max_steps} steps"
            )

        evidence_numbers = _numbers_from_event_and_tools(event, tool_trace)
        severity = assess_event_severity(
            event_type=event.get("event_type") or "",
            price_change_percent=evidence_numbers["price_change_percent"],
            volume=evidence_numbers["volume"],
            volume_ratio=evidence_numbers["volume_ratio"],
        )
        return {
            "investigation_summary": final_report.get("investigation_summary") or "",
            "confidence": _normalize_confidence(final_report.get("confidence")),
            "uncertainty": final_report.get("uncertainty") or "",
            "severity": severity["severity"],
            "severity_criteria": severity["criteria"],
            "price_change_percent": evidence_numbers["price_change_percent"],
            "volume_ratio": evidence_numbers["volume_ratio"],
            "previous_event_count": evidence_numbers["previous_event_count"],
            "evidence": {
                "spark_event": event,
                "tool_trace": tool_trace,
                "severity": severity,
                "llm_uncertainty": final_report.get("uncertainty"),
            },
        }


DEFAULT_TOOL_PLAN = (
    ("get_event_details", {}),
    ("get_recent_stock_history", {"minutes": 60}),
    ("calculate_price_movement", {"window": 15}),
    ("calculate_volume_anomaly", {"window": 20}),
    ("get_previous_stock_events", {"limit": 10}),
    ("assess_event_severity", {}),
)


def _run_default_tools(event: dict) -> list[dict]:
    trace = []
    for name, arguments in DEFAULT_TOOL_PLAN:
        result = execute_tool(name, arguments, event)
        trace.append(
            {
                "step": "default",
                "tool": name,
                "arguments": arguments,
                "result": result,
            }
        )
    return trace


def _extract_tool_calls(message: dict) -> list[dict]:
    calls = []
    for raw in message.get("tool_calls") or []:
        function = raw.get("function") or raw
        name = function.get("name")
        arguments = function.get("arguments") or {}
        if isinstance(arguments, str):
            try:
                arguments = json.loads(arguments)
            except json.JSONDecodeError:
                arguments = {}
        if name:
            calls.append({"name": name, "arguments": arguments or {}})
    if calls:
        return calls
    content = (message.get("content") or "").strip()
    parsed = _extract_json_object(content)
    if parsed and "tool" in parsed:
        return [{"name": parsed["tool"], "arguments": parsed.get("arguments") or {}}]
    return []


def _parse_final_report(content: str) -> dict | None:
    parsed = _extract_json_object(content)
    if not parsed:
        return None
    if "investigation_summary" in parsed:
        return parsed
    return None


def _extract_json_object(text: str) -> dict | None:
    if not text:
        return None
    try:
        value = json.loads(text)
        return value if isinstance(value, dict) else None
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            return None
        try:
            value = json.loads(match.group(0))
            return value if isinstance(value, dict) else None
        except json.JSONDecodeError:
            return None


def _normalize_confidence(value) -> str:
    text = str(value or "LOW").upper()
    if text in {"LOW", "MEDIUM", "HIGH"}:
        return text
    return "LOW"


def _numbers_from_event_and_tools(event: dict, tool_trace: list[dict]) -> dict:
    price = _to_float(event.get("avg_price_change_pct"))
    volume = _to_float(event.get("total_volume"))
    volume_ratio = None
    previous_count = None
    for item in tool_trace:
        result = item.get("result") or {}
        if item.get("tool") == "calculate_price_movement" and result.get("percent_change") is not None:
            price = _to_float(result.get("percent_change"))
        if item.get("tool") == "calculate_volume_anomaly" and result.get("volume_ratio") is not None:
            volume_ratio = _to_float(result.get("volume_ratio"))
        if item.get("tool") == "get_previous_stock_events":
            previous_count = result.get("previous_event_count")
    return {
        "price_change_percent": price,
        "volume": volume,
        "volume_ratio": volume_ratio,
        "previous_event_count": previous_count,
    }


def _to_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
