from agent.llm import LLMUnavailable
from agent.orchestrator import InvestigationAgent
from agent.severity import assess_event_severity
from agent.tools import execute_tool


class _FakeLLM:
    def __init__(self, script):
        self.script = list(script)
        self.base_url = "http://localhost:11434"

    def is_available(self):
        return True

    def chat(self, messages, tools=None):
        if not self.script:
            return {"content": "{}"}
        return self.script.pop(0)


def test_severity_bands_are_reproducible():
    low = assess_event_severity("PRICE_SPIKE", 1.2, 4000, None)
    high = assess_event_severity("PRICE_SPIKE", 3.2, 4000, 3.1)
    critical = assess_event_severity("VOLUME_SURGE", 0.2, 9000, 7.0)
    assert low["severity"] == "LOW"
    assert high["severity"] == "HIGH"
    assert critical["severity"] == "CRITICAL"


def test_unknown_tool_is_rejected():
    result = execute_tool("drop_table", {}, {"symbol": "TCS.NS"})
    assert "Unknown tool" in result["error"]


def test_agent_uses_tools_then_final_json():
    llm = _FakeLLM(
        [
            {
                "tool_calls": [
                    {
                        "function": {
                            "name": "assess_event_severity",
                            "arguments": {
                                "event_metrics": {
                                    "event_type": "PRICE_SPIKE",
                                    "price_change_percent": 2.0,
                                    "volume": 5000,
                                    "volume_ratio": 1.2,
                                }
                            },
                        }
                    }
                ]
            },
            {
                "content": (
                    '{"investigation_summary":"Spark detected a price spike. '
                    'No news source is integrated so the cause is unknown.",'
                    '"confidence":"MEDIUM","uncertainty":"No candle baseline."}'
                )
            },
        ]
    )
    agent = InvestigationAgent(llm=llm, max_steps=4)
    result = agent.investigate(
        {
            "symbol": "INFY.NS",
            "event_type": "PRICE_SPIKE",
            "avg_price_change_pct": 2.0,
            "total_volume": 5000,
        }
    )
    assert result["severity"] == "MEDIUM"
    assert result["confidence"] == "MEDIUM"
    assert result["evidence"]["tool_trace"][0]["tool"] == "assess_event_severity"
    assert "news" not in result["investigation_summary"].lower() or "no news" in result[
        "investigation_summary"
    ].lower()


def test_agent_raises_when_llm_unavailable():
    class Down:
        base_url = "http://localhost:11434"

        def is_available(self):
            return False

    agent = InvestigationAgent(llm=Down(), max_steps=1)
    try:
        agent.investigate({"symbol": "TCS.NS", "event_type": "VOLUME_SURGE"})
        assert False, "expected LLMUnavailable"
    except LLMUnavailable:
        pass
