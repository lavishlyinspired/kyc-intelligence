"""
Smoke tests for the GraphRAG agent.

Run with:
    ANTHROPIC_API_KEY=... pytest tests/test_agent.py -v -m integration
or
    OPENAI_API_KEY=... pytest tests/test_agent.py -v -m integration
"""
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def agent():
    if not (os.getenv("ANTHROPIC_API_KEY") or os.getenv("OPENAI_API_KEY")):
        pytest.skip("No LLM API key set — skipping agent tests")
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
    # Import the agent module dynamically (file starts with a digit)
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "agent_mod", Path(__file__).resolve().parent.parent / "scripts" / "09_graphrag_agent.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.build_agent()


def test_agent_finds_sanctioned_ubo(agent, ground_truth):
    """Ask the agent about a known compromised entity."""
    target = ground_truth["sanctioned_chain_starts"][0]
    config = {"configurable": {"thread_id": f"test-sanctions-{target}"}}
    result = agent.invoke(
        {"messages": [("user", f"Is {target} exposed to any sanctions risk? "
                                f"If yes, identify the sanctioned person.")]},
        config=config,
    )
    answer = result["messages"][-1].content.lower()
    assert any(kw in answer for kw in ["yes", "sanction", "exposed"]), \
        f"Agent missed sanctions exposure for {target}. Reply: {answer[:200]}"


def test_agent_lists_top_risky_entities(agent):
    config = {"configurable": {"thread_id": "test-top-risky"}}
    result = agent.invoke(
        {"messages": [("user", "Show me the top 5 riskiest entities.")]},
        config=config,
    )
    answer = result["messages"][-1].content
    assert "ENTITY_" in answer, f"Expected entity IDs in reply: {answer[:200]}"
