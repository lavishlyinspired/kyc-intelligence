---
name: graphrag-agent
description: "Use when building or extending the LangGraph KYC investigation agent; wiring Cypher queries as LangChain tools; using create_react_agent / StateGraph; configuring Anthropic or OpenAI LLMs; adding agent memory; debugging tool-calling errors. Covers latest LangGraph 0.2+ patterns, langchain-neo4j 0.4+ Neo4jGraph, neo4j-graphrag retrievers, and the Going Meta S27/S34/S43/S45 patterns."
---

# GraphRAG KYC Agent Skill

## When to use

User asks to "build a KYC chatbot", "add a new investigation tool to the agent", "make the agent ask follow-up questions", "use Claude/GPT to query the knowledge graph", "the agent doesn't call my tool".

## Architecture (latest LangGraph pattern, Apr 2026)

```
User question
    │
    ▼
┌────────────────────────────────────────────┐
│  langgraph.prebuilt.create_react_agent     │
│      (replaces deprecated AgentExecutor)    │
│                                             │
│  Tools (LangChain @tool functions):         │
│    • find_ubo                               │
│    • check_sanctions                        │
│    • get_risk_score                         │
│    • find_circular_ownership                │
│    • run_cypher  (free-form, sandboxed)     │
│    • run_sparql  (queries GraphDB)          │
│                                             │
│  Each tool wraps a Cypher/SPARQL query and  │
│  returns a string the LLM can read.         │
│                                             │
│  Optional: MemorySaver for multi-turn       │
└────────────────────────────────────────────┘
    │
    ▼
LLM (Claude Sonnet 4.5 or GPT-4o) synthesises a final answer with cited evidence
```

## Latest API (langgraph 0.2+ / langchain 0.3+)

### Setup
```python
from langgraph.prebuilt import create_react_agent
from langgraph.checkpoint.memory import MemorySaver
from langchain_anthropic import ChatAnthropic       # or langchain_openai.ChatOpenAI
from langchain_neo4j import Neo4jGraph              # NOT langchain_community
from langchain_core.tools import tool

llm = ChatAnthropic(model="claude-sonnet-4-5-20250929", temperature=0)
graph = Neo4jGraph(url=..., username=..., password=..., enhanced_schema=True)

@tool
def find_ubo(company_name: str) -> str:
    """Find Ultimate Beneficial Owner of a company by traversing
    DIRECTLY_OWNED_BY/CONTROLLED_BY relationships up to 10 hops."""
    rows = graph.query("MATCH ...", params={"name": company_name})
    return format_for_llm(rows)

agent = create_react_agent(
    model=llm,
    tools=[find_ubo, check_sanctions, get_risk_score, ...],
    checkpointer=MemorySaver(),     # enables thread_id-based memory
    prompt="You are a KYC investigator. Use the tools to investigate."
)

# Invoke
config = {"configurable": {"thread_id": "case-001"}}
result = agent.invoke(
    {"messages": [("user", "Who owns Shell Corp?")]},
    config=config
)
print(result["messages"][-1].content)
```

### Streaming (great for UIs)
```python
for chunk in agent.stream(
    {"messages": [("user", question)]},
    config=config,
    stream_mode="values"
):
    chunk["messages"][-1].pretty_print()
```

## Tool-writing rules (essential for the LLM to actually call them)

1. **Docstring is the contract** — the LLM reads it. Be explicit about *when* to use the tool.
2. **Return a string** the LLM can interpret, NOT a dict (those get stringified poorly).
3. **Surface uncertainty in the return** — "No matches found for X" beats an empty list.
4. **Validate inputs early** — return a friendly error string instead of throwing.
5. **One job per tool** — don't make a "do_everything" tool; the agent's strength is *composing* tools.
6. **Type hints** — LangChain reads them to build the JSON schema for the LLM.

Bad:
```python
@tool
def query_database(query: str) -> dict:
    return graph.query(query)  # raw Cypher = security hole + LLM gets confused
```

Good:
```python
@tool
def find_ubo(company_name: str, max_hops: int = 10) -> str:
    """Find the Ultimate Beneficial Owner of a company by traversing the
    ownership chain. Use this whenever the user asks who 'really owns',
    'ultimately controls', or 'is the UBO of' a company.

    Args:
        company_name: Substring of the company name (case-insensitive).
        max_hops: Maximum chain length (default 10).
    """
    rows = graph.query("MATCH ... LIMIT 5", params={"name": company_name})
    if not rows:
        return f"No company found matching '{company_name}'."
    lines = [f"UBO results for '{company_name}':"]
    for r in rows:
        lines.append(f"- {r['ubo']} via {r['hops']} hops, status: {r['status']}")
    return "\n".join(lines)
```

## Choosing the LLM

| LLM | Strengths for this use case |
|---|---|
| `claude-sonnet-4-5-20250929` (Anthropic) | Best tool calling, strong on chained reasoning, expensive |
| `gpt-4o-mini` (OpenAI) | Fastest + cheapest with good tool calling |
| `gpt-4o` (OpenAI) | Most reliable for complex investigations |
| Local (Ollama) | Going Meta S35 pattern, free, lower quality |

The agent code falls back gracefully: prefers Anthropic if `ANTHROPIC_API_KEY` is set, otherwise OpenAI.

## Adding a new investigation tool

1. Write the Cypher (test it in Neo4j Browser first).
2. Wrap in a `@tool` function with a great docstring.
3. Append to the `tools` list passed to `create_react_agent`.
4. Add a test scenario in `tests/test_agent.py` with a known-answer question.

## Memory & multi-turn

```python
checkpointer = MemorySaver()        # in-memory; for prod use SqliteSaver/PostgresSaver
agent = create_react_agent(model=llm, tools=tools, checkpointer=checkpointer)

# Same thread_id → agent remembers context
config = {"configurable": {"thread_id": "investigator-alice-case-42"}}
agent.invoke({"messages": [("user", "Show me Shell Corp's UBOs")]}, config=config)
agent.invoke({"messages": [("user", "Are any of them on sanctions?")]}, config=config)  # remembers!
```

## Optional: Text2Cypher fallback

For open-ended questions not covered by tools, use `langchain-neo4j`'s `GraphCypherQAChain`:

```python
from langchain_neo4j import GraphCypherQAChain

cypher_chain = GraphCypherQAChain.from_llm(
    llm=llm,
    graph=graph,
    verbose=True,
    allow_dangerous_requests=True   # required flag in 0.4+
)

@tool
def general_graph_question(question: str) -> str:
    """Answer any general question about the KYC graph that isn't covered by
    the specialised tools. Uses LLM-generated Cypher under the hood."""
    return cypher_chain.invoke({"query": question})["result"]
```

## Common pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| Agent never calls a tool | Tool docstring too vague | Add explicit trigger phrases ("Use this when ...") |
| `Tool schema validation error` | Mixed type hints (e.g. `Union[str, int]`) | Stick to simple types; use Pydantic models for complex inputs |
| `RateLimitError` | Hammering the LLM | Add `model_kwargs={"max_tokens": 1024}`, use streaming |
| `Could not import langchain_anthropic` | Missing pkg | `pip install langchain-anthropic` |
| `Neo4jGraph schema is empty` | Driver auth fail or empty DB | Check `.env`, run `MATCH (n) RETURN count(n)` |
| Agent hallucinates entity names | Lacks tool result; or low-quality LLM | Force lookup tool first via system prompt |
| Memory doesn't persist across runs | Using `MemorySaver` (in-memory) | Switch to `SqliteSaver.from_conn_string("checkpoints.sqlite")` |

## Reference

- `scripts/09_graphrag_agent.py` — full implementation
- `tests/test_agent.py` — scenario tests
- Going Meta S27 (LangGraph reflection), S34 (ontology-driven tools), S43 (memory), S45 (skills)
