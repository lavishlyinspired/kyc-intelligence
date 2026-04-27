"""
Script 09 — KYC GraphRAG agent (LangGraph + LangChain + Neo4j).

Architecture
------------
- LangGraph `create_react_agent` (the modern way) with `MemorySaver` for
  conversation history.
- 5 explicit Cypher tools (find_ubo, check_sanctions, get_risk_score,
  find_circular_ownership, top_risky_entities) — these are deterministic and
  return structured data.
- 1 fallback `general_graph_question` tool that uses GraphCypherQAChain to
  let the LLM write its own read-only Cypher for open questions.
- Auto-selects Anthropic Claude → OpenAI GPT-4o → fail with helpful message.

Skill applied: graphrag-agent

    python scripts/09_graphrag_agent.py "Who really owns ENTITY_0123?"
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from langchain_core.tools import tool
from langgraph.checkpoint.memory import MemorySaver
from langgraph.prebuilt import create_react_agent

from src.kg_client import Neo4jClient

# ─── LLM selection (Anthropic preferred, then OpenAI) ────────────────────────
def get_llm():
    if os.getenv("ANTHROPIC_API_KEY"):
        from langchain_anthropic import ChatAnthropic
        model = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-5-20250929")
        print(f"  Using Anthropic: {model}")
        return ChatAnthropic(model=model, temperature=0)
    if os.getenv("OPENAI_API_KEY"):
        from langchain_openai import ChatOpenAI
        model = os.getenv("OPENAI_MODEL", "gpt-4o")
        print(f"  Using OpenAI: {model}")
        return ChatOpenAI(model=model, temperature=0)
    raise RuntimeError(
        "No LLM API key found. Set ANTHROPIC_API_KEY or OPENAI_API_KEY in .env"
    )


# ─── Module-level Neo4j client (shared by all tools) ─────────────────────────
NEO = Neo4jClient()


# ─── Tools ───────────────────────────────────────────────────────────────────
@tool
def find_ubo(entity_id: str) -> str:
    """Find the Ultimate Beneficial Owner(s) of a legal entity by traversing
    the ownership chain (up to 6 hops). Use this when asked who really owns,
    controls, or is the beneficial owner of an entity.

    Args:
        entity_id: The entity ID, e.g. 'ENTITY_0042'.
    """
    rows = NEO.query("""
        MATCH path = (e:LegalEntity {id: $id})
              -[:DIRECTLY_OWNED_BY*0..6]->()
              -[:CONTROLLED_BY]->(p:NaturalPerson)
        RETURN p.id AS person_id, p.name AS name, p.nationality AS nationality,
               p.isPEP AS is_pep, p.isSanctioned AS is_sanctioned,
               length(path) AS chain_length
        ORDER BY chain_length, name
        LIMIT 10
    """, {"id": entity_id})
    if not rows:
        return f"No UBO found for {entity_id}."
    out = [f"Found {len(rows)} UBO(s) for {entity_id}:"]
    for r in rows:
        flags = []
        if r["is_sanctioned"]: flags.append("⚠ SANCTIONED")
        if r["is_pep"]:        flags.append("PEP")
        flag = f"  [{', '.join(flags)}]" if flags else ""
        out.append(f"  • {r['name']} ({r['nationality']}) "
                   f"— {r['chain_length']} hops away{flag}")
    return "\n".join(out)


@tool
def check_sanctions(entity_id: str) -> str:
    """Check whether an entity has any sanctioned person in its ownership or
    control chain (any depth). Returns YES/NO with details.

    Args:
        entity_id: The entity ID to check.
    """
    rows = NEO.query("""
        MATCH (e:LegalEntity {id: $id})
        OPTIONAL MATCH path = (e)-[:DIRECTLY_OWNED_BY*0..6]->()
                              -[:CONTROLLED_BY]->(p:NaturalPerson {isSanctioned: true})
        RETURN e.name AS entity_name,
               p.name AS sanctioned_person,
               length(path) AS hops
        LIMIT 5
    """, {"id": entity_id})
    if not rows or not rows[0]["sanctioned_person"]:
        return f"NO sanctioned person found in the ownership chain of {entity_id}."
    out = [f"YES — {rows[0]['entity_name']} has sanctioned UBO(s):"]
    for r in rows:
        if r["sanctioned_person"]:
            out.append(f"  ⚠ {r['sanctioned_person']} ({r['hops']} hops away)")
    return "\n".join(out)


@tool
def get_risk_score(entity_id: str) -> str:
    """Return the composite KYC risk score for an entity (0-100) plus the
    breakdown of factors driving the score.

    Args:
        entity_id: The entity ID.
    """
    rows = NEO.query("""
        MATCH (e:LegalEntity {id: $id})
        RETURN e.name AS name, e.jurisdiction AS jurisdiction,
               e.riskTier AS risk_tier, e.kycRiskScore AS score,
               e.pageRankScore AS pagerank, e.louvainCommunityId AS community
    """, {"id": entity_id})
    if not rows:
        return f"Entity {entity_id} not found."
    r = rows[0]
    return (f"{r['name']} ({entity_id})\n"
            f"  Jurisdiction:  {r['jurisdiction']} (risk tier: {r['risk_tier']})\n"
            f"  KYC score:     {r['score']}/100\n"
            f"  PageRank:      {r['pagerank']}\n"
            f"  Community:     {r['community']}")


@tool
def find_circular_ownership() -> str:
    """List all detected circular ownership rings (entities owning each other
    in a loop — strong indicator of shell company structure)."""
    rows = NEO.query("""
        MATCH (e:LegalEntity)
        WITH e.sccComponentId AS component, collect(e) AS members
        WHERE size(members) > 1
        RETURN component, [m IN members | m.id + ' (' + m.name + ')'] AS entities
        ORDER BY size(members) DESC
        LIMIT 20
    """)
    if not rows:
        return "No circular ownership detected."
    out = [f"Found {len(rows)} circular ownership ring(s):"]
    for r in rows:
        out.append(f"  Ring #{r['component']}: " + " → ".join(r["entities"]))
    return "\n".join(out)


@tool
def top_risky_entities(limit: int = 10) -> str:
    """Return the top N riskiest entities by composite KYC score."""
    rows = NEO.query("""
        MATCH (e:LegalEntity) WHERE e.kycRiskScore > 0
        RETURN e.id AS id, e.name AS name,
               e.jurisdiction AS jurisdiction, e.kycRiskScore AS score
        ORDER BY score DESC LIMIT $limit
    """, {"limit": limit})
    return "\n".join(
        f"  [{r['score']:>3}] {r['id']}  {r['name']}  ({r['jurisdiction']})"
        for r in rows
    ) or "No risky entities found."


@tool
def general_graph_question(question: str) -> str:
    """Fallback for open-ended questions about the graph that don't match the
    other tools. The LLM will write its own read-only Cypher.

    Args:
        question: A natural-language question about the data.
    """
    from langchain_neo4j import Neo4jGraph, GraphCypherQAChain
    graph = Neo4jGraph(
        url=os.getenv("NEO4J_URL", "bolt://localhost:7687"),
        username=os.getenv("NEO4J_USER", "neo4j"),
        password=os.getenv("NEO4J_PASSWORD", "kycpassword123"),
    )
    chain = GraphCypherQAChain.from_llm(
        llm=get_llm(),
        graph=graph,
        verbose=False,
        allow_dangerous_requests=True,
        return_intermediate_steps=False,
    )
    return chain.invoke({"query": question})["result"]


# ─── Build the agent ─────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are a KYC/AML investigation assistant.

You have a knowledge graph of legal entities, natural persons, ownership and
control relationships, and transactions. Use the available tools to answer
questions about beneficial ownership, sanctions exposure, circular ownership,
and risk scores.

Always reason step-by-step, cite the entity IDs you used, and flag any sanctions
or PEP exposure prominently. If the user asks an open question that doesn't
match a specific tool, use `general_graph_question`.
"""


def build_agent():
    print("→ Building GraphRAG agent ...")
    llm = get_llm()
    tools = [
        find_ubo, check_sanctions, get_risk_score,
        find_circular_ownership, top_risky_entities, general_graph_question,
    ]
    memory = MemorySaver()
    agent = create_react_agent(llm, tools, checkpointer=memory, prompt=SYSTEM_PROMPT)
    print(f"  ✓ {len(tools)} tools registered")
    return agent


def main(question: str | None = None) -> int:
    agent = build_agent()
    config = {"configurable": {"thread_id": "kyc-session-1"}}

    if question is None:
        print("\n💬 KYC Agent ready. Type 'quit' to exit.\n")
        while True:
            try:
                q = input("You: ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if not q or q.lower() in {"quit", "exit"}:
                break
            result = agent.invoke({"messages": [("user", q)]}, config=config)
            print(f"\nAgent: {result['messages'][-1].content}\n")
        return 0

    result = agent.invoke({"messages": [("user", question)]}, config=config)
    print(f"\n{result['messages'][-1].content}\n")
    return 0


if __name__ == "__main__":
    q = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else None
    raise SystemExit(main(q))
