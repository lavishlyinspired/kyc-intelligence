"""
KYC Intelligence — Agentic Chat Engine (LangGraph + LangChain + Neo4j)

Architecture
────────────
• LangGraph ReAct agent with MemorySaver for multi-turn conversation.
• 15+ deterministic tools (Cypher/SPARQL) — results come directly from the
  graph, not from LLM generation, preventing hallucination.
• 1 schema-constrained Text2Cypher fallback for open-ended questions.
• 1 SPARQL tool for ontology/GraphDB questions.
• Session-based memory — supports follow-up questions within a session.
• LLM priority: Anthropic → OpenAI → DeepSeek → Ollama (local).

Anti-hallucination strategy:
  1. All structured tools return data DIRECTLY from Neo4j/GraphDB.
  2. The Text2Cypher fallback is constrained by the full schema definition.
  3. The system prompt instructs the LLM to NEVER fabricate data.
  4. Results are always attributed to the graph query that produced them.
"""
from __future__ import annotations

import json
import os
import sys
import re
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langgraph.checkpoint.memory import MemorySaver
from langgraph.prebuilt import create_react_agent

from src.kg_client import Neo4jClient, GraphDBClient

# ─── Shared Clients ──────────────────────────────────────────────────────────
_neo = Neo4jClient()
_gdb = GraphDBClient()


# ─── LLM Selection ───────────────────────────────────────────────────────────
def _get_llm():
    """Auto-select LLM: Anthropic → OpenAI → DeepSeek → Ollama."""
    if os.getenv("ANTHROPIC_API_KEY"):
        from langchain_anthropic import ChatAnthropic
        model = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-5-20250929")
        return ChatAnthropic(model=model, temperature=0)

    if os.getenv("OPENAI_API_KEY"):
        from langchain_openai import ChatOpenAI
        model = os.getenv("OPENAI_MODEL", "gpt-4o")
        return ChatOpenAI(model=model, temperature=0)

    if os.getenv("DEEPSEEK_API_KEY"):
        from langchain_openai import ChatOpenAI
        model = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")
        return ChatOpenAI(
            model=model, temperature=0,
            api_key=os.environ["DEEPSEEK_API_KEY"],
            base_url=os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1"),
        )

    if os.getenv("OLLAMA_MODEL"):
        from langchain_openai import ChatOpenAI
        model = os.environ["OLLAMA_MODEL"]
        base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1")
        return ChatOpenAI(
            model=model, temperature=0,
            api_key="ollama",
            base_url=base_url,
        )

    raise RuntimeError(
        "No LLM configured. Set ONE of: ANTHROPIC_API_KEY, OPENAI_API_KEY, "
        "DEEPSEEK_API_KEY, or OLLAMA_MODEL in .env"
    )


# ─── Neo4j Schema (for grounding the LLM) ────────────────────────────────────
NEO4J_SCHEMA = """
Node Labels and Properties:
- LegalEntity: id, name, lei, jurisdiction, jurisdictionName, category, incorporatedDate,
  isActive, hasOperationalAddress, kycRiskScore, riskTier, pageRankScore, betweennessScore,
  louvainCommunityId, sccComponentId, wccComponentId
- NaturalPerson: id, name, nationality, dob, isPEP, isSanctioned, pageRankScore,
  betweennessScore, louvainCommunityId, sccComponentId, wccComponentId
- PoliticallyExposedPerson (extends NaturalPerson): same properties, isPEP=true
- SanctionedEntity (extends NaturalPerson): same properties, isSanctioned=true

Relationships:
- (LegalEntity)-[:DIRECTLY_OWNED_BY {percentage, since}]->(LegalEntity)
  Meaning: child entity is directly owned by parent entity with given percentage
- (LegalEntity)-[:CONTROLLED_BY {role, since}]->(NaturalPerson)
  Meaning: entity is controlled by a natural person (ultimate controller)
- (LegalEntity)-[:TRANSACTION {id, date, amount, currency, isSuspicious}]->(LegalEntity)
  Meaning: financial transaction between entities

Key domain facts:
- Entities with sccComponentId != null are in circular ownership rings
- Entities with hasOperationalAddress=false are potential shell companies
- Offshore jurisdictions: KY (Cayman), VG (BVI), PA (Panama), SC (Seychelles)
- Risk tiers: low (0-24), medium (25-49), high (50-74), critical (75-100)
- UBO discovery: traverse DIRECTLY_OWNED_BY chain then CONTROLLED_BY to find ultimate controllers
- GDS scores written back: pageRankScore, betweennessScore, louvainCommunityId, wccComponentId, sccComponentId
"""

GRAPHDB_SCHEMA = """
GraphDB contains RDF/OWL ontologies:
- FIBO (Financial Industry Business Ontology) classes for legal entities, ownership, control
- GLEIF entity instances mapped to FIBO classes
- KYC-KG custom ontology extending FIBO
- LCC ISO-3166 country codes
- Named graphs: http://kg/fibo, http://kg/glei/instances, http://kg/kyc-kg, http://kg/lcc/iso3166, etc.
Use SPARQL SELECT queries only. No INSERT/DELETE/UPDATE.
"""


# ─── Tools ────────────────────────────────────────────────────────────────────

@tool
def find_ultimate_beneficial_owners(entity_id: str) -> str:
    """Find the Ultimate Beneficial Owner(s) of a legal entity by traversing
    the ownership chain up to 6 hops. Shows who really controls the entity.

    Args:
        entity_id: The entity ID, e.g. 'ENTITY_0042'.
    """
    rows = _neo.query("""
        MATCH path = (e:LegalEntity {id: $id})
              -[:DIRECTLY_OWNED_BY*0..6]->()
              -[:CONTROLLED_BY]->(p:NaturalPerson)
        RETURN p.id AS person_id, p.name AS name, p.nationality AS nationality,
               p.isPEP AS is_pep, p.isSanctioned AS is_sanctioned,
               length(path) AS chain_length,
               [n IN nodes(path) | coalesce(n.name, n.id)] AS chain
        ORDER BY chain_length, name
        LIMIT 10
    """, {"id": entity_id})
    if not rows:
        return f"No UBO found for {entity_id}. The entity may have no ownership chain to a natural person."
    out = [f"UBO analysis for {entity_id} — {len(rows)} beneficial owner(s):"]
    for r in rows:
        flags = []
        if r["is_sanctioned"]: flags.append("🔴 SANCTIONED")
        if r["is_pep"]: flags.append("🟣 PEP")
        flag = f"  [{', '.join(flags)}]" if flags else ""
        chain_str = " → ".join(str(x) for x in r["chain"])
        out.append(f"  • {r['name']} ({r['nationality']}) — {r['chain_length']} hops{flag}")
        out.append(f"    Chain: {chain_str}")
    return "\n".join(out)


@tool
def check_sanctions_exposure(entity_id: str) -> str:
    """Check if an entity has any sanctioned persons in its ownership/control
    chain at any depth. Critical for KYC compliance.

    Args:
        entity_id: The entity ID to check.
    """
    rows = _neo.query("""
        MATCH (e:LegalEntity {id: $id})
        OPTIONAL MATCH path = (e)-[:DIRECTLY_OWNED_BY*0..6]->()
                              -[:CONTROLLED_BY]->(p:NaturalPerson {isSanctioned: true})
        RETURN e.name AS entity_name, e.kycRiskScore AS score,
               p.name AS sanctioned_person, p.nationality AS nationality,
               length(path) AS hops
        LIMIT 10
    """, {"id": entity_id})
    if not rows:
        return f"Entity {entity_id} not found in the knowledge graph."
    if not rows[0]["sanctioned_person"]:
        return f"✅ NO sanctioned person found in ownership chain of {entity_id} ({rows[0]['entity_name']})."
    ename = rows[0]["entity_name"]
    out = [f"⚠️ SANCTIONS EXPOSURE for {entity_id} ({ename}, risk score {rows[0]['score']}):"]
    for r in rows:
        if r["sanctioned_person"]:
            out.append(f"  🔴 {r['sanctioned_person']} ({r['nationality']}) — {r['hops']} hops away")
    return "\n".join(out)


@tool
def get_entity_risk_profile(entity_id: str) -> str:
    """Get comprehensive risk profile for an entity including KYC score,
    risk tier, GDS analytics (PageRank, betweenness, community), ownership
    structure, and red flags.

    Args:
        entity_id: The entity ID.
    """
    rows = _neo.query("""
        MATCH (e:LegalEntity {id: $id})
        OPTIONAL MATCH (e)-[:CONTROLLED_BY]->(ctrl:NaturalPerson)
        OPTIONAL MATCH (e)-[r:DIRECTLY_OWNED_BY]->(parent:LegalEntity)
        OPTIONAL MATCH (child:LegalEntity)-[:DIRECTLY_OWNED_BY]->(e)
        OPTIONAL MATCH (e)-[t:TRANSACTION]-(other:LegalEntity)
        WITH e, collect(DISTINCT {name: ctrl.name, pep: ctrl.isPEP, sanctioned: ctrl.isSanctioned}) AS controllers,
             count(DISTINCT parent) AS parentCount, count(DISTINCT child) AS childCount,
             count(DISTINCT t) AS txnCount,
             sum(CASE WHEN t.isSuspicious THEN 1 ELSE 0 END) AS suspiciousTxns
        RETURN e {.*} AS entity, controllers, parentCount, childCount, txnCount, suspiciousTxns
    """, {"id": entity_id})
    if not rows:
        return f"Entity {entity_id} not found."
    r = rows[0]
    e = r["entity"]
    ctrls = [c for c in r["controllers"] if c.get("name")]
    flags = []
    if e.get("sccComponentId") is not None: flags.append("In circular ownership ring")
    if not e.get("hasOperationalAddress"): flags.append("No operational address (shell indicator)")
    if e.get("jurisdiction") in ["KY", "VG", "PA", "SC"]: flags.append("Offshore jurisdiction")
    for c in ctrls:
        if c.get("sanctioned"): flags.append(f"Sanctioned controller: {c['name']}")
        if c.get("pep"): flags.append(f"PEP controller: {c['name']}")
    if r["suspiciousTxns"] > 0: flags.append(f"{r['suspiciousTxns']} suspicious transactions")

    out = [
        f"Risk Profile: {e.get('name')} ({entity_id})",
        f"  Jurisdiction: {e.get('jurisdiction')} ({e.get('jurisdictionName', '')})",
        f"  Category: {e.get('category')}",
        f"  Risk Tier: {e.get('riskTier')} | KYC Score: {e.get('kycRiskScore')}/100",
        f"  PageRank: {e.get('pageRankScore', 0):.4f} | Betweenness: {e.get('betweennessScore', 0):.2f}",
        f"  Community: #{e.get('louvainCommunityId')} | SCC Ring: {e.get('sccComponentId', 'None')}",
        f"  Parents: {r['parentCount']} | Children: {r['childCount']} | Transactions: {r['txnCount']}",
        f"  Controllers: {', '.join(c['name'] for c in ctrls) if ctrls else 'None'}",
        f"  Red Flags: {'; '.join(flags) if flags else 'None detected'}",
    ]
    return "\n".join(out)


@tool
def find_circular_ownership_rings() -> str:
    """Find all circular ownership rings (entities owning each other in loops).
    Strong indicator of shell company structures or layered ownership for
    obfuscation purposes."""
    rows = _neo.query("""
        MATCH (e:LegalEntity)
        WHERE e.sccComponentId IS NOT NULL
        WITH e.sccComponentId AS ring, collect(e) AS members
        WHERE size(members) > 1
        RETURN ring,
               [m IN members | {id: m.id, name: m.name, jur: m.jurisdiction, score: m.kycRiskScore}] AS entities,
               size(members) AS ringSize,
               toInteger(reduce(s=0.0, m IN members | s + m.kycRiskScore) / size(members)) AS avgRisk
        ORDER BY avgRisk DESC
    """)
    if not rows:
        return "No circular ownership rings detected in the graph."
    out = [f"Found {len(rows)} circular ownership ring(s):\n"]
    for r in rows:
        ents = r["entities"]
        out.append(f"Ring #{r['ring']} — {r['ringSize']} entities, avg risk: {r['avgRisk']}")
        for e in ents[:5]:
            out.append(f"  • {e['id']} {e['name']} ({e['jur']}) — score {e['score']}")
        if len(ents) > 5:
            out.append(f"  ... and {len(ents) - 5} more")
        out.append("")
    return "\n".join(out)


@tool
def find_shell_companies() -> str:
    """Find entities that appear to be shell companies (no operational address,
    potentially in offshore jurisdictions). Shell companies are high-risk for
    money laundering and ownership obfuscation."""
    rows = _neo.query("""
        MATCH (e:LegalEntity {hasOperationalAddress: false})
        OPTIONAL MATCH (e)-[:CONTROLLED_BY]->(p:NaturalPerson)
        RETURN e.id AS id, e.name AS name, e.jurisdiction AS jur,
               e.kycRiskScore AS score, e.riskTier AS tier,
               e.sccComponentId AS ring,
               collect(DISTINCT {name: p.name, sanctioned: p.isSanctioned, pep: p.isPEP}) AS controllers
        ORDER BY e.kycRiskScore DESC LIMIT 20
    """)
    if not rows:
        return "No shell companies detected."
    out = [f"Found {len(rows)} potential shell companies (no operational address):\n"]
    for r in rows:
        flags = []
        for c in r["controllers"]:
            if c.get("sanctioned"): flags.append(f"🔴 {c['name']}")
            if c.get("pep"): flags.append(f"🟣 {c['name']}")
        ring_note = f" [Ring #{r['ring']}]" if r["ring"] is not None else ""
        out.append(f"  {r['id']} {r['name']} ({r['jur']}) — score {r['score']}, tier {r['tier']}{ring_note}")
        if flags:
            out.append(f"    Controllers: {', '.join(flags)}")
    return "\n".join(out)


@tool
def get_top_risk_entities(limit: int = 15) -> str:
    """Get the entities with highest KYC risk scores. Useful for identifying
    the most concerning entities in the knowledge graph.

    Args:
        limit: Number of results (default 15, max 50).
    """
    limit = min(max(limit, 1), 50)
    rows = _neo.query("""
        MATCH (e:LegalEntity)
        WHERE e.kycRiskScore > 0
        RETURN e.id AS id, e.name AS name, e.jurisdiction AS jur,
               e.kycRiskScore AS score, e.riskTier AS tier, e.category AS category,
               e.sccComponentId AS ring
        ORDER BY e.kycRiskScore DESC LIMIT $limit
    """, {"limit": limit})
    out = [f"Top {len(rows)} riskiest entities:\n"]
    for r in rows:
        ring = f" [Ring #{r['ring']}]" if r["ring"] is not None else ""
        out.append(f"  [{r['score']:>3}] {r['id']}  {r['name']}  ({r['jur']}, {r['category']}){ring}")
    return "\n".join(out)


@tool
def find_pep_connections() -> str:
    """Find all Politically Exposed Persons (PEPs) and the entities they
    control. PEPs require enhanced due diligence."""
    rows = _neo.query("""
        MATCH (p:NaturalPerson {isPEP: true})<-[:CONTROLLED_BY]-(e:LegalEntity)
        RETURN p.id AS pepId, p.name AS pepName, p.nationality AS nationality,
               collect({id: e.id, name: e.name, jur: e.jurisdiction, score: e.kycRiskScore}) AS entities,
               count(e) AS entityCount
        ORDER BY entityCount DESC
    """)
    if not rows:
        return "No PEP connections found."
    out = [f"Found {len(rows)} PEPs controlling entities:\n"]
    for r in rows:
        out.append(f"  🟣 {r['pepName']} ({r['nationality']}) — controls {r['entityCount']} entities:")
        for e in r["entities"][:5]:
            out.append(f"    • {e['id']} {e['name']} ({e['jur']}, score {e['score']})")
    return "\n".join(out)


@tool
def find_sanctioned_persons() -> str:
    """List all sanctioned persons and entities they control/are linked to.
    Critical for compliance — these connections must be reported."""
    rows = _neo.query("""
        MATCH (p:NaturalPerson {isSanctioned: true})
        OPTIONAL MATCH (p)<-[:CONTROLLED_BY]-(e:LegalEntity)
        RETURN p.id AS id, p.name AS name, p.nationality AS nationality,
               collect({id: e.id, name: e.name, jur: e.jurisdiction, score: e.kycRiskScore}) AS entities
        ORDER BY p.name
    """)
    if not rows:
        return "No sanctioned persons in the graph."
    out = [f"Found {len(rows)} sanctioned persons:\n"]
    for r in rows:
        ents = r["entities"]
        ent_str = ", ".join(f"{e['id']} ({e['jur']})" for e in ents if e.get("id")) if ents else "None"
        out.append(f"  🔴 {r['name']} ({r['nationality']}) — controls: {ent_str}")
    return "\n".join(out)


@tool
def analyze_suspicious_transactions(entity_id: str | None = None) -> str:
    """Find suspicious transactions, optionally filtered to a specific entity.
    Shows transaction details including amount, counterparty, and date.

    Args:
        entity_id: Optional entity ID to filter to. If None, shows all suspicious transactions.
    """
    if entity_id:
        rows = _neo.query("""
            MATCH (a:LegalEntity {id: $id})-[t:TRANSACTION {isSuspicious: true}]-(b:LegalEntity)
            RETURN a.id AS from_id, a.name AS from_name,
                   b.id AS to_id, b.name AS to_name,
                   t.amount AS amount, t.currency AS currency, t.date AS date
            ORDER BY t.amount DESC LIMIT 20
        """, {"id": entity_id})
    else:
        rows = _neo.query("""
            MATCH (a:LegalEntity)-[t:TRANSACTION {isSuspicious: true}]->(b:LegalEntity)
            RETURN a.id AS from_id, a.name AS from_name,
                   b.id AS to_id, b.name AS to_name,
                   t.amount AS amount, t.currency AS currency, t.date AS date
            ORDER BY t.amount DESC LIMIT 20
        """)
    if not rows:
        return f"No suspicious transactions found{' for ' + entity_id if entity_id else ''}."
    out = [f"Suspicious transactions{' for ' + entity_id if entity_id else ''} ({len(rows)} found):\n"]
    for r in rows:
        out.append(f"  {r['from_id']} ({r['from_name']}) → {r['to_id']} ({r['to_name']})")
        out.append(f"    Amount: {r['currency']} {r['amount']:,.2f} | Date: {r['date']}")
    return "\n".join(out)


@tool
def find_path_between_entities(entity_id_1: str, entity_id_2: str) -> str:
    """Find the shortest ownership path between two entities. Useful for
    discovering hidden connections between apparently unrelated companies.

    Args:
        entity_id_1: First entity ID (e.g. 'ENTITY_0001').
        entity_id_2: Second entity ID (e.g. 'ENTITY_0050').
    """
    rows = _neo.query("""
        MATCH (a:LegalEntity {id: $id1}), (b:LegalEntity {id: $id2}),
              path = shortestPath((a)-[*..10]-(b))
        RETURN [n IN nodes(path) | coalesce(n.name, n.id)] AS chain,
               [r IN relationships(path) | type(r)] AS relTypes,
               length(path) AS hops
    """, {"id1": entity_id_1, "id2": entity_id_2})
    if not rows:
        return f"No path found between {entity_id_1} and {entity_id_2} within 10 hops."
    r = rows[0]
    chain_parts = []
    for i, name in enumerate(r["chain"]):
        chain_parts.append(str(name))
        if i < len(r["relTypes"]):
            chain_parts.append(f" —[{r['relTypes'][i]}]→ ")
    return f"Path between {entity_id_1} and {entity_id_2} ({r['hops']} hops):\n  {''.join(chain_parts)}"


@tool
def get_entity_detail(entity_id: str) -> str:
    """Get complete details for an entity including all properties, ownership
    structure, controllers, and transaction summary.

    Args:
        entity_id: The entity ID.
    """
    rows = _neo.query("""
        MATCH (e:LegalEntity {id: $id})
        OPTIONAL MATCH (e)-[r:DIRECTLY_OWNED_BY]->(parent:LegalEntity)
        OPTIONAL MATCH (child:LegalEntity)-[r2:DIRECTLY_OWNED_BY]->(e)
        OPTIONAL MATCH (e)-[:CONTROLLED_BY]->(ctrl:NaturalPerson)
        OPTIONAL MATCH (e)-[t:TRANSACTION]-(other:LegalEntity)
        WITH e,
             collect(DISTINCT {name: parent.name, id: parent.id, pct: r.percentage}) AS parents,
             collect(DISTINCT {name: child.name, id: child.id, pct: r2.percentage}) AS children,
             collect(DISTINCT {name: ctrl.name, id: ctrl.id, pep: ctrl.isPEP, sanctioned: ctrl.isSanctioned}) AS controllers,
             count(DISTINCT t) AS txnCount,
             sum(CASE WHEN t.isSuspicious THEN 1 ELSE 0 END) AS suspTxns
        RETURN e {.*} AS entity, parents, children, controllers, txnCount, suspTxns
    """, {"id": entity_id})
    if not rows:
        return f"Entity {entity_id} not found."
    r = rows[0]
    e = r["entity"]
    parents = [p for p in r["parents"] if p.get("id")]
    children = [c for c in r["children"] if c.get("id")]
    ctrls = [c for c in r["controllers"] if c.get("id")]

    parent_str = ", ".join(f"{p['name']} ({p.get('pct','')}%)" for p in parents) or "None"
    child_str = ", ".join(f"{c['name']} ({c.get('pct','')}%)" for c in children) or "None"
    ctrl_str = ", ".join(
        f"{c['name']}" + (" 🔴" if c.get("sanctioned") else "") + (" 🟣" if c.get("pep") else "")
        for c in ctrls
    ) or "None"

    out = [
        f"Entity: {e.get('name')} ({entity_id})",
        f"  LEI: {e.get('lei', 'N/A')}",
        f"  Jurisdiction: {e.get('jurisdiction')} ({e.get('jurisdictionName', '')})",
        f"  Category: {e.get('category')} | Active: {e.get('isActive')}",
        f"  Operational Address: {e.get('hasOperationalAddress')}",
        f"  Incorporated: {e.get('incorporatedDate', 'N/A')}",
        f"  Risk: {e.get('riskTier')} tier, score {e.get('kycRiskScore')}/100",
        f"  PageRank: {e.get('pageRankScore', 0):.4f} | Betweenness: {e.get('betweennessScore', 0):.2f}",
        f"  Community: #{e.get('louvainCommunityId')} | SCC: {e.get('sccComponentId', 'None')}",
        f"  Parents (owned by): {parent_str}",
        f"  Children (subsidiaries): {child_str}",
        f"  Controllers: {ctrl_str}",
        f"  Transactions: {r['txnCount']} total, {r['suspTxns']} suspicious",
    ]
    return "\n".join(out)


@tool
def compare_entities(entity_id_1: str, entity_id_2: str) -> str:
    """Compare two entities side by side — risk scores, jurisdictions,
    categories, and key metrics.

    Args:
        entity_id_1: First entity ID.
        entity_id_2: Second entity ID.
    """
    rows = _neo.query("""
        MATCH (e:LegalEntity) WHERE e.id IN [$id1, $id2]
        RETURN e.id AS id, e.name AS name, e.jurisdiction AS jur,
               e.category AS category, e.kycRiskScore AS score,
               e.riskTier AS tier, e.pageRankScore AS pageRank,
               e.betweennessScore AS betweenness,
               e.louvainCommunityId AS community,
               e.sccComponentId AS scc,
               e.hasOperationalAddress AS hasAddr
        ORDER BY e.id
    """, {"id1": entity_id_1, "id2": entity_id_2})
    if len(rows) < 2:
        return f"Could not find both entities. Found: {[r['id'] for r in rows]}"
    a, b = rows[0], rows[1]
    return (
        f"Comparison:\n"
        f"{'Metric':<20} {a['id']:<25} {b['id']:<25}\n"
        f"{'─'*70}\n"
        f"{'Name':<20} {a['name']:<25} {b['name']:<25}\n"
        f"{'Jurisdiction':<20} {a['jur']:<25} {b['jur']:<25}\n"
        f"{'Category':<20} {a['category']:<25} {b['category']:<25}\n"
        f"{'Risk Score':<20} {str(a['score']):<25} {str(b['score']):<25}\n"
        f"{'Risk Tier':<20} {a['tier']:<25} {b['tier']:<25}\n"
        f"{'PageRank':<20} {a['pageRank']:.4f}{'':<20} {b['pageRank']:.4f}\n"
        f"{'Community':<20} {str(a['community']):<25} {str(b['community']):<25}\n"
        f"{'In Ring':<20} {str(a['scc'] is not None):<25} {str(b['scc'] is not None):<25}\n"
        f"{'Has Address':<20} {str(a['hasAddr']):<25} {str(b['hasAddr']):<25}"
    )


@tool
def get_jurisdiction_risk_analysis(jurisdiction: str | None = None) -> str:
    """Analyze risk by jurisdiction. If a specific jurisdiction is given,
    show details for it. Otherwise show a summary across all jurisdictions.

    Args:
        jurisdiction: Optional 2-letter country code (e.g. 'KY', 'US', 'DE'). If None, shows all.
    """
    if jurisdiction:
        rows = _neo.query("""
            MATCH (e:LegalEntity {jurisdiction: $jur})
            RETURN e.jurisdiction AS jur, count(e) AS cnt,
                   toInteger(avg(e.kycRiskScore)) AS avgRisk,
                   max(e.kycRiskScore) AS maxRisk,
                   collect(CASE WHEN e.kycRiskScore >= 70 THEN e.id END) AS criticalEntities
        """, {"jur": jurisdiction.upper()})
    else:
        rows = _neo.query("""
            MATCH (e:LegalEntity)
            WITH e.jurisdiction AS jur, collect(e) AS entities
            RETURN jur, size(entities) AS cnt,
                   toInteger(reduce(s=0.0, e IN entities | s + e.kycRiskScore) / size(entities)) AS avgRisk,
                   max([e IN entities | e.kycRiskScore]) AS maxRisk
            ORDER BY avgRisk DESC
        """)
    if not rows:
        return f"No data for jurisdiction {jurisdiction}." if jurisdiction else "No jurisdictions found."
    out = ["Jurisdiction Risk Analysis:\n"]
    for r in rows:
        critical = f" | Critical: {', '.join(str(e) for e in r.get('criticalEntities', []) if e)}" if r.get("criticalEntities") else ""
        out.append(f"  {r['jur']}: {r['cnt']} entities, avg risk {r['avgRisk']}, max {r['maxRisk']}{critical}")
    return "\n".join(out)


@tool
def get_community_analysis(community_id: int | None = None) -> str:
    """Analyze Louvain community clusters. If a community ID is given, show
    its members. Otherwise show summary of all communities.

    Args:
        community_id: Optional community number. If None, shows summary of all communities.
    """
    if community_id is not None:
        rows = _neo.query("""
            MATCH (e:LegalEntity {louvainCommunityId: $cid})
            RETURN e.id AS id, e.name AS name, e.jurisdiction AS jur,
                   e.kycRiskScore AS score, e.category AS category
            ORDER BY e.kycRiskScore DESC
        """, {"cid": community_id})
        if not rows:
            return f"Community #{community_id} not found."
        out = [f"Community #{community_id} — {len(rows)} members:\n"]
        for r in rows:
            out.append(f"  {r['id']} {r['name']} ({r['jur']}, {r['category']}) — score {r['score']}")
        return "\n".join(out)
    else:
        rows = _neo.query("""
            MATCH (e:LegalEntity) WHERE e.louvainCommunityId IS NOT NULL
            WITH e.louvainCommunityId AS cid, collect(e) AS members
            RETURN cid,
                   size(members) AS cnt,
                   toInteger(reduce(s=0.0, m IN members | s + m.kycRiskScore) / size(members)) AS avgRisk,
                   collect(DISTINCT [m IN members | m.jurisdiction][0]) AS jurisdictions
            ORDER BY avgRisk DESC
        """)
        out = [f"Found {len(rows)} Louvain communities:\n"]
        for r in rows:
            out.append(f"  Community #{r['cid']}: {r['cnt']} members, avg risk {r['avgRisk']}")
        return "\n".join(out)


@tool
def get_graph_statistics() -> str:
    """Get overall statistics of the knowledge graph — entity counts, relationship
    counts, risk distribution, etc."""
    stats = {}
    stats["entities"] = _neo.query("MATCH (e:LegalEntity) RETURN count(e) AS c")[0]["c"]
    stats["persons"] = _neo.query("MATCH (p:NaturalPerson) RETURN count(p) AS c")[0]["c"]
    stats["peps"] = _neo.query("MATCH (p:NaturalPerson {isPEP: true}) RETURN count(p) AS c")[0]["c"]
    stats["sanctioned"] = _neo.query("MATCH (p:NaturalPerson {isSanctioned: true}) RETURN count(p) AS c")[0]["c"]
    stats["ownership_rels"] = _neo.query("MATCH ()-[r:DIRECTLY_OWNED_BY]->() RETURN count(r) AS c")[0]["c"]
    stats["control_rels"] = _neo.query("MATCH ()-[r:CONTROLLED_BY]->() RETURN count(r) AS c")[0]["c"]
    stats["transactions"] = _neo.query("MATCH ()-[t:TRANSACTION]->() RETURN count(t) AS c")[0]["c"]
    stats["suspicious_txns"] = _neo.query("MATCH ()-[t:TRANSACTION {isSuspicious: true}]->() RETURN count(t) AS c")[0]["c"]
    risk_dist = _neo.query("""
        MATCH (e:LegalEntity)
        RETURN e.riskTier AS tier, count(e) AS cnt
        ORDER BY cnt DESC
    """)
    risk_str = ", ".join(f"{r['tier']}: {r['cnt']}" for r in risk_dist)
    return (
        f"Knowledge Graph Statistics:\n"
        f"  Legal Entities: {stats['entities']}\n"
        f"  Natural Persons: {stats['persons']} (PEPs: {stats['peps']}, Sanctioned: {stats['sanctioned']})\n"
        f"  Ownership relationships: {stats['ownership_rels']}\n"
        f"  Control relationships: {stats['control_rels']}\n"
        f"  Transactions: {stats['transactions']} ({stats['suspicious_txns']} suspicious)\n"
        f"  Risk Distribution: {risk_str}"
    )


@tool
def find_money_laundering_indicators(entity_id: str | None = None) -> str:
    """Detect potential money laundering indicators: entities combining multiple
    red flags (high risk + suspicious txns + shell + offshore + sanctions links).

    Args:
        entity_id: Optional entity to check. If None, scans all entities for multi-flag patterns.
    """
    if entity_id:
        rows = _neo.query("""
            MATCH (e:LegalEntity {id: $id})
            OPTIONAL MATCH (e)-[t:TRANSACTION {isSuspicious: true}]-(other)
            WITH e, count(t) AS suspTxns
            OPTIONAL MATCH (e)-[:DIRECTLY_OWNED_BY*0..4]->()-[:CONTROLLED_BY]->(p:NaturalPerson)
            WHERE p.isSanctioned = true OR p.isPEP = true
            WITH e, suspTxns,
                 collect(DISTINCT CASE WHEN p.isSanctioned THEN p.name END) AS sanctioned,
                 collect(DISTINCT CASE WHEN p.isPEP THEN p.name END) AS peps
            RETURN e.id AS id, e.name AS name, e.jurisdiction AS jur,
                   e.kycRiskScore AS score,
                   e.hasOperationalAddress AS hasAddr,
                   e.sccComponentId AS ring,
                   suspTxns,
                   [s IN sanctioned WHERE s IS NOT NULL] AS sanctionedLinks,
                   [p IN peps WHERE p IS NOT NULL] AS pepLinks
        """, {"id": entity_id})
    else:
        rows = _neo.query("""
            MATCH (e:LegalEntity) WHERE e.kycRiskScore >= 35
            OPTIONAL MATCH (e)-[t:TRANSACTION {isSuspicious: true}]-(other)
            WITH e, count(t) AS suspTxns
            WHERE suspTxns > 0 OR e.hasOperationalAddress = false OR e.sccComponentId IS NOT NULL
            RETURN e.id AS id, e.name AS name, e.jurisdiction AS jur,
                   e.kycRiskScore AS score,
                   e.hasOperationalAddress AS hasAddr,
                   e.sccComponentId AS ring,
                   suspTxns
            ORDER BY e.kycRiskScore DESC LIMIT 15
        """)
    if not rows:
        return f"No money laundering indicators found{' for ' + entity_id if entity_id else ''}."
    out = ["Money Laundering Risk Indicators:\n"]
    for r in rows:
        flags = []
        if r["score"] >= 50: flags.append("HIGH RISK SCORE")
        if not r.get("hasAddr", True): flags.append("SHELL (no address)")
        if r.get("ring") is not None: flags.append("CIRCULAR OWNERSHIP")
        if r.get("jur") in ["KY", "VG", "PA", "SC"]: flags.append("OFFSHORE")
        if r.get("suspTxns", 0) > 0: flags.append(f"{r['suspTxns']} SUSP. TXNS")
        if r.get("sanctionedLinks"): flags.append(f"SANCTIONED: {', '.join(r['sanctionedLinks'])}")
        if r.get("pepLinks"): flags.append(f"PEP: {', '.join(r['pepLinks'])}")
        out.append(f"  {r['id']} {r['name']} ({r['jur']}, score {r['score']})")
        out.append(f"    Flags: {' | '.join(flags)}")
    return "\n".join(out)


@tool
def query_ontology(question: str) -> str:
    """Query the FIBO/GLEIF/KYC ontology in GraphDB using SPARQL. Use this for
    questions about how the ontology models ownership, what classes exist,
    how ontologies relate, entity types, etc.

    Args:
        question: Natural language question about the ontology.
    """
    # Map common ontology questions to SPARQL queries
    q_lower = question.lower()

    if "class" in q_lower and ("fibo" in q_lower or "ontology" in q_lower or "what" in q_lower):
        sparql = """PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?class ?label WHERE {
  ?class a owl:Class . ?class rdfs:label ?label .
} ORDER BY ?label LIMIT 30"""
    elif "ownership" in q_lower or "control" in q_lower:
        sparql = """PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?class ?label ?parent ?parentLabel WHERE {
  ?class a owl:Class . ?class rdfs:label ?label .
  OPTIONAL { ?class rdfs:subClassOf ?parent . ?parent rdfs:label ?parentLabel . FILTER(isIRI(?parent)) }
  FILTER(CONTAINS(STR(?class), "Ownership") || CONTAINS(STR(?class), "Control") ||
         CONTAINS(STR(?class), "Owner") || CONTAINS(STR(?class), "Shareholder"))
} ORDER BY ?label"""
    elif "named graph" in q_lower or "what graph" in q_lower:
        sparql = """SELECT ?graph (COUNT(*) AS ?triples) WHERE {
  GRAPH ?graph { ?s ?p ?o }
} GROUP BY ?graph ORDER BY DESC(?triples)"""
    elif "gleif" in q_lower and ("type" in q_lower or "classif" in q_lower):
        sparql = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?type (COUNT(?entity) AS ?count) ?typeLabel WHERE {
  GRAPH <http://kg/glei/instances> { ?entity rdf:type ?type . }
  OPTIONAL { ?type rdfs:label ?typeLabel }
  FILTER(?type != <http://www.w3.org/2002/07/owl#NamedIndividual>)
} GROUP BY ?type ?typeLabel ORDER BY DESC(?count)"""
    elif "cross" in q_lower and "ontology" in q_lower:
        sparql = """PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?subject ?predicate ?object WHERE {
  { ?subject owl:equivalentClass ?object . BIND(owl:equivalentClass AS ?predicate) }
  UNION {
    ?subject rdfs:subClassOf ?object . FILTER(isIRI(?object))
    FILTER((CONTAINS(STR(?subject), "kyc-kg") && CONTAINS(STR(?object), "edmcouncil")) ||
           (CONTAINS(STR(?subject), "edmcouncil") && CONTAINS(STR(?object), "kyc-kg")))
    BIND(rdfs:subClassOf AS ?predicate)
  }
} LIMIT 30"""
    elif "country" in q_lower or "iso" in q_lower:
        sparql = """PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?country ?label WHERE {
  GRAPH <http://kg/lcc/iso3166> {
    ?country a ?type . ?country rdfs:label ?label .
    FILTER(LANG(?label) = "en" || LANG(?label) = "")
  }
} ORDER BY ?label LIMIT 25"""
    else:
        # Generic ontology exploration
        sparql = """PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?class ?label ?comment WHERE {
  ?class a owl:Class .
  OPTIONAL { ?class rdfs:label ?label }
  OPTIONAL { ?class rdfs:comment ?comment }
  FILTER(CONTAINS(STR(?class), "edmcouncil") || CONTAINS(STR(?class), "kyc-kg"))
} ORDER BY ?label LIMIT 20"""

    try:
        rows = _gdb.query(sparql)
        if not rows:
            return "No results from ontology query."
        # Format results as text
        out = [f"SPARQL results ({len(rows)} rows):\n"]
        for r in rows[:20]:
            parts = []
            for k, v in r.items():
                # Shorten URIs for readability
                val = str(v)
                if val.startswith("http"):
                    val = val.split("/")[-1].split("#")[-1]
                parts.append(f"{k}: {val}")
            out.append(f"  {' | '.join(parts)}")
        if len(rows) > 20:
            out.append(f"  ... and {len(rows) - 20} more rows")
        return "\n".join(out)
    except Exception as e:
        return f"SPARQL query failed: {e}"


@tool
def run_custom_cypher(cypher_query: str) -> str:
    """Execute a custom read-only Cypher query against Neo4j. Use this ONLY when
    no other tool can answer the question. The query MUST be read-only (no CREATE,
    DELETE, SET, MERGE, REMOVE operations).

    IMPORTANT: Only generate Cypher that uses the known schema:
    - Node labels: LegalEntity, NaturalPerson
    - Relationships: DIRECTLY_OWNED_BY, CONTROLLED_BY, TRANSACTION
    - Use properties exactly as documented in the schema.

    Args:
        cypher_query: A valid read-only Cypher query.
    """
    # Security: block write operations
    upper = cypher_query.upper()
    write_ops = ["CREATE", "DELETE", "SET ", "MERGE", "REMOVE", "DROP", "DETACH"]
    for op in write_ops:
        if op in upper:
            return f"ERROR: Write operation '{op.strip()}' not allowed. Only read queries permitted."

    try:
        rows = _neo.query(cypher_query)
        if not rows:
            return "Query returned no results."
        # Format up to 20 rows
        out = [f"Query returned {len(rows)} row(s):\n"]
        for r in rows[:20]:
            parts = []
            for k, v in r.items():
                if isinstance(v, float):
                    parts.append(f"{k}: {v:.4f}")
                elif isinstance(v, list):
                    parts.append(f"{k}: [{', '.join(str(x) for x in v[:5])}{'...' if len(v) > 5 else ''}]")
                else:
                    parts.append(f"{k}: {v}")
            out.append(f"  {' | '.join(parts)}")
        if len(rows) > 20:
            out.append(f"  ... and {len(rows) - 20} more rows")
        return "\n".join(out)
    except Exception as e:
        return f"Cypher execution error: {e}"


@tool
def find_hidden_controllers() -> str:
    """Find persons who exert control over many entities through indirect
    ownership chains (multiple hops). These 'shadow controllers' may be
    using corporate layers to obscure their influence."""
    rows = _neo.query("""
        MATCH path = (e:LegalEntity)-[:DIRECTLY_OWNED_BY*1..6]->()
              -[:CONTROLLED_BY]->(p:NaturalPerson)
        WHERE length(path) >= 3
        WITH p, collect(DISTINCT e) AS indirectEntities,
             collect(DISTINCT e.jurisdiction) AS jurisdictions
        WHERE size(indirectEntities) >= 2
        OPTIONAL MATCH (direct:LegalEntity)-[:CONTROLLED_BY]->(p)
        WITH p, indirectEntities, jurisdictions,
             collect(DISTINCT direct) AS directEntities
        RETURN p.name AS name, p.nationality AS nationality,
               p.isPEP AS isPEP, p.isSanctioned AS isSanctioned,
               size(directEntities) AS directControl,
               size(indirectEntities) AS indirectControl,
               jurisdictions
        ORDER BY size(indirectEntities) + size(directEntities) DESC LIMIT 10
    """)
    if not rows:
        return "No hidden controllers found with significant indirect influence."
    out = ["Hidden Controllers (shadow ownership via multi-layer chains):\n"]
    for r in rows:
        flags = []
        if r["isSanctioned"]: flags.append("🔴 SANCTIONED")
        if r["isPEP"]: flags.append("🟣 PEP")
        flag_str = f" {' '.join(flags)}" if flags else ""
        juris = ", ".join(str(j) for j in r["jurisdictions"][:5])
        out.append(f"  {r['name']} ({r['nationality']}){flag_str}")
        out.append(f"    Direct: {r['directControl']} | Indirect: {r['indirectControl']} | Jurisdictions: {juris}")
    return "\n".join(out)


@tool
def search_entity_by_name(name: str) -> str:
    """Search for entities or persons by name (case-insensitive partial match).
    Use this when the user mentions an entity by name instead of ID.

    Args:
        name: Full or partial name to search for.
    """
    rows = _neo.query("""
        MATCH (n)
        WHERE (n:LegalEntity OR n:NaturalPerson)
          AND toLower(n.name) CONTAINS toLower($name)
        RETURN labels(n) AS labels, n.id AS id, n.name AS name,
               n.jurisdiction AS jurisdiction, n.nationality AS nationality,
               n.kycRiskScore AS score
        ORDER BY n.name LIMIT 10
    """, {"name": name})
    if not rows:
        return f"No entities or persons found matching '{name}'."
    out = [f"Search results for '{name}' ({len(rows)} found):\n"]
    for r in rows:
        label = "Entity" if "LegalEntity" in r["labels"] else "Person"
        loc = r.get("jurisdiction") or r.get("nationality") or ""
        score_str = f", score {r['score']}" if r.get("score") else ""
        out.append(f"  [{label}] {r['id']} — {r['name']} ({loc}{score_str})")
    return "\n".join(out)


# ─── System Prompt ────────────────────────────────────────────────────────────
SYSTEM_PROMPT = f"""You are a KYC/AML Investigation Intelligence Assistant with access to a financial
knowledge graph containing legal entities, natural persons, ownership chains, control
relationships, and transaction data.

YOUR ROLE:
- Answer questions about beneficial ownership, sanctions exposure, risk assessment,
  money laundering indicators, corporate structures, and regulatory compliance.
- Use the available tools to query the knowledge graph for EVERY factual question.
- NEVER fabricate or hallucinate data. If a tool returns no results, say so clearly.
- Support follow-up questions — remember the conversation context.

GROUNDING RULES (CRITICAL):
1. ALL factual claims MUST come from tool results. Never invent entity IDs, names, scores, or relationships.
2. If you don't know an entity ID, use search_entity_by_name first.
3. If no tool fits perfectly, use run_custom_cypher with a valid read-only Cypher query.
4. For ontology/FIBO/GLEIF questions, use query_ontology.
5. Always cite the entity IDs and data source in your answer.
6. If the graph doesn't contain the answer, say "The knowledge graph does not contain this information."
7. Use AT MOST 3 tool calls per question. After gathering data, synthesize your answer immediately.
8. For follow-up questions, use conversation context and only call tools for NEW information needed.

SCHEMA CONTEXT:
{NEO4J_SCHEMA}

{GRAPHDB_SCHEMA}

When the user asks about:
- "who owns X" or "UBO" → use find_ultimate_beneficial_owners
- "sanctions" or "compliance" → use check_sanctions_exposure
- "risk" or "risk score" → use get_entity_risk_profile
- "shell companies" → use find_shell_companies
- "circular" or "rings" → use find_circular_ownership_rings
- "PEP" or "politically exposed" → use find_pep_connections
- "transactions" or "suspicious" → use analyze_suspicious_transactions
- "path between" or "connection" → use find_path_between_entities
- "compare" → use compare_entities
- "jurisdiction" → use get_jurisdiction_risk_analysis
- "community" or "cluster" → use get_community_analysis
- "statistics" or "overview" → use get_graph_statistics
- "money laundering" or "red flags" → use find_money_laundering_indicators
- "hidden controller" or "shadow" → use find_hidden_controllers
- "ontology" or "FIBO" or "SPARQL" → use query_ontology
- Any other graph question → use run_custom_cypher

Be concise but thorough. Format output with clear structure. Flag any sanctions
or PEP exposure prominently with ⚠️ warnings.
"""


# ─── Agent Factory ────────────────────────────────────────────────────────────
ALL_TOOLS = [
    find_ultimate_beneficial_owners,
    check_sanctions_exposure,
    get_entity_risk_profile,
    find_circular_ownership_rings,
    find_shell_companies,
    get_top_risk_entities,
    find_pep_connections,
    find_sanctioned_persons,
    analyze_suspicious_transactions,
    find_path_between_entities,
    get_entity_detail,
    compare_entities,
    get_jurisdiction_risk_analysis,
    get_community_analysis,
    get_graph_statistics,
    find_money_laundering_indicators,
    find_hidden_controllers,
    search_entity_by_name,
    query_ontology,
    run_custom_cypher,
]

# Module-level agent singleton (lazy init)
_agent = None
_memory = None


def _get_agent():
    """Lazy-initialize the LangGraph agent with memory."""
    global _agent, _memory
    if _agent is None:
        llm = _get_llm()
        _memory = MemorySaver()
        _agent = create_react_agent(
            llm,
            ALL_TOOLS,
            checkpointer=_memory,
            prompt=SYSTEM_PROMPT,
        )
    return _agent


def process_agent_chat(message: str, session_id: str = "default") -> dict:
    """Process a chat message through the agentic system.

    Args:
        message: User's natural language question.
        session_id: Session ID for conversation memory (supports follow-ups).

    Returns:
        dict with keys: reply, tools_used, session_id
    """
    agent = _get_agent()
    config = {"configurable": {"thread_id": session_id}, "recursion_limit": 25}

    try:
        result = agent.invoke(
            {"messages": [("user", message)]},
            config=config,
        )

        # Extract the final AI response
        ai_message = result["messages"][-1]
        reply = ai_message.content if hasattr(ai_message, "content") else str(ai_message)

        # Track which tools were called
        tools_used = []
        for msg in result["messages"]:
            if hasattr(msg, "tool_calls") and msg.tool_calls:
                for tc in msg.tool_calls:
                    tools_used.append(tc.get("name", "unknown"))

        return {
            "reply": reply,
            "tools_used": tools_used,
            "session_id": session_id,
        }
    except Exception as e:
        return {
            "reply": f"Error processing your question: {str(e)}. Please try rephrasing.",
            "tools_used": [],
            "session_id": session_id,
        }


def reset_session(session_id: str = "default") -> None:
    """Clear conversation memory for a session."""
    global _agent, _memory
    # Reset by recreating the agent (MemorySaver is in-memory)
    _agent = None
    _memory = None
