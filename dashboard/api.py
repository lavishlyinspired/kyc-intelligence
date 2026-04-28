"""
KYC Intelligence — FastAPI backend serving all dashboard data.

Run with:
    uvicorn dashboard.api:app --reload --port 8000
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.kg_client import Neo4jClient, GraphDBClient

app = FastAPI(title="KYC Intelligence API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Shared clients ──────────────────────────────────────────────────────────
neo = Neo4jClient()
gdb = GraphDBClient()


# ─── Pydantic models ─────────────────────────────────────────────────────────
class CypherRequest(BaseModel):
    query: str


class SparqlRequest(BaseModel):
    query: str


class ChatRequest(BaseModel):
    message: str


# ─── Serve static frontend ───────────────────────────────────────────────────
STATIC_DIR = Path(__file__).parent / "static"


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


# ─── KPIs ─────────────────────────────────────────────────────────────────────
@app.get("/api/kpis")
async def kpis():
    entities = neo.query("""
        MATCH (e:LegalEntity)
        RETURN count(e) AS entities,
               sum(CASE WHEN e.kycRiskScore >= 50 THEN 1 ELSE 0 END) AS highRisk,
               toInteger(coalesce(avg(e.kycRiskScore), 0)) AS avgScore
    """)[0]
    persons = neo.query("""
        MATCH (p:NaturalPerson)
        RETURN count(p) AS persons,
               sum(CASE WHEN p.isSanctioned THEN 1 ELSE 0 END) AS sanctioned,
               sum(CASE WHEN p.isPEP THEN 1 ELSE 0 END) AS peps
    """)[0]
    rings = neo.query("""
        MATCH (e:LegalEntity) WHERE e.sccComponentId IS NOT NULL
        WITH e.sccComponentId AS scc, count(e) AS sz WHERE sz > 1
        RETURN count(scc) AS rings
    """)[0]
    # Transaction data not currently ingested (no synthetic data per project policy).
    # Endpoint stays defensive so KPIs render cleanly until real wire/payment data lands.
    has_txn = neo.query("CALL db.relationshipTypes() YIELD relationshipType WHERE relationshipType = 'TRANSACTION' RETURN count(*) AS c")[0]["c"] > 0
    if has_txn:
        txns = neo.query("""
            MATCH ()-[t:TRANSACTION]->()
            RETURN count(t) AS totalTxns,
                   sum(CASE WHEN t.isSuspicious THEN 1 ELSE 0 END) AS suspiciousTxns
        """)[0]
    else:
        txns = {"totalTxns": 0, "suspiciousTxns": 0}
    rels = neo.query("""
        MATCH ()-[r:DIRECTLY_OWNED_BY]->()
        RETURN count(r) AS ownershipRels
    """)[0]
    ctrl = neo.query("""
        MATCH ()-[r:CONTROLLED_BY]->()
        RETURN count(r) AS controlRels
    """)[0]
    return {**entities, **persons, **rings, **txns, **rels, **ctrl}


# ─── Statistics ───────────────────────────────────────────────────────────────
@app.get("/api/stats")
async def stats():
    labels = neo.query("CALL db.labels() YIELD label RETURN label ORDER BY label")
    label_counts = []
    for l in labels:
        lbl = l["label"]
        if lbl.startswith("_"):
            continue
        cnt = neo.query(f"MATCH (n:`{lbl}`) RETURN count(n) AS c")[0]["c"]
        label_counts.append({"label": lbl, "count": cnt})
    rel_types = neo.query("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType ORDER BY relationshipType")
    rel_counts = []
    for r in rel_types:
        rt = r["relationshipType"]
        cnt = neo.query(f"MATCH ()-[r:`{rt}`]->() RETURN count(r) AS c")[0]["c"]
        rel_counts.append({"type": rt, "count": cnt})
    return {"labels": label_counts, "relationships": rel_counts}


# ─── Entity list ──────────────────────────────────────────────────────────────
@app.get("/api/entities")
async def entities(
    limit: int = Query(50, le=500),
    offset: int = 0,
    sort: str = "score",
    jurisdiction: str | None = None,
    risk_tier: str | None = None,
    category: str | None = None,
    search: str | None = None,
):
    where = []
    params: dict[str, Any] = {"limit": limit, "offset": offset}
    if jurisdiction:
        where.append("e.jurisdiction = $jur")
        params["jur"] = jurisdiction
    if risk_tier:
        where.append("e.riskTier = $tier")
        params["tier"] = risk_tier
    if category:
        where.append("e.category = $cat")
        params["cat"] = category
    if search:
        where.append("toLower(e.name) CONTAINS toLower($search)")
        params["search"] = search

    where_clause = "WHERE " + " AND ".join(where) if where else ""
    sort_field = {
        "score": "e.kycRiskScore DESC",
        "name": "e.name ASC",
        "jurisdiction": "e.jurisdiction ASC",
        "category": "e.category ASC",
    }.get(sort, "e.kycRiskScore DESC")

    rows = neo.query(f"""
        MATCH (e:LegalEntity)
        {where_clause}
        RETURN e.id AS id, e.name AS name, e.lei AS lei,
               e.jurisdiction AS jurisdiction, e.jurisdictionName AS jurisdictionName,
               e.riskTier AS riskTier,
               e.kycRiskScore AS score, e.isActive AS isActive,
               e.category AS category,
               e.pageRankScore AS pageRank,
               e.betweennessScore AS betweenness,
               e.louvainCommunityId AS community,
               e.sccComponentId AS sccId,
               e.hasOperationalAddress AS hasAddress
        ORDER BY {sort_field}
        SKIP $offset LIMIT $limit
    """, params)
    total = neo.query(f"""
        MATCH (e:LegalEntity) {where_clause} RETURN count(e) AS c
    """, params)[0]["c"]
    return {"items": rows, "total": total}


# ─── Entity detail ────────────────────────────────────────────────────────────
@app.get("/api/entities/{entity_id}")
async def entity_detail(entity_id: str):
    rows = neo.query("""
        MATCH (e:LegalEntity {id: $id})
        OPTIONAL MATCH (e)-[r:DIRECTLY_OWNED_BY]->(parent:LegalEntity)
        OPTIONAL MATCH (child:LegalEntity)-[r2:DIRECTLY_OWNED_BY]->(e)
        OPTIONAL MATCH (e)-[r3:CONTROLLED_BY]->(ctrl:NaturalPerson)
        RETURN e {.*} AS entity,
               collect(DISTINCT {id: parent.id, name: parent.name,
                                 pct: r.percentage, since: r.since}) AS parents,
               collect(DISTINCT {id: child.id, name: child.name,
                                 pct: r2.percentage, since: r2.since}) AS children,
               collect(DISTINCT {id: ctrl.id, name: ctrl.name, role: r3.role,
                                 isPEP: ctrl.isPEP,
                                 isSanctioned: ctrl.isSanctioned}) AS controllers
    """, {"id": entity_id})
    if not rows:
        raise HTTPException(404, f"Entity {entity_id} not found")
    row = rows[0]
    row["parents"] = [p for p in row["parents"] if p.get("id")]
    row["children"] = [c for c in row["children"] if c.get("id")]
    row["controllers"] = [c for c in row["controllers"] if c.get("id")]
    return row


# ─── UBO traversal ────────────────────────────────────────────────────────────
@app.get("/api/ubo/{entity_id}")
async def ubo_chain(entity_id: str, max_depth: int = Query(6, le=10)):
    depth = max(1, min(max_depth, 10))
    cypher = f"""
        MATCH path = (e:LegalEntity {{id: $id}})
              -[:DIRECTLY_OWNED_BY*0..{depth}]->()
              -[:CONTROLLED_BY]->(p:NaturalPerson)
        RETURN p.id AS personId, p.name AS name, p.nationality AS nationality,
               p.isPEP AS isPEP, p.isSanctioned AS isSanctioned,
               length(path) AS hops,
               [n IN nodes(path) | {{id: coalesce(n.id, ''), name: coalesce(n.name, ''),
                labels: labels(n)}}] AS chain
        ORDER BY hops LIMIT 20
    """
    rows = neo.query(cypher, {"id": entity_id})
    return {"entity_id": entity_id, "ubos": rows}


# ─── Ownership graph for visualization ────────────────────────────────────────
@app.get("/api/graph/{entity_id}")
async def ownership_graph(entity_id: str, depth: int = Query(2, le=6)):
    d = max(1, min(depth, 6))
    nodes_q = f"""
        MATCH path = (e:LegalEntity {{id: $id}})-[:DIRECTLY_OWNED_BY|CONTROLLED_BY*0..{d}]-(other)
        WITH nodes(path) AS ns
        UNWIND ns AS n
        WITH DISTINCT n
        RETURN collect({{
            id: n.id,
            name: coalesce(n.name, n.id),
            labels: labels(n),
            score: n.kycRiskScore,
            jurisdiction: n.jurisdiction,
            isPEP: n.isPEP,
            isSanctioned: n.isSanctioned,
            riskTier: n.riskTier
        }}) AS nodes
    """
    edges_q = f"""
        MATCH path = (e:LegalEntity {{id: $id}})-[:DIRECTLY_OWNED_BY|CONTROLLED_BY*0..{d}]-(other)
        UNWIND relationships(path) AS r
        WITH DISTINCT r
        RETURN startNode(r).id AS source, endNode(r).id AS target,
               type(r) AS type, r.percentage AS percentage, r.role AS role
    """
    rows = neo.query(nodes_q, {"id": entity_id})
    edges = neo.query(edges_q, {"id": entity_id})
    return {"nodes": rows[0]["nodes"] if rows else [], "edges": edges}


# ─── Risk distribution ────────────────────────────────────────────────────────
@app.get("/api/risk/distribution")
async def risk_distribution():
    return neo.query("""
        MATCH (e:LegalEntity)
        WITH CASE
            WHEN e.kycRiskScore >= 70 THEN 'Critical'
            WHEN e.kycRiskScore >= 50 THEN 'High'
            WHEN e.kycRiskScore >= 30 THEN 'Medium'
            WHEN e.kycRiskScore >= 10 THEN 'Low'
            ELSE 'Clean'
        END AS bucket, count(e) AS count
        RETURN bucket, count ORDER BY count DESC
    """)


@app.get("/api/risk/jurisdictions")
async def risk_by_jurisdiction():
    return neo.query("""
        MATCH (e:LegalEntity)
        WHERE e.jurisdiction IS NOT NULL AND e.kycRiskScore IS NOT NULL
        WITH e.jurisdiction AS jurisdiction, e.jurisdictionName AS name,
             collect(e.kycRiskScore) AS scores, count(e) AS count
        WHERE size(scores) > 0
        RETURN jurisdiction, name, count,
               toInteger(reduce(s = 0.0, x IN scores | s + x) / size(scores)) AS avgScore,
               reduce(mx = 0, x IN scores | CASE WHEN x > mx THEN x ELSE mx END) AS maxScore,
               size([s IN scores WHERE s >= 50]) AS highRiskCount
        ORDER BY avgScore DESC
        LIMIT 50
    """)


# ─── Circular ownership ──────────────────────────────────────────────────────
@app.get("/api/rings")
async def circular_ownership():
    return neo.query("""
        MATCH (e:LegalEntity) WHERE e.sccComponentId IS NOT NULL
        WITH e.sccComponentId AS scc, collect(e) AS members
        WHERE size(members) > 1
        RETURN scc,
               [m IN members | {id: m.id, name: m.name, jurisdiction: m.jurisdiction,
                                 score: m.kycRiskScore}] AS members,
               size(members) AS size
        ORDER BY size DESC LIMIT 30
    """)


# ─── Transactions ────────────────────────────────────────────────────────────────
# Transaction relationships are not currently in the graph (real wire/payment data
# pending; synthetic data prohibited by project policy). Endpoints return [] so
# the dashboard renders cleanly instead of erroring.
def _has_transaction_rel() -> bool:
    return neo.query(
        "CALL db.relationshipTypes() YIELD relationshipType "
        "WHERE relationshipType = 'TRANSACTION' RETURN count(*) AS c"
    )[0]["c"] > 0


@app.get("/api/transactions/suspicious")
async def suspicious_transactions():
    if not _has_transaction_rel():
        return []
    return neo.query("""
        MATCH (a:LegalEntity)-[t:TRANSACTION]->(b:LegalEntity)
        WHERE t.isSuspicious = true
        RETURN a.id AS fromId, a.name AS fromName,
               b.id AS toId, b.name AS toName,
               t.amount AS amount, t.currency AS currency,
               toString(t.date) AS date
        ORDER BY t.amount DESC LIMIT 50
    """)


@app.get("/api/transactions/structuring")
async def structuring_transactions():
    if not _has_transaction_rel():
        return []
    return neo.query("""
        MATCH (a:LegalEntity)-[t:TRANSACTION]->(b:LegalEntity)
        WHERE t.amount > 9000 AND t.amount < 10000
        WITH a, b, count(t) AS txns,
             collect({amount: t.amount, currency: t.currency,
                      date: toString(t.date), suspicious: t.isSuspicious}) AS details
        WHERE txns >= 2
        RETURN a.id AS fromId, a.name AS fromName,
               b.id AS toId, b.name AS toName,
               txns, details
        ORDER BY txns DESC LIMIT 30
    """)


@app.get("/api/transactions/entity/{entity_id}")
async def entity_transactions(entity_id: str):
    if not _has_transaction_rel():
        return []
    return neo.query("""
        MATCH (e:LegalEntity {id: $id})-[t:TRANSACTION]-(other:LegalEntity)
        RETURN e.id AS entityId,
               CASE WHEN startNode(t) = e THEN 'OUT' ELSE 'IN' END AS direction,
               other.id AS otherId, other.name AS otherName,
               t.amount AS amount, t.currency AS currency,
               toString(t.date) AS date, t.isSuspicious AS isSuspicious
        ORDER BY t.date DESC LIMIT 50
    """, {"id": entity_id})


# ─── Persons ──────────────────────────────────────────────────────────────────
@app.get("/api/persons")
async def persons(
    filter_type: str | None = None,
    search: str | None = None,
    limit: int = Query(100, le=500),
):
    where_parts = []
    if filter_type == "sanctioned":
        where_parts.append("p.isSanctioned = true")
    elif filter_type == "pep":
        where_parts.append("p.isPEP = true")
    if search:
        where_parts.append("toLower(p.name) CONTAINS toLower($search)")
    where = "WHERE " + " AND ".join(where_parts) if where_parts else ""
    params: dict[str, Any] = {"limit": limit}
    if search:
        params["search"] = search
    return neo.query(f"""
        MATCH (p:NaturalPerson)
        {where}
        OPTIONAL MATCH (e:LegalEntity)-[:CONTROLLED_BY]->(p)
        RETURN p.id AS id, p.name AS name, p.nationality AS nationality,
               toString(p.dob) AS dob,
               p.isPEP AS isPEP, p.isSanctioned AS isSanctioned,
               p.pageRankScore AS pageRank,
               collect(DISTINCT {{id: e.id, name: e.name}}) AS controlledEntities
        ORDER BY p.isSanctioned DESC, p.isPEP DESC, p.name
        LIMIT $limit
    """, params)


# ─── Community clusters ──────────────────────────────────────────────────────
@app.get("/api/communities")
async def communities():
    return neo.query("""
        MATCH (e:LegalEntity) WHERE e.louvainCommunityId IS NOT NULL
        WITH e.louvainCommunityId AS community, collect(e) AS members
        WHERE size(members) > 2
        RETURN community,
               size(members) AS size,
               [m IN members | {id: m.id, name: m.name, jurisdiction: m.jurisdiction,
                                score: m.kycRiskScore}][..10] AS topMembers,
               toInteger(reduce(s = 0.0, m IN members | s + m.kycRiskScore) / size(members)) AS avgScore,
               size([m IN members WHERE m.kycRiskScore >= 50]) AS highRiskCount
        ORDER BY size DESC LIMIT 30
    """)


# ─── Shell company indicators ────────────────────────────────────────────────
@app.get("/api/shells")
async def shell_companies():
    return neo.query("""
        MATCH (e:LegalEntity)
        WHERE e.hasOperationalAddress = false
        OPTIONAL MATCH (child:LegalEntity)-[:DIRECTLY_OWNED_BY]->(e)
        WITH e, count(child) AS subsidiaries
        RETURN e.id AS id, e.name AS name, e.jurisdiction AS jurisdiction,
               e.kycRiskScore AS score, e.isActive AS isActive,
               subsidiaries,
               e.hasOperationalAddress AS hasAddress,
               e.riskTier AS riskTier,
               e.category AS category
        ORDER BY e.kycRiskScore DESC, subsidiaries DESC
        LIMIT 50
    """)


# ─── GDS (Graph Data Science) ────────────────────────────────────────────────
@app.get("/api/gds/scores")
async def gds_scores(sort_by: str = "pageRank", limit: int = 30):
    sort_field = {
        "pageRank": "e.pageRankScore DESC",
        "betweenness": "e.betweennessScore DESC",
        "community": "e.louvainCommunityId ASC",
    }.get(sort_by, "e.pageRankScore DESC")
    return neo.query(f"""
        MATCH (e:LegalEntity)
        RETURN e.id AS id, e.name AS name, e.jurisdiction AS jurisdiction,
               e.pageRankScore AS pageRank,
               e.betweennessScore AS betweenness,
               e.louvainCommunityId AS community,
               e.wccComponentId AS wcc,
               e.sccComponentId AS scc,
               e.kycRiskScore AS riskScore
        ORDER BY {sort_field}
        LIMIT $limit
    """, {"limit": limit})


@app.get("/api/gds/summary")
async def gds_summary():
    wcc = neo.query("""
        MATCH (e:LegalEntity) WHERE e.wccComponentId IS NOT NULL
        WITH e.wccComponentId AS comp, count(e) AS sz
        RETURN count(comp) AS components,
               max(sz) AS largestComponent,
               min(sz) AS smallestComponent
    """)[0]
    louvain = neo.query("""
        MATCH (e:LegalEntity) WHERE e.louvainCommunityId IS NOT NULL
        WITH e.louvainCommunityId AS comm, count(e) AS sz
        RETURN count(comm) AS communities,
               max(sz) AS largestCommunity
    """)[0]
    pagerank = neo.query("""
        MATCH (e:LegalEntity)
        RETURN round(avg(e.pageRankScore) * 10000) / 10000 AS avgPageRank,
               round(max(e.pageRankScore) * 10000) / 10000 AS maxPageRank,
               round(min(e.pageRankScore) * 10000) / 10000 AS minPageRank
    """)[0]
    betweenness = neo.query("""
        MATCH (e:LegalEntity)
        RETURN round(avg(e.betweennessScore) * 100) / 100 AS avgBetweenness,
               round(max(e.betweennessScore) * 100) / 100 AS maxBetweenness
    """)[0]
    scc = neo.query("""
        MATCH (e:LegalEntity) WHERE e.sccComponentId IS NOT NULL
        WITH e.sccComponentId AS comp, count(e) AS sz
        WHERE sz > 1
        RETURN count(comp) AS rings, sum(sz) AS entitiesInRings
    """)[0]
    return {"wcc": wcc, "louvain": louvain, "pagerank": pagerank,
            "betweenness": betweenness, "scc": scc}


# ─── Ontology classes from Neo4j (n10s) ──────────────────────────────────────
@app.get("/api/ontology/classes")
async def ontology_classes():
    return neo.query("""
        MATCH (c:n4sch__Class)
        OPTIONAL MATCH (c)-[:n4sch__SCO]->(parent:n4sch__Class)
        RETURN c.uri AS uri,
               c.n4sch__label AS label,
               CASE WHEN c.uri CONTAINS '#'
                    THEN split(c.uri, '#')[-1]
                    ELSE split(c.uri, '/')[-1] END AS localName,
               parent.uri AS parentUri,
               CASE WHEN parent.uri CONTAINS '#'
                    THEN split(parent.uri, '#')[-1]
                    ELSE split(parent.uri, '/')[-1] END AS parentName
        ORDER BY localName
    """)


@app.get("/api/ontology/relationships")
async def ontology_relationships():
    return neo.query("""
        MATCH (r:n4sch__Relationship)
        OPTIONAL MATCH (r)-[:n4sch__DOMAIN]->(d)
        OPTIONAL MATCH (r)-[:n4sch__RANGE]->(rng)
        RETURN r.uri AS uri,
               CASE WHEN r.uri CONTAINS '#'
                    THEN split(r.uri, '#')[-1]
                    ELSE split(r.uri, '/')[-1] END AS localName,
               d.uri AS domainUri,
               rng.uri AS rangeUri
        ORDER BY localName
    """)


# ─── Chat (comprehensive dual-DB Q&A engine) ─────────────────────────────────
from dashboard.chat_engine import process_chat


class AgentChatRequest(BaseModel):
    message: str
    session_id: str = "default"


class ResetSessionRequest(BaseModel):
    session_id: str = "default"


@app.post("/api/chat")
async def chat(req: ChatRequest):
    """Legacy pattern-matching chat (kept for backward compatibility)."""
    msg = req.message.strip()
    if not msg:
        raise HTTPException(400, "Empty message")
    return process_chat(msg)


@app.post("/api/agent/chat")
async def agent_chat(req: AgentChatRequest):
    """Agentic chat endpoint — LangGraph ReAct agent with conversation memory.
    Supports follow-up questions within the same session_id."""
    msg = req.message.strip()
    if not msg:
        raise HTTPException(400, "Empty message")
    try:
        from dashboard.agent import process_agent_chat
        return process_agent_chat(msg, session_id=req.session_id)
    except RuntimeError as e:
        # LLM not configured
        raise HTTPException(503, str(e))
    except Exception as e:
        raise HTTPException(500, f"Agent error: {str(e)}")


@app.post("/api/agent/reset")
async def agent_reset(req: ResetSessionRequest):
    """Reset conversation memory for a session."""
    from dashboard.agent import reset_session
    reset_session(req.session_id)
    return {"status": "ok", "session_id": req.session_id}


# ─── Enrichment endpoint (Diffbot) ───────────────────────────────────────────
class EnrichRequest(BaseModel):
    entity_name: str | None = None
    url: str | None = None
    text: str | None = None


@app.post("/api/enrich")
async def enrich_graph(req: EnrichRequest):
    """Enrich the knowledge graph using Diffbot NLP from entity name, URL, or text."""
    from dashboard.agent import enrich_entity_from_web, extract_entities_from_url, extract_entities_from_text
    if req.entity_name:
        result = enrich_entity_from_web.invoke(req.entity_name)
    elif req.url:
        result = extract_entities_from_url.invoke(req.url)
    elif req.text:
        result = extract_entities_from_text.invoke(req.text)
    else:
        raise HTTPException(400, "Provide entity_name, url, or text")
    return {"result": result}


# ─── Vector search endpoint ───────────────────────────────────────────────────
class VectorSearchRequest(BaseModel):
    query: str
    k: int = 5


@app.post("/api/vector/search")
async def vector_search(req: VectorSearchRequest):
    """Semantic vector search across entities."""
    from dashboard.agent import semantic_search_entities
    result = semantic_search_entities.invoke({"query": req.query, "k": req.k})
    return {"result": result}


# ─── SPARQL pass-through (read-only) ─────────────────────────────────────────
@app.post("/api/sparql")
async def run_sparql(req: SparqlRequest):
    q = req.query.strip()
    upper = q.upper()
    for keyword in ["INSERT", "DELETE", "DROP", "CLEAR", "CREATE", "LOAD"]:
        if keyword in upper and "SELECT" not in upper[:upper.index(keyword)]:
            raise HTTPException(400, f"Write operations not allowed: {keyword}")
    try:
        if re.search(r"\bASK\b", q, re.IGNORECASE):
            return {"type": "ask", "result": gdb.ask(q)}
        rows = gdb.query(q)
        return {"type": "select", "rows": rows}
    except Exception as e:
        raise HTTPException(400, str(e))


# ─── Cypher pass-through (read-only) ─────────────────────────────────────────
@app.post("/api/cypher")
async def run_cypher(req: CypherRequest):
    q = req.query.strip()
    upper = q.upper()
    for keyword in ["CREATE", "MERGE", "DELETE", "DETACH", "SET ", "REMOVE", "DROP"]:
        if keyword in upper:
            raise HTTPException(400, f"Write operations not allowed: {keyword}")
    try:
        rows = neo.query(q)
        return {"rows": rows}
    except Exception as e:
        raise HTTPException(400, str(e))


# ─── Graph schema ─────────────────────────────────────────────────────────────
@app.get("/api/schema")
async def graph_schema():
    labels = neo.query("CALL db.labels() YIELD label RETURN label ORDER BY label")
    rels = neo.query("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType ORDER BY relationshipType")
    return {
        "labels": [r["label"] for r in labels],
        "relationshipTypes": [r["relationshipType"] for r in rels],
    }


# ─── Named graphs (GraphDB) ──────────────────────────────────────────────────
@app.get("/api/graphdb/graphs")
async def graphdb_graphs():
    try:
        graphs = gdb.list_named_graphs()
        return [{"graph": g, "triples": t} for g, t in graphs]
    except Exception:
        return []


# ─── Neo4j display name helper ───────────────────────────────────────────────
@app.get("/api/neo4j/display-config")
async def neo4j_display_config():
    return {
        "instructions": (
            "In Neo4j Browser, the n4sch__ prefix comes from n10s (neosemantics).\n"
            "To see cleaner names:\n"
            "1. Click the database icon (top-left) in Neo4j Browser\n"
            "2. Click on a node label like n4sch__Class\n"
            "3. Change 'Caption' to 'uri' or 'n4sch__label'\n"
            "4. For LegalEntity nodes, set Caption to 'name'\n"
            "5. For NaturalPerson nodes, set Caption to 'name'\n\n"
            "The n4sch__ labels are ontology schema nodes. "
            "Your data nodes (LegalEntity, NaturalPerson) have clean labels."
        ),
        "label_mapping": {
            "n4sch__Class": "Ontology Class (FIBO)",
            "n4sch__Relationship": "Ontology Relationship",
            "n4sch__Property": "Ontology Property",
            "Resource": "RDF Resource",
            "LegalEntity": "Legal Entity",
            "NaturalPerson": "Natural Person",
            "SanctionedEntity": "Sanctioned Entity",
            "PoliticallyExposedPerson": "PEP",
        },
    }


# ─── Mount static files last ─────────────────────────────────────────────────
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
