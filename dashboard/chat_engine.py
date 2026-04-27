"""
KYC Intelligence Chat Engine — Deterministic pattern-matching Q&A
that queries both Neo4j (Cypher) and GraphDB (SPARQL).

Returns the executed query alongside results so users can learn.
"""
from __future__ import annotations

import re
from typing import Any

from src.kg_client import Neo4jClient, GraphDBClient

neo = Neo4jClient()
gdb = GraphDBClient()


# ─── Helpers ──────────────────────────────────────────────────────────────────
def _run_cypher(query: str, params: dict | None = None) -> tuple[list[dict], str]:
    """Execute Cypher and return (rows, query_string)."""
    rows = neo.query(query, params or {})
    return rows, query.strip()


def _run_sparql(query: str) -> tuple[list[dict], str]:
    """Execute SPARQL and return (rows, query_string)."""
    if re.search(r"\bASK\b", query, re.IGNORECASE):
        result = gdb.ask(query)
        return [{"result": result}], query.strip()
    rows = gdb.query(query)
    return rows, query.strip()


def _extract_entity_id(text: str) -> str | None:
    """Pull an ENTITY_NNNN from free text."""
    m = re.search(r"(ENTITY_\d{4})", text, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    m = re.search(r"\b(\d{1,4})\b", text)
    if m:
        return f"ENTITY_{m.group(1).zfill(4)}"
    return None


def _extract_person_id(text: str) -> str | None:
    m = re.search(r"(PERSON_\d{4})", text, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    return None


def _fmt_num(n, d=0):
    if n is None:
        return "–"
    if isinstance(n, float):
        return f"{n:.{d}f}"
    return str(n)


def _fmt_rows_table(rows: list[dict], max_rows: int = 20) -> str:
    """Format rows as a markdown table."""
    if not rows:
        return "*No results*"
    keys = list(rows[0].keys())
    lines = ["| " + " | ".join(keys) + " |"]
    lines.append("| " + " | ".join(["---"] * len(keys)) + " |")
    for r in rows[:max_rows]:
        vals = []
        for k in keys:
            v = r.get(k)
            if v is None:
                vals.append("–")
            elif isinstance(v, float):
                vals.append(f"{v:.4f}" if v < 1 else f"{v:.2f}")
            elif isinstance(v, list):
                vals.append(", ".join(str(x) for x in v[:5]))
            else:
                vals.append(str(v))
        lines.append("| " + " | ".join(vals) + " |")
    if len(rows) > max_rows:
        lines.append(f"\n*...and {len(rows) - max_rows} more rows*")
    return "\n".join(lines)


# ─── Query Catalog ───────────────────────────────────────────────────────────
# Each entry: (patterns, handler_fn, category, description)
# Handler returns (reply_markdown, db_type, query_string)

def _handle_ubo(msg: str) -> tuple[str, str, str]:
    eid = _extract_entity_id(msg)
    if not eid:
        return "Please provide an entity ID, e.g. 'Who owns ENTITY_0042?'", "", ""
    q = f"""MATCH path = (e:LegalEntity {{id: $id}})
      -[:DIRECTLY_OWNED_BY*0..6]->()
      -[:CONTROLLED_BY]->(p:NaturalPerson)
RETURN p.name AS ubo, p.nationality AS nationality,
       p.isPEP AS isPEP, p.isSanctioned AS isSanctioned,
       length(path) AS hops,
       [n IN nodes(path) | coalesce(n.name, n.id)] AS chain
ORDER BY hops LIMIT 10"""
    rows, cypher = _run_cypher(q, {"id": eid})
    if not rows:
        return f"No UBO found for **{eid}**. This entity may not have ownership/control chains.", "cypher", cypher
    lines = [f"**UBO chain for {eid}:**\n"]
    for r in rows:
        flags = []
        if r.get("isSanctioned"): flags.append("🔴 SANCTIONED")
        if r.get("isPEP"): flags.append("🟣 PEP")
        flag = f" [{', '.join(flags)}]" if flags else ""
        chain_str = " → ".join(str(x) for x in r["chain"])
        lines.append(f"- **{r['ubo']}** ({r['nationality']}) — {r['hops']} hop(s){flag}")
        lines.append(f"  Chain: {chain_str}")
    return "\n".join(lines), "cypher", cypher


def _handle_risk(msg: str) -> tuple[str, str, str]:
    eid = _extract_entity_id(msg)
    if not eid:
        return "Please provide an entity ID, e.g. 'Risk of ENTITY_0100'", "", ""
    q = """MATCH (e:LegalEntity {id: $id})
OPTIONAL MATCH (e)-[:CONTROLLED_BY]->(ctrl:NaturalPerson)
RETURN e.name AS name, e.jurisdiction AS jurisdiction,
       e.jurisdictionName AS jname, e.riskTier AS tier,
       e.kycRiskScore AS score, e.category AS category,
       e.pageRankScore AS pageRank, e.betweennessScore AS betweenness,
       e.louvainCommunityId AS community, e.sccComponentId AS scc,
       e.hasOperationalAddress AS hasAddr, e.isActive AS active,
       collect(DISTINCT {name: ctrl.name, isPEP: ctrl.isPEP, isSanctioned: ctrl.isSanctioned}) AS controllers"""
    rows, cypher = _run_cypher(q, {"id": eid})
    if not rows:
        return f"Entity **{eid}** not found.", "cypher", cypher
    r = rows[0]
    ctrls = [c for c in (r.get("controllers") or []) if c.get("name")]
    ctrl_str = ", ".join(f"{c['name']}" + (" 🟣PEP" if c.get("isPEP") else "") + (" 🔴SANCTIONED" if c.get("isSanctioned") else "") for c in ctrls) if ctrls else "None"
    scc_str = f"⚠ Ring #{r['scc']}" if r.get("scc") is not None else "None"
    return f"""**{r['name']}** ({eid})

| Attribute | Value |
|---|---|
| Jurisdiction | {r['jurisdiction']} ({r.get('jname','')}) |
| Category | {r['category']} |
| Risk Tier | **{r['tier']}** |
| KYC Score | **{r['score']}/100** |
| PageRank | {_fmt_num(r.get('pageRank'), 4)} |
| Betweenness | {_fmt_num(r.get('betweenness'), 2)} |
| Community | #{r.get('community', '–')} |
| SCC Ring | {scc_str} |
| Operational Address | {'Yes' if r.get('hasAddr') else '**No**'} |
| Active | {'Yes' if r.get('active') else 'No'} |
| Controllers | {ctrl_str} |""", "cypher", cypher


def _handle_sanctions_check(msg: str) -> tuple[str, str, str]:
    eid = _extract_entity_id(msg)
    if not eid:
        return "Please provide an entity ID.", "", ""
    q = """MATCH (e:LegalEntity {id: $id})
OPTIONAL MATCH path = (e)-[:DIRECTLY_OWNED_BY*0..6]->()
      -[:CONTROLLED_BY]->(p:NaturalPerson {isSanctioned: true})
RETURN e.name AS entity, p.name AS sanctioned, p.nationality AS nat,
       length(path) AS hops
LIMIT 5"""
    rows, cypher = _run_cypher(q, {"id": eid})
    if not rows or not rows[0].get("sanctioned"):
        return f"✅ **No sanctioned persons** found in the ownership chain of **{eid}**.", "cypher", cypher
    lines = [f"🔴 **Sanctioned persons found for {rows[0]['entity']}:**\n"]
    for r in rows:
        if r.get("sanctioned"):
            lines.append(f"- **{r['sanctioned']}** ({r['nat']}) — {r['hops']} hops away")
    return "\n".join(lines), "cypher", cypher


def _handle_transactions(msg: str) -> tuple[str, str, str]:
    eid = _extract_entity_id(msg)
    if not eid:
        return "Please provide an entity ID.", "", ""
    q = """MATCH (e:LegalEntity {id: $id})-[t:TRANSACTION]-(other:LegalEntity)
RETURN CASE WHEN startNode(t) = e THEN 'OUT' ELSE 'IN' END AS direction,
       other.id AS counterpartyId, other.name AS counterparty,
       t.amount AS amount, t.currency AS currency,
       toString(t.date) AS date, t.isSuspicious AS suspicious
ORDER BY t.date DESC LIMIT 20"""
    rows, cypher = _run_cypher(q, {"id": eid})
    if not rows:
        return f"No transactions found for **{eid}**.", "cypher", cypher
    lines = [f"**Transactions for {eid}:**\n"]
    lines.append(_fmt_rows_table(rows))
    suspicious = [r for r in rows if r.get("suspicious")]
    if suspicious:
        lines.append(f"\n⚠ **{len(suspicious)} suspicious transaction(s) detected**")
    return "\n".join(lines), "cypher", cypher


def _handle_top_risk(msg: str) -> tuple[str, str, str]:
    limit = 10
    m = re.search(r"(\d+)", msg)
    if m and int(m.group(1)) <= 50:
        limit = int(m.group(1))
    q = f"""MATCH (e:LegalEntity) WHERE e.kycRiskScore > 0
RETURN e.id AS id, e.name AS name, e.jurisdiction AS jurisdiction,
       e.riskTier AS tier, e.kycRiskScore AS score, e.category AS category
ORDER BY score DESC LIMIT {limit}"""
    rows, cypher = _run_cypher(q)
    lines = ["**Top Risk Entities:**\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "cypher", cypher


def _handle_circular(msg: str) -> tuple[str, str, str]:
    q = """MATCH (e:LegalEntity) WHERE e.sccComponentId IS NOT NULL
WITH e.sccComponentId AS scc, collect(e) AS members
WHERE size(members) > 1
RETURN scc,
       [m IN members | m.id + ' (' + m.name + ')'] AS entities,
       size(members) AS size
ORDER BY size DESC LIMIT 20"""
    rows, cypher = _run_cypher(q)
    if not rows:
        return "No circular ownership rings detected.", "cypher", cypher
    lines = [f"**{len(rows)} circular ownership ring(s) detected:**\n"]
    for r in rows:
        lines.append(f"**Ring #{r['scc']}** ({r['size']} entities):")
        lines.append(f"  {' ↔ '.join(r['entities'])}\n")
    return "\n".join(lines), "cypher", cypher


def _handle_shells(msg: str) -> tuple[str, str, str]:
    q = """MATCH (e:LegalEntity) WHERE e.hasOperationalAddress = false
OPTIONAL MATCH (child:LegalEntity)-[:DIRECTLY_OWNED_BY]->(e)
WITH e, count(child) AS subsidiaries
RETURN e.id AS id, e.name AS name, e.jurisdiction AS jurisdiction,
       e.kycRiskScore AS score, e.riskTier AS tier,
       e.category AS category, subsidiaries
ORDER BY score DESC LIMIT 20"""
    rows, cypher = _run_cypher(q)
    lines = ["**Shell Company Indicators** (no operational address):\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "cypher", cypher


def _handle_entity_detail(msg: str) -> tuple[str, str, str]:
    eid = _extract_entity_id(msg)
    if not eid:
        return "Please provide an entity ID.", "", ""
    q = """MATCH (e:LegalEntity {id: $id})
OPTIONAL MATCH (e)-[r:DIRECTLY_OWNED_BY]->(parent:LegalEntity)
OPTIONAL MATCH (child:LegalEntity)-[r2:DIRECTLY_OWNED_BY]->(e)
OPTIONAL MATCH (e)-[r3:CONTROLLED_BY]->(ctrl:NaturalPerson)
OPTIONAL MATCH (e)-[t:TRANSACTION]-(other:LegalEntity)
WITH e, collect(DISTINCT {name: parent.name, id: parent.id, pct: r.percentage}) AS parents,
     collect(DISTINCT {name: child.name, id: child.id, pct: r2.percentage}) AS children,
     collect(DISTINCT {name: ctrl.name, id: ctrl.id, isPEP: ctrl.isPEP, isSanctioned: ctrl.isSanctioned, role: r3.role}) AS controllers,
     count(DISTINCT t) AS txnCount
RETURN e {.*} AS entity, parents, children, controllers, txnCount"""
    rows, cypher = _run_cypher(q, {"id": eid})
    if not rows:
        return f"Entity **{eid}** not found.", "cypher", cypher
    r = rows[0]
    e = r["entity"]
    parents = [p for p in r["parents"] if p.get("id")]
    children = [c for c in r["children"] if c.get("id")]
    controllers = [c for c in r["controllers"] if c.get("id")]
    parents_str = ", ".join(f"{p['name']} ({_fmt_num(p.get('pct'),1)}%)" for p in parents) if parents else "None"
    children_str = ", ".join(f"{c['name']} ({_fmt_num(c.get('pct'),1)}%)" for c in children) if children else "None"
    ctrl_str = ", ".join(f"{c['name']}" + (f" [{c.get('role','')}]" if c.get('role') else "") + (" 🟣PEP" if c.get("isPEP") else "") + (" 🔴SANCTIONED" if c.get("isSanctioned") else "") for c in controllers) if controllers else "None"
    return f"""**{e.get('name')}** ({eid})

| Field | Value |
|---|---|
| LEI | `{e.get('lei','')}` |
| Jurisdiction | {e.get('jurisdiction','')} ({e.get('jurisdictionName','')}) |
| Category | {e.get('category','')} |
| Risk Tier | **{e.get('riskTier','')}** |
| KYC Score | **{e.get('kycRiskScore',0)}/100** |
| Active | {'Yes' if e.get('isActive') else 'No'} |
| Operational Address | {'Yes' if e.get('hasOperationalAddress') else '**No**'} |
| PageRank | {_fmt_num(e.get('pageRankScore'), 4)} |
| Betweenness | {_fmt_num(e.get('betweennessScore'), 2)} |
| Community | #{e.get('louvainCommunityId','–')} |
| Parents (owned by) | {parents_str} |
| Children (subsidiaries) | {children_str} |
| Controllers | {ctrl_str} |
| Transactions | {r['txnCount']} |""", "cypher", cypher


def _handle_person_info(msg: str) -> tuple[str, str, str]:
    pid = _extract_person_id(msg)
    if pid:
        q = """MATCH (p:NaturalPerson {id: $id})
OPTIONAL MATCH (e:LegalEntity)-[:CONTROLLED_BY]->(p)
RETURN p.id AS id, p.name AS name, p.nationality AS nationality,
       toString(p.dob) AS dob, p.isPEP AS isPEP, p.isSanctioned AS isSanctioned,
       p.pageRankScore AS pageRank,
       collect(DISTINCT {id: e.id, name: e.name, jurisdiction: e.jurisdiction}) AS entities"""
        rows, cypher = _run_cypher(q, {"id": pid})
    else:
        # Search by name
        name_match = re.search(r"person\s+(?:named?\s+)?(.+)", msg, re.IGNORECASE)
        if not name_match:
            return "Provide a person ID (PERSON_0042) or name.", "", ""
        name = name_match.group(1).strip().strip('"').strip("'")
        q = """MATCH (p:NaturalPerson) WHERE toLower(p.name) CONTAINS toLower($name)
OPTIONAL MATCH (e:LegalEntity)-[:CONTROLLED_BY]->(p)
RETURN p.id AS id, p.name AS name, p.nationality AS nationality,
       toString(p.dob) AS dob, p.isPEP AS isPEP, p.isSanctioned AS isSanctioned,
       p.pageRankScore AS pageRank,
       collect(DISTINCT {id: e.id, name: e.name}) AS entities
LIMIT 5"""
        rows, cypher = _run_cypher(q, {"name": name})
    if not rows:
        return "Person not found.", "cypher", cypher
    lines = []
    for r in rows:
        entities = [e for e in (r.get("entities") or []) if e.get("id")]
        ent_str = ", ".join(f"{e['name']} ({e['id']})" for e in entities) if entities else "None"
        flags = []
        if r.get("isSanctioned"): flags.append("🔴 SANCTIONED")
        if r.get("isPEP"): flags.append("🟣 PEP")
        flag_str = " ".join(flags) if flags else "Clean"
        lines.append(f"""**{r['name']}** ({r['id']})
- Nationality: {r.get('nationality','–')} | DOB: {r.get('dob','–')}
- Status: {flag_str}
- PageRank: {_fmt_num(r.get('pageRank'),4)}
- Controls: {ent_str}
""")
    return "\n".join(lines), "cypher", cypher


def _handle_peps(msg: str) -> tuple[str, str, str]:
    q = """MATCH (p:PoliticallyExposedPerson)
OPTIONAL MATCH (e:LegalEntity)-[:CONTROLLED_BY]->(p)
RETURN p.id AS id, p.name AS name, p.nationality AS nationality,
       collect(DISTINCT e.name) AS controlledEntities
ORDER BY p.name"""
    rows, cypher = _run_cypher(q)
    lines = [f"**{len(rows)} Politically Exposed Person(s):**\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "cypher", cypher


def _handle_sanctioned_list(msg: str) -> tuple[str, str, str]:
    q = """MATCH (p:SanctionedEntity)
OPTIONAL MATCH (e:LegalEntity)-[:CONTROLLED_BY]->(p)
RETURN p.id AS id, p.name AS name, p.nationality AS nationality,
       collect(DISTINCT {name: e.name, id: e.id}) AS controlledEntities
ORDER BY p.name"""
    rows, cypher = _run_cypher(q)
    lines = [f"**{len(rows)} Sanctioned Person(s):**\n"]
    for r in rows:
        entities = [e for e in (r.get("controlledEntities") or []) if e.get("id")]
        ent_str = ", ".join(f"{e['name']} ({e['id']})" for e in entities) if entities else "None"
        lines.append(f"- 🔴 **{r['name']}** ({r['id']}) — {r.get('nationality','–')} — Controls: {ent_str}")
    return "\n".join(lines), "cypher", cypher


def _handle_pagerank(msg: str) -> tuple[str, str, str]:
    limit = 10
    m = re.search(r"(\d+)", msg)
    if m and int(m.group(1)) <= 50:
        limit = int(m.group(1))
    q = f"""MATCH (e:LegalEntity)
RETURN e.id AS id, e.name AS name, e.jurisdiction AS jurisdiction,
       e.pageRankScore AS pageRank, e.kycRiskScore AS score
ORDER BY e.pageRankScore DESC LIMIT {limit}"""
    rows, cypher = _run_cypher(q)
    lines = ["**Top Entities by PageRank** (most connected/influential):\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "cypher", cypher


def _handle_betweenness(msg: str) -> tuple[str, str, str]:
    q = """MATCH (e:LegalEntity)
RETURN e.id AS id, e.name AS name, e.jurisdiction AS jurisdiction,
       e.betweennessScore AS betweenness, e.kycRiskScore AS score
ORDER BY e.betweennessScore DESC LIMIT 10"""
    rows, cypher = _run_cypher(q)
    lines = ["**Top Entities by Betweenness** (key intermediaries/conduits):\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "cypher", cypher


def _handle_communities(msg: str) -> tuple[str, str, str]:
    q = """MATCH (e:LegalEntity) WHERE e.louvainCommunityId IS NOT NULL
WITH e.louvainCommunityId AS comm, collect(e) AS members
WHERE size(members) > 2
RETURN comm AS community, size(members) AS size,
       [m IN members | m.name][..8] AS topMembers,
       toInteger(reduce(s = 0.0, m IN members | s + m.kycRiskScore) / size(members)) AS avgRisk
ORDER BY size DESC LIMIT 15"""
    rows, cypher = _run_cypher(q)
    lines = ["**Louvain Communities** (dense entity clusters):\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "cypher", cypher


def _handle_subsidiaries(msg: str) -> tuple[str, str, str]:
    eid = _extract_entity_id(msg)
    if eid:
        q = """MATCH (child:LegalEntity)-[r:DIRECTLY_OWNED_BY]->(e:LegalEntity {id: $id})
RETURN child.id AS id, child.name AS name, child.jurisdiction AS jurisdiction,
       r.percentage AS ownership_pct, child.kycRiskScore AS score
ORDER BY r.percentage DESC"""
        rows, cypher = _run_cypher(q, {"id": eid})
        lines = [f"**Subsidiaries of {eid}:**\n", _fmt_rows_table(rows)]
    else:
        q = """MATCH (child:LegalEntity)-[:DIRECTLY_OWNED_BY]->(parent:LegalEntity)
WITH parent, count(child) AS subsidiaries
ORDER BY subsidiaries DESC LIMIT 10
RETURN parent.id AS id, parent.name AS name, parent.jurisdiction AS jurisdiction,
       subsidiaries"""
        rows, cypher = _run_cypher(q)
        lines = ["**Entities with Most Subsidiaries:**\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "cypher", cypher


def _handle_jurisdiction(msg: str) -> tuple[str, str, str]:
    # Check if asking about a specific jurisdiction
    jur_codes = {"us": "US", "gb": "GB", "uk": "GB", "de": "DE", "jp": "JP", "ch": "CH",
                 "ky": "KY", "cayman": "KY", "vg": "VG", "bvi": "VG", "pa": "PA", "panama": "PA",
                 "sc": "SC", "seychelles": "SC", "sg": "SG", "singapore": "SG",
                 "switzerland": "CH", "germany": "DE", "japan": "JP", "united kingdom": "GB",
                 "united states": "US"}
    found_jur = None
    lower = msg.lower()
    for key, code in jur_codes.items():
        if key in lower:
            found_jur = code
            break
    if found_jur:
        q = """MATCH (e:LegalEntity {jurisdiction: $jur})
RETURN e.id AS id, e.name AS name, e.category AS category,
       e.riskTier AS tier, e.kycRiskScore AS score
ORDER BY score DESC LIMIT 20"""
        rows, cypher = _run_cypher(q, {"jur": found_jur})
        lines = [f"**Entities in {found_jur}:**\n", _fmt_rows_table(rows)]
    else:
        q = """MATCH (e:LegalEntity)
WITH e.jurisdiction AS jurisdiction, e.jurisdictionName AS name,
     count(e) AS count,
     toInteger(avg(e.kycRiskScore)) AS avgScore,
     size(collect(CASE WHEN e.kycRiskScore >= 70 THEN 1 END)) AS highRisk
RETURN jurisdiction, name, count, avgScore, highRisk
ORDER BY avgScore DESC"""
        rows, cypher = _run_cypher(q)
        lines = ["**Jurisdiction Risk Summary:**\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "cypher", cypher


def _handle_suspicious_txns(msg: str) -> tuple[str, str, str]:
    q = """MATCH (a:LegalEntity)-[t:TRANSACTION]->(b:LegalEntity)
WHERE t.isSuspicious = true
RETURN a.id AS fromId, a.name AS fromName,
       b.id AS toId, b.name AS toName,
       t.amount AS amount, t.currency AS currency,
       toString(t.date) AS date
ORDER BY t.amount DESC LIMIT 20"""
    rows, cypher = _run_cypher(q)
    lines = [f"**Suspicious Transactions** ({len(rows)} shown):\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "cypher", cypher


def _handle_structuring(msg: str) -> tuple[str, str, str]:
    q = """MATCH (a:LegalEntity)-[t:TRANSACTION]->(b:LegalEntity)
WHERE t.amount > 9000 AND t.amount < 10000
WITH a, b, count(t) AS txns,
     collect({amount: t.amount, date: toString(t.date)}) AS details
WHERE txns >= 2
RETURN a.name AS from, b.name AS to, txns,
       [d IN details | d.amount] AS amounts
ORDER BY txns DESC LIMIT 15"""
    rows, cypher = _run_cypher(q)
    if not rows:
        return "No structuring patterns detected (pairs with 2+ txns in $9k-$10k range).", "cypher", cypher
    lines = ["**Structuring Detection** (transactions just below $10k):\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "cypher", cypher


def _handle_gds_summary(msg: str) -> tuple[str, str, str]:
    q1 = """MATCH (e:LegalEntity)
RETURN round(avg(e.pageRankScore) * 10000) / 10000 AS avgPageRank,
       round(max(e.pageRankScore) * 10000) / 10000 AS maxPageRank,
       round(avg(e.betweennessScore) * 100) / 100 AS avgBetweenness,
       round(max(e.betweennessScore) * 100) / 100 AS maxBetweenness"""
    rows1, c1 = _run_cypher(q1)
    q2 = """MATCH (e:LegalEntity) WHERE e.louvainCommunityId IS NOT NULL
WITH e.louvainCommunityId AS comm, count(e) AS sz
RETURN count(comm) AS communities, max(sz) AS largestCommunity"""
    rows2, c2 = _run_cypher(q2)
    q3 = """MATCH (e:LegalEntity) WHERE e.sccComponentId IS NOT NULL
WITH e.sccComponentId AS comp, count(e) AS sz WHERE sz > 1
RETURN count(comp) AS rings, sum(sz) AS entitiesInRings"""
    rows3, c3 = _run_cypher(q3)
    r1, r2, r3 = rows1[0], rows2[0], rows3[0]
    reply = f"""**Graph Data Science Summary:**

| Algorithm | Metric | Value |
|---|---|---|
| PageRank | Average | {r1['avgPageRank']} |
| PageRank | Maximum | {r1['maxPageRank']} |
| Betweenness | Average | {r1['avgBetweenness']} |
| Betweenness | Maximum | {r1['maxBetweenness']} |
| Louvain | Communities | {r2['communities']} |
| Louvain | Largest | {r2['largestCommunity']} entities |
| SCC | Rings | {r3['rings']} |
| SCC | Entities in rings | {r3['entitiesInRings']} |"""
    return reply, "cypher", f"{c1}\n\n{c2}\n\n{c3}"


def _handle_cross_jurisdiction(msg: str) -> tuple[str, str, str]:
    q = """MATCH path = (a:LegalEntity)-[:DIRECTLY_OWNED_BY*2..4]->(b:LegalEntity)
WHERE a.jurisdiction <> b.jurisdiction
WITH a, b, length(path) AS hops,
     [n IN nodes(path) | n.jurisdiction] AS jurisdictions
WHERE size(apoc.coll.toSet(jurisdictions)) >= 3
RETURN a.id AS fromId, a.name AS fromName, a.jurisdiction AS fromJur,
       b.id AS toId, b.name AS toName, b.jurisdiction AS toJur,
       hops, jurisdictions
LIMIT 15"""
    try:
        rows, cypher = _run_cypher(q)
        lines = ["**Multi-Jurisdiction Ownership Chains** (3+ countries):\n", _fmt_rows_table(rows)]
        return "\n".join(lines), "cypher", cypher
    except Exception:
        # Fallback without apoc
        q2 = """MATCH (a:LegalEntity)-[:DIRECTLY_OWNED_BY*2..3]->(b:LegalEntity)
WHERE a.jurisdiction <> b.jurisdiction
RETURN a.id AS fromId, a.name AS fromName, a.jurisdiction AS fromJur,
       b.id AS toId, b.name AS toName, b.jurisdiction AS toJur
LIMIT 15"""
        rows, cypher = _run_cypher(q2)
        lines = ["**Cross-Jurisdiction Ownership Chains:**\n", _fmt_rows_table(rows)]
        return "\n".join(lines), "cypher", cypher


def _handle_orphans(msg: str) -> tuple[str, str, str]:
    q = """MATCH (n) WHERE NOT (n)--()
WITH labels(n) AS lbls, count(n) AS cnt
WHERE NOT any(l IN lbls WHERE l STARTS WITH '_' OR l STARTS WITH 'n4sch__')
RETURN lbls AS labels, cnt AS count ORDER BY cnt DESC"""
    rows, cypher = _run_cypher(q)
    lines = ["**Orphan Nodes** (no relationships):\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "cypher", cypher


def _handle_graph_stats(msg: str) -> tuple[str, str, str]:
    q = """CALL db.labels() YIELD label
WITH label WHERE NOT label STARTS WITH '_'
CALL db.index.fulltext.queryNodes('', '') YIELD node
RETURN label, 0 AS count"""
    # Use simpler approach
    q = """MATCH (n)
WITH labels(n) AS lbls
UNWIND lbls AS label
WITH label WHERE NOT label STARTS WITH '_'
RETURN label, count(*) AS count ORDER BY count DESC"""
    rows, cypher = _run_cypher(q)
    q2 = """MATCH ()-[r]->()
RETURN type(r) AS type, count(r) AS count ORDER BY count DESC"""
    rows2, cypher2 = _run_cypher(q2)
    lines = ["**Node Counts:**\n", _fmt_rows_table(rows), "\n**Relationship Counts:**\n", _fmt_rows_table(rows2)]
    return "\n".join(lines), "cypher", f"{cypher}\n\n{cypher2}"


# ─── SPARQL handlers ─────────────────────────────────────────────────────────
def _handle_sparql_classes(msg: str) -> tuple[str, str, str]:
    q = """PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?class ?label WHERE {
  ?class a owl:Class .
  OPTIONAL { ?class rdfs:label ?label }
  FILTER(CONTAINS(STR(?class), "edmcouncil.org") || CONTAINS(STR(?class), "omg.org"))
} ORDER BY ?label LIMIT 40"""
    rows, sparql = _run_sparql(q)
    lines = [f"**FIBO Ontology Classes** ({len(rows)} shown):\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "sparql", sparql


def _handle_sparql_ownership_classes(msg: str) -> tuple[str, str, str]:
    q = """PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?class ?label WHERE {
  ?class a owl:Class .
  ?class rdfs:label ?label .
  FILTER(CONTAINS(STR(?class), "Ownership") || CONTAINS(STR(?class), "Control") || CONTAINS(STR(?class), "Owner"))
} ORDER BY ?label"""
    rows, sparql = _run_sparql(q)
    lines = [f"**FIBO Ownership & Control Classes** ({len(rows)} found):\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "sparql", sparql


def _handle_sparql_properties(msg: str) -> tuple[str, str, str]:
    q = """PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?prop ?label ?domain ?range WHERE {
  ?prop a owl:ObjectProperty .
  OPTIONAL { ?prop rdfs:label ?label }
  OPTIONAL { ?prop rdfs:domain ?domain }
  OPTIONAL { ?prop rdfs:range ?range }
} ORDER BY ?label LIMIT 30"""
    rows, sparql = _run_sparql(q)
    lines = ["**Ontology Object Properties:**\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "sparql", sparql


def _handle_sparql_named_graphs(msg: str) -> tuple[str, str, str]:
    q = """SELECT ?graph (COUNT(*) AS ?triples)
WHERE { GRAPH ?graph { ?s ?p ?o } }
GROUP BY ?graph ORDER BY DESC(?triples)"""
    rows, sparql = _run_sparql(q)
    lines = ["**GraphDB Named Graphs:**\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "sparql", sparql


def _handle_sparql_gleif(msg: str) -> tuple[str, str, str]:
    q = """PREFIX kyc: <http://kyc-kg.example.org/ontology#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?entity ?name ?jurisdiction WHERE {
  GRAPH <http://kg/glei/instances> {
    ?entity a kyc:RegisteredLegalEntity .
    ?entity rdfs:label ?name .
    OPTIONAL { ?entity kyc:hasJurisdiction ?jurisdiction }
  }
} LIMIT 20"""
    rows, sparql = _run_sparql(q)
    lines = [f"**GLEIF Entities in GraphDB** ({len(rows)} shown):\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "sparql", sparql


def _handle_sparql_subclasses(msg: str) -> tuple[str, str, str]:
    q = """PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX fibo: <https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/>
SELECT ?class ?label WHERE {
  ?class rdfs:subClassOf* fibo:LegalPerson .
  OPTIONAL { ?class rdfs:label ?label }
} ORDER BY ?label LIMIT 30"""
    rows, sparql = _run_sparql(q)
    lines = ["**LegalPerson Subclass Hierarchy:**\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "sparql", sparql


def _handle_sparql_mapping(msg: str) -> tuple[str, str, str]:
    q = """SELECT ?s ?p ?o WHERE {
  GRAPH <http://kg/mapping/fibo2glei> { ?s ?p ?o }
}"""
    rows, sparql = _run_sparql(q)
    lines = ["**FIBO↔GLEIF Mapping:**\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "sparql", sparql


def _handle_sparql_iso_countries(msg: str) -> tuple[str, str, str]:
    q = """PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX lcc: <https://www.omg.org/spec/LCC/Countries/ISO3166-1-CountryCodes/>
SELECT ?country ?label WHERE {
  GRAPH <http://kg/lcc/iso3166> {
    ?country a ?type .
    ?country rdfs:label ?label .
    FILTER(LANG(?label) = "en" || LANG(?label) = "")
  }
} ORDER BY ?label LIMIT 25"""
    rows, sparql = _run_sparql(q)
    lines = [f"**ISO 3166 Countries in Knowledge Graph** ({len(rows)} shown):\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "sparql", sparql


def _handle_sparql_custom(msg: str) -> tuple[str, str, str]:
    """Extract and run a raw SPARQL query from the message."""
    # Look for SPARQL between backticks or after 'sparql:'
    m = re.search(r"```(?:sparql)?\s*(SELECT.*?)```", msg, re.DOTALL | re.IGNORECASE)
    if not m:
        m = re.search(r"(?:run sparql|execute sparql|sparql query)[:\s]*((?:PREFIX|SELECT|ASK|CONSTRUCT).*)", msg, re.DOTALL | re.IGNORECASE)
    if not m:
        return "Provide a SPARQL query in backticks or after 'run sparql:'", "", ""
    query = m.group(1).strip()
    try:
        rows, sparql = _run_sparql(query)
        lines = ["**SPARQL Result:**\n", _fmt_rows_table(rows)]
        return "\n".join(lines), "sparql", sparql
    except Exception as e:
        return f"**SPARQL Error:** {str(e)}", "sparql", query


def _handle_cypher_custom(msg: str) -> tuple[str, str, str]:
    """Extract and run a raw Cypher query from the message."""
    m = re.search(r"```(?:cypher)?\s*(MATCH.*?)```", msg, re.DOTALL | re.IGNORECASE)
    if not m:
        m = re.search(r"(?:run cypher|execute cypher|cypher query)[:\s]*(MATCH.*)", msg, re.DOTALL | re.IGNORECASE)
    if not m:
        return "Provide a Cypher query in backticks or after 'run cypher:'", "", ""
    query = m.group(1).strip()
    # Security check
    upper = query.upper()
    for kw in ["CREATE", "MERGE", "DELETE", "DETACH", "SET ", "REMOVE", "DROP"]:
        if kw in upper:
            return f"**Blocked:** Write operations ({kw}) not allowed.", "", ""
    try:
        rows, cypher = _run_cypher(query)
        lines = ["**Cypher Result:**\n", _fmt_rows_table(rows)]
        return "\n".join(lines), "cypher", cypher
    except Exception as e:
        return f"**Cypher Error:** {str(e)}", "cypher", query


def _handle_compare_entity(msg: str) -> tuple[str, str, str]:
    """Compare two entities."""
    ids = re.findall(r"ENTITY_\d{4}", msg, re.IGNORECASE)
    if len(ids) < 2:
        return "Provide two entity IDs to compare, e.g. 'compare ENTITY_0042 and ENTITY_0100'", "", ""
    id1, id2 = ids[0].upper(), ids[1].upper()
    q = """MATCH (e:LegalEntity) WHERE e.id IN [$id1, $id2]
RETURN e.id AS id, e.name AS name, e.jurisdiction AS jurisdiction,
       e.category AS category, e.riskTier AS tier,
       e.kycRiskScore AS score, e.pageRankScore AS pageRank,
       e.betweennessScore AS betweenness, e.louvainCommunityId AS community,
       e.hasOperationalAddress AS hasAddr"""
    rows, cypher = _run_cypher(q, {"id1": id1, "id2": id2})
    lines = [f"**Comparison: {id1} vs {id2}:**\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "cypher", cypher


def _handle_path_between(msg: str) -> tuple[str, str, str]:
    """Find shortest path between two entities."""
    ids = re.findall(r"ENTITY_\d{4}", msg, re.IGNORECASE)
    if len(ids) < 2:
        return "Provide two entity IDs, e.g. 'path between ENTITY_0001 and ENTITY_0050'", "", ""
    id1, id2 = ids[0].upper(), ids[1].upper()
    q = """MATCH path = shortestPath(
  (a:LegalEntity {id: $id1})-[:DIRECTLY_OWNED_BY|CONTROLLED_BY*..10]-(b:LegalEntity {id: $id2})
)
RETURN [n IN nodes(path) | coalesce(n.name, n.id)] AS chain,
       [r IN relationships(path) | type(r)] AS relTypes,
       length(path) AS hops"""
    rows, cypher = _run_cypher(q, {"id1": id1, "id2": id2})
    if not rows:
        return f"No path found between **{id1}** and **{id2}** (up to 10 hops).", "cypher", cypher
    r = rows[0]
    chain_parts = []
    for i, name in enumerate(r["chain"]):
        chain_parts.append(f"**{name}**")
        if i < len(r["relTypes"]):
            chain_parts.append(f" —[{r['relTypes'][i]}]→ ")
    return f"**Path between {id1} and {id2}** ({r['hops']} hops):\n\n{''.join(chain_parts)}", "cypher", cypher


# ─── Complex natural-language handlers ────────────────────────────────────────

def _handle_sanctioned_exposure_full(msg: str) -> tuple[str, str, str]:
    """Which entities are ultimately controlled by sanctioned individuals?"""
    q = """MATCH path = (e:LegalEntity)-[:DIRECTLY_OWNED_BY*0..6]->()
      -[:CONTROLLED_BY]->(p:NaturalPerson {isSanctioned: true})
WITH e, p, length(path) AS hops,
     [n IN nodes(path) | coalesce(n.name, n.id)] AS chain
RETURN e.id AS entityId, e.name AS entity, e.jurisdiction AS jur,
       e.kycRiskScore AS score, e.riskTier AS tier,
       p.name AS sanctionedPerson, p.nationality AS nationality,
       hops, chain
ORDER BY hops, e.kycRiskScore DESC"""
    rows, cypher = _run_cypher(q)
    if not rows:
        return "No entities found with sanctioned persons in their ownership chain.", "cypher", cypher
    lines = [f"**{len(rows)} entities exposed to sanctioned individuals:**\n"]
    for r in rows:
        lines.append(f"- **{r['entity']}** ({r['entityId']}, {r['jur']}) — score {r['score']}, tier {r['tier']}")
        lines.append(f"  ↳ Controlled by 🔴 **{r['sanctionedPerson']}** ({r['nationality']}) via {r['hops']} hop(s): {' → '.join(str(x) for x in r['chain'])}")
    return "\n".join(lines), "cypher", cypher


def _handle_pep_controlled_high_risk(msg: str) -> tuple[str, str, str]:
    """Which high-risk entities are controlled by politically exposed persons?"""
    q = """MATCH (e:LegalEntity)-[:CONTROLLED_BY]->(p:PoliticallyExposedPerson)
WHERE e.kycRiskScore >= 50
OPTIONAL MATCH (e)-[:DIRECTLY_OWNED_BY]->(parent:LegalEntity)
RETURN e.id AS entityId, e.name AS entity, e.jurisdiction AS jur,
       e.kycRiskScore AS score, e.riskTier AS tier, e.category AS category,
       p.name AS pep, p.nationality AS pepNationality,
       parent.name AS parentEntity, parent.jurisdiction AS parentJur
ORDER BY e.kycRiskScore DESC"""
    rows, cypher = _run_cypher(q)
    if not rows:
        return "No high-risk entities (score >= 50) found under PEP control.", "cypher", cypher
    lines = [f"**{len(rows)} high-risk entities controlled by PEPs:**\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "cypher", cypher


def _handle_offshore_shells_sanctioned(msg: str) -> tuple[str, str, str]:
    """Find shell companies in offshore jurisdictions linked to sanctioned or PEP persons."""
    q = """MATCH (e:LegalEntity)
WHERE e.hasOperationalAddress = false
  AND e.jurisdiction IN ['KY', 'VG', 'PA', 'SC', 'BM', 'JE', 'GG', 'IM']
OPTIONAL MATCH (e)-[:DIRECTLY_OWNED_BY*0..4]->()-[:CONTROLLED_BY]->(p:NaturalPerson)
WHERE p.isSanctioned = true OR p.isPEP = true
WITH e, collect(DISTINCT {name: p.name, sanctioned: p.isSanctioned, pep: p.isPEP}) AS persons
WHERE size([p IN persons WHERE p.name IS NOT NULL]) > 0
RETURN e.id AS id, e.name AS entity, e.jurisdiction AS jur,
       e.kycRiskScore AS score, e.category AS category,
       [p IN persons WHERE p.sanctioned | p.name] AS sanctionedLinks,
       [p IN persons WHERE p.pep | p.name] AS pepLinks
ORDER BY e.kycRiskScore DESC"""
    rows, cypher = _run_cypher(q)
    if not rows:
        return "No offshore shell companies linked to sanctioned/PEP individuals found.", "cypher", cypher
    lines = [f"**Offshore shell companies with sanctioned/PEP links:**\n"]
    for r in rows:
        flags = []
        if r.get("sanctionedLinks"): flags.append(f"🔴 Sanctioned: {', '.join(r['sanctionedLinks'])}")
        if r.get("pepLinks"): flags.append(f"🟣 PEP: {', '.join(r['pepLinks'])}")
        lines.append(f"- **{r['entity']}** ({r['id']}, {r['jur']}) — score {r['score']}, {r['category']}")
        for f in flags:
            lines.append(f"  ↳ {f}")
    return "\n".join(lines), "cypher", cypher


def _handle_ring_risk_assessment(msg: str) -> tuple[str, str, str]:
    """Assess total risk exposure of all circular ownership rings."""
    q = """MATCH (e:LegalEntity) WHERE e.sccComponentId IS NOT NULL
WITH e.sccComponentId AS scc, collect(e) AS members
WHERE size(members) > 1
UNWIND members AS m
OPTIONAL MATCH (m)-[:CONTROLLED_BY]->(p:NaturalPerson)
WITH scc, members,
     collect(DISTINCT {name: p.name, sanctioned: p.isSanctioned, pep: p.isPEP}) AS controllers
RETURN scc AS ring,
       size(members) AS ringSize,
       [m IN members | m.name] AS entities,
       [m IN members | m.jurisdiction] AS jurisdictions,
       toInteger(reduce(s=0.0, m IN members | s + m.kycRiskScore) / size(members)) AS avgRisk,
       max([m IN members | m.kycRiskScore]) AS maxRisk,
       size([c IN controllers WHERE c.sanctioned]) AS sanctionedControllers,
       size([c IN controllers WHERE c.pep]) AS pepControllers,
       [c IN controllers WHERE c.name IS NOT NULL | c.name] AS controllerNames
ORDER BY avgRisk DESC"""
    rows, cypher = _run_cypher(q)
    if not rows:
        return "No circular ownership rings found.", "cypher", cypher
    lines = ["**Circular Ownership Ring Risk Assessment:**\n"]
    for r in rows:
        flags = []
        if r.get("sanctionedControllers", 0) > 0: flags.append("🔴 SANCTIONED LINKS")
        if r.get("pepControllers", 0) > 0: flags.append("🟣 PEP LINKS")
        flag_str = f" [{', '.join(flags)}]" if flags else ""
        juris = list(set(r["jurisdictions"]))
        lines.append(f"**Ring #{r['ring']}** — {r['ringSize']} entities, avg risk {r['avgRisk']}, max risk {r['maxRisk']}{flag_str}")
        lines.append(f"  Entities: {', '.join(r['entities'])}")
        lines.append(f"  Jurisdictions: {', '.join(juris)}")
        lines.append(f"  Controllers: {', '.join(r['controllerNames']) if r['controllerNames'] else 'None'}\n")
    return "\n".join(lines), "cypher", cypher


def _handle_hidden_controllers(msg: str) -> tuple[str, str, str]:
    """Find persons who control many entities indirectly (through multiple layers)."""
    q = """MATCH path = (e:LegalEntity)-[:DIRECTLY_OWNED_BY*1..6]->()
      -[:CONTROLLED_BY]->(p:NaturalPerson)
WHERE length(path) >= 3
WITH p, collect(DISTINCT e) AS indirectEntities,
     collect(DISTINCT e.jurisdiction) AS jurisdictions
WHERE size(indirectEntities) >= 2
OPTIONAL MATCH (direct:LegalEntity)-[:CONTROLLED_BY]->(p)
WITH p, indirectEntities, jurisdictions,
     collect(DISTINCT direct) AS directEntities
RETURN p.id AS id, p.name AS name, p.nationality AS nationality,
       p.isPEP AS isPEP, p.isSanctioned AS isSanctioned,
       size(directEntities) AS directControl,
       size(indirectEntities) AS indirectControl,
       size(indirectEntities) + size(directEntities) AS totalReach,
       jurisdictions
ORDER BY totalReach DESC LIMIT 15"""
    rows, cypher = _run_cypher(q)
    if not rows:
        return "No hidden controllers with multi-layer influence found.", "cypher", cypher
    lines = ["**Hidden Controllers** (persons with deep indirect ownership influence):\n"]
    for r in rows:
        flags = []
        if r.get("isSanctioned"): flags.append("🔴 SANCTIONED")
        if r.get("isPEP"): flags.append("🟣 PEP")
        flag_str = f" {' '.join(flags)}" if flags else ""
        juris = r.get("jurisdictions", [])
        lines.append(f"- **{r['name']}** ({r['nationality']}){flag_str}")
        lines.append(f"  Direct: {r['directControl']} entities | Indirect: {r['indirectControl']} entities | Total reach: {r['totalReach']}")
        lines.append(f"  Jurisdictions reached: {', '.join(str(j) for j in juris)}")
    return "\n".join(lines), "cypher", cypher


def _handle_money_laundering_risk(msg: str) -> tuple[str, str, str]:
    """Find entities that combine multiple red flags: high risk, suspicious transactions, offshore, shell."""
    q = """MATCH (e:LegalEntity)
WHERE e.kycRiskScore >= 35
OPTIONAL MATCH (e)-[t:TRANSACTION {isSuspicious: true}]-(other:LegalEntity)
WITH e, count(DISTINCT t) AS suspiciousTxns
OPTIONAL MATCH (e)-[:DIRECTLY_OWNED_BY*0..4]->()-[:CONTROLLED_BY]->(p:NaturalPerson)
WHERE p.isSanctioned = true OR p.isPEP = true
WITH e, suspiciousTxns,
     collect(DISTINCT CASE WHEN p.isSanctioned THEN p.name END) AS sanctioned,
     collect(DISTINCT CASE WHEN p.isPEP THEN p.name END) AS peps
WITH e, suspiciousTxns, sanctioned, peps,
     (CASE WHEN e.hasOperationalAddress = false THEN 1 ELSE 0 END) AS shellFlag,
     (CASE WHEN e.sccComponentId IS NOT NULL THEN 1 ELSE 0 END) AS ringFlag,
     (CASE WHEN e.jurisdiction IN ['KY','VG','PA','SC'] THEN 1 ELSE 0 END) AS offshoreFlag,
     (CASE WHEN size([s IN sanctioned WHERE s IS NOT NULL]) > 0 THEN 1 ELSE 0 END) AS sanctionedFlag,
     (CASE WHEN size([p IN peps WHERE p IS NOT NULL]) > 0 THEN 1 ELSE 0 END) AS pepFlag
WITH e, suspiciousTxns, sanctioned, peps,
     shellFlag + ringFlag + offshoreFlag + sanctionedFlag + pepFlag +
     (CASE WHEN suspiciousTxns > 0 THEN 1 ELSE 0 END) AS redFlagCount,
     shellFlag, ringFlag, offshoreFlag, sanctionedFlag, pepFlag
WHERE redFlagCount >= 2
RETURN e.id AS id, e.name AS entity, e.jurisdiction AS jur,
       e.kycRiskScore AS score, e.riskTier AS tier,
       redFlagCount,
       suspiciousTxns,
       CASE WHEN shellFlag = 1 THEN 'Yes' ELSE 'No' END AS isShell,
       CASE WHEN ringFlag = 1 THEN 'Yes' ELSE 'No' END AS inRing,
       CASE WHEN offshoreFlag = 1 THEN 'Yes' ELSE 'No' END AS offshore,
       [s IN sanctioned WHERE s IS NOT NULL] AS sanctionedLinks,
       [p IN peps WHERE p IS NOT NULL] AS pepLinks
ORDER BY redFlagCount DESC, e.kycRiskScore DESC LIMIT 20"""
    rows, cypher = _run_cypher(q)
    if not rows:
        return "No entities found with multiple AML red flags.", "cypher", cypher
    lines = [f"**Multi-Red-Flag Entities** ({len(rows)} found — 2+ AML indicators):\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "cypher", cypher


def _handle_contagion_risk(msg: str) -> tuple[str, str, str]:
    """Which entities are at risk because they share ownership chains with sanctioned entities?"""
    q = """MATCH (sanctioned_e:LegalEntity)
WHERE EXISTS {
  MATCH (sanctioned_e)-[:DIRECTLY_OWNED_BY*0..4]->()-[:CONTROLLED_BY]->(:NaturalPerson {isSanctioned: true})
}
WITH collect(sanctioned_e) AS sanctionedEntities
UNWIND sanctionedEntities AS se
MATCH (neighbor:LegalEntity)-[:DIRECTLY_OWNED_BY*1..2]-(se)
WHERE NOT neighbor IN sanctionedEntities
WITH DISTINCT neighbor, se
RETURN neighbor.id AS id, neighbor.name AS entity, neighbor.jurisdiction AS jur,
       neighbor.kycRiskScore AS score, neighbor.riskTier AS tier,
       collect(DISTINCT se.name) AS exposedVia
ORDER BY neighbor.kycRiskScore DESC LIMIT 20"""
    rows, cypher = _run_cypher(q)
    if not rows:
        return "No contagion risk entities found.", "cypher", cypher
    lines = [f"**Contagion Risk** — entities within 2 hops of sanctioned-linked companies:\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "cypher", cypher


def _handle_largest_ownership_trees(msg: str) -> tuple[str, str, str]:
    """What are the deepest/widest corporate ownership hierarchies?"""
    q = """MATCH path = (leaf:LegalEntity)-[:DIRECTLY_OWNED_BY*]->(root:LegalEntity)
WHERE NOT EXISTS { MATCH (root)-[:DIRECTLY_OWNED_BY]->() }
WITH root, max(length(path)) AS maxDepth,
     collect(DISTINCT leaf) AS leaves
OPTIONAL MATCH (desc:LegalEntity)-[:DIRECTLY_OWNED_BY*]->(root)
WITH root, maxDepth, leaves,
     count(DISTINCT desc) AS totalDescendants
WHERE totalDescendants >= 3
RETURN root.id AS rootId, root.name AS rootEntity, root.jurisdiction AS jur,
       root.kycRiskScore AS score,
       totalDescendants, maxDepth,
       [l IN leaves[..5] | l.name] AS sampleLeaves
ORDER BY totalDescendants DESC LIMIT 15"""
    rows, cypher = _run_cypher(q)
    if not rows:
        return "No significant ownership trees found.", "cypher", cypher
    lines = ["**Largest Corporate Ownership Trees:**\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "cypher", cypher


def _handle_risk_heatmap(msg: str) -> tuple[str, str, str]:
    """Show risk distribution across jurisdictions and entity categories."""
    q = """MATCH (e:LegalEntity)
WITH e.jurisdiction AS jur, e.category AS category,
     count(e) AS cnt,
     toInteger(avg(e.kycRiskScore)) AS avgRisk,
     max(e.kycRiskScore) AS maxRisk,
     size(collect(CASE WHEN e.kycRiskScore >= 70 THEN 1 END)) AS criticalCount,
     size(collect(CASE WHEN e.sccComponentId IS NOT NULL THEN 1 END)) AS inRings,
     size(collect(CASE WHEN e.hasOperationalAddress = false THEN 1 END)) AS shells
WHERE cnt >= 2
RETURN jur AS jurisdiction, category, cnt AS entities,
       avgRisk, maxRisk, criticalCount, inRings, shells
ORDER BY avgRisk DESC, cnt DESC"""
    rows, cypher = _run_cypher(q)
    lines = ["**Risk Heatmap by Jurisdiction x Category:**\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "cypher", cypher


def _handle_community_risk_outliers(msg: str) -> tuple[str, str, str]:
    """Find communities where the average risk is significantly higher than normal."""
    q = """MATCH (e:LegalEntity) WHERE e.louvainCommunityId IS NOT NULL
WITH e.louvainCommunityId AS comm, collect(e) AS members
WHERE size(members) >= 3
WITH comm, members,
     reduce(s=0.0, m IN members | s + m.kycRiskScore) / size(members) AS avgRisk,
     max([m IN members | m.kycRiskScore]) AS maxRisk,
     size(members) AS sz
WHERE avgRisk >= 25
UNWIND members AS m
OPTIONAL MATCH (m)-[:CONTROLLED_BY]->(p:NaturalPerson)
WITH comm, sz, avgRisk, maxRisk,
     collect(DISTINCT m.jurisdiction) AS jurisdictions,
     collect(DISTINCT CASE WHEN p.isSanctioned THEN p.name END) AS sanctioned,
     collect(DISTINCT CASE WHEN p.isPEP THEN p.name END) AS peps
RETURN comm AS community, sz AS size,
       toInteger(avgRisk) AS avgRisk, maxRisk,
       jurisdictions,
       [s IN sanctioned WHERE s IS NOT NULL] AS sanctionedPersons,
       [p IN peps WHERE p IS NOT NULL] AS pepPersons
ORDER BY avgRisk DESC"""
    rows, cypher = _run_cypher(q)
    if not rows:
        return "No high-risk community clusters found (threshold: avg risk >= 25).", "cypher", cypher
    lines = [f"**High-Risk Community Clusters** (avg risk >= 25):\n"]
    for r in rows:
        flags = []
        if r.get("sanctionedPersons"): flags.append(f"🔴 {', '.join(r['sanctionedPersons'])}")
        if r.get("pepPersons"): flags.append(f"🟣 {', '.join(r['pepPersons'])}")
        flag_str = f"\n  Flagged persons: {'; '.join(flags)}" if flags else ""
        lines.append(f"**Community #{r['community']}** — {r['size']} entities, avg risk {r['avgRisk']}, max {r['maxRisk']}")
        lines.append(f"  Jurisdictions: {', '.join(str(j) for j in r['jurisdictions'])}{flag_str}\n")
    return "\n".join(lines), "cypher", cypher


def _handle_transaction_network(msg: str) -> tuple[str, str, str]:
    """Which entities form the most active transaction networks?"""
    q = """MATCH (a:LegalEntity)-[t:TRANSACTION]->(b:LegalEntity)
WITH a, b, count(t) AS txnCount, sum(t.amount) AS totalAmount,
     sum(CASE WHEN t.isSuspicious THEN 1 ELSE 0 END) AS suspiciousCount
WHERE txnCount >= 2
RETURN a.id AS fromId, a.name AS fromEntity, a.jurisdiction AS fromJur,
       b.id AS toId, b.name AS toEntity, b.jurisdiction AS toJur,
       txnCount, round(totalAmount) AS totalAmount, suspiciousCount
ORDER BY txnCount DESC, totalAmount DESC LIMIT 15"""
    rows, cypher = _run_cypher(q)
    if not rows:
        return "No repeat-transaction pairs found.", "cypher", cypher
    lines = ["**Most Active Transaction Corridors:**\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "cypher", cypher


def _handle_sanctions_full_impact(msg: str) -> tuple[str, str, str]:
    """If we sanction a particular entity, what is the blast radius?"""
    eid = _extract_entity_id(msg)
    if not eid:
        return "Provide an entity ID, e.g. 'What is the impact if ENTITY_0385 is sanctioned?'", "", ""
    q = """MATCH (target:LegalEntity {id: $id})
OPTIONAL MATCH (child:LegalEntity)-[:DIRECTLY_OWNED_BY*1..4]->(target)
OPTIONAL MATCH (target)-[:DIRECTLY_OWNED_BY*1..4]->(parent:LegalEntity)
OPTIONAL MATCH (target)-[:CONTROLLED_BY]->(ctrl:NaturalPerson)
OPTIONAL MATCH (other:LegalEntity)-[:CONTROLLED_BY]->(ctrl)
WHERE other <> target
WITH target, collect(DISTINCT child) AS children,
     collect(DISTINCT parent) AS parents,
     collect(DISTINCT ctrl) AS controllers,
     collect(DISTINCT other) AS siblingEntities
RETURN target.name AS entity, target.jurisdiction AS jur,
       target.kycRiskScore AS score,
       size(children) AS downstreamEntities,
       [c IN children[..5] | c.name + ' (' + c.jurisdiction + ')'] AS sampleChildren,
       size(parents) AS upstreamEntities,
       [p IN parents[..5] | p.name + ' (' + p.jurisdiction + ')'] AS sampleParents,
       [c IN controllers | c.name + ' (' + c.nationality + ')'] AS controllers,
       size(siblingEntities) AS siblingsByController,
       [s IN siblingEntities[..5] | s.name] AS sampleSiblings"""
    rows, cypher = _run_cypher(q, {"id": eid})
    if not rows:
        return f"Entity **{eid}** not found.", "cypher", cypher
    r = rows[0]
    total = r.get("downstreamEntities", 0) + r.get("upstreamEntities", 0) + r.get("siblingsByController", 0)
    lines = [f"**Sanctions Blast Radius for {r['entity']}** ({eid}, {r['jur']}):\n"]
    lines.append(f"| Impact Zone | Count | Examples |")
    lines.append(f"|---|---|---|")
    lines.append(f"| Downstream (subsidiaries) | {r['downstreamEntities']} | {', '.join(r.get('sampleChildren', []))} |")
    lines.append(f"| Upstream (parents) | {r['upstreamEntities']} | {', '.join(r.get('sampleParents', []))} |")
    lines.append(f"| Siblings (same controllers) | {r['siblingsByController']} | {', '.join(r.get('sampleSiblings', []))} |")
    lines.append(f"| Controllers | {len(r.get('controllers', []))} | {', '.join(r.get('controllers', []))} |")
    lines.append(f"\n**Total impact radius: {total} entities**")
    return "\n".join(lines), "cypher", cypher


def _handle_beneficial_owner_across_jurisdictions(msg: str) -> tuple[str, str, str]:
    """Find individuals who control entities in 3+ different countries."""
    q = """MATCH (p:NaturalPerson)<-[:CONTROLLED_BY]-(e:LegalEntity)
WITH p, collect(DISTINCT e.jurisdiction) AS jurisdictions,
     collect(DISTINCT {id: e.id, name: e.name, jur: e.jurisdiction, score: e.kycRiskScore}) AS entities
WHERE size(jurisdictions) >= 3
RETURN p.id AS id, p.name AS name, p.nationality AS nationality,
       p.isPEP AS isPEP, p.isSanctioned AS isSanctioned,
       size(entities) AS entityCount, jurisdictions,
       toInteger(reduce(s=0.0, e IN entities | s + e.score) / size(entities)) AS avgRisk,
       [e IN entities | e.name + ' (' + e.jur + ')'] AS entityList
ORDER BY size(jurisdictions) DESC, entityCount DESC"""
    rows, cypher = _run_cypher(q)
    if not rows:
        return "No individuals found controlling entities across 3+ jurisdictions.", "cypher", cypher
    lines = ["**Individuals with cross-border control (3+ jurisdictions):**\n"]
    for r in rows:
        flags = []
        if r.get("isSanctioned"): flags.append("🔴 SANCTIONED")
        if r.get("isPEP"): flags.append("🟣 PEP")
        flag_str = f" {' '.join(flags)}" if flags else ""
        lines.append(f"**{r['name']}** ({r['nationality']}){flag_str}")
        lines.append(f"  {r['entityCount']} entities across {', '.join(str(j) for j in r['jurisdictions'])} — avg risk {r['avgRisk']}")
        lines.append(f"  Entities: {', '.join(str(e) for e in r['entityList'])}\n")
    return "\n".join(lines), "cypher", cypher


def _handle_weakest_link(msg: str) -> tuple[str, str, str]:
    """Find entities with highest betweenness that, if removed, would disconnect the most paths."""
    q = """MATCH (e:LegalEntity)
WHERE e.betweennessScore > 0
OPTIONAL MATCH (e)-[:DIRECTLY_OWNED_BY]->(parent:LegalEntity)
OPTIONAL MATCH (child:LegalEntity)-[:DIRECTLY_OWNED_BY]->(e)
OPTIONAL MATCH (e)-[:CONTROLLED_BY]->(ctrl:NaturalPerson)
WITH e, count(DISTINCT parent) AS parentCount, count(DISTINCT child) AS childCount,
     collect(DISTINCT ctrl.name) AS controllers
RETURN e.id AS id, e.name AS entity, e.jurisdiction AS jur,
       e.kycRiskScore AS score,
       e.betweennessScore AS betweenness, e.pageRankScore AS pageRank,
       parentCount, childCount,
       parentCount + childCount AS totalConnections,
       controllers
ORDER BY e.betweennessScore DESC LIMIT 10"""
    rows, cypher = _run_cypher(q)
    lines = ["**Critical Network Chokepoints** (highest betweenness — removing these disconnects the most paths):\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "cypher", cypher


def _handle_same_controller_diff_risk(msg: str) -> tuple[str, str, str]:
    """Find controllers whose entities span wide risk ranges — possible risk arbitrage."""
    q = """MATCH (p:NaturalPerson)<-[:CONTROLLED_BY]-(e:LegalEntity)
WITH p, collect(e) AS entities
WHERE size(entities) >= 2
WITH p, entities,
     min([e IN entities | e.kycRiskScore]) AS minRisk,
     max([e IN entities | e.kycRiskScore]) AS maxRisk
WHERE maxRisk - minRisk >= 30
RETURN p.id AS id, p.name AS name, p.nationality AS nationality,
       p.isPEP AS isPEP, p.isSanctioned AS isSanctioned,
       size(entities) AS entityCount, minRisk, maxRisk,
       maxRisk - minRisk AS riskSpread,
       [e IN entities | e.name + ' (score:' + toString(e.kycRiskScore) + ', ' + e.jurisdiction + ')'] AS entities
ORDER BY riskSpread DESC"""
    rows, cypher = _run_cypher(q)
    if not rows:
        return "No controllers found with wide risk spread (>=30 point difference).", "cypher", cypher
    lines = ["**Risk Arbitrage Detection** — controllers with entities spanning wide risk ranges:\n"]
    for r in rows:
        flags = []
        if r.get("isSanctioned"): flags.append("🔴")
        if r.get("isPEP"): flags.append("🟣")
        flag_str = f" {''.join(flags)}" if flags else ""
        lines.append(f"**{r['name']}** ({r['nationality']}){flag_str} — risk spread **{r['riskSpread']}** (min {r['minRisk']} -> max {r['maxRisk']})")
        lines.append(f"  Entities: {', '.join(str(e) for e in r['entities'])}\n")
    return "\n".join(lines), "cypher", cypher


def _handle_sparql_full_entity_portrait(msg: str) -> tuple[str, str, str]:
    """What does the ontology say about how ownership is modeled in FIBO?"""
    q = """PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?class ?label ?parent ?parentLabel WHERE {
  ?class a owl:Class .
  ?class rdfs:label ?label .
  OPTIONAL {
    ?class rdfs:subClassOf ?parent .
    ?parent rdfs:label ?parentLabel .
    FILTER(isIRI(?parent))
  }
  FILTER(
    CONTAINS(STR(?class), "Ownership") || CONTAINS(STR(?class), "Control") ||
    CONTAINS(STR(?class), "Owner") || CONTAINS(STR(?class), "Shareholder") ||
    CONTAINS(STR(?class), "Subsidiary") || CONTAINS(STR(?class), "Investor")
  )
} ORDER BY ?parent ?label"""
    rows, sparql = _run_sparql(q)
    lines = ["**FIBO Ownership Model — Class Hierarchy with Parents:**\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "sparql", sparql


def _handle_sparql_entity_type_analysis(msg: str) -> tuple[str, str, str]:
    """How are GLEIF entities typed across FIBO and KYC ontologies?"""
    q = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?type (COUNT(?entity) AS ?count) ?typeLabel WHERE {
  GRAPH <http://kg/glei/instances> {
    ?entity rdf:type ?type .
  }
  OPTIONAL { ?type rdfs:label ?typeLabel }
  FILTER(?type != <http://www.w3.org/2002/07/owl#NamedIndividual>)
} GROUP BY ?type ?typeLabel ORDER BY DESC(?count)"""
    rows, sparql = _run_sparql(q)
    lines = ["**GLEIF Entity Type Distribution** (how entities are classified across ontologies):\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "sparql", sparql


def _handle_sparql_ontology_completeness(msg: str) -> tuple[str, str, str]:
    """How complete is our knowledge graph? Which FIBO classes have instances?"""
    q = """PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?class ?label (COUNT(?instance) AS ?instanceCount) WHERE {
  ?class a owl:Class .
  OPTIONAL { ?class rdfs:label ?label }
  OPTIONAL { ?instance rdf:type ?class }
  FILTER(CONTAINS(STR(?class), "edmcouncil") || CONTAINS(STR(?class), "kyc-kg"))
} GROUP BY ?class ?label
ORDER BY DESC(?instanceCount)
LIMIT 30"""
    rows, sparql = _run_sparql(q)
    with_instances = sum(1 for r in rows if r.get("instanceCount", 0) > 0)
    lines = [f"**Ontology Coverage** — {with_instances}/{len(rows)} classes have instances:\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "sparql", sparql


def _handle_sparql_cross_ontology(msg: str) -> tuple[str, str, str]:
    """How do FIBO, GLEIF, and KYC ontologies relate to each other?"""
    q = """PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?subject ?predicate ?object WHERE {
  {
    ?subject owl:equivalentClass ?object .
    BIND(owl:equivalentClass AS ?predicate)
  } UNION {
    ?subject rdfs:subClassOf ?object .
    FILTER(isIRI(?object))
    FILTER(
      (CONTAINS(STR(?subject), "kyc-kg") && CONTAINS(STR(?object), "edmcouncil")) ||
      (CONTAINS(STR(?subject), "edmcouncil") && CONTAINS(STR(?object), "kyc-kg")) ||
      (CONTAINS(STR(?subject), "kyc-kg") && CONTAINS(STR(?object), "omg.org")) ||
      (CONTAINS(STR(?subject), "omg.org") && CONTAINS(STR(?object), "kyc-kg"))
    )
    BIND(rdfs:subClassOf AS ?predicate)
  }
} LIMIT 30"""
    rows, sparql = _run_sparql(q)
    lines = ["**Cross-Ontology Links** (FIBO <-> KYC <-> LCC mappings):\n", _fmt_rows_table(rows)]
    return "\n".join(lines), "sparql", sparql


def _handle_investigation_report(msg: str) -> tuple[str, str, str]:
    """Generate a full due diligence investigation report for an entity."""
    eid = _extract_entity_id(msg)
    if not eid:
        return "Provide an entity ID, e.g. 'Investigate ENTITY_0385'", "", ""
    q1 = """MATCH (e:LegalEntity {id: $id})
OPTIONAL MATCH (e)-[r:DIRECTLY_OWNED_BY]->(parent:LegalEntity)
OPTIONAL MATCH (child:LegalEntity)-[r2:DIRECTLY_OWNED_BY]->(e)
OPTIONAL MATCH (e)-[:CONTROLLED_BY]->(ctrl:NaturalPerson)
RETURN e {.*} AS entity,
       collect(DISTINCT {name: parent.name, id: parent.id, pct: r.percentage, jur: parent.jurisdiction}) AS parents,
       collect(DISTINCT {name: child.name, id: child.id, pct: r2.percentage, jur: child.jurisdiction}) AS children,
       collect(DISTINCT {name: ctrl.name, id: ctrl.id, pep: ctrl.isPEP, sanctioned: ctrl.isSanctioned, nat: ctrl.nationality}) AS controllers"""
    rows1, c1 = _run_cypher(q1, {"id": eid})
    if not rows1:
        return f"Entity **{eid}** not found.", "cypher", c1
    r = rows1[0]
    e = r["entity"]
    parents = [p for p in r["parents"] if p.get("id")]
    children = [c for c in r["children"] if c.get("id")]
    controllers = [c for c in r["controllers"] if c.get("id")]

    q2 = """MATCH path = (e:LegalEntity {id: $id})-[:DIRECTLY_OWNED_BY*0..6]->()-[:CONTROLLED_BY]->(p:NaturalPerson)
RETURN p.name AS ubo, p.nationality AS nat, p.isPEP AS isPEP, p.isSanctioned AS isSanctioned,
       length(path) AS hops, [n IN nodes(path) | coalesce(n.name, n.id)] AS chain
ORDER BY hops LIMIT 5"""
    ubos, c2 = _run_cypher(q2, {"id": eid})

    q3 = """MATCH (e:LegalEntity {id: $id})-[t:TRANSACTION]-(other:LegalEntity)
RETURN count(t) AS totalTxns,
       sum(CASE WHEN t.isSuspicious THEN 1 ELSE 0 END) AS suspiciousTxns,
       round(sum(t.amount)) AS totalVolume,
       count(DISTINCT other) AS counterparties"""
    txn_summary, c3 = _run_cypher(q3, {"id": eid})
    ts = txn_summary[0] if txn_summary else {}

    flags = []
    if e.get("sccComponentId") is not None: flags.append("In circular ownership ring")
    if not e.get("hasOperationalAddress"): flags.append("No operational address (shell indicator)")
    if e.get("jurisdiction") in ["KY", "VG", "PA", "SC"]: flags.append("Offshore jurisdiction")
    for c in controllers:
        if c.get("sanctioned"): flags.append(f"🔴 Sanctioned controller: {c['name']}")
        if c.get("pep"): flags.append(f"🟣 PEP controller: {c['name']}")
    if ts.get("suspiciousTxns", 0) > 0: flags.append(f"{ts['suspiciousTxns']} suspicious transactions")

    parent_str = ", ".join(f"{p['name']} ({p.get('pct','')}%, {p['jur']})" for p in parents) if parents else "None"
    child_str = ", ".join(f"{c['name']} ({c.get('pct','')}%, {c['jur']})" for c in children) if children else "None"
    ctrl_str = ", ".join(f"{c['name']} ({c['nat']})" + (" 🟣PEP" if c.get("pep") else "") + (" 🔴SANCTIONED" if c.get("sanctioned") else "") for c in controllers) if controllers else "None"
    ubo_str = "\n".join(f"  - **{u['ubo']}** ({u['nat']}) via {u['hops']} hops: {' -> '.join(str(x) for x in u['chain'])}" + (" 🔴" if u.get("isSanctioned") else "") + (" 🟣" if u.get("isPEP") else "") for u in ubos) if ubos else "  None found"
    flag_str = "\n".join(f"  - {f}" for f in flags) if flags else "  - No red flags detected"

    report = f"""**Due Diligence Report: {e.get('name')}** ({eid})

**1. ENTITY PROFILE**
| Field | Value |
|---|---|
| LEI | `{e.get('lei','')}` |
| Jurisdiction | {e.get('jurisdiction','')} ({e.get('jurisdictionName','')}) |
| Category | {e.get('category','')} |
| Active | {'Yes' if e.get('isActive') else 'No'} |
| Operational Address | {'Yes' if e.get('hasOperationalAddress') else '**No**'} |

**2. RISK ASSESSMENT**
| Metric | Value |
|---|---|
| Risk Tier | **{e.get('riskTier','')}** |
| KYC Score | **{e.get('kycRiskScore',0)}/100** |
| PageRank | {_fmt_num(e.get('pageRankScore'), 4)} |
| Betweenness | {_fmt_num(e.get('betweennessScore'), 2)} |
| Community | #{e.get('louvainCommunityId','N/A')} |
| SCC Ring | {'Ring #' + str(e.get('sccComponentId')) if e.get('sccComponentId') is not None else 'None'} |

**3. OWNERSHIP STRUCTURE**
- Parents: {parent_str}
- Subsidiaries: {child_str}
- Controllers: {ctrl_str}

**4. ULTIMATE BENEFICIAL OWNERS**
{ubo_str}

**5. TRANSACTION PROFILE**
| Metric | Value |
|---|---|
| Total Transactions | {ts.get('totalTxns', 0)} |
| Suspicious | {ts.get('suspiciousTxns', 0)} |
| Total Volume | ${ts.get('totalVolume', 0):,.0f} |
| Counterparties | {ts.get('counterparties', 0)} |

**6. RED FLAGS**
{flag_str}"""
    full_query = f"{c1}\n\n{c2}\n\n{c3}"
    return report, "cypher", full_query


# ─── Pattern registry ────────────────────────────────────────────────────────
PATTERNS: list[tuple[list[str], Any, str]] = [
    # (regex_patterns, handler_fn, description)
    # Neo4j - Entity Investigation
    ([r"who\s+owns?\s+", r"ubo\s+", r"beneficial\s+owner", r"ownership\s+chain"], _handle_ubo, "UBO Chain Traversal"),
    ([r"risk\s+(?:score\s+)?(?:of|for)\s+", r"how\s+risky\s+is\s+", r"risk\s+profile"], _handle_risk, "Entity Risk Profile"),
    ([r"sanction.*?check\s+", r"is\s+\w+\s+sanctioned", r"sanctions?\s+exposure"], _handle_sanctions_check, "Sanctions Check"),
    ([r"transactions?\s+(?:of|for)\s+", r"txn.*?(?:of|for)"], _handle_transactions, "Entity Transactions"),
    ([r"(?:detail|info|about)\s+(?:entity\s+)?ENTITY_", r"tell\s+me\s+about\s+ENTITY_", r"describe\s+ENTITY_"], _handle_entity_detail, "Entity Detail"),
    ([r"compare\s+", r"versus\s+", r"\bvs\b"], _handle_compare_entity, "Compare Entities"),
    ([r"path\s+between\s+", r"how\s+(?:is|are)\s+.*connected", r"connection\s+between"], _handle_path_between, "Path Between Entities"),
    ([r"subsidiaries?\s+(?:of\s+)?", r"children\s+of\s+", r"who\s+does\s+.*own"], _handle_subsidiaries, "Subsidiaries"),
    ([r"person\s+", r"PERSON_\d+"], _handle_person_info, "Person Info"),
    # Neo4j - Lists & Analytics
    ([r"top\s+\d*\s*risk", r"riskiest", r"high.?risk\s+entit", r"most\s+risky"], _handle_top_risk, "Top Risk Entities"),
    ([r"structuring", r"smurfing", r"below.*threshold", r"9.?000.*10.?000"], _handle_structuring, "Structuring Detection"),
    ([r"circular", r"\bring\b", r"\bloop\b", r"\bscc\b"], _handle_circular, "Circular Ownership"),
    ([r"shell\s+compan", r"no\s+(?:operational\s+)?address"], _handle_shells, "Shell Companies"),
    ([r"\bpeps?\b", r"politically\s+exposed"], _handle_peps, "PEP List"),
    ([r"sanctioned\s+(?:list|persons?|entities?)", r"all\s+sanctioned", r"list.*sanctioned"], _handle_sanctioned_list, "Sanctioned List"),
    ([r"pagerank", r"most\s+(?:connected|influential|central)"], _handle_pagerank, "PageRank Leaders"),
    ([r"betweenness", r"intermediar", r"conduit", r"bridge\s+entit"], _handle_betweenness, "Betweenness Leaders"),
    ([r"communit", r"cluster", r"louvain"], _handle_communities, "Community Clusters"),
    ([r"suspicious\s+transact", r"flagged\s+transact"], _handle_suspicious_txns, "Suspicious Transactions"),
    ([r"cross.?jurisdiction", r"multi.?jurisdiction", r"offshore\s+chain"], _handle_cross_jurisdiction, "Cross-Jurisdiction Chains"),
    ([r"jurisdiction", r"country\s+risk", r"entities?\s+in\s+"], _handle_jurisdiction, "Jurisdiction Analysis"),
    ([r"orphan", r"disconnected\s+node", r"isolated\s+node"], _handle_orphans, "Orphan Nodes"),
    ([r"graph\s+stat", r"node\s+count", r"how\s+(?:many|big)", r"database\s+stat"], _handle_graph_stats, "Graph Statistics"),
    ([r"gds\s+summary", r"algorithm\s+summary", r"analytics\s+summary"], _handle_gds_summary, "GDS Summary"),
    # GraphDB SPARQL
    ([r"fibo\s+class", r"ontology\s+class", r"what\s+classes"], _handle_sparql_classes, "FIBO Classes (SPARQL)"),
    ([r"ownership\s+class", r"control\s+class", r"fibo\s+ownership"], _handle_sparql_ownership_classes, "FIBO Ownership Classes"),
    ([r"(?:object\s+)?propert", r"ontology\s+propert"], _handle_sparql_properties, "Ontology Properties (SPARQL)"),
    ([r"named\s+graph", r"what\s+graphs?", r"graphdb\s+graph"], _handle_sparql_named_graphs, "Named Graphs (SPARQL)"),
    ([r"gleif\s+entit", r"lei\s+entit", r"gleif\s+data"], _handle_sparql_gleif, "GLEIF Entities (SPARQL)"),
    ([r"subclass", r"class\s+hierarch", r"legal\s*person\s+hierarch"], _handle_sparql_subclasses, "Subclass Hierarchy (SPARQL)"),
    ([r"fibo.*gleif\s+map", r"mapping\b"], _handle_sparql_mapping, "FIBO↔GLEIF Mapping (SPARQL)"),
    ([r"iso.*countr", r"country\s+code", r"iso\s*3166"], _handle_sparql_iso_countries, "ISO Countries (SPARQL)"),
    # Complex SPARQL
    ([r"how.*ownership.*model", r"fibo.*model.*ownership", r"ownership.*hierarchy.*parent"], _handle_sparql_full_entity_portrait, "FIBO Ownership Model (SPARQL)"),
    ([r"how.*gleif.*(?:typed|classified)", r"gleif.*type.*distribut", r"entity.*type.*analysis"], _handle_sparql_entity_type_analysis, "GLEIF Type Analysis (SPARQL)"),
    ([r"ontology.*complete", r"which.*classes.*instance", r"coverage.*ontology", r"how\s+complete.*knowledge", r"knowledge\s+graph.*complete"], _handle_sparql_ontology_completeness, "Ontology Coverage (SPARQL)"),
    ([r"how.*(?:fibo|ontolog).*relate", r"cross.?ontology", r"fibo.*kyc.*link"], _handle_sparql_cross_ontology, "Cross-Ontology Links (SPARQL)"),
    # Complex Neo4j — Natural language investigation queries
    ([r"investigate\s+", r"due\s+diligence", r"full\s+report\s+(?:on|for)"], _handle_investigation_report, "Due Diligence Report"),
    ([r"which.*entit.*(?:controlled|owned).*sanctioned", r"entities.*exposed.*sanction", r"sanction.*exposure.*all", r"who.*ultimately.*controlled.*sanctioned"], _handle_sanctioned_exposure_full, "Full Sanctions Exposure"),
    ([r"high.?risk.*(?:controlled|owned).*pep", r"pep.*control.*high.?risk", r"politically\s+exposed.*high.?risk"], _handle_pep_controlled_high_risk, "PEP-Controlled High Risk"),
    ([r"offshore.*shell.*(?:sanction|pep)", r"shell.*offshore.*(?:sanction|pep)", r"shell.*compan.*linked.*(?:sanction|pep)"], _handle_offshore_shells_sanctioned, "Offshore Shells + Sanctions/PEP"),
    ([r"ring.*risk.*assess", r"circular.*risk.*assess", r"how\s+dangerous.*ring", r"assess.*circular"], _handle_ring_risk_assessment, "Ring Risk Assessment"),
    ([r"hidden\s+controller", r"indirect.*control", r"who.*control.*behind.*scene", r"puppet\s+master", r"shadow.*control"], _handle_hidden_controllers, "Hidden Controllers"),
    ([r"money\s+laundering", r"aml.*red.?flag", r"multiple.*red.?flag", r"multi.*red.?flag", r"combined.*risk.*indicator"], _handle_money_laundering_risk, "Multi-Red-Flag AML"),
    ([r"contagion", r"spillover.*risk", r"neighbor.*sanctioned", r"knock.?on.*effect", r"entities.*(?:near|close).*sanctioned"], _handle_contagion_risk, "Contagion Risk"),
    ([r"largest.*(?:ownership|corporate).*tree", r"deepest.*hierarch", r"widest.*ownership", r"biggest.*corporate.*struct"], _handle_largest_ownership_trees, "Largest Ownership Trees"),
    ([r"risk\s+heatmap", r"risk.*distribut.*jurisdict.*categ", r"where.*risk.*concentrated"], _handle_risk_heatmap, "Risk Heatmap"),
    ([r"high.?risk.*communit", r"dangerous.*cluster", r"which.*communit.*risk", r"risky.*communit"], _handle_community_risk_outliers, "High-Risk Communities"),
    ([r"transaction.*network", r"most.*active.*transact", r"transaction.*corridor", r"heaviest.*transaction.*flow"], _handle_transaction_network, "Transaction Network"),
    ([r"blast\s+radius", r"impact.*if.*sanctioned", r"what.*happens.*if.*sanction", r"sanction.*impact"], _handle_sanctions_full_impact, "Sanctions Blast Radius"),
    ([r"control.*across.*(?:countr|jurisdict)", r"individual.*multi.*jurisdict", r"person.*control.*different.*countr", r"cross.?border.*control"], _handle_beneficial_owner_across_jurisdictions, "Cross-Border Controllers"),
    ([r"weakest\s+link", r"choke.?point", r"single\s+point.*failure", r"critical.*node", r"most.*critical.*entity"], _handle_weakest_link, "Network Chokepoints"),
    ([r"risk\s+arbitrage", r"same.*controller.*different.*risk", r"risk\s+spread", r"controller.*mix.*(?:high|low).*risk", r"controller.*span.*risk", r"entities.*span.*risk.*range", r"wide\s+risk\s+range"], _handle_same_controller_diff_risk, "Risk Arbitrage"),
    # Custom queries
    ([r"run\s+sparql", r"execute\s+sparql", r"```sparql"], _handle_sparql_custom, "Custom SPARQL"),
    ([r"run\s+cypher", r"execute\s+cypher", r"```cypher"], _handle_cypher_custom, "Custom Cypher"),
]


def process_chat(message: str) -> dict:
    """Main chat entry point. Returns dict with reply, db, query keys."""
    msg = message.strip()
    if not msg:
        return {"reply": "Please enter a question.", "db": "", "query": ""}

    lower = msg.lower()

    # Check for help
    if lower in ("help", "?", "commands", "what can you do", "hi", "hello"):
        return {"reply": _help_text(), "db": "", "query": ""}

    # Try pattern matching
    for patterns, handler, desc in PATTERNS:
        for pat in patterns:
            if re.search(pat, msg, re.IGNORECASE):
                try:
                    reply, db, query = handler(msg)
                    return {"reply": reply, "db": db, "query": query, "matched": desc}
                except Exception as e:
                    return {"reply": f"**Error** ({desc}): {str(e)}", "db": "", "query": ""}

    # Fallback
    return {
        "reply": "I didn't understand that. Type **help** to see available queries.\n\n"
                 "**Tip:** You can also run custom queries:\n"
                 "- `run cypher: MATCH (n:LegalEntity) RETURN n.name LIMIT 5`\n"
                 "- `run sparql: SELECT ?s WHERE { ?s a owl:Class } LIMIT 5`",
        "db": "", "query": ""
    }


def _help_text() -> str:
    return """**KYC Investigation Chat Assistant**

I can query both **Neo4j** (Cypher) and **GraphDB** (SPARQL). I'll show you the query I execute.

**Entity Investigation (Neo4j):**
- "Who owns ENTITY_0042?" — UBO chain traversal
- "Risk of ENTITY_0303" — Full risk profile
- "Sanctions check ENTITY_0042" — Sanctions exposure
- "Transactions of ENTITY_0000" — Transaction history
- "Detail ENTITY_0042" — Complete entity report
- "Compare ENTITY_0001 and ENTITY_0050" — Side-by-side
- "Path between ENTITY_0001 and ENTITY_0050" — Shortest path
- "Subsidiaries of ENTITY_0009" — Child entities
- "Person PERSON_0000" — Person details

**Analytics (Neo4j):**
- "Top 10 risk entities" — Highest risk scores
- "Circular ownership" — SCC rings
- "Shell companies" — No-address entities
- "Show PEPs" — Politically exposed persons
- "Sanctioned list" — All sanctioned persons
- "PageRank leaders" — Most influential entities
- "Betweenness leaders" — Key intermediaries
- "Communities" — Louvain clusters
- "Suspicious transactions" — Flagged transactions
- "Structuring detection" — Sub-$10k patterns
- "Jurisdiction analysis" — Country risk breakdown
- "Entities in Cayman Islands" — Filter by country
- "Cross-jurisdiction chains" — Multi-country ownership
- "Orphan nodes" — Disconnected entities
- "Graph statistics" — Node/relationship counts
- "GDS summary" — Algorithm results overview

**Ontology & Knowledge Graph (SPARQL → GraphDB):**
- "FIBO classes" — All ontology classes
- "Ownership classes" — FIBO ownership/control concepts
- "Ontology properties" — Object properties
- "Named graphs" — All GraphDB graphs
- "GLEIF entities" — LEI entities from GLEIF
- "Subclass hierarchy" — LegalPerson hierarchy
- "FIBO GLEIF mapping" — Cross-ontology mapping
- "ISO countries" — Country codes

**Custom Queries:**
- "Run cypher: MATCH (n) RETURN labels(n), count(n)"
- "Run sparql: SELECT ?s WHERE { ?s a owl:Class } LIMIT 5"
"""
