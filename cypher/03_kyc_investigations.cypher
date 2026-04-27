// === KYC investigation Cypher patterns ===
// Each query is independent — copy/paste into Neo4j Browser (http://localhost:7474).

// ─── 1. Ultimate Beneficial Owner (UBO) for one entity ───────────────────────
MATCH path = (e:LegalEntity {id: 'ENTITY_0042'})
      -[:DIRECTLY_OWNED_BY*0..6]->()
      -[:CONTROLLED_BY]->(p:NaturalPerson)
RETURN e.name AS entity, p.name AS ubo, length(path) AS hops
ORDER BY hops;

// ─── 2. Sanctions exposure across the entire portfolio ───────────────────────
MATCH (e:LegalEntity)-[:DIRECTLY_OWNED_BY*0..6]->()
      -[:CONTROLLED_BY]->(p:NaturalPerson {isSanctioned: true})
RETURN DISTINCT e.id AS entity_id, e.name AS name, p.name AS sanctioned_ubo;

// ─── 3. Circular ownership rings (using SCC results) ─────────────────────────
MATCH (e:LegalEntity) WHERE e.sccComponentId IS NOT NULL
WITH e.sccComponentId AS scc, collect(e) AS members
WHERE size(members) > 1
RETURN scc, [m IN members | m.id + ' (' + m.name + ')'] AS ring
ORDER BY size(members) DESC;

// ─── 4. Shell-company red flags ──────────────────────────────────────────────
MATCH (e:LegalEntity)
WHERE e.riskTier = 'high'
  AND e.hasOperationalAddress = false
  AND NOT (e)<-[:CONTROLLED_BY]-()       // no employees / directors of record
OPTIONAL MATCH (e)-[t:TRANSACTION]->()
WITH e, count(t) AS outgoing_txns
WHERE outgoing_txns < 3
RETURN e.id, e.name, e.jurisdiction, outgoing_txns;

// ─── 5. Structuring detection (transactions just under $10k) ─────────────────
MATCH (a:LegalEntity)-[t:TRANSACTION]->(b:LegalEntity)
WHERE t.amount > 9000 AND t.amount < 10000
WITH a, b, count(t) AS suspicious_count, sum(t.amount) AS total
WHERE suspicious_count >= 3
RETURN a.name AS sender, b.name AS receiver,
       suspicious_count, round(total) AS total_amount
ORDER BY suspicious_count DESC;

// ─── 6. PEP exposure ─────────────────────────────────────────────────────────
MATCH (e:LegalEntity)-[:CONTROLLED_BY]->(p:PoliticallyExposedPerson)
RETURN e.id, e.name, p.name AS pep, p.nationality;

// ─── 7. Top-risk entities (composite score from 08_gds_analysis.py) ──────────
MATCH (e:LegalEntity) WHERE e.kycRiskScore > 0
RETURN e.id, e.name, e.jurisdiction, e.kycRiskScore AS score
ORDER BY score DESC LIMIT 25;

// ─── 8. Same-community correlation (find adjacent risky entities) ────────────
MATCH (high:LegalEntity {id: 'ENTITY_0123'})
MATCH (peer:LegalEntity)
WHERE peer.louvainCommunityId = high.louvainCommunityId
  AND peer.id <> high.id
  AND peer.kycRiskScore >= 30
RETURN peer.id, peer.name, peer.kycRiskScore;
