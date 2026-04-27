# KYC Intelligence — Chat Q&A Catalog

> Comprehensive catalog of questions for the KYC Investigation Chat Assistant.
> Each question has been tested against the live system. The chat supports both
> **Neo4j (Cypher)** and **GraphDB (SPARQL)** and shows the executed query.

---

## 🟢 Simple — Entity Lookup & Basic Queries

### Q1. Entity Detail
**Question:** `Detail ENTITY_0042`
**Database:** Neo4j (Cypher)
**Category:** Entity Investigation
```cypher
MATCH (e:LegalEntity {id: $id})
OPTIONAL MATCH (e)-[r:DIRECTLY_OWNED_BY]->(parent:LegalEntity)
OPTIONAL MATCH (child:LegalEntity)-[r2:DIRECTLY_OWNED_BY]->(e)
OPTIONAL MATCH (e)-[r3:CONTROLLED_BY]->(ctrl:NaturalPerson)
OPTIONAL MATCH (e)-[t:TRANSACTION]-(other:LegalEntity)
WITH e, collect(DISTINCT {name: parent.name, id: parent.id, pct: r.percentage}) AS parents,
     collect(DISTINCT {name: child.name, id: child.id, pct: r2.percentage}) AS children,
     collect(DISTINCT {name: ctrl.name, id: ctrl.id, isPEP: ctrl.isPEP, isSanctioned: ctrl.isSanctioned, role: r3.role}) AS controllers,
     count(DISTINCT t) AS txnCount
RETURN e {.*} AS entity, parents, children, controllers, txnCount
```
**Expected Answer:** Full profile of Davids e.G. — LEI, jurisdiction (US), category (CORP), risk tier (low), KYC score, PageRank, betweenness, community, parents, children, controllers, transaction count.

---

### Q2. Entity Risk Profile
**Question:** `Risk of ENTITY_0303`
**Database:** Neo4j (Cypher)
**Category:** Entity Investigation
```cypher
MATCH (e:LegalEntity {id: $id})
OPTIONAL MATCH (e)-[:CONTROLLED_BY]->(ctrl:NaturalPerson)
RETURN e.name AS name, e.jurisdiction AS jurisdiction,
       e.jurisdictionName AS jname, e.riskTier AS tier,
       e.kycRiskScore AS score, e.category AS category,
       e.pageRankScore AS pageRank, e.betweennessScore AS betweenness,
       e.louvainCommunityId AS community, e.sccComponentId AS scc,
       e.hasOperationalAddress AS hasAddr, e.isActive AS active,
       collect(DISTINCT {name: ctrl.name, isPEP: ctrl.isPEP, isSanctioned: ctrl.isSanctioned}) AS controllers
```
**Expected Answer:** Carsten Vogt GmbH & Co. KG — Seychelles (SC), TRUST, high risk tier, KYC score 75/100, PageRank 0.2138, SCC Ring #303.

---

### Q3. Person Lookup
**Question:** `Person PERSON_0000`
**Database:** Neo4j (Cypher)
**Category:** Entity Investigation
```cypher
MATCH (p:NaturalPerson {id: $id})
OPTIONAL MATCH (e:LegalEntity)-[:CONTROLLED_BY]->(p)
RETURN p.id AS id, p.name AS name, p.nationality AS nationality,
       toString(p.dob) AS dob, p.isPEP AS isPEP, p.isSanctioned AS isSanctioned,
       p.pageRankScore AS pageRank,
       collect(DISTINCT {id: e.id, name: e.name, jurisdiction: e.jurisdiction}) AS entities
```
**Expected Answer:** June Khan — German (DE), PEP + SANCTIONED, controls 4 entities (Jackson-Allen, Wohlgemut, Campbell-Walker, Osborn Group).

---

### Q4. List All PEPs
**Question:** `Show PEPs`
**Database:** Neo4j (Cypher)
**Category:** Analytics
```cypher
MATCH (p:PoliticallyExposedPerson)
OPTIONAL MATCH (e:LegalEntity)-[:CONTROLLED_BY]->(p)
RETURN p.id AS id, p.name AS name, p.nationality AS nationality,
       collect(DISTINCT e.name) AS controlledEntities
ORDER BY p.name
```
**Expected Answer:** 10 PEPs including Conor Clark (SC), Dipl.-Ing. Marjan Noack MBA. (JP), Ing. Tadeusz Beckmann MBA. (GB), June Khan (DE), Kerstin Wernecke (GB), Patricia Armstrong (US), Raymond Scott (GB), Sara Barber (JP), Slavica Tröst (DE), Xavier Rowe (SG).

---

### Q5. List All Sanctioned Persons
**Question:** `Sanctioned list`
**Database:** Neo4j (Cypher)
**Category:** Analytics
```cypher
MATCH (p:SanctionedEntity)
OPTIONAL MATCH (e:LegalEntity)-[:CONTROLLED_BY]->(p)
RETURN p.id AS id, p.name AS name, p.nationality AS nationality,
       collect(DISTINCT {name: e.name, id: e.id}) AS controlledEntities
ORDER BY p.name
```
**Expected Answer:** 3 sanctioned persons: Dipl.-Ing. Marjan Noack MBA. (JP), June Khan (DE, controls 4 entities), Patricia Armstrong (US).

---

### Q6. Graph Statistics
**Question:** `Graph statistics`
**Database:** Neo4j (Cypher)
**Category:** Database
```cypher
MATCH (n) WITH labels(n) AS lbls UNWIND lbls AS label
WITH label WHERE NOT label STARTS WITH '_'
RETURN label, count(*) AS count ORDER BY count DESC

MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS count ORDER BY count DESC
```
**Expected Answer:** Node counts by label (LegalEntity, NaturalPerson, etc.) and relationship counts by type (DIRECTLY_OWNED_BY, CONTROLLED_BY, TRANSACTION, etc.).

---

### Q7. Subsidiaries of an Entity
**Question:** `Subsidiaries of ENTITY_0009`
**Database:** Neo4j (Cypher)
**Category:** Entity Investigation
```cypher
MATCH (child:LegalEntity)-[r:DIRECTLY_OWNED_BY]->(e:LegalEntity {id: $id})
RETURN child.id AS id, child.name AS name, child.jurisdiction AS jurisdiction,
       r.percentage AS ownership_pct, child.kycRiskScore AS score
ORDER BY r.percentage DESC
```
**Expected Answer:** List of child entities owned by ENTITY_0009 (James Group) with ownership percentages and risk scores.

---

### Q8. Entities by Jurisdiction
**Question:** `Entities in Cayman Islands`
**Database:** Neo4j (Cypher)
**Category:** Analytics
```cypher
MATCH (e:LegalEntity {jurisdiction: $jur})
RETURN e.id AS id, e.name AS name, e.category AS category,
       e.riskTier AS tier, e.kycRiskScore AS score
ORDER BY score DESC LIMIT 20
```
**Expected Answer:** Up to 20 entities registered in KY (Cayman Islands), ordered by risk score.

---

### Q9. Entity Transactions
**Question:** `Transactions of ENTITY_0000`
**Database:** Neo4j (Cypher)
**Category:** Entity Investigation
```cypher
MATCH (e:LegalEntity {id: $id})-[t:TRANSACTION]-(other:LegalEntity)
RETURN CASE WHEN startNode(t) = e THEN 'OUT' ELSE 'IN' END AS direction,
       other.id AS counterpartyId, other.name AS counterparty,
       t.amount AS amount, t.currency AS currency,
       toString(t.date) AS date, t.isSuspicious AS suspicious
ORDER BY t.date DESC LIMIT 20
```
**Expected Answer:** Up to 20 most recent transactions involving ENTITY_0000 with direction, counterparty, amount, currency, date, and suspicious flag.

---

## 🟡 Medium — Analytics & Investigation

### Q10. Top Risk Entities
**Question:** `Top 10 risk entities`
**Database:** Neo4j (Cypher)
**Category:** Risk Analytics
```cypher
MATCH (e:LegalEntity) WHERE e.kycRiskScore > 0
RETURN e.id AS id, e.name AS name, e.jurisdiction AS jurisdiction,
       e.riskTier AS tier, e.kycRiskScore AS score, e.category AS category
ORDER BY score DESC LIMIT 10
```
**Expected Answer:** Top 10 entities by KYC risk score. Highest scores are 75 (Carsten Vogt SC, Schleich AG VG).

---

### Q11. Circular Ownership Rings
**Question:** `Circular ownership`
**Database:** Neo4j (Cypher)
**Category:** AML Detection
```cypher
MATCH (e:LegalEntity) WHERE e.sccComponentId IS NOT NULL
WITH e.sccComponentId AS scc, collect(e) AS members
WHERE size(members) > 1
RETURN scc,
       [m IN members | m.id + ' (' + m.name + ')'] AS entities,
       size(members) AS size
ORDER BY size DESC LIMIT 20
```
**Expected Answer:** 5 circular ownership rings detected, each with 3 entities. Ring #402: Eimer ↔ Sheppard Inc ↔ Lopez PLC, etc.

---

### Q12. Shell Companies
**Question:** `Shell companies`
**Database:** Neo4j (Cypher)
**Category:** AML Detection
```cypher
MATCH (e:LegalEntity) WHERE e.hasOperationalAddress = false
OPTIONAL MATCH (child:LegalEntity)-[:DIRECTLY_OWNED_BY]->(e)
WITH e, count(child) AS subsidiaries
RETURN e.id AS id, e.name AS name, e.jurisdiction AS jurisdiction,
       e.kycRiskScore AS score, e.riskTier AS tier,
       e.category AS category, subsidiaries
ORDER BY score DESC LIMIT 20
```
**Expected Answer:** Entities without operational addresses, ordered by risk. These are potential shell company indicators.

---

### Q13. Suspicious Transactions
**Question:** `Suspicious transactions`
**Database:** Neo4j (Cypher)
**Category:** AML Detection
```cypher
MATCH (a:LegalEntity)-[t:TRANSACTION]->(b:LegalEntity)
WHERE t.isSuspicious = true
RETURN a.id AS fromId, a.name AS fromName,
       b.id AS toId, b.name AS toName,
       t.amount AS amount, t.currency AS currency,
       toString(t.date) AS date
ORDER BY t.amount DESC LIMIT 20
```
**Expected Answer:** Top 20 flagged suspicious transactions with sender, receiver, amount, and date.

---

### Q14. Structuring Detection
**Question:** `Structuring detection`
**Database:** Neo4j (Cypher)
**Category:** AML Detection
```cypher
MATCH (a:LegalEntity)-[t:TRANSACTION]->(b:LegalEntity)
WHERE t.amount > 9000 AND t.amount < 10000
WITH a, b, count(t) AS txns,
     collect({amount: t.amount, date: toString(t.date)}) AS details
WHERE txns >= 2
RETURN a.name AS from, b.name AS to, txns,
       [d IN details | d.amount] AS amounts
ORDER BY txns DESC LIMIT 15
```
**Expected Answer:** Entity pairs with multiple transactions just below the $10,000 reporting threshold (structuring/smurfing indicator). Returns "No structuring patterns detected" if no matches.

---

### Q15. Jurisdiction Risk Summary
**Question:** `Jurisdiction analysis`
**Database:** Neo4j (Cypher)
**Category:** Risk Analytics
```cypher
MATCH (e:LegalEntity)
WITH e.jurisdiction AS jurisdiction, e.jurisdictionName AS name,
     count(e) AS count,
     toInteger(avg(e.kycRiskScore)) AS avgScore,
     size(collect(CASE WHEN e.kycRiskScore >= 70 THEN 1 END)) AS highRisk
RETURN jurisdiction, name, count, avgScore, highRisk
ORDER BY avgScore DESC
```
**Expected Answer:** All jurisdictions ranked by average risk score, with entity counts and high-risk counts.

---

### Q16. PageRank Leaders
**Question:** `PageRank leaders`
**Database:** Neo4j (Cypher)
**Category:** GDS Analytics
```cypher
MATCH (e:LegalEntity)
RETURN e.id AS id, e.name AS name, e.jurisdiction AS jurisdiction,
       e.pageRankScore AS pageRank, e.kycRiskScore AS score
ORDER BY e.pageRankScore DESC LIMIT 10
```
**Expected Answer:** Top 10 entities by PageRank (most connected/influential). Highest: Barth Stiftung & Co. KGaA (0.9823).

---

### Q17. Betweenness Leaders
**Question:** `Betweenness leaders`
**Database:** Neo4j (Cypher)
**Category:** GDS Analytics
```cypher
MATCH (e:LegalEntity)
RETURN e.id AS id, e.name AS name, e.jurisdiction AS jurisdiction,
       e.betweennessScore AS betweenness, e.kycRiskScore AS score
ORDER BY e.betweennessScore DESC LIMIT 10
```
**Expected Answer:** Top 10 entities by betweenness centrality (key intermediaries/conduits). Max betweenness: 25.0.

---

### Q18. Louvain Community Clusters
**Question:** `Communities`
**Database:** Neo4j (Cypher)
**Category:** GDS Analytics
```cypher
MATCH (e:LegalEntity) WHERE e.louvainCommunityId IS NOT NULL
WITH e.louvainCommunityId AS comm, collect(e) AS members
WHERE size(members) > 2
RETURN comm AS community, size(members) AS size,
       [m IN members | m.name][..8] AS topMembers,
       toInteger(reduce(s = 0.0, m IN members | s + m.kycRiskScore) / size(members)) AS avgRisk
ORDER BY size DESC LIMIT 15
```
**Expected Answer:** 15 Louvain communities (size > 2), largest has 17 entities. Shows top members and average risk per community.

---

### Q19. GDS Algorithm Summary
**Question:** `GDS summary`
**Database:** Neo4j (Cypher)
**Category:** GDS Analytics
```cypher
-- Query 1: PageRank & Betweenness stats
MATCH (e:LegalEntity)
RETURN round(avg(e.pageRankScore) * 10000) / 10000 AS avgPageRank,
       round(max(e.pageRankScore) * 10000) / 10000 AS maxPageRank,
       round(avg(e.betweennessScore) * 100) / 100 AS avgBetweenness,
       round(max(e.betweennessScore) * 100) / 100 AS maxBetweenness

-- Query 2: Louvain community stats
MATCH (e:LegalEntity) WHERE e.louvainCommunityId IS NOT NULL
WITH e.louvainCommunityId AS comm, count(e) AS sz
RETURN count(comm) AS communities, max(sz) AS largestCommunity

-- Query 3: SCC ring stats
MATCH (e:LegalEntity) WHERE e.sccComponentId IS NOT NULL
WITH e.sccComponentId AS comp, count(e) AS sz WHERE sz > 1
RETURN count(comp) AS rings, sum(sz) AS entitiesInRings
```
**Expected Answer:** PageRank avg 0.2219 / max 0.9823, Betweenness avg 1.17 / max 25.0, 276 Louvain communities (largest 17), 5 SCC rings (15 entities).

---

### Q20. UBO Chain Traversal
**Question:** `Who owns ENTITY_0042?`
**Database:** Neo4j (Cypher)
**Category:** Ownership Investigation
```cypher
MATCH path = (e:LegalEntity {id: $id})
      -[:DIRECTLY_OWNED_BY*0..6]->()
      -[:CONTROLLED_BY]->(p:NaturalPerson)
RETURN p.name AS ubo, p.nationality AS nationality,
       p.isPEP AS isPEP, p.isSanctioned AS isSanctioned,
       length(path) AS hops,
       [n IN nodes(path) | coalesce(n.name, n.id)] AS chain
ORDER BY hops LIMIT 10
```
**Expected Answer:** 2 UBOs — Brandon Archer (SG, 1 hop: Davids e.G. → Brandon Archer), Connor Murphy (US, 2 hops: Davids e.G. → Smith PLC → Connor Murphy).

---

### Q21. Sanctions Exposure Check
**Question:** `Sanctions check ENTITY_0042`
**Database:** Neo4j (Cypher)
**Category:** Compliance
```cypher
MATCH (e:LegalEntity {id: $id})
OPTIONAL MATCH path = (e)-[:DIRECTLY_OWNED_BY*0..6]->()
      -[:CONTROLLED_BY]->(p:NaturalPerson {isSanctioned: true})
RETURN e.name AS entity, p.name AS sanctioned, p.nationality AS nat,
       length(path) AS hops
LIMIT 5
```
**Expected Answer:** ✅ No sanctioned persons found in the ownership chain of ENTITY_0042.

---

### Q22. Compare Two Entities
**Question:** `Compare ENTITY_0001 and ENTITY_0050`
**Database:** Neo4j (Cypher)
**Category:** Entity Investigation
```cypher
MATCH (e:LegalEntity) WHERE e.id IN [$id1, $id2]
RETURN e.id AS id, e.name AS name, e.jurisdiction AS jurisdiction,
       e.category AS category, e.riskTier AS tier,
       e.kycRiskScore AS score, e.pageRankScore AS pageRank,
       e.betweennessScore AS betweenness, e.louvainCommunityId AS community,
       e.hasOperationalAddress AS hasAddr
```
**Expected Answer:** Side-by-side comparison table of both entities with all key attributes.

---

## 🔴 Complex — Advanced Multi-Hop & Cross-Database

### Q23. Cross-Jurisdiction Chains
**Question:** `Cross-jurisdiction chains`
**Database:** Neo4j (Cypher)
**Category:** AML Detection
```cypher
MATCH path = (a:LegalEntity)-[:DIRECTLY_OWNED_BY*2..4]->(b:LegalEntity)
WHERE a.jurisdiction <> b.jurisdiction
WITH a, b, length(path) AS hops,
     [n IN nodes(path) | n.jurisdiction] AS jurisdictions
WHERE size(apoc.coll.toSet(jurisdictions)) >= 3
RETURN a.id AS fromId, a.name AS fromName, a.jurisdiction AS fromJur,
       b.id AS toId, b.name AS toName, b.jurisdiction AS toJur,
       hops, jurisdictions
LIMIT 15
```
**Expected Answer:** Multi-jurisdiction ownership chains spanning 3+ countries. E.g., Baker-Bowers (CH) → ... → Smith PLC (JP) via CH, US, JP.

---

### Q24. Shortest Path Between Entities
**Question:** `Path between ENTITY_0001 and ENTITY_0050`
**Database:** Neo4j (Cypher)
**Category:** Graph Traversal
```cypher
MATCH path = shortestPath(
  (a:LegalEntity {id: $id1})-[:DIRECTLY_OWNED_BY|CONTROLLED_BY*..10]-(b:LegalEntity {id: $id2})
)
RETURN [n IN nodes(path) | coalesce(n.name, n.id)] AS chain,
       [r IN relationships(path) | type(r)] AS relTypes,
       length(path) AS hops
```
**Expected Answer:** Shortest path with relationship types between the two entities (or "No path found" if disconnected).

---

### Q25. Orphan Nodes
**Question:** `Orphan nodes`
**Database:** Neo4j (Cypher)
**Category:** Data Quality
```cypher
MATCH (n) WHERE NOT (n)--()
WITH labels(n) AS lbls, count(n) AS cnt
WHERE NOT any(l IN lbls WHERE l STARTS WITH '_' OR l STARTS WITH 'n4sch__')
RETURN lbls AS labels, cnt AS count ORDER BY cnt DESC
```
**Expected Answer:** Count of disconnected nodes by label type (nodes with no relationships).

---

## 🔷 SPARQL — GraphDB Ontology & Knowledge Graph

### Q26. FIBO Ontology Classes
**Question:** `FIBO classes`
**Database:** GraphDB (SPARQL)
**Category:** Ontology Exploration
```sparql
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?class ?label WHERE {
  ?class a owl:Class .
  OPTIONAL { ?class rdfs:label ?label }
  FILTER(CONTAINS(STR(?class), "edmcouncil.org") || CONTAINS(STR(?class), "omg.org"))
} ORDER BY ?label LIMIT 40
```
**Expected Answer:** 40 FIBO ontology classes from EDMC and OMG including Agent, LegalEntity, LegalPerson, Corporation, StockCorporation, Affiliate, Subsidiary, etc.

---

### Q27. FIBO Ownership & Control Classes
**Question:** `Ownership classes`
**Database:** GraphDB (SPARQL)
**Category:** Ontology Exploration
```sparql
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?class ?label WHERE {
  ?class a owl:Class .
  ?class rdfs:label ?label .
  FILTER(CONTAINS(STR(?class), "Ownership") || CONTAINS(STR(?class), "Control") || CONTAINS(STR(?class), "Owner"))
} ORDER BY ?label
```
**Expected Answer:** 43 FIBO classes related to ownership and control: BeneficialOwner, Shareholder, Subsidiary, GlobalUltimateParent, ControlledParty, EntityOwner, Investor, JointVenturePartner, VotingShareholder, etc.

---

### Q28. Ontology Object Properties
**Question:** `Ontology properties`
**Database:** GraphDB (SPARQL)
**Category:** Ontology Exploration
```sparql
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?prop ?label ?domain ?range WHERE {
  ?prop a owl:ObjectProperty .
  OPTIONAL { ?prop rdfs:label ?label }
  OPTIONAL { ?prop rdfs:domain ?domain }
  OPTIONAL { ?prop rdfs:range ?range }
} ORDER BY ?label LIMIT 30
```
**Expected Answer:** Object properties with domain/range including hasOwnership, isControlledBy, isOwnedBy, etc.

---

### Q29. GraphDB Named Graphs
**Question:** `Named graphs`
**Database:** GraphDB (SPARQL)
**Category:** Database
```sparql
SELECT ?graph (COUNT(*) AS ?triples)
WHERE { GRAPH ?graph { ?s ?p ?o } }
GROUP BY ?graph ORDER BY DESC(?triples)
```
**Expected Answer:** 14 named graphs. Largest: lcc/iso3166 (8725), glei/instances (4200), fibo/fbc/fse (901), lcc/countries (385), fibo/be/control (376), fibo/be/ownership (350).

---

### Q30. GLEIF Entities
**Question:** `GLEIF entities`
**Database:** GraphDB (SPARQL)
**Category:** Data Exploration
```sparql
PREFIX kyc: <http://kyc-kg.example.org/ontology#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?entity ?name ?jurisdiction WHERE {
  GRAPH <http://kg/glei/instances> {
    ?entity a kyc:RegisteredLegalEntity .
    ?entity rdfs:label ?name .
    OPTIONAL { ?entity kyc:hasJurisdiction ?jurisdiction }
  }
} LIMIT 20
```
**Expected Answer:** Up to 20 GLEIF LEI entities with their names and jurisdictions from the GraphDB knowledge graph.

---

### Q31. LegalPerson Subclass Hierarchy
**Question:** `Subclass hierarchy`
**Database:** GraphDB (SPARQL)
**Category:** Ontology Exploration
```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX fibo: <https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/>
SELECT ?class ?label WHERE {
  ?class rdfs:subClassOf* fibo:LegalPerson .
  OPTIONAL { ?class rdfs:label ?label }
} ORDER BY ?label LIMIT 30
```
**Expected Answer:** Subclass hierarchy under LegalPerson including Corporation, StockCorporation, PrivatelyHeldCompany, etc.

---

### Q32. FIBO↔GLEIF Mapping
**Question:** `FIBO GLEIF mapping`
**Database:** GraphDB (SPARQL)
**Category:** Ontology Exploration
```sparql
SELECT ?s ?p ?o WHERE {
  GRAPH <http://kg/mapping/fibo2glei> { ?s ?p ?o }
}
```
**Expected Answer:** 6 triples mapping FIBO ontology concepts to GLEIF data model (e.g., owl:equivalentClass, rdfs:subClassOf between FIBO and KYC ontology classes).

---

### Q33. ISO Country Codes
**Question:** `ISO countries`
**Database:** GraphDB (SPARQL)
**Category:** Data Exploration
```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?country ?label WHERE {
  GRAPH <http://kg/lcc/iso3166> {
    ?country a ?type .
    ?country rdfs:label ?label .
    FILTER(LANG(?label) = "en" || LANG(?label) = "")
  }
} ORDER BY ?label LIMIT 25
```
**Expected Answer:** 25 ISO 3166 country entries with English labels from the LCC ontology.

---

## � Complex — Advanced Natural Language Investigations

### Q34. Full Sanctions Exposure Analysis
**Question:** `Which entities are ultimately controlled by sanctioned individuals?`
**Database:** Neo4j (Cypher)
**Category:** Sanctions Investigation
**Expected Answer:** List of entities linked through multi-hop ownership chains to sanctioned persons, showing entity details, sanctioned person, nationality, hop count, and full ownership chain.

---

### Q35. PEP-Controlled High-Risk Entities
**Question:** `Which high-risk entities are controlled by politically exposed persons?`
**Database:** Neo4j (Cypher)
**Category:** PEP Investigation
**Expected Answer:** Entities with KYC score ≥50 directly controlled by PEPs, showing entity profile, PEP name, nationality, and parent entity.

---

### Q36. Offshore Shell Companies Linked to Sanctioned/PEP
**Question:** `Find shell companies in offshore jurisdictions linked to sanctioned or PEP persons`
**Database:** Neo4j (Cypher)
**Category:** Shell Company Investigation
**Expected Answer:** Shell entities (no operational address) in offshore jurisdictions (KY, VG, PA, SC, etc.) with multi-hop links to sanctioned or PEP controllers.

---

### Q37. Circular Ownership Ring Risk Assessment
**Question:** `Assess the total risk exposure of all circular ownership rings`
**Database:** Neo4j (Cypher)
**Category:** Ownership Ring Analysis
**Expected Answer:** 5 SCC rings with size, member entities, jurisdictions, avg/max risk scores, and flagged sanctioned/PEP controllers.

---

### Q38. Hidden Controllers (Shadow Ownership)
**Question:** `Who controls the most entities behind the scenes through multiple layers?`
**Database:** Neo4j (Cypher)
**Category:** Hidden Ownership Investigation
**Expected Answer:** Persons with significant indirect control (3+ ownership hops) over multiple entities, showing direct vs indirect reach and jurisdictions.

---

### Q39. Multi-Red-Flag AML Detection
**Question:** `Which entities combine multiple money laundering red flags?`
**Database:** Neo4j (Cypher)
**Category:** AML Investigation
**Expected Answer:** Entities with 2+ simultaneous red flags: high risk score, suspicious transactions, shell status, circular ownership, offshore jurisdiction, sanctioned/PEP links.

---

### Q40. Sanctions Contagion Risk
**Question:** `Which entities are at risk because they are close to sanctioned entities in the ownership network?`
**Database:** Neo4j (Cypher)
**Category:** Contagion Analysis
**Expected Answer:** Entities within 2 hops of sanctioned-linked companies, showing their exposure paths and risk scores.

---

### Q41. Largest Corporate Ownership Trees
**Question:** `What are the deepest and widest corporate ownership hierarchies in the network?`
**Database:** Neo4j (Cypher)
**Category:** Ownership Structure
**Expected Answer:** Top 15 root entities by number of descendants, showing tree depth, sample leaf entities, and risk scores.

---

### Q42. Risk Heatmap by Jurisdiction & Category
**Question:** `Where is risk concentrated across jurisdictions and entity categories?`
**Database:** Neo4j (Cypher)
**Category:** Risk Distribution
**Expected Answer:** Table showing jurisdiction × category breakdown with entity counts, avg/max risk, critical entities, ring membership, and shell counts.

---

### Q43. High-Risk Community Clusters
**Question:** `Which Louvain communities have dangerously high average risk scores?`
**Database:** Neo4j (Cypher)
**Category:** Community Analysis
**Expected Answer:** Communities with avg risk ≥25, showing size, risk stats, jurisdictions, and flagged sanctioned/PEP persons within.

---

### Q44. Most Active Transaction Corridors
**Question:** `Which entity pairs form the most active transaction networks?`
**Database:** Neo4j (Cypher)
**Category:** Transaction Analysis
**Expected Answer:** Top 15 entity pairs by repeat transaction count, with total volume and suspicious transaction count.

---

### Q45. Sanctions Blast Radius
**Question:** `What is the blast radius if ENTITY_0385 is sanctioned?`
**Database:** Neo4j (Cypher)
**Category:** Impact Analysis
**Expected Answer:** Downstream subsidiaries, upstream parents, sibling entities (same controllers), and total impact count for the target entity.

---

### Q46. Cross-Border Controllers
**Question:** `Find individuals who control entities across 3 or more different countries`
**Database:** Neo4j (Cypher)
**Category:** Cross-Jurisdiction Investigation
**Expected Answer:** Persons controlling entities in 3+ jurisdictions, showing entity counts, jurisdiction spread, avg risk, and PEP/sanctions flags.

---

### Q47. Network Chokepoints (Weakest Links)
**Question:** `Which entities are the weakest links — if removed, the most paths break?`
**Database:** Neo4j (Cypher)
**Category:** Network Analysis
**Expected Answer:** Top 10 entities by betweenness centrality showing connection counts, PageRank, and controllers.

---

### Q48. Risk Arbitrage Detection
**Question:** `Find controllers whose entities span wide risk ranges — possible risk arbitrage`
**Database:** Neo4j (Cypher)
**Category:** Risk Pattern Detection
**Expected Answer:** Persons controlling entities with ≥30-point risk spread, suggesting potential use of low-risk entities to mask high-risk activities.

---

### Q49. FIBO Ownership Ontology Model
**Question:** `How does the FIBO ontology model ownership and control relationships?`
**Database:** GraphDB (SPARQL)
**Category:** Ontology Investigation
**Expected Answer:** FIBO classes related to ownership, control, shareholders, subsidiaries, and investor relationships with their parent class hierarchy.

---

### Q50. GLEIF Entity Type Distribution
**Question:** `How are GLEIF entities typed and classified across FIBO and KYC ontologies?`
**Database:** GraphDB (SPARQL)
**Category:** Ontology Investigation
**Expected Answer:** Distribution of RDF types across GLEIF entity instances, showing how entities are classified in different ontology layers.

---

### Q51. Ontology Coverage Analysis
**Question:** `How complete is our knowledge graph? Which FIBO classes actually have instances?`
**Database:** GraphDB (SPARQL)
**Category:** Ontology Investigation
**Expected Answer:** Class-by-class instance counts showing which parts of the FIBO/KYC ontology are populated vs empty.

---

### Q52. Cross-Ontology Relationship Map
**Question:** `How do the FIBO, GLEIF, and KYC ontologies relate to each other?`
**Database:** GraphDB (SPARQL)
**Category:** Ontology Investigation
**Expected Answer:** Cross-ontology links showing equivalentClass and subClassOf relationships bridging FIBO, GLEIF, and KYC schemas.

---

### Q53. Full Due Diligence Investigation Report
**Question:** `Investigate ENTITY_0303`
**Database:** Neo4j (Cypher — multi-query)
**Category:** Due Diligence
**Expected Answer:** Comprehensive 6-section report: entity profile, risk assessment (score, PageRank, betweenness, community, SCC), ownership structure (parents, children, controllers), UBO chain, transaction profile, and red flags summary.

---

## 🛠️ Custom Queries

### Q54. Run Custom Cypher
**Question:** `Run cypher: MATCH (e:LegalEntity) RETURN e.jurisdiction AS jur, count(e) AS cnt ORDER BY cnt DESC LIMIT 5`
**Database:** Neo4j (Cypher)
**Category:** Custom
**Expected Answer:** Top 5 jurisdictions by entity count.

---

### Q55. Run Custom SPARQL
**Question:** `Run sparql: SELECT ?s WHERE { ?s a <http://www.w3.org/2002/07/owl#Class> } LIMIT 5`
**Database:** GraphDB (SPARQL)
**Category:** Custom
**Expected Answer:** 5 OWL classes from GraphDB.

---

## Test Summary

| Category | Count | Pass Rate |
|----------|-------|-----------|
| Simple (Entity Lookup) | 9 | 9/9 ✅ |
| Medium (Analytics & Investigation) | 13 | 13/13 ✅ |
| Complex (Neo4j — Advanced NL) | 16 | pending |
| Complex (SPARQL — Advanced NL) | 4 | pending |
| SPARQL (GraphDB — Basic) | 8 | 8/8 ✅ |
| Due Diligence Report | 1 | pending |
| Custom | 2 | (manual) |
| **Total** | **55** | **33/33 ✅ + 21 pending** |

33 original automated tests pass. 20 complex + 1 due diligence queries added.

---

## Capabilities Covered

### Neo4j (Cypher) — 41 queries
- **Entity Investigation:** detail, risk profile, transactions, subsidiaries, person lookup
- **Ownership Analysis:** UBO chain traversal (6 hops), sanctions exposure check, largest ownership trees
- **AML Detection:** circular ownership (SCC), shell companies, structuring, suspicious transactions, multi-red-flag detection
- **Risk Analytics:** top risk entities, jurisdiction risk, cross-jurisdiction chains, risk heatmap, risk arbitrage
- **GDS Analytics:** PageRank, betweenness centrality, Louvain communities, GDS summary
- **Graph Analysis:** shortest path, entity comparison, orphan nodes, graph statistics, network chokepoints
- **Complex Investigations:** full sanctions exposure, PEP-controlled high-risk, offshore shell + sanctions/PEP links, ring risk assessment, hidden controllers, contagion risk, community risk outliers, transaction networks, blast radius analysis, cross-border controllers, due diligence reports

### GraphDB (SPARQL) — 12 queries
- **Ontology Exploration:** FIBO classes, ownership/control classes, object properties, subclass hierarchy
- **Data Exploration:** named graphs, GLEIF entities, ISO countries
- **Mapping:** FIBO↔GLEIF cross-ontology mapping
- **Advanced Ontology:** FIBO ownership model, GLEIF entity type analysis, ontology coverage, cross-ontology relationship mapping

### Additional Features
- **Query Transparency:** Every response shows the exact Cypher/SPARQL query executed
- **Database Indicator:** Responses tagged with `cypher` or `sparql` to show which database was queried
- **Custom Queries:** Users can run arbitrary Cypher or SPARQL (read-only, write operations blocked)
- **Suggested Questions:** Sidebar with categorized quick-action buttons
