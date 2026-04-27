---
name: cypher-kyc-queries
description: "Use when writing Neo4j Cypher queries for KYC scenarios — UBO discovery via ownership chain traversal, sanctions proximity within N hops, circular ownership detection, shell company indicators, transaction structuring patterns, PEP exposure, address sharing, jurisdiction risk. Covers variable-length path patterns, EXISTS subqueries, COLLECT aggregation, and indexes/constraints to make queries fast."
---

# Cypher for KYC Queries

## When to use

User asks any KYC investigation question: "find UBO", "who is connected to sanctioned X", "detect rings", "shell companies", "structuring transactions", "PEP exposure".

## Schema reference (what the queries assume)

```
Nodes:
  (:LegalEntity {id, lei, name, jurisdiction, riskTier, isActive, hasOperationalAddress, kycRiskScore, pageRankScore, betweennessScore})
  (:NaturalPerson {id, name, nationality, dob, isPEP, isSanctioned})
  (:SanctionedEntity)              -- additional label on sanctioned NaturalPerson
  (:PoliticallyExposedPerson)      -- additional label on PEPs
  (:Address {city, country, postalCode})

Relationships:
  -[:DIRECTLY_OWNED_BY {percentage, since}]->
  -[:CONTROLLED_BY {role, since}]->
  -[:TRANSACTION {amount, currency, date, isSuspicious}]->
  -[:HAS_ADDRESS]->
```

## Pattern library (8 essential queries)

### 1. UBO discovery — owner with no further owner

```cypher
MATCH (e:LegalEntity {lei: $lei})
MATCH path = (e)-[:DIRECTLY_OWNED_BY|CONTROLLED_BY*1..10]->(ubo)
WHERE NOT (ubo)-[:DIRECTLY_OWNED_BY|CONTROLLED_BY]->()
RETURN ubo.name AS ubo,
       labels(ubo) AS type,
       length(path) AS hops,
       [n IN nodes(path) | coalesce(n.name, n.id)] AS chain
ORDER BY hops ASC LIMIT 5
```

### 2. Sanctions proximity (N-hop)

```cypher
MATCH (e:LegalEntity {lei: $lei})
MATCH path = (e)-[:DIRECTLY_OWNED_BY|CONTROLLED_BY*1..3]-(s:SanctionedEntity)
RETURN s.name AS sanctioned,
       length(path) AS hops,
       [n IN nodes(path) | coalesce(n.name, n.id)] AS chain
ORDER BY hops ASC LIMIT 5
```

### 3. Circular ownership (3-cycle, fixed length — fast)

```cypher
MATCH (a:LegalEntity)-[:DIRECTLY_OWNED_BY]->(b:LegalEntity)
       -[:DIRECTLY_OWNED_BY]->(c:LegalEntity)
       -[:DIRECTLY_OWNED_BY]->(a)
RETURN DISTINCT a.name, b.name, c.name
```

### 4. Circular ownership (any length, via SCC)

Run GDS SCC first (see `gds-analysis` skill), then:
```cypher
MATCH (n:LegalEntity) WHERE n.sccId IS NOT NULL
WITH n.sccId AS scc, collect(n.name) AS members
WHERE size(members) > 1
RETURN scc, size(members) AS ringSize, members
```

### 5. Shell company indicators

```cypher
MATCH (e:LegalEntity)<-[:DIRECTLY_OWNED_BY]-(child)
WITH e, count(DISTINCT child) AS subsidiaryCount
WHERE subsidiaryCount > 5
  AND e.hasOperationalAddress = false
  AND e.riskTier IN ['medium', 'high']
RETURN e.name, e.jurisdiction, subsidiaryCount
ORDER BY subsidiaryCount DESC LIMIT 20
```

### 6. Transaction structuring (just below $10k reporting threshold)

```cypher
MATCH (from:LegalEntity)-[t:TRANSACTION]->(to:LegalEntity)
WHERE t.amount > 9000 AND t.amount < 10000
WITH from, to, count(t) AS suspicious_txns, collect(t.date) AS dates
WHERE suspicious_txns >= 3
RETURN from.name, to.name, suspicious_txns, dates
ORDER BY suspicious_txns DESC LIMIT 20
```

### 7. PEP exposure

```cypher
MATCH (e:LegalEntity)-[:DIRECTLY_OWNED_BY|CONTROLLED_BY*1..5]-(pep:PoliticallyExposedPerson)
RETURN e.name, e.jurisdiction, pep.name, pep.nationality
LIMIT 20
```

### 8. Composite risk dashboard

```cypher
MATCH (e:LegalEntity)
WHERE e.kycRiskScore IS NOT NULL
OPTIONAL MATCH (e)-[:DIRECTLY_OWNED_BY|CONTROLLED_BY*1..3]-(s:SanctionedEntity)
WITH e, count(DISTINCT s) AS sanctionsLinks
RETURN e.name, e.lei, e.jurisdiction, e.riskTier,
       round(e.kycRiskScore, 1) AS riskScore,
       sanctionsLinks
ORDER BY riskScore DESC LIMIT 20
```

## Performance rules

1. **Always create indexes** on lookup properties:
   ```cypher
   CREATE INDEX entity_lei IF NOT EXISTS FOR (e:LegalEntity) ON (e.lei);
   CREATE INDEX entity_name IF NOT EXISTS FOR (e:LegalEntity) ON (e.name);
   CREATE CONSTRAINT entity_lei_unique IF NOT EXISTS
   FOR (e:LegalEntity) REQUIRE e.lei IS UNIQUE;
   ```

2. **Bound variable-length paths**: `*1..10` not `*` (unbounded → expensive).

3. **Use parameters** (`$lei`) not string concatenation — query plans get cached.

4. **`PROFILE` / `EXPLAIN`** prefix to see the plan:
   ```cypher
   PROFILE MATCH (e:LegalEntity {lei: $lei}) ...
   ```
   Look for `NodeByLabelScan` (slow) → fix with index.

## Useful helpers

| Pattern | What it does |
|---|---|
| `EXISTS { MATCH ... }` | Filter on whether a sub-pattern matches (faster than COUNT > 0) |
| `[n IN nodes(path) \| n.name]` | Extract a property from every node on a path |
| `apoc.coll.toSet(list)` | Deduplicate |
| `apoc.path.subgraphAll(start, {maxLevel: 3})` | Get N-hop neighbourhood as one result |
| `apoc.create.relationship(a, type, props, b)` | Dynamic rel type at runtime |

## Common pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| Query takes minutes | Unbounded path or missing index | `PROFILE` and add index, bound the `*` |
| Cartesian product warning | Two MATCHes not joined | Use single MATCH or WHERE to relate them |
| `count(*)` instead of `count(DISTINCT ...)` | Duplicate paths to same node | Use `DISTINCT` |
| `OPTIONAL MATCH` returns null and pattern fails | Pattern after OPTIONAL got filtered | Move filter into OPTIONAL block |
| Direction wrong: returns nothing | `-->` vs `<--` flipped | Drop direction (`--`) to test, then add it back |

## Reference

- `cypher/03_kyc_investigations.cypher` — full set of investigation queries
- `cypher/04_data_quality.cypher` — quality checks
- `scripts/09_graphrag_agent.py` — wraps these as agent tools
