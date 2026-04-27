---
name: gds-analysis
description: "Use when running Neo4j Graph Data Science algorithms — PageRank, Louvain, WCC, SCC, Betweenness, Shortest Path, Node Similarity; projecting graphs into GDS memory; computing KYC risk scores; debugging 'graph already exists' or projection-not-found errors. Covers the project→stream/write→consume pattern, projection configuration, undirected vs natural orientation, and which algorithm fits which KYC question."
---

# Graph Data Science (GDS) Skill

## When to use

User says: "find suspicious clusters", "compute risk score", "detect circular ownership", "find central entities", "run Louvain/PageRank", "shortest path between two entities".

## Algorithm → KYC question map

| Algorithm | Answers | Cypher procedure |
|---|---|---|
| **WCC** (Weakly Connected Components) | "What isolated entity clusters exist?" | `gds.wcc.*` |
| **SCC** (Strongly Connected Components) | "What ownership cycles (A→B→C→A) exist?" | `gds.scc.*` |
| **Louvain** | "What suspicious tight communities exist?" | `gds.louvain.*` |
| **PageRank** | "Which entities are systemically most connected?" | `gds.pageRank.*` |
| **Betweenness** | "Which entities sit on many money-flow paths (conduits)?" | `gds.betweenness.*` |
| **Degree** | "Which entities have many subsidiaries?" | `gds.degree.*` |
| **Shortest Path (Dijkstra)** | "How are entity X and entity Y connected?" | `gds.shortestPath.dijkstra.*` |
| **Node Similarity** | "Which entities look like a known bad actor?" | `gds.nodeSimilarity.*` |
| **Triangle Count** | "Which entities are in many triangle structures?" | `gds.triangleCount.*` |

## The mandatory 3-step workflow

```cypher
-- 1. PROJECT a subgraph into GDS memory (you can't run algorithms on the live graph)
CALL gds.graph.project('myGraph', nodeSpec, relSpec)

-- 2. RUN the algorithm with .stream / .write / .mutate / .stats
CALL gds.<algo>.<mode>('myGraph', {config})

-- 3. DROP when done (frees RAM)
CALL gds.graph.drop('myGraph')
```

### Mode comparison

| Mode | Returns | Side-effect | Use when |
|---|---|---|---|
| `stream` | Rows | None | Inspecting results, no persistence |
| `write` | Stats | Writes back to Neo4j as node property | Want score available in future Cypher queries (most common for KYC) |
| `mutate` | Stats | Writes to in-memory projection only | Pipelining multiple algorithms |
| `stats` | Summary stats | None | Just want counts/distribution |

## Projection patterns

### Simple projection (node labels + rel types)
```cypher
CALL gds.graph.project(
  'kyc-graph',
  ['LegalEntity', 'NaturalPerson'],
  {
    DIRECTLY_OWNED_BY: {orientation: 'UNDIRECTED', properties: ['percentage']},
    CONTROLLED_BY: {orientation: 'UNDIRECTED'},
    TRANSACTION: {orientation: 'NATURAL', properties: ['amount']}
  }
)
```

### Cypher projection (full flexibility, slower)
```cypher
CALL gds.graph.project.cypher(
  'kyc-graph',
  'MATCH (n:LegalEntity) WHERE n.riskTier = "high" RETURN id(n) AS id',
  'MATCH (a)-[r:DIRECTLY_OWNED_BY]->(b) RETURN id(a) AS source, id(b) AS target, r.percentage AS weight'
)
```

## Orientation rules

| Algorithm | Best orientation | Why |
|---|---|---|
| WCC, Louvain, Node Similarity | `UNDIRECTED` | These don't care about direction |
| **SCC** (cycle detection) | `NATURAL` | MUST be directed — direction defines the cycle |
| PageRank | `NATURAL` (default) or `REVERSE` | Direction of "endorsement" matters |
| Betweenness | `NATURAL` for money flow, `UNDIRECTED` for ownership | Depends on semantics |
| Shortest Path | Either, depending on semantics | |

## KYC risk score recipe (composite)

```cypher
MATCH (n:LegalEntity)
WHERE n.pageRankScore IS NOT NULL
SET n.kycRiskScore =
    (CASE n.riskTier WHEN 'high' THEN 50 WHEN 'medium' THEN 25 ELSE 0 END)
  + (n.pageRankScore * 100)
  + (coalesce(n.betweennessScore, 0) / 100)
  + (CASE WHEN n.isActive = false THEN 20 ELSE 0 END)
  + (CASE WHEN n.hasOperationalAddress = false THEN 15 ELSE 0 END)
```

## Common pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| `Graph 'kyc-graph' already exists` | Previous projection wasn't dropped | `CALL gds.graph.drop('kyc-graph', false)` (false = don't fail if missing) |
| `Graph 'kyc-graph' does not exist` | Forgot to project | Run project step first |
| SCC returns empty | Used `UNDIRECTED` orientation | Re-project with `NATURAL` |
| PageRank scores all ≈ 1/N | Damping/iterations off | Use defaults: `dampingFactor: 0.85, maxIterations: 20` |
| OOM error projecting | Graph too big for heap | Use Cypher projection with filter, or increase `NEO4J_server_memory_heap_max__size` |
| `gds.util.asNode(nodeId)` errors | Algorithm streaming, node deleted between calls | Materialise: `WITH gds.util.asNode(nodeId) AS n RETURN n.name` |

## Idempotent project pattern (always use this)

```cypher
CALL gds.graph.exists('kyc-graph') YIELD exists
WITH exists WHERE exists
CALL gds.graph.drop('kyc-graph') YIELD graphName
RETURN graphName
UNION
CALL gds.graph.exists('kyc-graph') YIELD exists
WITH exists WHERE NOT exists
CALL gds.graph.project('kyc-graph', ['LegalEntity'], {DIRECTLY_OWNED_BY: {orientation: 'UNDIRECTED'}})
YIELD graphName RETURN graphName
```

Or simply, in Python: `run("CALL gds.graph.drop('kyc-graph', false)")` — the `false` makes it not fail.

## Reference

- `scripts/08_gds_analysis.py` — full pipeline for all 5 algorithms + composite score
- `cypher/02_gds_projections.cypher` — standalone GDS query collection
