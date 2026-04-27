# KYC Intelligence — AI Agent Project Instructions

This is a **KYC/AML Beneficial Ownership Intelligence System** built on FIBO, GLEIF, GraphDB, Neo4j, neosemantics, GDS, and LangGraph.

## Architecture (read first)

```
GraphDB (RDF + SPARQL + OWL reasoning)  ←──[neosemantics bridge]──→  Neo4j (Cypher + GDS + GraphRAG)
```

- **GraphDB** is the **ontology authority** (FIBO/GLEIF/LCC) — used for SPARQL, OWL inference, SHACL.
- **Neo4j** is the **analytics + application engine** — used for Cypher, GDS algorithms, the GraphRAG agent.
- **neosemantics (n10s)** is the bridge that imports RDF/OWL into Neo4j and exports back.

Always think: *which database does this concern belong in?* Ontology semantics → GraphDB. Pattern matching, algorithms, AI agents → Neo4j.

## When to load skills

This project ships specialised skills under `.github/skills/`. Load the relevant `SKILL.md` BEFORE starting work in that area:

| User intent | Skill to load |
|---|---|
| Loading FIBO/LCC into GraphDB, repo creation | `load-fibo-ontology` |
| Writing/debugging SPARQL queries against GraphDB | `sparql-exploration` |
| Bridging GraphDB↔Neo4j via n10s, importing OWL/RDF into Neo4j | `n10s-bridge` |
| Running graph algorithms (PageRank, Louvain, SCC, etc.) | `gds-analysis` |
| Defining or running SHACL validation | `shacl-validation` |
| Writing KYC investigation Cypher queries (UBO, sanctions, rings) | `cypher-kyc-queries` |
| Building or extending the LangGraph KYC agent | `graphrag-agent` |

If the user's request spans multiple areas, load multiple skills.

## Conventions

- Python: 4-space indent. Use `python-dotenv` to load `.env` — never hardcode credentials.
- Use the helpers in `src/kg_client.py` (`GraphDBClient`, `Neo4jClient`) — don't recreate connection logic.
- All scripts in `scripts/` are numbered (`01_*.py`, `02_*.py`, ...) and runnable standalone.
- All standalone queries live under `sparql/` and `cypher/` — keep them organised by purpose.
- Tests live under `tests/` and use `pytest`. Mark integration tests that need running databases with `@pytest.mark.integration`.

## Environment

- Bring up databases: `docker compose up -d` (waits ~60s for both to be healthy).
- GraphDB UI: http://localhost:7200 — Neo4j Browser: http://localhost:7474 (user `neo4j`, pwd `kycpassword123`).
- Default repo: `kyc-kg`. Default Neo4j db: `neo4j` (community edition has only one).
