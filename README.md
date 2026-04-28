# KYC Beneficial Ownership Intelligence System

Hybrid **FIBO ontology (GraphDB) + property-graph analytics (Neo4j) + GraphRAG agent (LangGraph)** for AML/KYC investigation, built **end-to-end on real public data** — no synthetic records, no hard-coded entities, no LLM-generated facts in the production pipeline.

Built on the [Going Meta / Jesús Barrasa](https://neo4j.com/blog/going-meta-knowledge-graph/) pattern (sessions 21–22, 28–32).

| Layer | Tech | Purpose |
|-------|------|---------|
| Ontology authority | **GraphDB** + FIBO/LCC | Semantic schema, OWL reasoning, SPARQL |
| Data quality | **SHACL** (pyshacl) | Shape-driven validation of graph contents |
| Analytics engine | **Neo4j 6** + GDS + APOC | UBO traversal, community detection, risk scoring |
| Bridge | **neosemantics (n10s)** | Round-trip RDF ↔ property graph |
| Reasoning agent | **LangGraph + LangChain** | Natural-language KYC investigation |
| Reference data | **GLEIF** Levels 1 & 2 | Real LEIs + parent–child ownership |
| Semantic search | **Neo4j Vector + Ollama** | Embedding-based entity search |
| UI | **FastAPI + SPA** | Dashboard + Chat |

---

## Data Pipeline (real data only)

| Step | Script | What it produces |
|------|--------|------------------|
| 1 | `01_setup_graphdb.py` | GraphDB repository `kyc-kg`, named graphs |
| 2 | `02_load_fibo.py` | FIBO modules into `<http://kg/fibo>` |
| 3 | `03_load_fibo2glei_mapping.py` | Cross-ontology alignment |
| 4 | `04_sparql_exploration.py` | Smoke-test SPARQL queries |
| 5 | `05_load_gleif_data.py` | 350 real LEI records (`data/glei/raw_records.json`) |
| 6 | `11_load_real_kg.py` | Wipes Neo4j, initialises n10s, imports 5 FIBO modules, loads 350 `:LegalEntity` from GLEIF, links each to FIBO `LegalPerson` via `:INSTANCE_OF` |
| 7 | `14_load_gleif_l2_ownership.py` | Resolves 20 well-known parent names via GLEIF search API, fetches **real `direct-children` Level-2 records**, materialises 825 `:DIRECTLY_OWNED_BY` relationships (1 195 total `:LegalEntity`) |
| 8 | `13_embed_entities.py` | Per-label vector + fulltext indexes using Ollama `nomic-embed-text` (768-dim cosine) |
| 9 | `08_gds_analysis.py` | PageRank, Louvain communities, SCC, Betweenness; computes `kycRiskScore` |
| 10 | `10_shacl_validate.py` | Validates the whole graph against `shacl/kyc_shapes.ttl` (LEI regex, jurisdiction codes, ownership %, etc.) |

There is no Diffbot dump and no `Organization`/`Person`/`Skill` blind labels — every node is a real `:LegalEntity` (FIBO LegalPerson) keyed by its real LEI.

A separate experimental script — `12_ontology_guided_enrichment.py` — implements the *Going Meta session 30* ontology-driven LLM extraction pattern (pulls the application ontology dynamically via SPARQL `CONSTRUCT`, derives the NL description programmatically, validates with pyshacl). It is not part of the default pipeline because cloud-routed Ollama models cannot reliably emit native tool calls.

---

## Quick Launch

```bash
# 1. Start the databases
docker compose up -d
# → GraphDB: http://localhost:7200
# → Neo4j:  http://localhost:7474  (neo4j / kycpassword123)

# 2. Install Python deps
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env
# Edit .env — set OLLAMA_MODEL=qwen3.5:27b-q4_K_M (or any tool-capable LLM)

# 4. Pull required Ollama models
ollama pull qwen3.5:27b-q4_K_M     # LLM (must support `tools`)
ollama pull nomic-embed-text       # 768-dim embeddings

# 5. Build the knowledge graph end to end
python scripts/01_setup_graphdb.py
python scripts/02_load_fibo.py
python scripts/05_load_gleif_data.py
python scripts/11_load_real_kg.py
python scripts/14_load_gleif_l2_ownership.py
python scripts/13_embed_entities.py
python scripts/08_gds_analysis.py
python scripts/10_shacl_validate.py    # → "Data conforms to all SHACL shapes"

# 6. Launch backend + frontend
uvicorn dashboard.api:app --reload --port 8000
open http://localhost:8000
```

---

## Key Features

### 1. Agentic Chat (LangGraph ReAct Agent)
- **24 tools** for KYC/AML investigation, all FIBO-aligned
- Multi-turn conversation with session memory
- Grounded answers — all data comes from Neo4j/GraphDB, never hallucinated
- LLM priority: Anthropic → OpenAI → DeepSeek → Ollama
- **Important:** the chosen LLM must support native tool calling. `qwen3.5:27b-q4_K_M` and `gpt-4o-mini` work; `deepseek-v3.2:cloud` does *not* (Ollama proxies it without function-calling support)

### 2. Real GLEIF Level-2 Ownership
- 825 `:DIRECTLY_OWNED_BY` edges sourced directly from `https://api.gleif.org/api/v1/.../direct-children`
- Parents are resolved at runtime by name (no hard-coded LEIs); children are upserted with their full GLEIF attributes (legal form, jurisdiction, HQ city/country, status, …)

### 3. Neo4j Vector Store (Semantic Search)
- Ollama `nomic-embed-text` embeddings (768-dim, cosine)
- Per-label indexes: `entity_embeddings_legalentity`, `entity_embeddings_naturalperson` and matching fulltext indexes (Neo4j 6 does not support multi-label vector indexes)
- Tool: `semantic_search_entities` or `POST /api/vector/search`

### 4. SHACL-validated graph
- `shacl/kyc_shapes.ttl` enforces: LEI regex `^[A-Z0-9]{20}$`, mandatory `legalName` + `hasJurisdiction`, ISO-3166-1 alpha-2 nationality, ownership percentages in [0..100]
- Validated end-to-end after every ingest by `10_shacl_validate.py`

---

## Performance Tips

The Ollama bridge proxies many cloud-routed models, but **only models that advertise the `tools` capability work with the agent** (e.g. `qwen3.5:27b-q4_K_M`). For faster cloud responses, set `OPENAI_API_KEY` or `ANTHROPIC_API_KEY` — the agent picks them up automatically.

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/agent/chat` | Agentic chat (message + session_id) |
| POST | `/api/agent/reset` | Reset session memory |
| POST | `/api/enrich` | Enrich graph via Diffbot (entity_name/url/text) |
| POST | `/api/vector/search` | Semantic vector search |
| POST | `/api/chat` | Legacy pattern-matching chat |
| GET | `/api/kpis` | Dashboard KPIs |
| GET | `/api/entities` | Entity list with filters |
| GET | `/api/entities/{id}` | Entity detail |
| GET | `/api/ubo/{id}` | UBO chain |
| GET | `/api/graph/{id}` | Ownership graph visualization |
| POST | `/api/cypher` | Run read-only Cypher |
| POST | `/api/sparql` | Run read-only SPARQL |

---

## Project Layout

```
kyc-intelligence/
├── dashboard/
│   ├── api.py              # FastAPI backend (all endpoints)
│   ├── agent.py            # LangGraph ReAct agent (24 tools)
│   ├── chat_engine.py      # Legacy pattern-matching engine
│   └── static/index.html   # SPA frontend
├── scripts/
│   ├── 01-10               # Ontology, GLEIF L1, GDS, SHACL pipeline
│   ├── 11_load_real_kg.py  # FIBO + 350 real :LegalEntity from GLEIF
│   ├── 13_embed_entities.py# Per-label vector + fulltext indexes
│   └── 14_load_gleif_l2_ownership.py # Real ownership from GLEIF L2 API
├── src/kg_client.py        # Neo4j & GraphDB helper clients
├── sparql/                 # Standalone SPARQL queries
├── cypher/                 # Standalone Cypher queries
├── shacl/                  # SHACL data quality shapes
├── tests/                  # pytest integration tests
├── .env                    # Configuration (API keys, models)
├── docker-compose.yml      # GraphDB + Neo4j
└── requirements.txt
```

---

## Environment Variables (.env)

```bash
# Databases
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=kycpassword123
GRAPHDB_URL=http://localhost:7200
GRAPHDB_REPO=kyc-kg

# LLM (pick one — first match wins; must support native tool calling)
ANTHROPIC_API_KEY=           # Best quality
OPENAI_API_KEY=              # Fast fallback
DEEPSEEK_API_KEY=            # Budget option
OLLAMA_MODEL=qwen3.5:27b-q4_K_M  # Local, supports tools

# Embeddings
OLLAMA_BASE_URL=http://localhost:11434/v1
OLLAMA_EMBED_MODEL=nomic-embed-text
```

---

## Testing Use Cases

After loading real data (`scripts/11_load_real_data.py`), try these in the chat:

1. **Diffbot Enrichment**: "Enrich the graph with data about Deutsche Bank"
2. **Semantic Search**: "Search for entities related to money laundering scandals"
3. **Graph Exploration**: "What organizations are in the graph?"
4. **Relationship Discovery**: "What connections exist between HSBC and other entities?"
5. **Custom Cypher**: "Show me all Person nodes and their connections"
6. **Ontology**: "How does FIBO model ownership?"
7. **Statistics**: "Graph statistics"
8. **Follow-up**: Ask any follow-up question in the same session

---

## Architecture diagrams & deeper docs

- **Master plan**: [docs/CONSOLIDATED_FINANCIAL_KG_PLAN.md](docs/CONSOLIDATED_FINANCIAL_KG_PLAN.md)
