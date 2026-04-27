# KYC Beneficial Ownership Intelligence System

Hybrid **FIBO ontology (GraphDB) + property-graph analytics (Neo4j) + GraphRAG agent (LangGraph)** for AML/KYC investigation.

Built on the [Going Meta / Jesús Barrasa](https://neo4j.com/blog/going-meta-knowledge-graph/) pattern:

| Layer | Tech | Purpose |
|-------|------|---------|
| Ontology authority | **GraphDB** + FIBO/LCC | Semantic schema, OWL reasoning, SHACL validation |
| Analytics engine | **Neo4j** + GDS + APOC | UBO traversal, community detection, risk scoring |
| Bridge | **neosemantics (n10s)** | Round-trip RDF ↔ property graph |
| Reasoning agent | **LangGraph + LangChain** | Natural-language KYC investigation |
| UI | **Streamlit + Jupyter** | Dashboard + exploratory notebook |

---

## Quickstart

```bash
# 1. Start the databases
docker compose up -d
# → GraphDB:  http://localhost:7200
# → Neo4j:   http://localhost:7474   (neo4j / kycpassword123)

# 2. Install Python deps
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Configure secrets (optional — only needed for the GraphRAG agent)
cp .env.example .env
# Then edit .env and add ANTHROPIC_API_KEY=...  (or OPENAI_API_KEY=...)

# 4. End-to-end pipeline (run scripts in order)
python scripts/01_setup_graphdb.py             # Create kyc-kg repo
python scripts/02_load_fibo.py                 # Pull FIBO + LCC ontologies
python scripts/03_load_fibo2glei_mapping.py    # FIBO↔GLEIF mapping + KYC ontology
python scripts/04_sparql_exploration.py        # Tour the loaded ontologies
python scripts/05_load_gleif_data.py           # Real entities from GLEIF API
python scripts/06_generate_synthetic_data.py   # Synthetic data + planted crimes
python scripts/07_load_neo4j.py                # n10s setup + load all data
python scripts/08_gds_analysis.py              # GDS algorithms + risk scoring
python scripts/10_shacl_validate.py            # SHACL data quality check
python scripts/09_graphrag_agent.py            # Start the chat agent

# 5. Verify with tests
pytest -m integration -v

# 6. UI
#streamlit run dashboard/app.py
#jupyter lab notebooks/01_exploration.ipynb


#New UI
cd /Users/akash/KG_Projects/KG_GraphDB_Neo4j/KG_Finance/finance-Intelligence-Projects/kyc-intelligence
source /Users/akash/KG_Projects/KG_GraphDB_Neo4j/KG_Finance/.venv/bin/activate
uvicorn dashboard.api:app --reload --port 8000
```

---

## Project layout

```
kyc-intelligence/
├── docker-compose.yml          # GraphDB + Neo4j + n10s + GDS + APOC
├── requirements.txt
├── .env.example
├── src/kg_client.py            # Helper clients (GraphDBClient, Neo4jClient)
├── scripts/                    # Numbered, idempotent pipeline scripts (01-10)
├── sparql/                     # Standalone SPARQL queries (paste in Workbench)
├── cypher/                     # Standalone Cypher queries (paste in Browser)
├── shacl/kyc_shapes.ttl        # Data quality rules
├── tests/                      # pytest with @integration marker
├── notebooks/                  # Walk-through Jupyter notebook
├── dashboard/app.py            # Streamlit dashboard
├── docs/                       # CONSOLIDATED_FINANCIAL_KG_PLAN.md
└── .github/
    ├── copilot-instructions.md # AI assistant guide
    └── skills/                 # 8 reusable Copilot skills
```

---

## What gets detected?

The synthetic dataset (script `06`) **plants** financial-crime patterns and the
test suite (`tests/test_detection.py`) **asserts** the queries find them:

| Pattern | How it's planted | How it's detected |
|---------|------------------|-------------------|
| Sanctioned UBO behind shells | 5 chains of depth 3, ending in `PERSON_0000` | Variable-length `[:DIRECTLY_OWNED_BY*0..6]` traversal |
| Circular ownership | 5 rings of size 3 | GDS Strongly Connected Components |
| Structuring | Transactions $9,000–$10,000 | Cypher `WHERE amount > 9000 AND amount < 10000` |
| Shell company | High-risk jurisdiction + no operational address + no directors | Composite Cypher predicate |
| PEP exposure | First 10 persons flagged `isPEP=true` | `:PoliticallyExposedPerson` label |

---

## Architecture diagrams & deeper docs

- **Master plan**: [docs/CONSOLIDATED_FINANCIAL_KG_PLAN.md](docs/CONSOLIDATED_FINANCIAL_KG_PLAN.md)
- **Skills**: [.github/skills/](.github/skills) — each skill encodes lessons learned for one part of the stack. Copilot loads them on-demand based on your prompt.

---

## Credentials needed

Only **one** of these is required (and only for the GraphRAG agent in script `09`):

```bash
# In .env — pick one:
ANTHROPIC_API_KEY=sk-ant-...    # Preferred (claude-sonnet-4-5)
OPENAI_API_KEY=sk-...            # Fallback  (gpt-4o)
```

All other scripts run fully offline against the local Docker stack.
