# Financial Knowledge Graph — Consolidated Implementation Plan
## KYC/AML Beneficial Ownership Intelligence System
### FIBO · GLEIF · LCC · GraphDB · Neo4j · neosemantics · GDS · GraphRAG

> **Last Updated**: April 2026
> **Source Material**: FINANCIAL_KG_MASTER_PLAN_v2.md, NEUBAUTEN_GRAPH_IMPLEMENTATION_PLAN.md, kyc_architecture.jsx, jbarrasa/goingmeta sessions 1–45

---

## TABLE OF CONTENTS

- [Part A — Conceptual Foundation (Read First)](#part-a--conceptual-foundation)
- [Part B — Technology Stack & Dependencies](#part-b--technology-stack--dependencies)
- [Part C — Environment Setup](#part-c--environment-setup)
- [Part D — Module 1: GraphDB & Ontologies](#part-d--module-1-graphdb--ontologies)
- [Part E — Module 2: Neo4j & neosemantics Bridge](#part-e--module-2-neo4j--neosemantics-bridge)
- [Part F — Module 3: Data Loading (Real + Synthetic)](#part-f--module-3-data-loading)
- [Part G — Module 4: Graph Algorithms (GDS)](#part-g--module-4-graph-algorithms-gds)
- [Part H — Module 5: SHACL Validation & Data Quality](#part-h--module-5-shacl-validation--data-quality)
- [Part I — Module 6: GraphRAG KYC Agent](#part-i--module-6-graphrag-kyc-agent)
- [Part J — Module 7: Dashboard & Visualization](#part-j--module-7-dashboard--visualization)
- [Part K — Skills Reference (For Implementation)](#part-k--skills-reference)
- [Part L — Learning Path & Checklist](#part-l--learning-path--checklist)
- [Part M — Key URLs & Resources](#part-m--key-urls--resources)
- [Part N — Project Folder Structure](#part-n--project-folder-structure)

---

# PART A — CONCEPTUAL FOUNDATION

Before writing any code, understand these five concepts. Everything else builds on them.

## A.1 What is a Knowledge Graph?

A Knowledge Graph stores facts as **relationships between things** — not rows in a table.

```
Traditional DB:   Table: companies | id=1 | name=Apple | country=US |
Knowledge Graph:  (Apple) --[HEADQUARTERED_IN]--> (United States)
                  (Apple) --[ISSUES]--> (AAPL stock)
                  (Tim Cook) --[CEO_OF]--> (Apple)
```

**Why it matters for KYC**: To find who ultimately owns a shell company, you trace a chain of ownership relationships — potentially 10+ levels deep across countries. This is a graph traversal. In SQL, this requires 10+ self-joins. In a graph database, it's one query.

## A.2 RDF vs Property Graph — Two Ways to Model a Graph

There are two major graph data models. This project uses **both**.

| | RDF (Resource Description Framework) | Property Graph |
|---|---|---|
| **Unit of data** | Triple: `Subject → Predicate → Object` | Node + Relationship + Properties |
| **Example** | `<Apple> <hasName> "Apple Inc."` | `(:Company {name: "Apple Inc."})` |
| **Query language** | SPARQL | Cypher |
| **Strengths** | Ontology reasoning, schema validation, global IDs | Fast traversals, algorithms, app-friendly |
| **Database** | GraphDB (Ontotext) | Neo4j |
| **Best for** | Defining what terms MEAN | Analyzing what data SHOWS |

**Key insight (from Jesús Barrasa's Going Meta series)**: You don't choose one — you use GraphDB to CURATE and VALIDATE ontology-aligned data, then Neo4j to QUERY, ANALYZE, and BUILD APPLICATIONS.

## A.3 What is an Ontology?

An ontology is a formal, machine-readable definition of concepts in a domain and how they relate.

Think of it as a **dictionary + grammar** for a specific field:
- **Classes**: The types of things (LegalEntity, NaturalPerson, Address)
- **Properties**: Attributes things have (name, lei, dateOfIncorporation)
- **Relationships**: How things connect (OWNED_BY, REGISTERED_IN)
- **Rules**: Constraints and inferences ("if X owns Y and Y owns Z, then X transitively owns Z")

**FIBO** (Financial Industry Business Ontology) is the standard ontology for finance — 2,437+ OWL classes covering legal entities, ownership, securities, loans, derivatives, etc.

## A.4 The Architecture — Why Two Databases?

```
┌────────────────────────────────────────────────────────────────────────┐
│              BENEFICIAL OWNERSHIP INTELLIGENCE SYSTEM                   │
└────────────────────────────────────────────────────────────────────────┘

LAYER 0: Sources          LAYER 1: Ontology Store      LAYER 2: Analytics Store
─────────────────         ──────────────────────        ─────────────────────────
                          ┌──────────────────┐          ┌────────────────────┐
FIBO OWL ontology ──────► │   GraphDB        │          │   Neo4j            │
GLEIF LEI data ─────────► │                  │          │                    │
LCC country codes ──────► │  • Loads FIBO    │  n10s    │  • Cypher queries  │
OFAC sanctions ─────────► │  • SPARQL queries│ ───────► │  • GDS algorithms  │
OpenCorporates ─────────► │  • OWL reasoning │          │  • APOC procedures │
Wikidata PEPs ──────────► │  • SHACL valid.  │          │  • GraphRAG agent  │
                          └──────────────────┘          └────────┬───────────┘
                                                                 │
                                                        ┌────────┴───────────┐
                                                        │ LangGraph Agent    │
                                                        │ + Claude/GPT LLM   │
                                                        │ = KYC Investigator  │
                                                        └────────┬───────────┘
                                                                 │
                                                        ┌────────┴───────────┐
                                                        │ Streamlit Dashboard │
                                                        └────────────────────┘
```

**GraphDB** = The librarian (knows what every term means, validates correctness)
**Neo4j** = The detective (finds patterns, runs algorithms, powers the AI agent)
**neosemantics (n10s)** = The translator (moves data between them)

## A.5 The Financial Ontology Ecosystem

```
EDM Council (spec.edmcouncil.org)
│
├── FIBO — Financial Industry Business Ontology (OWL)
│   ├── FND  — Foundations (dates, quantities, core relations)
│   ├── BE   — Business Entities (corps, LLCs, trusts, partnerships)  ← PRIMARY
│   ├── FBC  — Financial Business & Commerce (regulators, markets)
│   ├── SEC  — Securities (equities, bonds, funds)
│   ├── DER  — Derivatives (swaps, options, futures)
│   ├── LOAN — Loans (mortgages, commercial)
│   ├── BP   — Business Processes (settlement, issuance)
│   └── IND  — Indices & Indicators (benchmarks)
│
├── FIB-DM — FIBO as ER Data Model (3,173 entities, Q4/2025)
│   └── Use as schema reference when designing Neo4j labels
│
└── LCC — Languages, Countries & Codes (ISO 3166, ISO 639)

External Data:
├── GLEIF — 2M+ Legal Entity Identifiers (free API)
├── OFAC SDN — US sanctions list
├── OpenCorporates — 200+ jurisdictions company data
└── Wikidata — Politically Exposed Persons (PEPs)
```

**For this project, you primarily need**: FIBO BE (Business Entities), FIBO FND (Foundations), LCC, and GLEIF data.

---

# PART B — TECHNOLOGY STACK & DEPENDENCIES

## B.1 Complete Dependency Map

### Infrastructure
| Tool | Version | Purpose | Install |
|---|---|---|---|
| Docker Desktop | Latest | Container runtime | docker.com |
| Docker Compose | v2+ (bundled) | Multi-container orchestration | Included with Docker Desktop |

### Databases
| Tool | Version | Purpose | Access |
|---|---|---|---|
| GraphDB | 10.7+ | RDF triplestore, OWL reasoning, SPARQL | Docker: `ontotext/graphdb:10.7.0` |
| Neo4j | 5.20+ Community | Property graph, Cypher, algorithms | Docker: `neo4j:5.20-community` |

### Neo4j Plugins (auto-installed via Docker)
| Plugin | Purpose | Key Procedures |
|---|---|---|
| **neosemantics (n10s)** | RDF ↔ Neo4j bridge | `n10s.onto.import.fetch()`, `n10s.rdf.import.fetch()`, `n10s.rdf.export.*`, `n10s.validation.shacl.*` |
| **APOC** | 300+ utility procedures | `apoc.load.json()`, `apoc.load.csv()`, `apoc.create.relationship()`, `apoc.meta.schema()` |
| **GDS** | Graph algorithms (in-memory) | `gds.louvain.*`, `gds.pageRank.*`, `gds.wcc.*`, `gds.betweenness.*`, `gds.shortestPath.*` |

### Python Libraries

```
# Core RDF/SPARQL
rdflib==7.0.0              # Parse and create RDF in Python
SPARQLWrapper==2.0.0       # Query SPARQL endpoints from Python
rdflib-neo4j==1.0.0        # Direct RDF→Neo4j loader (Going Meta S12, S30)

# Neo4j
neo4j==5.20.0              # Official Neo4j Python driver (Bolt protocol)

# Data & API
requests==2.31.0           # HTTP requests (GLEIF API, GraphDB REST)
httpx==0.27.0              # Async HTTP (optional, faster for bulk)
pandas==2.2.0              # DataFrames for tabular analysis
faker==30.0.0              # Synthetic test data generation

# AI / RAG
langchain==0.3.0           # LLM orchestration framework
langchain-neo4j==0.2.0     # Neo4j-specific LangChain components
langchain-anthropic==0.3.0 # Claude LLM provider
langgraph==0.2.0           # Stateful agent workflows
neo4j-graphrag==1.0.0      # Neo4j's native GraphRAG library (Going Meta S22-45)
openai==1.30.0             # OpenAI API (embeddings, optional LLM)

# Dashboard
streamlit==1.39.0          # Web dashboard
plotly==5.24.0             # Interactive charts

# Utilities
python-dotenv==1.0.0       # Environment variable management
tqdm==4.66.0               # Progress bars
pydantic==2.7.0            # Data validation (Going Meta S30 pattern)
jupyter==1.0.0             # Notebooks for exploration
```

### Key Libraries Explained

| Library | What it does | When you use it |
|---|---|---|
| `rdflib` | Parse OWL/TTL/RDF files in Python, create RDF triples programmatically | Loading FIBO files locally, converting GLEIF JSON→RDF, creating FIBO2GLEI mapping |
| `SPARQLWrapper` | Send SPARQL queries to GraphDB from Python | Querying GraphDB repository, exploring ontology classes |
| `neo4j` | Connect to Neo4j via Bolt, run Cypher queries | All Neo4j interactions — loading data, running queries, GDS algorithms |
| `neo4j-graphrag` | High-level GraphRAG: `SimpleKGPipeline`, `VectorRetriever`, `Text2CypherRetriever`, `GraphSchema` | Building KG from documents, RAG retrieval, end-to-end pipeline (Going Meta S22-45) |
| `langchain-neo4j` | `Neo4jGraph`, `GraphCypherQAChain` — LangChain's Neo4j integration | Natural-language-to-Cypher, investigation agent |
| `langgraph` | Stateful multi-step agent workflows with `StateGraph` | KYC investigation agent that uses multiple tools |
| `rdflib-neo4j` | Loads RDF triples directly into Neo4j (bypasses n10s) | Alternative to n10s for bulk RDF loading (Going Meta S12, S30) |

---

# PART C — ENVIRONMENT SETUP

## C.1 Docker Compose — Both Databases

Create `docker-compose.yml` in your project root:

```yaml
version: "3.8"
services:

  graphdb:
    image: ontotext/graphdb:10.7.0
    container_name: graphdb
    ports:
      - "7200:7200"     # GraphDB Workbench UI
    volumes:
      - graphdb_data:/opt/graphdb/home
      - ./graphdb_config:/opt/graphdb/dist/configs
    environment:
      GDB_JAVA_OPTS: >-
        -Xmx4g -Xms2g
        -Dgraphdb.home=/opt/graphdb/home
    restart: unless-stopped

  neo4j:
    image: neo4j:5.20-community
    container_name: neo4j_kyc
    ports:
      - "7474:7474"     # Neo4j Browser UI
      - "7687:7687"     # Bolt protocol (driver connections)
    volumes:
      - neo4j_data:/data
      - neo4j_plugins:/plugins
      - ./import:/var/lib/neo4j/import    # For loading local files
    environment:
      NEO4J_AUTH: neo4j/kycpassword123
      NEO4J_PLUGINS: '["apoc", "graph-data-science", "n10s"]'
      NEO4J_dbms_security_procedures_unrestricted: "apoc.*,n10s.*,gds.*"
      NEO4J_dbms_security_procedures_allowlist: "apoc.*,n10s.*,gds.*"
      NEO4J_server_memory_heap_initial__size: "2G"
      NEO4J_server_memory_heap_max__size: "4G"
      NEO4J_server_memory_pagecache_size: "1G"
    restart: unless-stopped

volumes:
  graphdb_data:
  neo4j_data:
  neo4j_plugins:
```

## C.2 Start & Verify

```bash
# Start both databases
docker-compose up -d

# Wait ~60 seconds for startup, then verify
docker-compose ps

# Expected: Both containers "Up"
# GraphDB UI:   http://localhost:7200
# Neo4j Browser: http://localhost:7474  (login: neo4j / kycpassword123)
```

## C.3 Python Environment

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate    # macOS/Linux

# Install all dependencies
pip install \
  rdflib==7.0.0 \
  SPARQLWrapper==2.0.0 \
  rdflib-neo4j==1.0.0 \
  neo4j==5.20.0 \
  requests==2.31.0 \
  httpx==0.27.0 \
  pandas==2.2.0 \
  faker==30.0.0 \
  langchain==0.3.0 \
  langchain-neo4j==0.2.0 \
  langchain-anthropic==0.3.0 \
  langgraph==0.2.0 \
  neo4j-graphrag==1.0.0 \
  openai==1.30.0 \
  streamlit==1.39.0 \
  plotly==5.24.0 \
  python-dotenv==1.0.0 \
  tqdm==4.66.0 \
  pydantic==2.7.0 \
  jupyter==1.0.0
```

## C.4 Environment Variables

Create `.env` in project root:

```env
# GraphDB
GRAPHDB_URL=http://localhost:7200
GRAPHDB_REPO=kyc-kg

# Neo4j
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=kycpassword123

# LLM (choose one or both)
ANTHROPIC_API_KEY=sk-ant-your-key-here
OPENAI_API_KEY=sk-your-key-here
```

---

# PART D — MODULE 1: GraphDB & Ontologies

> **Goal**: Load the FIBO ontology stack into GraphDB, learn SPARQL, understand what ontologies give you.
>
> **Going Meta sessions to study**: S01 (RDF import basics), S05 (ontology-driven KG construction), S08 (RDF integration patterns)

## D.1 Create GraphDB Repository

**What you're doing**: Creating a database in GraphDB with OWL reasoning enabled. The `owl-horst-optimized` ruleset means GraphDB will automatically infer facts you didn't explicitly state (e.g., transitive ownership chains).

### Via Python Script: `scripts/01_setup_graphdb.py`

```python
"""
Step 1: Create the KYC Knowledge Graph repository in GraphDB.
The key setting is 'ruleset: owl-horst-optimized' which enables
OWL inference — GraphDB will automatically derive facts like:
  If A owns B and B owns C → A transitively owns C
"""
import requests
import os
from dotenv import load_dotenv

load_dotenv()
GRAPHDB_URL = os.getenv("GRAPHDB_URL", "http://localhost:7200")
REPO_ID = os.getenv("GRAPHDB_REPO", "kyc-kg")

REPO_CONFIG = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix rep: <http://www.openrdf.org/config/repository#> .
@prefix sr: <http://www.openrdf.org/config/repository/sail#> .
@prefix sail: <http://www.openrdf.org/config/sail#> .
@prefix graphdb: <http://www.ontotext.com/config/graphdb#> .

[] a rep:Repository ;
   rep:repositoryID "{repo_id}" ;
   rdfs:label "KYC Beneficial Ownership Knowledge Graph" ;
   rep:repositoryImpl [
      rep:repositoryType "graphdb:FreeSailRepository" ;
      sr:sailImpl [
         sail:sailType "graphdb:FreeSail" ;
         graphdb:ruleset "owl-horst-optimized" ;
         graphdb:entity-index-size "10000000" ;
         graphdb:entity-id-size "32" ;
         graphdb:enable-context-index "true" ;
         graphdb:enablePredicateList "true" ;
         graphdb:in-memory-literal-properties "true" ;
         graphdb:enable-literal-index "true" ;
      ]
   ] .
""".replace("{repo_id}", REPO_ID)

def create_repo():
    # Write config to temp file
    config_path = "graphdb_config/kyc-repo-config.ttl"
    os.makedirs("graphdb_config", exist_ok=True)
    with open(config_path, "w") as f:
        f.write(REPO_CONFIG)

    # Create via REST API
    with open(config_path, "rb") as f:
        r = requests.post(
            f"{GRAPHDB_URL}/rest/repositories",
            files={"config": f},
            headers={"Accept": "application/json"}
        )
    print(f"Repository '{REPO_ID}' created: HTTP {r.status_code}")

    # Verify
    r = requests.get(f"{GRAPHDB_URL}/rest/repositories")
    repos = [repo["id"] for repo in r.json()]
    print(f"Available repositories: {repos}")

if __name__ == "__main__":
    create_repo()
```

### Via GraphDB UI (Alternative)
1. Open http://localhost:7200
2. Click "Setup" → "Repositories" → "Create new repository"
3. Choose "GraphDB Free" → Set ID: `kyc-kg`
4. Set Ruleset: `OWL-Horst (Optimized)`
5. Click "Create"

**Skill: Understanding Rulesets**
- `owl-horst-optimized`: Best balance of reasoning power and performance. Supports `rdfs:subClassOf` inference, `owl:TransitiveProperty`, `owl:equivalentClass`
- `rdfs-plus-optimized`: Lighter — only RDFS inference (subclass/subproperty)
- `owl2-rl-optimized`: Full OWL 2 RL profile — most powerful but slowest
- **For this project**: `owl-horst-optimized` is the right choice

## D.2 Load FIBO Ontology into GraphDB

**What you're doing**: Loading the "dictionary of finance" into GraphDB. FIBO has 200+ modules — you only need a subset for KYC.

### Understanding: What is an Ontology File?

Ontology files (`.ttl`, `.owl`, `.rdf`) contain RDF triples that define classes and properties:

```turtle
# This is Turtle syntax (.ttl) — most common for FIBO
@prefix fibo-be: <https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

fibo-be:LegalPerson
    a owl:Class ;                                   # "LegalPerson is a Class"
    rdfs:label "legal person" ;                     # "its human-readable name"
    rdfs:subClassOf fibo-be:LegalEntity ;           # "it's a kind of LegalEntity"
    rdfs:comment "An entity with legal rights." .   # "here's what it means"
```

### Script: `scripts/02_load_fibo.py`

```python
"""
Step 2: Load FIBO ontology modules into GraphDB.

CRITICAL: Load order matters — dependencies first!
  Foundation modules → LCC → Business Entities → Ownership & Control

Strategy: Download locally first (more reliable than live URL loading).
"""
import requests
import os
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()
GRAPHDB_URL = os.getenv("GRAPHDB_URL")
REPO_ID = os.getenv("GRAPHDB_REPO")

# ── FIBO Modules to Load (ordered by dependency) ──────────────────────────────
FIBO_MODULES = [
    # === TIER 1: Foundations (everything depends on these) ===
    {
        "url": "https://spec.edmcouncil.org/fibo/ontology/FND/Utilities/AnnotationVocabulary/",
        "graph": "http://kg/fibo/fnd/annotations",
        "name": "FIBO Annotation Vocabulary",
        "why": "Base annotations used by all FIBO modules"
    },
    {
        "url": "https://spec.edmcouncil.org/fibo/ontology/FND/Relations/Relations/",
        "graph": "http://kg/fibo/fnd/relations",
        "name": "FIBO Relations",
        "why": "Core relations like hasName, isClassifiedBy"
    },
    {
        "url": "https://spec.edmcouncil.org/fibo/ontology/FND/AgentsAndPeople/Agents/",
        "graph": "http://kg/fibo/fnd/agents",
        "name": "FIBO Agents",
        "why": "Base class for any actor (person or org)"
    },

    # === TIER 2: LCC (needed by FIBO BE for jurisdictions) ===
    {
        "url": "https://www.omg.org/spec/LCC/Countries/CountryRepresentation/",
        "graph": "http://kg/lcc/countries",
        "name": "LCC Country Representation",
        "why": "Abstract model for countries/territories"
    },
    {
        "url": "https://www.omg.org/spec/LCC/Countries/ISO3166-1-CountryCodes/",
        "graph": "http://kg/lcc/iso3166",
        "name": "LCC ISO 3166-1 Country Codes",
        "why": "Actual country codes (US, GB, DE, KY...)"
    },

    # === TIER 3: Business Entities (the KYC core) ===
    {
        "url": "https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/",
        "graph": "http://kg/fibo/be/legal-persons",
        "name": "FIBO Legal Persons",
        "why": "Defines what a LegalEntity/LegalPerson IS"
    },
    {
        "url": "https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/CorporateBodies/",
        "graph": "http://kg/fibo/be/corporate-bodies",
        "name": "FIBO Corporate Bodies",
        "why": "Corporations, LLCs, partnerships"
    },
    {
        "url": "https://spec.edmcouncil.org/fibo/ontology/BE/OwnershipAndControl/Ownership/",
        "graph": "http://kg/fibo/be/ownership",
        "name": "FIBO Ownership",
        "why": "Ownership relations — the core of UBO tracing"
    },
    {
        "url": "https://spec.edmcouncil.org/fibo/ontology/BE/OwnershipAndControl/Control/",
        "graph": "http://kg/fibo/be/control",
        "name": "FIBO Control",
        "why": "Control relations (board seats, voting rights)"
    },
    {
        "url": "https://spec.edmcouncil.org/fibo/ontology/BE/Corporations/Corporations/",
        "graph": "http://kg/fibo/be/corporations",
        "name": "FIBO Corporations",
        "why": "Corporation-specific properties"
    },

    # === TIER 4: Financial Business & Commerce (optional but useful) ===
    {
        "url": "https://spec.edmcouncil.org/fibo/ontology/FBC/FunctionalEntities/FinancialServicesEntities/",
        "graph": "http://kg/fibo/fbc/fse",
        "name": "FIBO Financial Services Entities",
        "why": "Banks, regulators, financial market participants"
    },
]

def load_from_url(module):
    """Load an ontology from URL directly into GraphDB using SPARQL LOAD."""
    sparql = f"LOAD <{module['url']}> INTO GRAPH <{module['graph']}>"
    r = requests.post(
        f"{GRAPHDB_URL}/repositories/{REPO_ID}/statements",
        data=sparql,
        headers={"Content-Type": "application/sparql-update"},
        timeout=120
    )
    return r.ok, r.status_code

def download_and_load(module, data_dir="data/fibo"):
    """Download TTL locally first, then upload to GraphDB (more reliable)."""
    os.makedirs(data_dir, exist_ok=True)
    filename = module["graph"].split("/")[-1] + ".ttl"
    filepath = os.path.join(data_dir, filename)

    # Download
    if not os.path.exists(filepath):
        r = requests.get(module["url"], headers={"Accept": "text/turtle"}, timeout=60)
        if r.ok:
            with open(filepath, "w") as f:
                f.write(r.text)
        else:
            print(f"  Download failed: {r.status_code}")
            return False, r.status_code

    # Upload to GraphDB
    with open(filepath, "rb") as f:
        r = requests.post(
            f"{GRAPHDB_URL}/repositories/{REPO_ID}/rdf-graphs/service",
            params={"graph": module["graph"]},
            data=f,
            headers={"Content-Type": "text/turtle"}
        )
    return r.ok, r.status_code

if __name__ == "__main__":
    print("Loading FIBO ontology stack into GraphDB...")
    print(f"Target: {GRAPHDB_URL}/repositories/{REPO_ID}\n")

    for module in tqdm(FIBO_MODULES, desc="Loading"):
        ok, status = download_and_load(module)
        icon = "✓" if ok else "✗"
        print(f"  {icon} {module['name']}: HTTP {status}")
        if not ok:
            print(f"    Reason: {module['why']}")
            print(f"    Fallback: trying direct URL load...")
            ok, status = load_from_url(module)
            print(f"    {'✓' if ok else '✗'} URL load: HTTP {status}")

    print("\nDone! Verify in GraphDB Workbench → Repositories → kyc-kg")
```

**Skill: Named Graphs**
Each ontology is loaded into its own **named graph** (like a folder). This lets you:
- Query one ontology at a time: `GRAPH <http://kg/fibo/be/ownership> { ... }`
- Query across all: just omit the `GRAPH` clause
- Delete/replace one without affecting others

## D.3 FIBO2GLEI Mapping Ontology

**What you're doing**: Creating a "translator" that says "FIBO's LegalPerson = GLEIF's RegisteredEntity". Without this, the two datasets can't talk to each other.

```python
# scripts/03_load_fibo2glei_mapping.py
"""
Step 3: Create and load the FIBO-to-GLEIF mapping ontology.
This maps FIBO classes to GLEIF data model classes using owl:equivalentClass.
"""
import requests
import os
from dotenv import load_dotenv

load_dotenv()
GRAPHDB_URL = os.getenv("GRAPHDB_URL")
REPO_ID = os.getenv("GRAPHDB_REPO")

FIBO2GLEI_TTL = """
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix fibo-be: <https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/> .
@prefix fibo-fnd: <https://spec.edmcouncil.org/fibo/ontology/FND/Relations/Relations/> .
@prefix lei: <https://www.gleif.org/ontology/L1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix f2g: <http://kyc-kg.example.org/fibo2glei#> .

<http://kyc-kg.example.org/fibo2glei>
    a owl:Ontology ;
    rdfs:label "FIBO to GLEI Mapping Ontology" ;
    rdfs:comment "Maps between FIBO and GLEIF LEI concepts for KYC use case." .

# Class equivalences
fibo-be:LegalPerson owl:equivalentClass lei:RegisteredEntity .

# Property mappings
lei:legalName rdfs:subPropertyOf fibo-fnd:hasName .
"""

r = requests.post(
    f"{GRAPHDB_URL}/repositories/{REPO_ID}/rdf-graphs/service",
    params={"graph": "http://kg/fibo2glei"},
    data=FIBO2GLEI_TTL.encode(),
    headers={"Content-Type": "text/turtle"}
)
print(f"FIBO2GLEI mapping loaded: HTTP {r.status_code}")
```

## D.4 SPARQL Queries — Exploring the Ontology

**What you're doing**: Querying GraphDB to understand what FIBO contains. Run these in GraphDB Workbench (http://localhost:7200 → SPARQL tab) or via Python.

### Script: `scripts/04_sparql_exploration.py`

```python
"""
Step 4: Learn SPARQL by exploring what you loaded.
Each query teaches a SPARQL concept.
"""
from SPARQLWrapper import SPARQLWrapper, JSON
import os
from dotenv import load_dotenv

load_dotenv()
GRAPHDB_URL = os.getenv("GRAPHDB_URL")
REPO_ID = os.getenv("GRAPHDB_REPO")

sparql = SPARQLWrapper(f"{GRAPHDB_URL}/repositories/{REPO_ID}")
sparql.setReturnFormat(JSON)

def run_query(name, query, limit=10):
    """Execute a SPARQL query and print results."""
    print(f"\n{'='*60}")
    print(f"QUERY: {name}")
    print(f"{'='*60}")
    sparql.setQuery(query)
    results = sparql.query().convert()
    rows = results["results"]["bindings"]
    for row in rows[:limit]:
        print({k: v["value"] for k, v in row.items()})
    print(f"({len(rows)} results total)")
    return rows

# ── Query 1: What named graphs exist? ─────────────────────────────────────────
# TEACHES: Named graphs, COUNT, GROUP BY, ORDER BY
run_query("List All Named Graphs", """
    SELECT ?graph (COUNT(?s) as ?triples)
    WHERE { GRAPH ?graph { ?s ?p ?o } }
    GROUP BY ?graph
    ORDER BY DESC(?triples)
""")

# ── Query 2: What OWL classes are in FIBO? ─────────────────────────────────────
# TEACHES: Filtering by type, OPTIONAL for labels, FILTER with CONTAINS
run_query("FIBO OWL Classes", """
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?class ?label WHERE {
        ?class a owl:Class .
        OPTIONAL { ?class rdfs:label ?label }
        FILTER(CONTAINS(STR(?class), "edmcouncil.org"))
    }
    LIMIT 30
""")

# ── Query 3: FIBO Class Hierarchy (subclass chain) ────────────────────────────
# TEACHES: rdfs:subClassOf traversal — the backbone of ontology structure
run_query("FIBO Class Hierarchy", """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?child ?childLabel ?parent ?parentLabel WHERE {
        ?child rdfs:subClassOf ?parent .
        FILTER(CONTAINS(STR(?child), "edmcouncil.org"))
        FILTER(CONTAINS(STR(?parent), "edmcouncil.org"))
        OPTIONAL { ?child rdfs:label ?childLabel }
        OPTIONAL { ?parent rdfs:label ?parentLabel }
    }
    LIMIT 30
""")

# ── Query 4: Properties defined in FIBO Ownership module ──────────────────────
# TEACHES: OWL ObjectProperty, domain/range — what connects to what
run_query("FIBO Ownership Properties", """
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?prop ?label ?domain ?range WHERE {
        GRAPH <http://kg/fibo/be/ownership> {
            ?prop a owl:ObjectProperty .
            OPTIONAL { ?prop rdfs:label ?label }
            OPTIONAL { ?prop rdfs:domain ?domain }
            OPTIONAL { ?prop rdfs:range ?range }
        }
    }
""")

# ── Query 5: SPARQL Property Paths (traversal) ────────────────────────────────
# TEACHES: The + operator = "one or more hops" (like Cypher *1..n)
run_query("Subclass Chains (Property Paths)", """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX fibo-be: <https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/>
    SELECT ?descendant ?label WHERE {
        ?descendant rdfs:subClassOf+ fibo-be:LegalPerson .
        OPTIONAL { ?descendant rdfs:label ?label }
    }
""")

# ── Query 6: Cross-graph query (shows power of named graphs) ──────────────────
# TEACHES: Querying across multiple named graphs simultaneously
run_query("Cross-Graph: Ownership + Legal Entity Classes", """
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?class ?label ?fromGraph WHERE {
        GRAPH ?fromGraph {
            ?class a owl:Class .
            ?class rdfs:label ?label .
        }
        FILTER(?fromGraph IN (
            <http://kg/fibo/be/ownership>,
            <http://kg/fibo/be/legal-persons>
        ))
    }
    ORDER BY ?fromGraph
""")
```

**Skill: SPARQL Cheat Sheet**

| Pattern | Meaning | Example |
|---|---|---|
| `?s ?p ?o` | Match any triple | Basic pattern |
| `FILTER(...)` | Filter results | `FILTER(?age > 30)` |
| `OPTIONAL { }` | Left join (may be null) | `OPTIONAL { ?x rdfs:label ?label }` |
| `GRAPH <uri> { }` | Restrict to named graph | `GRAPH <http://kg/fibo> { ... }` |
| `?a :rel+ ?b` | One or more hops | Property path (like Cypher `*1..n`) |
| `?a :rel* ?b` | Zero or more hops | Includes self |
| `VALUES ?x { 1 2 3 }` | Inline data | Like SQL `IN (1,2,3)` |
| `BIND(expr AS ?var)` | Compute a value | `BIND(STR(?x) AS ?name)` |
| `GROUP BY` / `HAVING` | Aggregation | Same as SQL |

---

# PART E — MODULE 2: Neo4j & neosemantics Bridge

> **Goal**: Set up Neo4j, initialize neosemantics (n10s), import the FIBO ontology structure, understand the Barrasa Bridge Pattern.
>
> **Going Meta sessions to study**: S01 (n10s basics), S03 (SHACL), S05 (ontology-driven construction), S12 (RDFLib→Neo4j), S18 (triplestore migration)

## E.1 Initialize neosemantics (n10s)

**What you're doing**: Configuring n10s to handle the translation between RDF (GraphDB world) and property graph (Neo4j world).

Open Neo4j Browser at http://localhost:7474 and run:

```cypher
// Step 1: Initialize n10s configuration
// handleVocabUris: SHORTEN turns long URIs into short prefixes
// handleRDFTypes: LABELS_AND_NODES creates both labels and type nodes
// applyNeo4jNaming: true converts camelCase to Neo4j-friendly names
CALL n10s.graphconfig.init({
  handleVocabUris: 'SHORTEN',
  handleMultival: 'ARRAY',
  handleRDFTypes: 'LABELS_AND_NODES',
  keepLangTag: false,
  applyNeo4jNaming: true
});

// Step 2: Create the required uniqueness constraint
// n10s uses 'uri' property to track RDF origins of each node
CREATE CONSTRAINT n10s_unique_uri IF NOT EXISTS
FOR (r:Resource) REQUIRE r.uri IS UNIQUE;
```

**Skill: n10s Configuration Options**

| Option | Values | Meaning |
|---|---|---|
| `handleVocabUris` | `SHORTEN`, `IGNORE`, `KEEP`, `MAP` | How to handle long URIs. SHORTEN creates prefix mappings |
| `handleMultival` | `OVERWRITE`, `ARRAY` | What to do when same property has multiple values |
| `handleRDFTypes` | `LABELS`, `NODES`, `LABELS_AND_NODES` | How to represent `rdf:type` — as Neo4j labels, separate nodes, or both |
| `applyNeo4jNaming` | true/false | Convert `camelCase` URIs to Neo4j-friendly names |

## E.2 Import FIBO Ontology Structure into Neo4j

**What you're doing**: Using `n10s.onto.import.fetch()` to pull the ontology CLASS structure into Neo4j. This creates `:Class` and `:Property` nodes — the "schema" of your knowledge graph.

```cypher
// Import FIBO Legal Persons ontology (class definitions)
CALL n10s.onto.import.fetch(
  "https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/",
  "Turtle"
);

// Import FIBO Ownership ontology
CALL n10s.onto.import.fetch(
  "https://spec.edmcouncil.org/fibo/ontology/BE/OwnershipAndControl/Ownership/",
  "Turtle"
);

// Import FIBO Control ontology
CALL n10s.onto.import.fetch(
  "https://spec.edmcouncil.org/fibo/ontology/BE/OwnershipAndControl/Control/",
  "Turtle"
);

// ── Verify: What classes did we get? ──────────────────────────────────────────
MATCH (c:Class)
RETURN c.name AS className, c.uri AS classURI
ORDER BY c.name
LIMIT 30;

// ── Verify: What's the class hierarchy? ───────────────────────────────────────
// SCO = SubClass Of
MATCH path = (child:Class)-[:SCO*1..3]->(parent:Class)
WHERE parent.name = 'LegalPerson'
RETURN path
LIMIT 50;

// ── Verify: What properties exist? ────────────────────────────────────────────
MATCH (p:Property)
RETURN p.name, p.uri
ORDER BY p.name
LIMIT 30;
```

**Key distinction**:
- `n10s.onto.import.fetch()` → imports **ontology structure** (classes, properties, hierarchy)
- `n10s.rdf.import.fetch()` → imports **instance data** (actual companies, people)

## E.3 Import RDF Instance Data via n10s

**What you're doing**: Loading actual entity data (from GLEIF or your GraphDB export) into Neo4j.

```cypher
// Option A: Load from a local TTL file (copy to Neo4j import folder first)
// First: docker cp data/glei/sample.ttl neo4j_kyc:/var/lib/neo4j/import/
CALL n10s.rdf.import.fetch(
  "file:///var/lib/neo4j/import/sample.ttl",
  "Turtle"
);

// Option B: Load directly from GraphDB's SPARQL endpoint
// (This queries GraphDB and imports results into Neo4j)
CALL n10s.rdf.import.fetch(
  "http://graphdb:7200/repositories/kyc-kg",
  "Turtle",
  { headerParams: { Accept: "text/turtle" } }
);

// Verify loaded data
MATCH (e:RegisteredEntity)
RETURN e.leiCode, e.legalName, e.legalJurisdiction
LIMIT 20;
```

## E.4 The Barrasa Bridge Pattern — All 4 Techniques

> From Going Meta series: The pattern of using GraphDB for semantic truth and Neo4j for operational power.

### Technique 1: Ontology Import (covered above)
```cypher
CALL n10s.onto.import.fetch(url, format);    -- Schema: Classes + Properties
```

### Technique 2: Data Import
```cypher
CALL n10s.rdf.import.fetch(url, format);     -- Instances: Actual entities
```

### Technique 3: Export Neo4j → RDF (round-trip back to GraphDB)
```cypher
// Export high-risk entities as RDF triples
CALL n10s.rdf.export.cypher(
  'MATCH (e:LegalEntity) WHERE e.riskTier = "high" RETURN e',
  {}
)
YIELD data
RETURN data;
// Then POST this RDF data to GraphDB via REST API
```

### Technique 4: Neo4j as RDF Endpoint
```
// n10s exposes Neo4j as an RDF endpoint at:
// http://localhost:7474/rdf/neo4j/describe/node
// GraphDB can query Neo4j via SPARQL SERVICE clause (federation)
```

---

# PART F — MODULE 3: DATA LOADING

> **Goal**: Load real GLEIF data + generate synthetic KYC test data with embedded crime patterns.
>
> **Going Meta sessions to study**: S29 (ontology-guided extraction), S30 (Pydantic structured output), S32 (mixed data ingestion)

## F.1 Load GLEIF Data into GraphDB

**What you're doing**: Fetching real company data from GLEIF's free API and converting it to FIBO-aligned RDF.

### Script: `scripts/05_load_gleif_data.py`

```python
"""
Step 5: Load GLEIF LEI data into GraphDB as FIBO-aligned RDF.

The distinction:
  Ontology (FIBO) = SCHEMA — what a LegalEntity IS conceptually
  GLEIF data      = INSTANCES — "Apple Inc" IS a LegalEntity with LEI=5493001KJTIIGC8Y1R12

We align GLEIF data to FIBO by using FIBO class URIs as rdf:type.
"""
import requests
import json
import os
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, RDFS, XSD, OWL
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()
GRAPHDB_URL = os.getenv("GRAPHDB_URL")
REPO_ID = os.getenv("GRAPHDB_REPO")

# Namespaces — these must match what FIBO uses
FIBO_BE = Namespace("https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/")
KYC = Namespace("http://kyc-kg.example.org/ontology#")
LEI_NS = Namespace("https://www.gleif.org/data/lei/")
GLEIF_API = "https://api.gleif.org/api/v1"

def fetch_entities(country_code="US", page_size=50):
    """Fetch LEI records from GLEIF free API."""
    r = requests.get(
        f"{GLEIF_API}/lei-records",
        params={
            "filter[entity.legalJurisdiction]": country_code,
            "page[size]": page_size,
            "page[number]": 1,
        },
        timeout=30
    )
    r.raise_for_status()
    return r.json()["data"]

def entities_to_rdf(records):
    """Convert GLEIF API response to FIBO-aligned RDF graph."""
    g = Graph()
    g.bind("fibo-be", FIBO_BE)
    g.bind("kyc", KYC)
    g.bind("lei", LEI_NS)

    for record in records:
        attrs = record["attributes"]
        lei_code = attrs["lei"]
        entity = attrs.get("entity", {})

        entity_uri = URIRef(f"https://www.gleif.org/data/lei/{lei_code}")

        # Type assertion using FIBO class — this is the ontology alignment step
        g.add((entity_uri, RDF.type, FIBO_BE.LegalPerson))
        g.add((entity_uri, KYC.leiCode, Literal(lei_code)))

        legal_name = entity.get("legalName", {}).get("name")
        if legal_name:
            g.add((entity_uri, RDFS.label, Literal(legal_name)))
            g.add((entity_uri, KYC.legalName, Literal(legal_name)))

        jurisdiction = entity.get("jurisdiction")
        if jurisdiction:
            g.add((entity_uri, KYC.hasJurisdiction, Literal(jurisdiction)))

        # Address
        addr = entity.get("legalAddress", {})
        if addr:
            addr_uri = URIRef(f"http://kyc-kg.example.org/address/{lei_code}")
            g.add((entity_uri, KYC.hasLegalAddress, addr_uri))
            g.add((addr_uri, RDF.type, KYC.Address))
            for prop, key in [("city", "city"), ("country", "country"), ("postalCode", "postalCode")]:
                if addr.get(key):
                    g.add((addr_uri, KYC[prop], Literal(addr[key])))

    return g

def load_to_graphdb(g, named_graph):
    """Upload RDF graph to GraphDB."""
    ttl = g.serialize(format="turtle")
    r = requests.post(
        f"{GRAPHDB_URL}/repositories/{REPO_ID}/rdf-graphs/service",
        params={"graph": named_graph},
        data=ttl.encode("utf-8"),
        headers={"Content-Type": "text/turtle"},
        timeout=60
    )
    return r.status_code

if __name__ == "__main__":
    os.makedirs("data/glei", exist_ok=True)

    # Fetch from multiple jurisdictions for diversity
    all_records = []
    for country in ["US", "GB", "DE", "JP", "CH", "KY"]:  # KY = Cayman Islands
        print(f"Fetching entities from {country}...")
        try:
            records = fetch_entities(country, page_size=50)
            all_records.extend(records)
            print(f"  Got {len(records)} entities")
        except Exception as e:
            print(f"  Failed: {e}")

    # Save raw JSON
    with open("data/glei/raw_records.json", "w") as f:
        json.dump(all_records, f, indent=2)

    # Convert to RDF
    g = entities_to_rdf(all_records)
    ttl = g.serialize(format="turtle")
    with open("data/glei/entities.ttl", "w") as f:
        f.write(ttl)
    print(f"\nGenerated {len(g)} RDF triples → data/glei/entities.ttl")

    # Load into GraphDB
    status = load_to_graphdb(g, "http://kg/glei/instances")
    print(f"Loaded to GraphDB: HTTP {status}")
```

## F.2 Generate Synthetic KYC Dataset

**What you're doing**: Creating a realistic test dataset with known crime patterns (sanctioned UBOs hidden behind shell companies, circular ownership rings, suspicious transactions). This is your "ground truth" for testing.

### Script: `scripts/06_generate_synthetic_data.py`

```python
"""
Step 6: Generate synthetic KYC dataset with embedded financial crime patterns.

Creates:
  - 500 legal entities with LEI-like codes
  - 200 natural persons (directors, UBOs)
  - Ownership chains up to 8 levels deep
  - 3 hidden sanctioned UBOs (test: can your system find them?)
  - 5 circular ownership rings (test: can your system detect them?)
  - 2 PEP (Politically Exposed Person) connections
  - 1,000 transactions with structuring patterns
"""
import json
import random
import string
from faker import Faker
from datetime import datetime, timedelta
import os

fake = Faker(["en_US", "en_GB", "de_DE"])
random.seed(42)  # Reproducible

def gen_lei():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=20))

JURISDICTIONS = [
    ("US", "United States", "low"),
    ("GB", "United Kingdom", "low"),
    ("DE", "Germany", "low"),
    ("JP", "Japan", "low"),
    ("SG", "Singapore", "medium"),
    ("CH", "Switzerland", "medium"),
    ("KY", "Cayman Islands", "high"),
    ("VG", "British Virgin Islands", "high"),
    ("PA", "Panama", "high"),
    ("SC", "Seychelles", "high"),
]

def generate_entities(n=500):
    entities = []
    for i in range(n):
        juris = random.choice(JURISDICTIONS)
        entities.append({
            "id": f"ENTITY_{i:04d}",
            "lei": gen_lei(),
            "name": fake.company(),
            "jurisdiction": juris[0],
            "jurisdiction_name": juris[1],
            "risk_tier": juris[2],
            "category": random.choice(["BRANCH", "FUND", "TRUST", "PARTNERSHIP", "LIMITED_PARTNERSHIP"]),
            "incorporated_date": fake.date_between(start_date="-30y", end_date="-1y").isoformat(),
            "is_active": random.random() > 0.1,
            "has_operational_address": random.random() > 0.3,
        })
    return entities

def generate_persons(n=200):
    persons = []
    for i in range(n):
        persons.append({
            "id": f"PERSON_{i:04d}",
            "name": fake.name(),
            "nationality": random.choice([j[0] for j in JURISDICTIONS]),
            "dob": fake.date_of_birth(minimum_age=30, maximum_age=80).isoformat(),
            "is_pep": i < 10,        # First 10 are PEPs
            "is_sanctioned": i < 3,   # First 3 are sanctioned (GROUND TRUTH)
        })
    return persons

def generate_ownership(entities, persons):
    """Create ownership chains including crime patterns."""
    rels = []

    # Normal corporate hierarchy
    for i, entity in enumerate(entities[:300]):
        if i > 20 and random.random() > 0.4:
            parent = random.choice(entities[:i])
            rels.append({
                "from": entity["id"], "to": parent["id"],
                "type": "DIRECTLY_OWNED_BY",
                "percentage": round(random.uniform(50, 100), 2),
                "since": fake.date_between(start_date="-10y", end_date="-1y").isoformat(),
            })
        if random.random() > 0.5:
            person = random.choice(persons)
            rels.append({
                "from": entity["id"], "to": person["id"],
                "type": "CONTROLLED_BY",
                "role": random.choice(["Director", "CEO", "Shareholder", "Nominee"]),
                "since": fake.date_between(start_date="-10y", end_date="-1y").isoformat(),
            })

    # CRIME PATTERN 1: Sanctioned UBO hidden behind 3 shell companies
    sanctioned = persons[0]  # Known sanctioned person
    for entity in random.sample(entities[100:150], 5):
        shell1 = random.choice(entities[300:350])
        shell2 = random.choice(entities[350:400])
        rels.extend([
            {"from": entity["id"], "to": shell1["id"], "type": "DIRECTLY_OWNED_BY",
             "percentage": 100.0, "since": "2018-01-01"},
            {"from": shell1["id"], "to": shell2["id"], "type": "DIRECTLY_OWNED_BY",
             "percentage": 100.0, "since": "2018-01-01"},
            {"from": shell2["id"], "to": sanctioned["id"], "type": "CONTROLLED_BY",
             "role": "Ultimate Beneficial Owner", "since": "2018-01-01"},
        ])

    # CRIME PATTERN 2: Circular ownership rings (A→B→C→A)
    for i in range(5):
        ring = random.sample(entities[400:450], 3)
        rels.extend([
            {"from": ring[0]["id"], "to": ring[1]["id"], "type": "DIRECTLY_OWNED_BY",
             "percentage": 51.0, "since": "2020-01-01"},
            {"from": ring[1]["id"], "to": ring[2]["id"], "type": "DIRECTLY_OWNED_BY",
             "percentage": 51.0, "since": "2020-01-01"},
            {"from": ring[2]["id"], "to": ring[0]["id"], "type": "DIRECTLY_OWNED_BY",
             "percentage": 51.0, "since": "2020-01-01"},
        ])

    return rels

def generate_transactions(entities, n=1000):
    txns = []
    ids = [e["id"] for e in entities]
    for i in range(n):
        amount = random.choice([
            random.uniform(9000, 9999),       # Structuring (just below $10k reporting)
            random.uniform(100, 5000),         # Normal
            random.uniform(100000, 5000000),   # Large
        ])
        txns.append({
            "id": f"TXN_{i:05d}",
            "from_entity": random.choice(ids),
            "to_entity": random.choice(ids),
            "amount": round(amount, 2),
            "currency": random.choice(["USD", "EUR", "GBP", "CHF"]),
            "date": fake.date_between(start_date="-2y", end_date="today").isoformat(),
            "is_suspicious": 9000 < amount < 10000,
        })
    return txns

if __name__ == "__main__":
    os.makedirs("data/synthetic", exist_ok=True)

    entities = generate_entities(500)
    persons = generate_persons(200)
    rels = generate_ownership(entities, persons)
    txns = generate_transactions(entities, 1000)

    dataset = {"entities": entities, "persons": persons,
               "relationships": rels, "transactions": txns}

    with open("data/synthetic/kyc_dataset.json", "w") as f:
        json.dump(dataset, f, indent=2)

    print(f"Generated: {len(entities)} entities, {len(persons)} persons, "
          f"{len(rels)} relationships, {len(txns)} transactions")
    print(f"Sanctioned persons: {sum(1 for p in persons if p['is_sanctioned'])}")
    print(f"PEPs: {sum(1 for p in persons if p['is_pep'])}")
    print(f"Saved to data/synthetic/kyc_dataset.json")
```

## F.3 Load Synthetic Data into Neo4j

### Script: `scripts/07_load_neo4j.py`

```python
"""
Step 7: Load synthetic KYC data into Neo4j.

Two loading patterns demonstrated:
  1. n10s import — for ontology + RDF data from GraphDB (covered in Module 2)
  2. Direct Cypher MERGE — for JSON/CSV data (used here)
"""
from neo4j import GraphDatabase
import json
import os
from dotenv import load_dotenv

load_dotenv()
driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))
)

def run(cypher, params=None):
    with driver.session() as s:
        return list(s.run(cypher, params or {}))

# ── Step 1: Create indexes (ALWAYS do this before bulk loading) ────────────────
INDEXES = [
    "CREATE INDEX entity_lei IF NOT EXISTS FOR (e:LegalEntity) ON (e.lei)",
    "CREATE INDEX entity_name IF NOT EXISTS FOR (e:LegalEntity) ON (e.name)",
    "CREATE INDEX entity_id IF NOT EXISTS FOR (e:LegalEntity) ON (e.id)",
    "CREATE INDEX person_id IF NOT EXISTS FOR (p:NaturalPerson) ON (p.id)",
    "CREATE INDEX person_name IF NOT EXISTS FOR (p:NaturalPerson) ON (p.name)",
    "CREATE CONSTRAINT entity_lei_unique IF NOT EXISTS FOR (e:LegalEntity) REQUIRE e.lei IS UNIQUE",
]
for idx in INDEXES:
    run(idx)
print("Indexes and constraints created")

# ── Step 2: Load dataset ──────────────────────────────────────────────────────
with open("data/synthetic/kyc_dataset.json") as f:
    dataset = json.load(f)

with driver.session() as session:
    # Load legal entities
    session.run("""
    UNWIND $entities AS e
    MERGE (n:LegalEntity {lei: e.lei})
    SET n.id = e.id, n.name = e.name,
        n.jurisdiction = e.jurisdiction,
        n.jurisdictionName = e.jurisdiction_name,
        n.riskTier = e.risk_tier,
        n.category = e.category,
        n.incorporatedDate = e.incorporated_date,
        n.isActive = e.is_active,
        n.hasOperationalAddress = e.has_operational_address
    """, entities=dataset["entities"])
    print(f"Loaded {len(dataset['entities'])} entities")

    # Load natural persons (with Sanctioned/PEP labels)
    session.run("""
    UNWIND $persons AS p
    MERGE (n:NaturalPerson {id: p.id})
    SET n.name = p.name, n.nationality = p.nationality,
        n.dob = p.dob, n.isPEP = p.is_pep, n.isSanctioned = p.is_sanctioned
    WITH n, p
    FOREACH (_ IN CASE WHEN p.is_sanctioned THEN [1] ELSE [] END |
      SET n:SanctionedEntity)
    FOREACH (_ IN CASE WHEN p.is_pep THEN [1] ELSE [] END |
      SET n:PoliticallyExposedPerson)
    """, persons=dataset["persons"])
    print(f"Loaded {len(dataset['persons'])} persons")

    # Load ownership relationships
    session.run("""
    UNWIND $rels AS r
    MATCH (from {id: r.from})
    MATCH (to {id: r.to})
    CALL apoc.create.relationship(from, r.type,
        {percentage: r.percentage, since: r.since, role: r.role}, to)
    YIELD rel RETURN count(rel)
    """, rels=dataset["relationships"])
    print(f"Loaded {len(dataset['relationships'])} relationships")

    # Load transactions
    session.run("""
    UNWIND $txns AS t
    MATCH (from:LegalEntity {id: t.from_entity})
    MATCH (to:LegalEntity {id: t.to_entity})
    CREATE (from)-[:TRANSACTION {
        id: t.id, amount: t.amount, currency: t.currency,
        date: t.date, isSuspicious: t.is_suspicious
    }]->(to)
    """, txns=dataset["transactions"])
    print(f"Loaded {len(dataset['transactions'])} transactions")

print("Neo4j loading complete!")
driver.close()
```

## F.4 The Neo4j Graph Schema

After loading, your Neo4j graph has this structure:

```
Node Labels:
  (:LegalEntity)               — companies, corps, trusts
  (:NaturalPerson)              — actual humans
  (:SanctionedEntity)           — additional label on sanctioned nodes
  (:PoliticallyExposedPerson)   — additional label on PEPs
  (:Class)                      — FIBO ontology classes (from n10s)
  (:Property)                   — FIBO properties (from n10s)

Relationship Types:
  -[:DIRECTLY_OWNED_BY {percentage, since}]->    — corporate ownership
  -[:CONTROLLED_BY {role, since}]->              — human control
  -[:TRANSACTION {amount, currency, date}]->     — money flow
  -[:SCO]->                                       — SubClassOf (ontology)
  -[:DOMAIN]->                                    — property domain (ontology)
  -[:RANGE]->                                     — property range (ontology)
```

---

# PART G — MODULE 4: GRAPH ALGORITHMS (GDS)

> **Goal**: Use Neo4j Graph Data Science to detect suspicious patterns, score risk, find hidden structures.
>
> **This is impossible in GraphDB** — graph algorithms are the primary reason Neo4j exists in this stack.
>
> **Going Meta sessions to study**: S06 (ontology learning from graph data)

## G.1 Why GDS for KYC?

| Algorithm | KYC Use Case | What it finds |
|---|---|---|
| **WCC** (Weakly Connected Components) | Entity clustering | Isolated groups of related entities |
| **Louvain** (Community Detection) | Shell company rings | Tight communities = potential collusion |
| **PageRank** | Systemic risk scoring | Most connected/influential entities |
| **Betweenness Centrality** | Bridge entity detection | Entities that sit between money flows |
| **SCC** (Strongly Connected) | Circular ownership | A→B→C→A rings |
| **Shortest Path** | Investigation links | How two entities connect |
| **Node Similarity** | Look-alike detection | Entities similar to known bad actors |

## G.2 GDS Workflow: Project → Algorithm → Write Back

### Script: `scripts/08_gds_analysis.py`

```python
"""
Step 8: Graph Data Science algorithms for KYC risk analysis.

GDS workflow (3 steps for every algorithm):
  1. PROJECT — create an in-memory subgraph from Neo4j
  2. STREAM/WRITE — run the algorithm
  3. USE — query the results in Cypher

All algorithms are run via Cypher using CALL gds.<algorithm>.*
"""
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()
driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))
)

def run(cypher, params=None):
    with driver.session() as s:
        return [dict(r) for r in s.run(cypher, params or {})]

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1: Create GDS Graph Projection
# ══════════════════════════════════════════════════════════════════════════════
print("Creating GDS graph projection...")

# Drop if exists (idempotent)
run("CALL gds.graph.drop('kyc-graph', false) YIELD graphName")

# Project: select which nodes/relationships to analyze
run("""
CALL gds.graph.project(
  'kyc-graph',                                    -- projection name
  ['LegalEntity', 'NaturalPerson'],               -- node labels to include
  {
    DIRECTLY_OWNED_BY: {
      orientation: 'UNDIRECTED',                   -- treat as bidirectional
      properties: ['percentage']                   -- include this property
    },
    CONTROLLED_BY: {
      orientation: 'UNDIRECTED'
    },
    TRANSACTION: {
      orientation: 'NATURAL',                      -- keep direction
      properties: ['amount']
    }
  }
)
""")
print("  Graph projected into GDS memory")

# ══════════════════════════════════════════════════════════════════════════════
# ALGORITHM 1: Weakly Connected Components (WCC)
# Finds: Isolated clusters of related entities
# ══════════════════════════════════════════════════════════════════════════════
print("\n--- Algorithm 1: Weakly Connected Components ---")
run("CALL gds.wcc.write('kyc-graph', {writeProperty: 'componentId'})")

result = run("""
MATCH (n) WHERE n.componentId IS NOT NULL
WITH n.componentId AS comp, count(*) AS size
WHERE size > 5
RETURN comp, size ORDER BY size DESC LIMIT 10
""")
for r in result[:5]:
    print(f"  Component {r['comp']}: {r['size']} entities")

# ══════════════════════════════════════════════════════════════════════════════
# ALGORITHM 2: Louvain Community Detection
# Finds: Tight-knit groups (potential collusion rings)
# ══════════════════════════════════════════════════════════════════════════════
print("\n--- Algorithm 2: Louvain Community Detection ---")
run("CALL gds.louvain.write('kyc-graph', {writeProperty: 'communityId'})")

result = run("""
MATCH (n) WHERE n.communityId IS NOT NULL
WITH n.communityId AS comm, collect(n.name)[0..5] AS sample, count(*) AS size
WHERE size BETWEEN 3 AND 10
RETURN comm, size, sample
ORDER BY size DESC LIMIT 10
""")
print("Suspicious communities (3-10 members):")
for r in result[:5]:
    print(f"  Community {r['comm']}: {r['size']} members — {r['sample']}")

# ══════════════════════════════════════════════════════════════════════════════
# ALGORITHM 3: PageRank (Systemic Risk)
# Finds: Most connected/influential entities
# ══════════════════════════════════════════════════════════════════════════════
print("\n--- Algorithm 3: PageRank ---")
run("""
CALL gds.pageRank.write('kyc-graph', {
  writeProperty: 'pageRankScore',
  dampingFactor: 0.85,
  maxIterations: 20
})
""")

result = run("""
MATCH (n:LegalEntity) WHERE n.pageRankScore IS NOT NULL
RETURN n.name, n.jurisdiction, n.riskTier,
       round(n.pageRankScore, 4) AS score
ORDER BY score DESC LIMIT 10
""")
print("Most systemically important entities:")
for r in result:
    print(f"  {r['n.name']} ({r['n.jurisdiction']}, {r['n.riskTier']}): {r['score']}")

# ══════════════════════════════════════════════════════════════════════════════
# ALGORITHM 4: Betweenness Centrality
# Finds: Bridge entities (money laundering conduits)
# ══════════════════════════════════════════════════════════════════════════════
print("\n--- Algorithm 4: Betweenness Centrality ---")
run("CALL gds.betweenness.write('kyc-graph', {writeProperty: 'betweennessScore'})")

result = run("""
MATCH (n:LegalEntity) WHERE n.betweennessScore > 0
RETURN n.name, n.jurisdiction, round(n.betweennessScore, 2) AS score
ORDER BY score DESC LIMIT 10
""")
print("Bridge entities (high betweenness = potential conduits):")
for r in result[:5]:
    print(f"  {r['n.name']} ({r['n.jurisdiction']}): {r['score']}")

# ══════════════════════════════════════════════════════════════════════════════
# ALGORITHM 5: Strongly Connected Components (Circular Ownership)
# Finds: A→B→C→A ownership rings
# ══════════════════════════════════════════════════════════════════════════════
print("\n--- Algorithm 5: SCC (Circular Ownership Detection) ---")

# Need a directed projection for SCC
run("CALL gds.graph.drop('kyc-directed', false) YIELD graphName")
run("""
CALL gds.graph.project(
  'kyc-directed',
  'LegalEntity',
  {DIRECTLY_OWNED_BY: {orientation: 'NATURAL'}}
)
""")
run("CALL gds.scc.write('kyc-directed', {writeProperty: 'sccId'})")

result = run("""
MATCH (n:LegalEntity) WHERE n.sccId IS NOT NULL
WITH n.sccId AS scc, collect(n.name) AS members, count(*) AS size
WHERE size > 1
RETURN scc, size, members
""")
print(f"Circular ownership rings found: {len(result)}")
for r in result:
    print(f"  Ring ({r['size']} entities): {r['members']}")

# ══════════════════════════════════════════════════════════════════════════════
# COMPOSITE: KYC Risk Score (combining all signals)
# ══════════════════════════════════════════════════════════════════════════════
print("\n--- Computing Composite KYC Risk Scores ---")
run("""
MATCH (n:LegalEntity)
WHERE n.pageRankScore IS NOT NULL AND n.betweennessScore IS NOT NULL
SET n.kycRiskScore = (
  CASE n.riskTier
    WHEN 'high' THEN 50
    WHEN 'medium' THEN 25
    ELSE 0 END
  + (n.pageRankScore * 100)
  + (n.betweennessScore / 100)
  + CASE WHEN NOT n.isActive THEN 20 ELSE 0 END
  + CASE WHEN NOT n.hasOperationalAddress THEN 15 ELSE 0 END
)
""")

result = run("""
MATCH (n:LegalEntity) WHERE n.kycRiskScore IS NOT NULL
RETURN n.name, n.lei, n.jurisdiction, n.riskTier,
       round(n.kycRiskScore, 2) AS riskScore
ORDER BY riskScore DESC LIMIT 15
""")
print("\nTOP 15 HIGHEST KYC RISK ENTITIES:")
for r in result:
    print(f"  [{r['riskScore']}] {r['n.name']} — {r['n.jurisdiction']} ({r['n.riskTier']})")

driver.close()
```

**Skill: GDS Cheat Sheet**

| Step | Cypher Pattern | Purpose |
|---|---|---|
| Project | `CALL gds.graph.project('name', nodes, rels)` | Create in-memory subgraph |
| Stream | `CALL gds.<algo>.stream('name', {config})` | Get results as rows (read-only) |
| Write | `CALL gds.<algo>.write('name', {writeProperty: 'prop'})` | Write results back to nodes |
| Mutate | `CALL gds.<algo>.mutate('name', {mutateProperty: 'prop'})` | Write to in-memory graph only |
| Stats | `CALL gds.<algo>.stats('name', {config})` | Just get statistics |
| Drop | `CALL gds.graph.drop('name')` | Free memory |

---

# PART H — MODULE 5: SHACL VALIDATION & DATA QUALITY

> **Goal**: Define rules about what valid data looks like, then automatically check your graph against those rules.
>
> **Going Meta sessions to study**: S03 (SHACL basics), S11 (Graph Expectations), S44 (modern SHACL on Neo4j)
>
> **Why this matters**: Regulators require data quality evidence. SHACL gives you auditable validation.

## H.1 What is SHACL?

SHACL (Shapes Constraint Language) defines "shapes" — rules like:
- "Every LegalEntity MUST have a `lei` property" (cardinality)
- "The `lei` must be exactly 20 characters" (string constraint)
- "Every LegalEntity MUST be `REGISTERED_IN` at least one Jurisdiction" (relationship constraint)

## H.2 Define KYC SHACL Shapes

Create `shacl/kyc_shapes.ttl`:

```turtle
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix kyc: <http://kyc-kg.example.org/ontology#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

# ── Shape 1: Every LegalEntity must have a valid LEI ──────────────────────────
kyc:LegalEntityShape
    a sh:NodeShape ;
    sh:targetClass kyc:LegalEntity ;
    sh:property [
        sh:path kyc:leiCode ;
        sh:minCount 1 ;
        sh:maxCount 1 ;
        sh:datatype xsd:string ;
        sh:minLength 20 ;
        sh:maxLength 20 ;
        sh:pattern "^[A-Z0-9]{20}$" ;
        sh:name "LEI Code" ;
        sh:message "LEI must be exactly 20 alphanumeric characters" ;
        sh:severity sh:Violation ;
    ] ;
    sh:property [
        sh:path kyc:legalName ;
        sh:minCount 1 ;
        sh:name "Legal Name" ;
        sh:message "Every entity must have a legal name" ;
        sh:severity sh:Violation ;
    ] ;
    sh:property [
        sh:path kyc:hasJurisdiction ;
        sh:minCount 1 ;
        sh:name "Jurisdiction" ;
        sh:message "Entity must be registered in at least one jurisdiction" ;
        sh:severity sh:Warning ;
    ] .

# ── Shape 2: NaturalPerson must have name and nationality ─────────────────────
kyc:NaturalPersonShape
    a sh:NodeShape ;
    sh:targetClass kyc:NaturalPerson ;
    sh:property [
        sh:path kyc:name ;
        sh:minCount 1 ;
        sh:name "Person Name" ;
    ] ;
    sh:property [
        sh:path kyc:nationality ;
        sh:minCount 1 ;
        sh:pattern "^[A-Z]{2}$" ;
        sh:name "Nationality (ISO)" ;
        sh:message "Nationality must be 2-letter ISO code" ;
    ] .
```

## H.3 Validate in Neo4j via n10s

```cypher
// Load SHACL shapes
// First: docker cp shacl/kyc_shapes.ttl neo4j_kyc:/var/lib/neo4j/import/
CALL n10s.validation.shacl.import.fetch(
  "file:///var/lib/neo4j/import/kyc_shapes.ttl",
  "Turtle"
);

// Run validation
CALL n10s.validation.shacl.validate()
YIELD focusNode, nodeType, shapeId, propertyShape,
      offendingValue, resultPath, severity, resultMessage
RETURN focusNode, severity, resultMessage
ORDER BY severity
LIMIT 50;

// List all loaded shapes
CALL n10s.validation.shacl.listShapes()
YIELD target, propertyOrRelationship, constraint
RETURN target, propertyOrRelationship, constraint;
```

## H.4 Validate in GraphDB via SPARQL + SHACL

GraphDB has native SHACL support — you can load shapes and validate on the triplestore side too:

```sparql
# In GraphDB Workbench:
# 1. Go to "Setup" → "Validations"
# 2. Upload kyc_shapes.ttl
# 3. GraphDB will automatically validate incoming data
```

---

# PART I — MODULE 6: GraphRAG KYC AGENT

> **Goal**: Build an AI investigation agent that can answer natural-language KYC questions using the knowledge graph.
>
> **Going Meta sessions to study**: S22 (basic RAG), S24 (ontology-driven RAG), S27 (LangGraph agent), S29-32 (ontology-guided extraction), S34 (ontology tool calling), S43 (agent memory), S45 (agent skills)

## I.1 Architecture

```
Investigator types: "Who really controls Shell Corp X? Are they sanctioned?"
    │
    ▼
┌─────────────────────────────────────────────────┐
│  LangGraph Agent (stateful workflow)             │
│                                                   │
│  Decides which tools to use based on question:    │
│  ┌──────────────────────────────────────────┐    │
│  │ Tool: find_ubo                            │    │
│  │   → Cypher: ownership chain traversal     │    │
│  ├──────────────────────────────────────────┤    │
│  │ Tool: sanctions_check                     │    │
│  │   → Cypher: N-hop proximity to sanctions  │    │
│  ├──────────────────────────────────────────┤    │
│  │ Tool: risk_score                          │    │
│  │   → Returns GDS-computed PageRank score   │    │
│  ├──────────────────────────────────────────┤    │
│  │ Tool: circular_ownership                  │    │
│  │   → Cypher: SCC-based ring detection      │    │
│  ├──────────────────────────────────────────┤    │
│  │ Tool: sparql_query                        │    │
│  │   → SPARQL: queries GraphDB for semantics │    │
│  └──────────────────────────────────────────┘    │
│                                                   │
│  LLM (Claude) synthesizes answer + cites evidence │
└─────────────────────────────────────────────────┘
```

## I.2 Implementation

### Script: `scripts/09_graphrag_agent.py`

```python
"""
Step 9: GraphRAG KYC Investigation Agent.

Patterns used (from Going Meta series):
  - S24: Ontology-driven RAG (schema → Cypher generation)
  - S27: LangGraph reflection agent
  - S31: VectorRetriever + Text2CypherRetriever dual retrieval
  - S34: Ontology-driven tool calling (tools as graph nodes)
  - S43: Agent memory for multi-turn investigations
"""
from langchain_neo4j import Neo4jGraph
from langchain_anthropic import ChatAnthropic
from langchain.tools import tool
from langgraph.prebuilt import create_react_agent
import os
from dotenv import load_dotenv

load_dotenv()

# Connect to Neo4j
graph = Neo4jGraph(
    url=os.getenv("NEO4J_URI"),
    username=os.getenv("NEO4J_USER"),
    password=os.getenv("NEO4J_PASSWORD")
)

# LLM
llm = ChatAnthropic(model="claude-sonnet-4-20250514")

# ── TOOL 1: UBO Discovery ────────────────────────────────────────────────────
@tool
def find_ubo(company_name: str) -> str:
    """Find the Ultimate Beneficial Owner (UBO) of a company by traversing
    ownership chains up to 10 levels deep."""
    result = graph.query("""
    MATCH (e:LegalEntity)
    WHERE toLower(e.name) CONTAINS toLower($name)
    WITH e LIMIT 1
    MATCH path = (e)-[:DIRECTLY_OWNED_BY*1..10]->(owner)
    WHERE NOT (owner)-[:DIRECTLY_OWNED_BY]->()
    RETURN e.name AS company, owner.name AS ultimate_owner,
           length(path) AS hops, owner.isSanctioned AS is_sanctioned,
           owner.isPEP AS is_pep, owner.nationality AS nationality
    ORDER BY hops LIMIT 5
    """, params={"name": company_name})

    if not result:
        return f"No UBO found for '{company_name}'"

    lines = [f"UBO results for '{company_name}':"]
    for r in result:
        flags = []
        if r.get("is_sanctioned"): flags.append("SANCTIONED")
        if r.get("is_pep"): flags.append("PEP")
        status = ", ".join(flags) or "Clear"
        lines.append(f"  {r['company']} → (via {r['hops']} hops) → "
                      f"{r['ultimate_owner']} ({r['nationality']}) [{status}]")
    return "\n".join(lines)

# ── TOOL 2: Sanctions Proximity ───────────────────────────────────────────────
@tool
def check_sanctions(entity_name: str, max_hops: int = 3) -> str:
    """Check if an entity is within N hops of any sanctioned entity."""
    result = graph.query("""
    MATCH (e:LegalEntity)
    WHERE toLower(e.name) CONTAINS toLower($name)
    WITH e LIMIT 1
    MATCH path = (e)-[:DIRECTLY_OWNED_BY|CONTROLLED_BY*1..3]-(risky:SanctionedEntity)
    RETURN e.name AS entity, risky.name AS sanctioned,
           length(path) AS hops,
           [n IN nodes(path) | coalesce(n.name, 'unknown')] AS chain
    ORDER BY hops LIMIT 5
    """, params={"name": entity_name})

    if not result:
        return f"No sanctions proximity found for '{entity_name}' within 3 hops"

    lines = [f"SANCTIONS ALERT for '{entity_name}':"]
    for r in result:
        lines.append(f"  Connected to {r['sanctioned']} via {r['hops']} hops")
        lines.append(f"  Chain: {' → '.join(r['chain'])}")
    return "\n".join(lines)

# ── TOOL 3: Risk Score ────────────────────────────────────────────────────────
@tool
def get_risk_score(company_name: str) -> str:
    """Get the computed KYC risk score and breakdown for a company."""
    result = graph.query("""
    MATCH (e:LegalEntity)
    WHERE toLower(e.name) CONTAINS toLower($name)
    RETURN e.name, e.lei, e.jurisdiction, e.riskTier,
           round(e.kycRiskScore, 2) AS riskScore,
           round(e.pageRankScore, 4) AS systemicImportance,
           round(e.betweennessScore, 2) AS bridgeScore
    ORDER BY riskScore DESC LIMIT 3
    """, params={"name": company_name})

    if not result:
        return f"No entity found matching '{company_name}'"

    r = result[0]
    return (f"Risk Assessment: {r['e.name']}\n"
            f"  LEI: {r['e.lei']}\n"
            f"  Risk Score: {r['riskScore']}\n"
            f"  Jurisdiction: {r['e.jurisdiction']} (tier: {r['e.riskTier']})\n"
            f"  PageRank: {r['systemicImportance']}\n"
            f"  Betweenness: {r['bridgeScore']}")

# ── TOOL 4: Circular Ownership Detection ──────────────────────────────────────
@tool
def find_circular_ownership() -> str:
    """Detect circular ownership structures (A owns B owns C owns A)."""
    result = graph.query("""
    MATCH (a:LegalEntity)-[:DIRECTLY_OWNED_BY]->(b:LegalEntity)
           -[:DIRECTLY_OWNED_BY]->(c:LegalEntity)
           -[:DIRECTLY_OWNED_BY]->(a)
    RETURN DISTINCT a.name AS entity_a, b.name AS entity_b,
           c.name AS entity_c, a.jurisdiction AS juris
    LIMIT 10
    """)

    if not result:
        return "No circular ownership detected"

    lines = [f"CIRCULAR OWNERSHIP DETECTED ({len(result)} rings):"]
    for r in result:
        lines.append(f"  {r['entity_a']} → {r['entity_b']} → {r['entity_c']} → (back)")
    return "\n".join(lines)

# ── Create Agent ──────────────────────────────────────────────────────────────
tools = [find_ubo, check_sanctions, get_risk_score, find_circular_ownership]
agent = create_react_agent(llm, tools)

def investigate(question: str):
    """Run a KYC investigation."""
    print(f"\nINVESTIGATOR: {question}")
    print("-" * 60)
    result = agent.invoke({"messages": [("user", question)]})
    answer = result["messages"][-1].content
    print(f"AGENT: {answer}")
    return answer

if __name__ == "__main__":
    investigate("Who ultimately owns 'Shell Corp'? Are they on any sanctions list?")
    investigate("Find any circular ownership structures in the database")
    investigate("What are the top 5 highest risk entities and why?")
```

## I.3 Advanced Pattern: Ontology-Driven Tool Generation (Going Meta S34)

Instead of hardcoding tools, store them as graph nodes:

```cypher
// Store investigation tools as graph nodes (Going Meta S34 pattern)
CREATE (:Tool {
  name: 'find_ubo',
  description: 'Find Ultimate Beneficial Owner via ownership chain traversal',
  cypher_query: 'MATCH (e:LegalEntity) WHERE toLower(e.name) CONTAINS toLower($name) WITH e LIMIT 1 MATCH path = (e)-[:DIRECTLY_OWNED_BY*1..10]->(owner) WHERE NOT (owner)-[:DIRECTLY_OWNED_BY]->() RETURN e.name AS company, owner.name AS ubo, length(path) AS hops',
  parameters: '{"name": "string"}'
})

CREATE (:Tool {
  name: 'sanctions_proximity',
  description: 'Check entity proximity to sanctioned entities within N hops',
  cypher_query: 'MATCH (e:LegalEntity) WHERE toLower(e.name) CONTAINS toLower($name) WITH e LIMIT 1 MATCH path = (e)-[:DIRECTLY_OWNED_BY|CONTROLLED_BY*1..3]-(risky:SanctionedEntity) RETURN e.name, risky.name, length(path) AS hops',
  parameters: '{"name": "string"}'
})
```

Then dynamically load tools at runtime:

```python
# Going Meta S34: Dynamic tool loading from graph
from langchain.tools import StructuredTool

def load_tools_from_graph(graph):
    """Load investigation tools from Neo4j Tool nodes."""
    tool_nodes = graph.query("MATCH (t:Tool) RETURN t.name, t.description, t.cypher_query, t.parameters")
    tools = []
    for t in tool_nodes:
        def make_runner(cypher):
            def runner(**kwargs):
                return str(graph.query(cypher, params=kwargs))
            return runner
        tools.append(StructuredTool.from_function(
            func=make_runner(t["t.cypher_query"]),
            name=t["t.name"],
            description=t["t.description"],
        ))
    return tools
```

## I.4 Advanced Pattern: Dual Retrieval RAG (Going Meta S31)

Combine vector search (for fuzzy text matching) with Cypher (for structured graph queries):

```python
from neo4j_graphrag.retrievers import VectorRetriever, Text2CypherRetriever
from neo4j_graphrag.llm import OpenAILLM

# Vector retriever — finds similar text chunks
vector_retriever = VectorRetriever(
    driver=driver,
    index_name="chunk-index",
    embedder=OpenAIEmbeddings()
)

# Text2Cypher retriever — converts questions to graph queries
text2cypher_retriever = Text2CypherRetriever(
    driver=driver,
    llm=llm,
    neo4j_schema=graph.schema  # auto-detected from graph
)

# Use both and let the LLM synthesize
vector_results = vector_retriever.search(query=question)
graph_results = text2cypher_retriever.search(query=question)
```

---

# PART J — MODULE 7: DASHBOARD & VISUALIZATION

> **Goal**: Build a Streamlit dashboard for visual KYC investigation.
>
> **Going Meta sessions to study**: S15 (Streamlit semantic app), S23 (Streamlit RAG apps)

### Script: `dashboard/app.py`

```python
"""
Step 10: Streamlit KYC Intelligence Dashboard.
"""
import streamlit as st
from neo4j import GraphDatabase
import plotly.express as px
import pandas as pd
import os

st.set_page_config(page_title="KYC Intelligence", layout="wide")

@st.cache_resource
def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        auth=(os.getenv("NEO4J_USER", "neo4j"),
              os.getenv("NEO4J_PASSWORD", "kycpassword123"))
    )

def query(cypher, params=None):
    with get_driver().session() as s:
        return [dict(r) for r in s.run(cypher, params or {})]

st.title("KYC Beneficial Ownership Intelligence")
st.caption("FIBO · GLEIF · Neo4j GDS · GraphRAG")

# ── KPIs ──────────────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
stats = query("""
MATCH (e:LegalEntity)
RETURN count(e) AS total,
       sum(CASE WHEN e.riskTier='high' THEN 1 ELSE 0 END) AS highRisk
""")[0]
sanctioned = query("MATCH (p:SanctionedEntity) RETURN count(p) AS n")[0]["n"]
rings = query("""
MATCH (a)-[:DIRECTLY_OWNED_BY]->(b)-[:DIRECTLY_OWNED_BY]->(c)-[:DIRECTLY_OWNED_BY]->(a)
RETURN count(DISTINCT a) AS n
""")[0]["n"]

c1.metric("Total Entities", stats["total"])
c2.metric("High Risk", stats["highRisk"])
c3.metric("Sanctioned Links", sanctioned)
c4.metric("Circular Rings", rings)

# ── Risk Chart ────────────────────────────────────────────────────────────────
st.subheader("Risk Distribution by Jurisdiction")
risk_data = query("""
MATCH (e:LegalEntity) WHERE e.kycRiskScore IS NOT NULL
RETURN e.jurisdiction AS jurisdiction, e.riskTier AS tier,
       avg(e.kycRiskScore) AS avgScore, count(*) AS count
""")
if risk_data:
    df = pd.DataFrame(risk_data)
    fig = px.scatter(df, x="jurisdiction", y="avgScore", size="count",
                     color="tier",
                     color_discrete_map={"high":"red","medium":"orange","low":"green"})
    st.plotly_chart(fig, use_container_width=True)

# ── UBO Search ────────────────────────────────────────────────────────────────
st.subheader("UBO Investigation")
company = st.text_input("Search company name:")
if company:
    results = query("""
    MATCH (e:LegalEntity)
    WHERE toLower(e.name) CONTAINS toLower($name)
    WITH e LIMIT 1
    MATCH path = (e)-[:DIRECTLY_OWNED_BY*1..10]->(owner)
    WHERE NOT (owner)-[:DIRECTLY_OWNED_BY]->()
    RETURN e.name AS company, owner.name AS ubo, length(path) AS hops,
           owner.isSanctioned AS sanctioned, owner.nationality AS nationality
    ORDER BY hops LIMIT 5
    """, {"name": company})
    for r in results:
        icon = "🚨" if r.get("sanctioned") else "✅"
        st.write(f"{icon} **{r['company']}** → ({r['hops']} hops) → "
                 f"**{r['ubo']}** ({r['nationality']})")

# ── Top Risk Entities Table ───────────────────────────────────────────────────
st.subheader("Highest Risk Entities")
top = query("""
MATCH (e:LegalEntity) WHERE e.kycRiskScore IS NOT NULL
RETURN e.name AS name, e.lei AS lei, e.jurisdiction AS jurisdiction,
       e.riskTier AS tier, round(e.kycRiskScore, 1) AS riskScore
ORDER BY riskScore DESC LIMIT 20
""")
if top:
    st.dataframe(pd.DataFrame(top), use_container_width=True)
```

Run with: `streamlit run dashboard/app.py`

---

# PART K — SKILLS REFERENCE (For Implementation)

These are patterns and techniques to apply during implementation. Each maps to a Going Meta session.

## Skill 1: Ontology-to-Neo4j Schema Conversion
> Source: Going Meta S32 (`getSchemaFromOnto`), S45 (`owl_to_graphrag_schema.py`)

Convert a FIBO OWL module to `neo4j-graphrag` `SchemaConfig` for pipeline use:

```python
from rdflib import Graph
from rdflib.namespace import RDF, OWL, RDFS

def get_schema_from_ontology(ttl_path):
    """Convert OWL/TTL ontology to neo4j-graphrag compatible schema."""
    g = Graph()
    g.parse(ttl_path, format="turtle")

    entities = []
    relations = []

    # Extract classes
    for cls in g.subjects(RDF.type, OWL.Class):
        label = str(g.value(cls, RDFS.label) or cls.split("/")[-1])
        entities.append({"label": label, "description": str(g.value(cls, RDFS.comment) or "")})

    # Extract object properties as relations
    for prop in g.subjects(RDF.type, OWL.ObjectProperty):
        label = str(g.value(prop, RDFS.label) or prop.split("/")[-1])
        domain = str(g.value(prop, RDFS.domain) or "").split("/")[-1]
        range_ = str(g.value(prop, RDFS.range) or "").split("/")[-1]
        relations.append({"label": label, "source": domain, "target": range_})

    return {"entities": entities, "relations": relations}
```

## Skill 2: Ontology-Constrained LLM Extraction
> Source: Going Meta S29 (`getNLOntology`)

Feed the ontology to the LLM so it only extracts entities/relations defined in your schema:

```python
def get_nl_ontology(ttl_path):
    """Convert OWL ontology to natural language for LLM prompting."""
    g = Graph()
    g.parse(ttl_path, format="turtle")

    lines = ["The ontology defines these concepts:\n"]
    for cls in g.subjects(RDF.type, OWL.Class):
        label = g.value(cls, RDFS.label) or cls.split("/")[-1]
        comment = g.value(cls, RDFS.comment) or ""
        lines.append(f"- {label}: {comment}")

    lines.append("\nRelationships:")
    for prop in g.subjects(RDF.type, OWL.ObjectProperty):
        label = g.value(prop, RDFS.label) or prop.split("/")[-1]
        domain = (g.value(prop, RDFS.domain) or "?").split("/")[-1]
        range_ = (g.value(prop, RDFS.range) or "?").split("/")[-1]
        lines.append(f"- {domain} --[{label}]--> {range_}")

    return "\n".join(lines)

# Use in LLM prompt:
# "Extract entities using EXCLUSIVELY the terms in the ontology below.
#  ONTOLOGY: {get_nl_ontology('fibo-be.ttl')}
#  DOCUMENT: {document_text}"
```

## Skill 3: Pydantic Structured Output for Financial Entities
> Source: Going Meta S30

```python
from pydantic import BaseModel
from typing import List, Optional

class LegalEntity(BaseModel):
    name: str
    lei: Optional[str] = None
    jurisdiction: Optional[str] = None
    entity_type: Optional[str] = None  # Corporation, Trust, Fund, etc.

class OwnershipRelation(BaseModel):
    owner: str
    owned: str
    percentage: Optional[float] = None
    role: Optional[str] = None

class FinancialDocument(BaseModel):
    entities: List[LegalEntity]
    ownership_relations: List[OwnershipRelation]

# Use with OpenAI structured output:
# response = client.beta.chat.completions.parse(
#     model="gpt-4o",
#     response_format=FinancialDocument,
#     messages=[{"role": "user", "content": document_text}]
# )
```

## Skill 4: SimpleKGPipeline for Document Ingestion
> Source: Going Meta S32, S45

```python
from neo4j_graphrag.experimental.pipeline import SimpleKGPipeline
from neo4j_graphrag.experimental.components.text_splitters.fixed_size_splitter import FixedSizeSplitter

schema = get_schema_from_ontology("fibo-kyc.ttl")

kg_builder = SimpleKGPipeline(
    llm=llm,
    driver=driver,
    text_splitter=FixedSizeSplitter(chunk_size=500, chunk_overlap=50),
    embedder=embedder,
    entities=schema["entities"],
    relations=schema["relations"],
    from_pdf=True    # Can ingest PDFs directly
)

# Ingest a financial filing
await kg_builder.run(file_path="data/annual_report.pdf")
```

## Skill 5: Competency Question Evaluation
> Source: Going Meta S42, S45

Before deploying your ontology, validate it can answer the questions you need:

```python
COMPETENCY_QUESTIONS = [
    "Who is the ultimate beneficial owner of entity X?",
    "Is entity X within 3 hops of a sanctioned entity?",
    "What entities are in a circular ownership structure?",
    "Which entities in jurisdiction Y have the highest risk score?",
    "What is the ownership chain from entity X to person Z?",
    "Which entities share an address with a sanctioned entity?",
]

# For each CQ, verify your ontology has the classes/properties to answer it.
# Then generate a test Cypher query per CQ to validate against the loaded graph.
```

## Skill 6: Auto-Classification via APOC Triggers
> Source: Going Meta S04

Automatically label entities based on rules when data is written:

```cypher
// When a new entity is created in a high-risk jurisdiction, auto-label it
CALL apoc.trigger.add('auto-risk-label',
  'UNWIND $createdNodes AS n
   WITH n WHERE n:LegalEntity AND n.jurisdiction IN ["KY","VG","PA","SC"]
   SET n:HighRiskJurisdiction',
  {phase: 'afterAsync'}
);
```

## Skill 7: Python Helper Library

```python
# src/kg_client.py — Reusable client for both databases
from SPARQLWrapper import SPARQLWrapper, JSON
from neo4j import GraphDatabase
import requests
import os
from dotenv import load_dotenv

load_dotenv()

class GraphDBClient:
    def __init__(self):
        self.url = os.getenv("GRAPHDB_URL")
        self.repo = os.getenv("GRAPHDB_REPO")
        self.endpoint = f"{self.url}/repositories/{self.repo}"

    def query(self, sparql_query):
        wrapper = SPARQLWrapper(self.endpoint)
        wrapper.setQuery(sparql_query)
        wrapper.setReturnFormat(JSON)
        results = wrapper.query().convert()
        return [{k: v["value"] for k, v in row.items()}
                for row in results["results"]["bindings"]]

    def load_turtle(self, ttl, named_graph):
        return requests.post(
            f"{self.endpoint}/rdf-graphs/service",
            params={"graph": named_graph},
            data=ttl.encode(),
            headers={"Content-Type": "text/turtle"}
        ).status_code

    def count_triples(self, graph=None):
        q = f"SELECT (COUNT(*) AS ?c) WHERE {{ GRAPH <{graph}> {{ ?s ?p ?o }} }}" if graph \
            else "SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }"
        r = self.query(q)
        return int(r[0]["c"]) if r else 0


class Neo4jClient:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            os.getenv("NEO4J_URI"),
            auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))
        )

    def query(self, cypher, params=None):
        with self.driver.session() as s:
            return [dict(r) for r in s.run(cypher, params or {})]

    def close(self):
        self.driver.close()
```

---

# PART L — LEARNING PATH & CHECKLIST

## Phase 1: Foundations (Week 1)

| # | Task | Script | Validates |
|---|---|---|---|
| 1 | Start Docker containers | `docker-compose up -d` | Both UIs accessible |
| 2 | Create GraphDB repository | `scripts/01_setup_graphdb.py` | Repository appears in UI |
| 3 | Load FIBO + LCC ontologies | `scripts/02_load_fibo.py` | Named graphs visible |
| 4 | Load FIBO2GLEI mapping | `scripts/03_load_fibo2glei_mapping.py` | Mapping graph loaded |
| 5 | Run SPARQL exploration | `scripts/04_sparql_exploration.py` | Classes and hierarchy visible |
| 6 | Use GraphDB Visual Graph | UI: Explore → Visual Graph | Can navigate FIBO classes |
| **Concepts learned** | RDF triples, named graphs, SPARQL basics, OWL classes, ontology hierarchy ||

## Phase 2: Data (Week 2)

| # | Task | Script | Validates |
|---|---|---|---|
| 7 | Load GLEIF data | `scripts/05_load_gleif_data.py` | Real entities in GraphDB |
| 8 | Generate synthetic data | `scripts/06_generate_synthetic_data.py` | JSON file with 500+ entities |
| 9 | Initialize n10s in Neo4j | Neo4j Browser: run Cypher | n10s config active |
| 10 | Import FIBO into Neo4j | Cypher: `n10s.onto.import.fetch()` | :Class nodes visible |
| 11 | Load synthetic data | `scripts/07_load_neo4j.py` | Entities + relationships in Neo4j |
| **Concepts learned** | Ontology vs instance data, n10s configuration, Cypher MERGE, APOC loading ||

## Phase 3: Algorithms (Week 3)

| # | Task | Script | Validates |
|---|---|---|---|
| 12 | Run GDS algorithms | `scripts/08_gds_analysis.py` | Risk scores on all entities |
| 13 | Define SHACL shapes | `shacl/kyc_shapes.ttl` | Shapes file created |
| 14 | Run SHACL validation | n10s SHACL in Neo4j Browser | Violations detected |
| **Concepts learned** | GDS project→run→write pattern, PageRank/Louvain/SCC, SHACL validation ||

## Phase 4: AI Agent (Week 4)

| # | Task | Script | Validates |
|---|---|---|---|
| 15 | Build GraphRAG agent | `scripts/09_graphrag_agent.py` | Agent answers KYC questions |
| 16 | Build Streamlit dashboard | `dashboard/app.py` | Dashboard shows risk data |
| **Concepts learned** | LangGraph tools, Cypher as tool, ontology-driven RAG, Streamlit + Neo4j ||

## Phase 5: Advanced (Week 5)

| # | Task | Reference | Validates |
|---|---|---|---|
| 17 | Ontology-to-schema conversion | Skill 1 (S32 pattern) | FIBO→GraphSchema working |
| 18 | Document ingestion pipeline | Skill 4 (S32/S45 pattern) | PDF→KG pipeline working |
| 19 | Competency question evaluation | Skill 5 (S42 pattern) | All CQs pass validation |
| 20 | Query Neubauten public endpoint | `scripts/10_query_neubauten.py` | Live data accessible |
| **Concepts learned** | Production patterns, ontology quality, document processing ||

---

# PART M — KEY URLS & RESOURCES

## Primary References

| Resource | URL | Use |
|---|---|---|
| FIBO Ontology Spec | https://spec.edmcouncil.org/fibo/ | Download OWL modules |
| FIBO GitHub | https://github.com/edmcouncil/fibo | All 200+ modules, browsable |
| FIB-DM | https://fib-dm.com | FIBO as ER model (schema reference) |
| GLEIF API | https://api.gleif.org/api/v1 | Free LEI data API |
| GLEIF Golden Copy | https://www.gleif.org/en/lei-data/gleif-golden-copy | Full LEI dataset download |
| LCC (OMG) | https://www.omg.org/spec/LCC/ | Country/language code ontology |
| Neubauten Public | http://neubauten.ontotext.com:7200/ | Live demo SPARQL endpoint |

## Database Documentation

| Resource | URL | Use |
|---|---|---|
| GraphDB Docs | https://graphdb.ontotext.com/documentation/ | SPARQL, reasoning, SHACL |
| neosemantics (n10s) | https://neo4j.com/labs/neosemantics/ | All n10s procedures |
| APOC Manual | https://neo4j.com/docs/apoc/current/ | 300+ utility procedures |
| GDS Manual | https://neo4j.com/docs/graph-data-science/ | All graph algorithms |
| Cypher Manual | https://neo4j.com/docs/cypher-manual/ | Query language reference |

## Standards

| Resource | URL | Use |
|---|---|---|
| W3C SPARQL 1.1 | https://www.w3.org/TR/sparql11-query/ | SPARQL reference |
| W3C OWL Primer | https://www.w3.org/TR/owl-primer/ | Understand OWL basics |
| W3C SHACL | https://www.w3.org/TR/shacl/ | Validation language spec |
| W3C Turtle | https://www.w3.org/TR/turtle/ | RDF serialization format |

## Going Meta Series (Most Relevant Sessions)

| Session | Topic | Direct Use |
|---|---|---|
| S01 | Cypher vs SPARQL, n10s basics | Foundation for all RDF→Neo4j |
| S03 | SHACL validation | Data quality enforcement |
| S05 | Ontology-driven KG construction | FIBO-driven ETL pattern |
| S12 | RDFLib→Neo4j (Python) | Loading FIBO via Python |
| S24 | Ontology-driven RAG | Schema→Cypher generation |
| S29 | Ontology-guided LLM extraction (code) | Financial document processing |
| S30 | Pydantic structured output | Type-safe entity extraction |
| S31 | End-to-end GraphRAG | Full RAG pipeline |
| S32 | `getSchemaFromOnto()` + SimpleKGPipeline | Ontology→pipeline conversion |
| S34 | Ontology-driven tool calling | Dynamic KYC agent tools |
| S42 | Competency question evaluation | Ontology quality validation |
| S44 | Modern SHACL on Neo4j | Updated validation patterns |
| S45 | Complete ontology builder workflow | Full ontology engineering skill |

---

# PART N — PROJECT FOLDER STRUCTURE

```
kyc-intelligence/
│
├── docker-compose.yml              # GraphDB + Neo4j containers
├── .env                            # Connection strings, API keys
├── requirements.txt                # All Python dependencies
│
├── graphdb_config/
│   └── kyc-repo-config.ttl         # GraphDB repository configuration
│
├── data/
│   ├── fibo/                       # Downloaded FIBO TTL modules
│   ├── glei/                       # GLEIF API data (JSON + TTL)
│   ├── lcc/                        # LCC country code ontology
│   └── synthetic/                  # Generated KYC test dataset
│       └── kyc_dataset.json
│
├── shacl/
│   └── kyc_shapes.ttl              # SHACL validation shapes
│
├── scripts/
│   ├── 01_setup_graphdb.py         # Create GraphDB repository
│   ├── 02_load_fibo.py             # Load FIBO + LCC ontologies
│   ├── 03_load_fibo2glei_mapping.py # FIBO↔GLEI bridge
│   ├── 04_sparql_exploration.py    # SPARQL learning queries
│   ├── 05_load_gleif_data.py       # Real GLEIF → GraphDB
│   ├── 06_generate_synthetic_data.py # Synthetic KYC dataset
│   ├── 07_load_neo4j.py            # Data → Neo4j (n10s + Cypher)
│   ├── 08_gds_analysis.py          # GDS algorithms (risk scoring)
│   └── 09_graphrag_agent.py        # LangGraph KYC agent
│
├── src/
│   └── kg_client.py                # Reusable GraphDB + Neo4j client
│
├── dashboard/
│   └── app.py                      # Streamlit KYC dashboard
│
├── sparql/                         # Standalone SPARQL queries
│   ├── 01_list_graphs.sparql
│   ├── 02_fibo_classes.sparql
│   └── 03_cross_ontology.sparql
│
├── cypher/                         # Standalone Cypher queries
│   ├── 01_n10s_setup.cypher
│   ├── 02_gds_projections.cypher
│   └── 03_kyc_investigations.cypher
│
├── notebooks/                      # Jupyter exploration
│   ├── 01_fibo_exploration.ipynb
│   ├── 02_gleif_analysis.ipynb
│   └── 03_gds_risk_scoring.ipynb
│
└── referenceDocs/                  # Planning documents
    ├── FINANCIAL_KG_MASTER_PLAN_v2.md
    ├── NEUBAUTEN_GRAPH_IMPLEMENTATION_PLAN.md
    └── kyc_architecture.jsx
```

---

# QUICK REFERENCE: Key Cypher Queries for KYC

```cypher
-- 1. UBO Discovery (trace ownership to the top)
MATCH path = (e:LegalEntity {lei: $lei})-[:DIRECTLY_OWNED_BY*1..10]->(ubo:NaturalPerson)
RETURN ubo.name, length(path) AS hops ORDER BY hops;

-- 2. Sanctions Proximity (within N hops)
MATCH (e:LegalEntity {lei: $lei})-[:DIRECTLY_OWNED_BY|CONTROLLED_BY*1..3]-(risky:SanctionedEntity)
RETURN risky.name, "SANCTIONS_PROXIMITY";

-- 3. Shell Company Detection (many subsidiaries, no real address)
MATCH (e:LegalEntity)<-[:DIRECTLY_OWNED_BY]-(sub)
WITH e, count(sub) AS subs WHERE subs > 5 AND NOT e.hasOperationalAddress
RETURN e.name, subs ORDER BY subs DESC;

-- 4. Circular Ownership
MATCH (a:LegalEntity)-[:DIRECTLY_OWNED_BY]->(b)-[:DIRECTLY_OWNED_BY]->(c)-[:DIRECTLY_OWNED_BY]->(a)
RETURN a.name, b.name, c.name;

-- 5. Transaction Structuring Detection ($9k-$10k pattern)
MATCH (from)-[t:TRANSACTION]->(to)
WHERE t.amount > 9000 AND t.amount < 10000
RETURN from.name, to.name, t.amount, t.date ORDER BY t.date;

-- 6. GDS Community Detection
CALL gds.louvain.stream('kyc-graph') YIELD nodeId, communityId
RETURN gds.util.asNode(nodeId).name, communityId;

-- 7. GDS PageRank Risk Score
CALL gds.pageRank.stream('kyc-graph') YIELD nodeId, score
RETURN gds.util.asNode(nodeId).name, score ORDER BY score DESC LIMIT 10;
```

# QUICK REFERENCE: Key SPARQL Queries for Ontology Exploration

```sparql
-- 1. List all named graphs
SELECT ?graph (COUNT(*) AS ?triples)
WHERE { GRAPH ?graph { ?s ?p ?o } }
GROUP BY ?graph ORDER BY DESC(?triples)

-- 2. FIBO classes with labels
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?class ?label WHERE {
  ?class a owl:Class . OPTIONAL { ?class rdfs:label ?label }
  FILTER(CONTAINS(STR(?class), "edmcouncil.org"))
} LIMIT 50

-- 3. Ownership chain (property path)
PREFIX fibo: <https://spec.edmcouncil.org/fibo/ontology/BE/OwnershipAndControl/>
SELECT ?entity ?owner WHERE {
  ?entity fibo:isOwnedBy+ ?owner .
}

-- 4. Cross-ontology query (FIBO + GLEIF)
PREFIX fibo-be: <https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/>
PREFIX lei: <https://www.gleif.org/ontology/L1/>
SELECT ?entity ?name WHERE {
  ?entity a fibo-be:LegalPerson ; lei:legalName ?name .
}
```
