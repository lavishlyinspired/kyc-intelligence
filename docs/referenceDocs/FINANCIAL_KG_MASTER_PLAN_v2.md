# Financial Knowledge Graph — Master Implementation Plan v2
## KYC/AML Beneficial Ownership Intelligence System
### Built on FIBO · GLEI · LCC · FIB-DM · GraphDB · Neo4j · neosemantics · GDS · GraphRAG

---

## PART 0 — WHY BOTH GraphDB AND Neo4j? (The Core Question)

This is the most important conceptual question. Here is a precise answer.

### They solve completely different problems

```
GraphDB (Ontotext)                    Neo4j
─────────────────────────────         ────────────────────────────────────
WHAT IT IS: RDF Triplestore           WHAT IT IS: Property Graph Database
QUERY LANG: SPARQL 1.1                QUERY LANG: Cypher / GQL
DATA MODEL: Subject-Predicate-Object  DATA MODEL: Nodes + Relationships + Props
STRENGTHS:  OWL reasoning             STRENGTHS: Graph algorithms, App dev
            Ontology management                  Pattern matching at scale
            SHACL validation                     LLM/AI integration
            Federated SPARQL                     GDS (ML algorithms)
            Inference engine                     Cypher is developer-friendly
WEAKNESS:   Hard to query for devs    WEAKNESS:  No native OWL reasoning
            No graph algorithms               No SPARQL support natively
            Not LLM-friendly                  Ontologies need neosemantics
```

### The Barrasa Pattern (from Going Meta series)

Jesús Barrasa's entire "Going Meta" series (jbarrasa/goingmeta) is built around one insight:
**You use GraphDB or any triplestore to CURATE and VALIDATE ontology-aligned data, then you use Neo4j to QUERY, ANALYZE, and BUILD APPLICATIONS on top of it.**

The flow is:

```
 FIBO + GLEI + LCC + FIB-DM          GraphDB                  Neo4j
 (Raw Ontologies & Data)    ──→   (Reason, Validate,   ──→  (Analyze, Query,
                                   Enrich, Infer)            Build, Serve LLMs)
      RDF Triples                 SPARQL + OWL              Cypher + GDS + GraphRAG
```

### Concrete example: Why Neo4j for KYC?

Scenario: An investigator asks — *"Show me all entities that share an address with a sanctioned company, within 3 hops of any transaction over $1M in the last 30 days."*

**In SPARQL (GraphDB)** — This is painful. SPARQL property paths work, but the syntax is complex and it cannot run GDS algorithms (Louvain, PageRank) natively.

**In Cypher (Neo4j)** — This is natural:
```cypher
MATCH (sanctioned:Entity {isSanctioned: true})
      <-[:SHARES_ADDRESS]-()
      -[:TRANSACTED_WITH*1..3]->
      (suspect)
WHERE EXISTS {
  MATCH (suspect)-[t:TRANSACTION]->()
  WHERE t.amount > 1000000
    AND t.date > date() - duration('P30D')
}
RETURN suspect
```

**GDS on Neo4j** — Run Louvain community detection to find suspicious clusters, PageRank to score systemic risk — impossible on GraphDB without exporting data.

**GraphRAG on Neo4j** — Connect an LLM to ask natural-language questions over the graph — this is the *Going Meta* Season 2-3 trajectory (LLMs + KGs).

### The Barrasa Proof: neosemantics as the bridge

`neosemantics` (n10s) — authored by Barrasa himself — is the explicit bridge:
- Import FIBO OWL ontology structure into Neo4j as a **Class/Property graph**
- Import GLEI RDF instance data into Neo4j as **Entity nodes**
- Run Cypher queries that respect the ontology structure
- Export Neo4j data back to RDF for SPARQL federation

Without n10s, you lose semantic structure when you move to Neo4j. With n10s, you keep it.

---

## PART 1 — Financial Ontology Ecosystem (Complete Map)

### 1.1 The EDM Council Stack

The Enterprise Data Management Council (EDM Council, which acquired OMG in October 2025) publishes the industry-standard ontology stack for financial services:

```
spec.edmcouncil.org
│
├── FIBO — Financial Industry Business Ontology
│   ├── FND  — Foundations (core relations, dates, quantities)
│   ├── BE   — Business Entities (corps, LLCs, trusts, partnerships)
│   ├── FBC  — Financial Business & Commerce (markets, regulators)
│   ├── SEC  — Securities (equities, bonds, funds)
│   ├── DER  — Derivatives (swaps, options, futures)
│   ├── LOAN — Loans (mortgages, commercial, consumer)
│   ├── BP   — Business Processes (issuance, settlement workflows)
│   └── IND  — Indices & Indicators (market benchmarks)
│
├── FIB-DM — Financial Industry Business Data Model
│   └── FIBO translated into ER/relational model (3,173 entities in Q4/2025)
│       Used by data architects who don't know OWL
│
└── LCC — Languages, Countries & Codes
    ├── ISO 3166-1 Country Codes
    ├── ISO 3166-2 Subdivision Codes
    └── ISO 639 Language Codes
```

### 1.2 External Datasets That Complete the Picture

| Dataset | What it contains | Format | URL |
|---|---|---|---|
| GLEI/GLEIF | 2M+ legal entity identifiers (LEIs) | XML/CSV/RDF | gleif.org |
| LEI2ISIN | LEI ↔ ISIN security mappings | CSV | gleif.org |
| FIBO2GLEI | Mapping ontology between FIBO and GLEI | OWL/TTL | Neubauten demo |
| OFAC SDN | US sanctions list (Specially Designated Nationals) | XML/CSV | treasury.gov |
| OpenCorporates | Company registry data, 200+ jurisdictions | JSON API | opencorporates.com |
| Wikidata | Structured data on public figures (PEPs) | SPARQL/JSON | wikidata.org |
| PermID | Thomson Reuters entity IDs linked to FIBO | RDF | permid.org |
| ISIN.org | ISIN registry | API | isin.org |

### 1.3 FIB-DM vs FIBO — Why Both Matter

- **FIBO** is an OWL ontology: machine-readable, supports inference, used in triplestores
- **FIB-DM** is a derived data model: 3,173 entities in Q4/2025 release, in formats data architects understand (ER diagrams, PowerDesigner)
- **For your use case**: Use FIBO in GraphDB for reasoning; use FIB-DM as your **schema reference** when designing Neo4j node/relationship types

FIB-DM tells you: *"Here are all the entities a bank needs: Account, Loan, LegalEntity, Address, OwnershipRelation, SecurityInstrument..."* — this maps directly to your Neo4j node labels.

---

## PART 2 — THE USE CASE: Beneficial Ownership & KYC Intelligence System

### 2.1 Why This Use Case?

<details>
<summary>Business Context (click to expand)</summary>

**The Problem**: Regulators globally (FATF, FinCEN, EU AML Directives) require financial institutions to identify the **Ultimate Beneficial Owner (UBO)** of every corporate customer — the real human(s) who ultimately own or control an entity, even through layers of shell companies. This is called **KYC (Know Your Customer)**.

**Why graphs?**: A company can be owned by 5 holding companies, each owned by 3 trusts, each controlled by 2 nominees. To find the UBO, you must traverse up to 10+ levels of corporate ownership. This is trivially expressed as a graph traversal and nearly impossible in SQL.

**The scale**: The Panama Papers exposed 214,000 offshore entities. The Pandora Papers involved 11.9 million documents. Real UBO chains span countries, jurisdictions, and decades.

**The AI angle (2025)**: GraphRAG (Graph Retrieval Augmented Generation) lets compliance investigators ask natural-language questions over this ownership graph — "Who really controls this company?" — and get traced, explainable answers.

</details>

### 2.2 System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    BENEFICIAL OWNERSHIP INTELLIGENCE SYSTEM                  │
│                    Built on FIBO · GLEI · LCC · GDS · GraphRAG              │
└─────────────────────────────────────────────────────────────────────────────┘

DATA SOURCES                  LAYER 1: ONTOLOGY STORE         LAYER 2: ANALYTICS STORE
──────────                    ────────────────────────         ────────────────────────
GLEIF API ──────────────────► GraphDB (Ontotext)              Neo4j
FIBO OWL files ─────────────► • Loads FIBO/LCC/GLEI           • Imports via neosemantics
OFAC Sanctions list ────────► • Validates via SHACL           • Adds OFAC, OpenCorp data
OpenCorporates API ─────────► • OWL reasoning (UBO paths)     • Runs GDS algorithms
Wikidata PEPs ──────────────► • SPARQL API                    • Cypher queries
                                    │                               │
                                    │ n10s.rdf.import.fetch()       │
                                    └───────────────────────────────┘
                                                                     │
                                                              LangGraph Agent
                                                              + Claude/GPT
                                                              = GraphRAG KYC
                                                                     │
                                                              ┌──────┴──────┐
                                                              │  Streamlit  │
                                                              │  Dashboard  │
                                                              └─────────────┘
```

### 2.3 Neo4j Graph Schema (Informed by FIBO + FIB-DM)

```
Node Labels (from FIB-DM entity model):
  (:LegalEntity)    — companies, corporations, LLCs
  (:NaturalPerson)  — actual humans (UBOs, directors)
  (:Address)        — registered/operational address
  (:Jurisdiction)   — countries, territories (from LCC)
  (:Security)       — ISIN instruments
  (:SanctionEntry)  — OFAC/UN/EU sanctions
  (:PEPEntry)       — Politically Exposed Persons
  (:Alert)          — KYC investigation alerts

Relationship Types (from FIBO BE OAC):
  (:LegalEntity)-[:DIRECTLY_OWNED_BY {percentage, since}]->(:LegalEntity)
  (:LegalEntity)-[:CONTROLLED_BY {role, since}]->(:NaturalPerson)
  (:LegalEntity)-[:REGISTERED_IN]->(:Jurisdiction)
  (:LegalEntity)-[:HAS_ADDRESS]->(:Address)
  (:LegalEntity)-[:ISSUES]->(:Security)
  (:LegalEntity)-[:MATCHED_SANCTION]->(:SanctionEntry)
  (:NaturalPerson)-[:IS_PEP]->(:PEPEntry)
  (:NaturalPerson)-[:SHARES_ADDRESS_WITH]->(:NaturalPerson)
  (:Security)-[:IDENTIFIED_BY_ISIN {isin}]->(:Security)
```

### 2.4 The Key Queries This System Enables

**Q1 — UBO Discovery (Requires graph traversal)**
```cypher
// Who ultimately owns "Shell Corp X"?
MATCH path = (:LegalEntity {lei: $lei})
             -[:DIRECTLY_OWNED_BY*1..10]->
             (ubo:NaturalPerson)
RETURN ubo.name, ubo.nationality, length(path) as hops
ORDER BY hops
```

**Q2 — Sanctions Proximity (2-hop risk)**
```cypher
// Is this entity within 2 hops of a sanctioned entity?
MATCH (e:LegalEntity {lei: $lei})
      -[:DIRECTLY_OWNED_BY|CONTROLLED_BY*1..2]->
      (risky)
WHERE EXISTS {
  MATCH (risky)-[:MATCHED_SANCTION]->(:SanctionEntry)
}
RETURN risky, "SANCTIONS_PROXIMITY_RISK"
```

**Q3 — Shell Company Detection (GDS)**
```cypher
// Flag entities with high in-degree (many subsidiaries) but no real business
CALL gds.degree.stream('ownership-graph', {orientation: 'REVERSE'})
YIELD nodeId, score as subsidiaryCount
WITH gds.util.asNode(nodeId) AS entity, subsidiaryCount
WHERE subsidiaryCount > 5
  AND entity.hasOperationalAddress = false
  AND entity.hasRealEmployees = false
RETURN entity.name, subsidiaryCount
ORDER BY subsidiaryCount DESC
```

**Q4 — Circular Ownership Detection (GDS)**
```cypher
// Find ownership cycles (A owns B owns C owns A)
CALL gds.scc.stream('ownership-graph')
YIELD nodeId, componentId
WITH componentId, collect(gds.util.asNode(nodeId)) AS members
WHERE size(members) > 1
RETURN members, "CIRCULAR_OWNERSHIP_DETECTED"
```

**Q5 — GraphRAG Natural Language Query (LLM layer)**
```python
# Investigator types: "Find me all customers connected to Mossack Fonseca"
# LangGraph agent:
# 1. Extracts "Mossack Fonseca" as entity
# 2. Looks up in Neo4j via text search
# 3. Runs traversal query
# 4. Summarizes findings via LLM
```

---

## PART 3 — IMPLEMENTATION PLAN (Claude Code Instructions)

### Phase 0: Environment (Week 1, Day 1-2)

#### File: `docker-compose.yml`
```yaml
version: "3.8"
services:
  graphdb:
    image: ontotext/graphdb:10.7.0
    container_name: graphdb
    ports: ["7200:7200"]
    volumes:
      - graphdb_data:/opt/graphdb/home
      - ./graphdb_config:/opt/graphdb/dist/configs
    environment:
      GDB_JAVA_OPTS: "-Xmx6g -Xms2g"
    restart: unless-stopped

  neo4j:
    image: neo4j:5.20-community
    container_name: neo4j_kyc
    ports: ["7474:7474", "7687:7687"]
    volumes:
      - neo4j_data:/data
      - neo4j_plugins:/plugins
      - ./import:/var/lib/neo4j/import
      - ./conf/neo4j.conf:/conf/neo4j.conf
    environment:
      NEO4J_AUTH: neo4j/kycpassword123
      NEO4J_PLUGINS: '["apoc", "graph-data-science", "n10s"]'
      NEO4J_dbms_security_procedures_unrestricted: "apoc.*,n10s.*,gds.*"
      NEO4J_dbms_security_procedures_allowlist: "apoc.*,n10s.*,gds.*"
      NEO4J_server_memory_heap_initial__size: "3G"
      NEO4J_server_memory_heap_max__size: "6G"
      NEO4J_server_memory_pagecache_size: "2G"
    restart: unless-stopped

  streamlit:
    build: ./dashboard
    container_name: kyc_dashboard
    ports: ["8501:8501"]
    environment:
      NEO4J_URI: bolt://neo4j_kyc:7687
      NEO4J_USER: neo4j
      NEO4J_PASSWORD: kycpassword123
      GRAPHDB_URL: http://graphdb:7200
    depends_on: [neo4j, graphdb]
    restart: unless-stopped

volumes:
  graphdb_data:
  neo4j_data:
  neo4j_plugins:
```

#### File: `requirements.txt`
```
rdflib==7.0.0
SPARQLWrapper==2.0.0
requests==2.31.0
pandas==2.2.0
neo4j==5.20.0
python-dotenv==1.0.0
tqdm==4.66.0
langchain==0.3.0
langchain-neo4j==0.2.0
langchain-anthropic==0.3.0
langgraph==0.2.0
streamlit==1.39.0
plotly==5.24.0
httpx==0.27.0
faker==30.0.0
```

---

### Phase 1: GraphDB — Load Ontologies (Week 1, Day 3-5)

#### Script: `scripts/01_setup_graphdb.py`
```python
"""
Create the KYC Knowledge Graph repository in GraphDB
and load the full ontology stack: FIBO + LCC + GLEI ontology.

Learning: GraphDB is the "ontology authority" — the source of truth
for what concepts mean in the financial domain.
"""
import requests
import os

GRAPHDB_URL = "http://localhost:7200"
REPO_ID = "kyc-kg"

# Step 1: Create repository with OWL reasoning enabled
REPO_CONFIG = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix rep: <http://www.openrdf.org/config/repository#> .
@prefix sr: <http://www.openrdf.org/config/repository/sail#> .
@prefix sail: <http://www.openrdf.org/config/sail#> .
@prefix graphdb: <http://www.ontotext.com/config/graphdb#> .

[] a rep:Repository ;
   rep:repositoryID "kyc-kg" ;
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
"""

def create_repo():
    with open("/tmp/kyc-config.ttl", "w") as f:
        f.write(REPO_CONFIG)
    with open("/tmp/kyc-config.ttl", "rb") as f:
        r = requests.post(
            f"{GRAPHDB_URL}/rest/repositories",
            files={"config": f}
        )
    print(f"Repository created: {r.status_code}")

# Step 2: Load ontology modules
# CRITICAL: Load order matters — dependencies first!
ONTOLOGY_LOAD_ORDER = [
    # 1. FIBO Foundations (everything depends on these)
    ("https://spec.edmcouncil.org/fibo/ontology/FND/Utilities/AnnotationVocabulary/",
     "http://kg/fibo/fnd/annotations", "FIBO Annotations"),
    ("https://spec.edmcouncil.org/fibo/ontology/FND/Relations/Relations/",
     "http://kg/fibo/fnd/relations", "FIBO Relations"),
    ("https://spec.edmcouncil.org/fibo/ontology/FND/AgentsAndPeople/Agents/",
     "http://kg/fibo/fnd/agents", "FIBO Agents"),
    
    # 2. LCC (needed by FIBO BE for jurisdictions)
    ("https://www.omg.org/spec/LCC/Countries/CountryRepresentation/",
     "http://kg/lcc/countries", "LCC Countries"),
    ("https://www.omg.org/spec/LCC/Countries/ISO3166-1-CountryCodes/",
     "http://kg/lcc/iso3166", "LCC ISO 3166-1"),
    
    # 3. FIBO Business Entities (legal entities, ownership, control)
    ("https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/",
     "http://kg/fibo/be/legal-persons", "FIBO Legal Persons"),
    ("https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/CorporateBodies/",
     "http://kg/fibo/be/corporate-bodies", "FIBO Corporate Bodies"),
    ("https://spec.edmcouncil.org/fibo/ontology/BE/OwnershipAndControl/Ownership/",
     "http://kg/fibo/be/ownership", "FIBO Ownership"),
    ("https://spec.edmcouncil.org/fibo/ontology/BE/OwnershipAndControl/Control/",
     "http://kg/fibo/be/control", "FIBO Control"),
    ("https://spec.edmcouncil.org/fibo/ontology/BE/Corporations/Corporations/",
     "http://kg/fibo/be/corporations", "FIBO Corporations"),
    
    # 4. FIBO Financial Business & Commerce (regulators, markets)
    ("https://spec.edmcouncil.org/fibo/ontology/FBC/FunctionalEntities/FinancialServicesEntities/",
     "http://kg/fibo/fbc/fse", "FIBO Financial Services Entities"),
]

def load_ontology(url, named_graph, name):
    """Load an ontology via SPARQL LOAD command."""
    sparql = f"LOAD <{url}> INTO GRAPH <{named_graph}>"
    r = requests.post(
        f"{GRAPHDB_URL}/repositories/{REPO_ID}/statements",
        data=sparql,
        headers={"Content-Type": "application/sparql-update"},
        timeout=60
    )
    icon = "✓" if r.ok else "✗"
    print(f"{icon} {name}: HTTP {r.status_code}")
    if not r.ok:
        print(f"  Error: {r.text[:200]}")
    return r.ok

if __name__ == "__main__":
    create_repo()
    print("\nLoading ontology stack...")
    for url, graph, name in ONTOLOGY_LOAD_ORDER:
        load_ontology(url, graph, name)
```

---

### Phase 2: GLEI Data Loading into GraphDB (Week 2, Day 1-2)

#### Script: `scripts/02_load_glei_data.py`
```python
"""
Load GLEIF LEI data into GraphDB as RDF instance data.

The distinction:
  Ontology (FIBO) = SCHEMA — what a LegalEntity IS conceptually
  GLEI data = INSTANCES — Apple Inc IS a LegalEntity with LEI=5493001KJTIIGC8Y1R12

This script fetches real entities from GLEIF API and creates RDF triples
conforming to the FIBO ontology (using FIBO class URIs as rdf:type).
"""
import requests
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, RDFS, XSD, OWL
import json
import os
from tqdm import tqdm

# Namespaces
FIBO_BE = Namespace("https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/")
FIBO_OAC = Namespace("https://spec.edmcouncil.org/fibo/ontology/BE/OwnershipAndControl/Ownership/")
LEI_NS = Namespace("https://www.gleif.org/data/lei/")
LCC_CTRY = Namespace("https://www.omg.org/spec/LCC/Countries/ISO3166-1-CountryCodes/")
KYC = Namespace("http://kyc-kg.example.org/ontology#")

GLEIF_API = "https://api.gleif.org/api/v1"
GRAPHDB_URL = "http://localhost:7200"
REPO_ID = "kyc-kg"

def fetch_entities_by_country(country_code="US", page_size=50):
    """Fetch LEI records filtered by jurisdiction."""
    url = f"{GLEIF_API}/lei-records"
    params = {
        "filter[entity.legalJurisdiction]": country_code,
        "page[size]": page_size,
        "page[number]": 1,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()["data"]

def fetch_relationships_for_lei(lei):
    """Fetch ownership relationships for an entity."""
    url = f"{GLEIF_API}/lei-records/{lei}/direct-children"
    try:
        r = requests.get(url, timeout=15)
        if r.ok:
            return r.json().get("data", [])
    except:
        pass
    return []

def entities_to_rdf(entities):
    """
    Convert GLEIF entities to FIBO-aligned RDF triples.
    
    Key design decision: We use FIBO class URIs as the rdf:type.
    This means SPARQL queries using FIBO vocabulary work directly.
    """
    g = Graph()
    g.bind("fibo-be", FIBO_BE)
    g.bind("fibo-oac", FIBO_OAC)
    g.bind("lei", LEI_NS)
    g.bind("lcc", LCC_CTRY)
    g.bind("kyc", KYC)
    
    for record in entities:
        attrs = record["attributes"]
        lei_code = attrs["lei"]
        entity = attrs.get("entity", {})
        registration = attrs.get("registration", {})
        
        entity_uri = URIRef(f"https://www.gleif.org/data/lei/{lei_code}")
        
        # Type it using FIBO class — this is the key ontology alignment step
        g.add((entity_uri, RDF.type, FIBO_BE.LegalPerson))
        
        # Also add GLEIF-specific type for convenience
        g.add((entity_uri, RDF.type, KYC.RegisteredLegalEntity))
        
        # Core GLEIF properties
        g.add((entity_uri, KYC.leiCode, Literal(lei_code)))
        
        legal_name = entity.get("legalName", {}).get("name")
        if legal_name:
            g.add((entity_uri, RDFS.label, Literal(legal_name)))
            g.add((entity_uri, KYC.legalName, Literal(legal_name)))
        
        jurisdiction = entity.get("jurisdiction")
        if jurisdiction and len(jurisdiction) == 2:
            ctry_uri = URIRef(f"https://www.omg.org/spec/LCC/Countries/ISO3166-1-CountryCodes/{jurisdiction}")
            g.add((entity_uri, KYC.hasJurisdiction, ctry_uri))
        
        entity_status = entity.get("status")
        if entity_status:
            g.add((entity_uri, KYC.entityStatus, Literal(entity_status)))
        
        entity_category = entity.get("category")
        if entity_category:
            g.add((entity_uri, KYC.entityCategory, Literal(entity_category)))
        
        # Address
        legal_addr = entity.get("legalAddress", {})
        if legal_addr:
            addr_uri = URIRef(f"http://kyc-kg.example.org/address/{lei_code}")
            g.add((entity_uri, KYC.hasLegalAddress, addr_uri))
            g.add((addr_uri, RDF.type, KYC.Address))
            if legal_addr.get("city"):
                g.add((addr_uri, KYC.city, Literal(legal_addr["city"])))
            if legal_addr.get("country"):
                g.add((addr_uri, KYC.country, Literal(legal_addr["country"])))
            if legal_addr.get("postalCode"):
                g.add((addr_uri, KYC.postalCode, Literal(legal_addr["postalCode"])))
        
        # Registration status
        if registration.get("status") == "LAPSED":
            g.add((entity_uri, KYC.isLapsed, Literal(True, datatype=XSD.boolean)))
    
    return g

def load_to_graphdb(g, named_graph):
    ttl = g.serialize(format="turtle")
    r = requests.post(
        f"{GRAPHDB_URL}/repositories/{REPO_ID}/rdf-graphs/service",
        params={"graph": named_graph},
        data=ttl.encode("utf-8"),
        headers={"Content-Type": "text/turtle"},
        timeout=60
    )
    return r.status_code, len(g)

if __name__ == "__main__":
    os.makedirs("data/glei", exist_ok=True)
    
    # Load entities from multiple jurisdictions for diversity
    all_entities = []
    for country in ["US", "GB", "DE", "JP", "CH", "KY"]:  # KY = Cayman Islands
        print(f"Fetching entities from {country}...")
        try:
            entities = fetch_entities_by_country(country, page_size=50)
            all_entities.extend(entities)
            print(f"  Got {len(entities)} entities")
        except Exception as e:
            print(f"  Failed: {e}")
    
    print(f"\nTotal entities: {len(all_entities)}")
    
    # Save raw data
    with open("data/glei/entities_raw.json", "w") as f:
        json.dump(all_entities, f, indent=2)
    
    # Convert to RDF
    g = entities_to_rdf(all_entities)
    ttl = g.serialize(format="turtle")
    with open("data/glei/entities.ttl", "w") as f:
        f.write(ttl)
    print(f"Generated {len(g)} RDF triples")
    
    # Load into GraphDB
    status, count = load_to_graphdb(g, "http://kg/glei/instances")
    print(f"Loaded to GraphDB: HTTP {status}, {count} triples")
```

---

### Phase 3: Synthetic KYC Dataset Generation (Week 2, Day 3-4)

#### Script: `scripts/03_generate_kyc_dataset.py`
```python
"""
Generate synthetic KYC dataset with realistic financial crime patterns.

This creates:
  - 500 legal entities (companies) with LEI-like codes
  - 200 natural persons (directors, UBOs)
  - Ownership chains up to 8 levels deep
  - 3 hidden UBOs who are sanctioned
  - 5 circular ownership structures (shell company rings)
  - 2 PEP (Politically Exposed Person) links
  - 1,000+ transactions with suspicious patterns

Inspired by: Neo4j KYC Agent demo (neo4j.com/blog/developer/graphrag-in-action-know-your-customer/)
"""
import json
import random
import string
from faker import Faker
from datetime import datetime, timedelta
import os

fake = Faker(["en_US", "en_GB", "de_DE", "ja_JP", "zh_CN"])
random.seed(42)

def gen_lei():
    """Generate LEI-like 20-char alphanumeric code."""
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=20))

def gen_isin(country="US"):
    """Generate ISIN-like code."""
    return country + ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

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

ENTITY_CATEGORIES = [
    "BRANCH", "FUND", "SOLE_PROPRIETORSHIP", "PARTNERSHIP",
    "LIMITED_PARTNERSHIP", "TRUST", "FUND_MANAGER",
    "PENSION_FUND", "OTHER"
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
            "category": random.choice(ENTITY_CATEGORIES),
            "incorporated_date": fake.date_between(start_date="-30y", end_date="-1y").isoformat(),
            "is_active": random.random() > 0.1,
            "has_operational_address": random.random() > 0.3,
            "isin": gen_isin(juris[0]) if random.random() > 0.7 else None,
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
            "is_pep": i < 10,  # First 10 are PEPs
            "is_sanctioned": i < 3,  # First 3 are sanctioned
        })
    return persons

def generate_ownership_structure(entities, persons):
    """
    Create realistic ownership chains including:
    - Normal corporate hierarchy
    - Shell company chains (high-risk jurisdictions)  
    - Circular ownership (for detection testing)
    - Sanctioned UBO chains
    """
    relationships = []
    
    # Normal ownership chains (entities owning entities)
    entity_ids = [e["id"] for e in entities]
    person_ids = [p["id"] for p in persons]
    
    # Create 3-level holding structure for 60% of entities
    for i, entity in enumerate(entities[:300]):
        # Direct parent (another entity)
        if i > 20 and random.random() > 0.4:
            parent = random.choice(entities[:i])
            relationships.append({
                "from": entity["id"],
                "to": parent["id"],
                "type": "DIRECTLY_OWNED_BY",
                "percentage": round(random.uniform(50, 100), 2),
                "since": fake.date_between(start_date="-10y", end_date="-1y").isoformat(),
            })
        
        # Ultimate human controller
        if random.random() > 0.5:
            person = random.choice(persons)
            relationships.append({
                "from": entity["id"],
                "to": person["id"],
                "type": "CONTROLLED_BY",
                "role": random.choice(["Director", "CEO", "Shareholder", "Nominee"]),
                "since": fake.date_between(start_date="-10y", end_date="-1y").isoformat(),
            })
    
    # Sanctioned UBO chains: 5 entities ultimately controlled by sanctioned person
    sanctioned_person = persons[0]  # First person is sanctioned
    for entity in random.sample(entities[100:150], 5):
        # Create 3-hop chain: entity -> shell1 -> shell2 -> sanctioned_person
        shell1 = random.choice(entities[300:350])
        shell2 = random.choice(entities[350:400])
        relationships.extend([
            {"from": entity["id"], "to": shell1["id"], "type": "DIRECTLY_OWNED_BY",
             "percentage": 100.0, "since": "2018-01-01"},
            {"from": shell1["id"], "to": shell2["id"], "type": "DIRECTLY_OWNED_BY",
             "percentage": 100.0, "since": "2018-01-01"},
            {"from": shell2["id"], "to": sanctioned_person["id"], "type": "CONTROLLED_BY",
             "role": "Ultimate Beneficial Owner", "since": "2018-01-01"},
        ])
    
    # Circular ownership: A owns B owns C owns A
    for i in range(5):
        ring = random.sample(entities[400:450], 3)
        relationships.extend([
            {"from": ring[0]["id"], "to": ring[1]["id"], "type": "DIRECTLY_OWNED_BY",
             "percentage": 51.0, "since": "2020-01-01"},
            {"from": ring[1]["id"], "to": ring[2]["id"], "type": "DIRECTLY_OWNED_BY",
             "percentage": 51.0, "since": "2020-01-01"},
            {"from": ring[2]["id"], "to": ring[0]["id"], "type": "DIRECTLY_OWNED_BY",
             "percentage": 51.0, "since": "2020-01-01"},
        ])
    
    return relationships

def generate_transactions(entities, n=1000):
    transactions = []
    entity_ids = [e["id"] for e in entities]
    
    for i in range(n):
        # Some structuring: multiple transactions just below $10k threshold
        amount = random.choice([
            random.uniform(9000, 9999),  # Structuring
            random.uniform(100, 5000),   # Normal
            random.uniform(100000, 5000000),  # Large
        ])
        transactions.append({
            "id": f"TXN_{i:05d}",
            "from_entity": random.choice(entity_ids),
            "to_entity": random.choice(entity_ids),
            "amount": round(amount, 2),
            "currency": random.choice(["USD", "EUR", "GBP", "CHF"]),
            "date": fake.date_between(start_date="-2y", end_date="today").isoformat(),
            "is_suspicious": amount > 9000 and amount < 10000,
        })
    
    return transactions

if __name__ == "__main__":
    os.makedirs("data/synthetic", exist_ok=True)
    
    print("Generating synthetic KYC dataset...")
    entities = generate_entities(500)
    persons = generate_persons(200)
    relationships = generate_ownership_structure(entities, persons)
    transactions = generate_transactions(entities, 1000)
    
    dataset = {
        "entities": entities,
        "persons": persons,
        "relationships": relationships,
        "transactions": transactions,
    }
    
    with open("data/synthetic/kyc_dataset.json", "w") as f:
        json.dump(dataset, f, indent=2)
    
    print(f"Generated:")
    print(f"  {len(entities)} legal entities")
    print(f"  {len(persons)} natural persons ({sum(1 for p in persons if p['is_sanctioned'])} sanctioned)")
    print(f"  {len(relationships)} ownership relationships")
    print(f"  {len(transactions)} transactions")
    print(f"Saved to data/synthetic/kyc_dataset.json")
```

---

### Phase 4: Load Synthetic Data into Neo4j (Week 3, Day 1-2)

#### Script: `scripts/04_load_neo4j.py`
```python
"""
Load KYC dataset into Neo4j using:
1. neosemantics (n10s) — for the ontology structure from GraphDB
2. Direct Cypher MERGE — for synthetic instance data

This script shows THE BRIDGE PATTERN:
  GraphDB → export RDF → n10s import → Neo4j

And the DIRECT LOAD PATTERN:
  JSON data → APOC load → Neo4j
"""
from neo4j import GraphDatabase
import json
import os

NEO4J_URI = "bolt://localhost:7687"
NEO4J_AUTH = ("neo4j", "kycpassword123")

driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)

def run(cypher, params=None):
    with driver.session() as s:
        return list(s.run(cypher, params or {}))

# ── STEP 1: Initialize neosemantics ──────────────────────────────────────────
print("Initializing neosemantics...")
run("CALL n10s.graphconfig.init({handleVocabUris: 'SHORTEN', applyNeo4jNaming: true})")
run("CREATE CONSTRAINT n10s_unique_uri IF NOT EXISTS FOR (r:Resource) REQUIRE r.uri IS UNIQUE")

# ── STEP 2: Import FIBO class structure from GraphDB via n10s ─────────────────
# This gives us the ontology as a graph of :Class nodes
print("Importing FIBO ontology structure via neosemantics...")
run("""
CALL n10s.onto.import.fetch(
    "https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/",
    "Turtle"
)
""")
run("""
CALL n10s.onto.import.fetch(
    "https://spec.edmcouncil.org/fibo/ontology/BE/OwnershipAndControl/Ownership/",
    "Turtle"
)
""")

print("FIBO classes loaded in Neo4j:")
result = run("MATCH (c:Class) RETURN c.name LIMIT 20")
for r in result:
    print(f"  Class: {r['c.name']}")

# ── STEP 3: Create indexes for performance ────────────────────────────────────
INDEXES = [
    "CREATE INDEX entity_lei IF NOT EXISTS FOR (e:LegalEntity) ON (e.lei)",
    "CREATE INDEX entity_name IF NOT EXISTS FOR (e:LegalEntity) ON (e.name)",
    "CREATE INDEX person_name IF NOT EXISTS FOR (p:NaturalPerson) ON (p.name)",
    "CREATE INDEX txn_date IF NOT EXISTS FOR (t:Transaction) ON (t.date)",
    "CREATE INDEX txn_amount IF NOT EXISTS FOR (t:Transaction) ON (t.amount)",
    "CREATE CONSTRAINT entity_lei_unique IF NOT EXISTS FOR (e:LegalEntity) REQUIRE e.lei IS UNIQUE",
]
for idx in INDEXES:
    run(idx)
print("Indexes created")

# ── STEP 4: Load entities ─────────────────────────────────────────────────────
print("Loading entities into Neo4j...")
with open("data/synthetic/kyc_dataset.json") as f:
    dataset = json.load(f)

RISK_COLORS = {"low": "#27ae60", "medium": "#f39c12", "high": "#e74c3c"}

with driver.session() as session:
    # Batch load legal entities
    session.run("""
    UNWIND $entities AS e
    MERGE (n:LegalEntity {lei: e.lei})
    SET n.id = e.id,
        n.name = e.name,
        n.jurisdiction = e.jurisdiction,
        n.jurisdiction_name = e.jurisdiction_name,
        n.riskTier = e.risk_tier,
        n.category = e.category,
        n.incorporatedDate = e.incorporated_date,
        n.isActive = e.is_active,
        n.hasOperationalAddress = e.has_operational_address,
        n.isin = e.isin
    """, entities=dataset["entities"])
    print(f"  Loaded {len(dataset['entities'])} entities")
    
    # Load natural persons
    session.run("""
    UNWIND $persons AS p
    MERGE (n:NaturalPerson {id: p.id})
    SET n.name = p.name,
        n.nationality = p.nationality,
        n.dob = p.dob,
        n.isPEP = p.is_pep,
        n.isSanctioned = p.is_sanctioned
    WITH n, p
    FOREACH (_ IN CASE WHEN p.is_sanctioned THEN [1] ELSE [] END |
      SET n:SanctionedEntity
    )
    FOREACH (_ IN CASE WHEN p.is_pep THEN [1] ELSE [] END |
      SET n:PoliticallyExposedPerson
    )
    """, persons=dataset["persons"])
    print(f"  Loaded {len(dataset['persons'])} persons")
    
    # Load ownership relationships
    session.run("""
    UNWIND $rels AS r
    MATCH (from {id: r.from})
    MATCH (to {id: r.to})
    CALL apoc.create.relationship(from, r.type, 
        {percentage: r.percentage, since: r.since, role: r.role}, to)
    YIELD rel RETURN rel
    """, rels=dataset["relationships"])
    print(f"  Loaded {len(dataset['relationships'])} relationships")
    
    # Load transactions
    session.run("""
    UNWIND $txns AS t
    MATCH (from:LegalEntity {id: t.from_entity})
    MATCH (to:LegalEntity {id: t.to_entity})
    CREATE (from)-[:TRANSACTION {
        id: t.id,
        amount: t.amount,
        currency: t.currency,
        date: t.date,
        isSuspicious: t.is_suspicious
    }]->(to)
    """, txns=dataset["transactions"])
    print(f"  Loaded {len(dataset['transactions'])} transactions")

print("Neo4j loading complete!")
driver.close()
```

---

### Phase 5: neosemantics — The Barrasa Bridge Pattern (Week 3, Day 3)

#### Script: `scripts/05_barrasa_bridge_pattern.py`
```python
"""
The Barrasa Bridge Pattern: GraphDB ↔ Neo4j

Jesús Barrasa's core contribution (Going Meta, neosemantics):
"Use RDF/SPARQL for semantic precision, Neo4j/Cypher for application power."

This script demonstrates the 4 bridge techniques from the Going Meta series:
1. n10s.onto.import.fetch — import ontology structure
2. n10s.rdf.import.fetch  — import RDF instance data
3. n10s.rdf.export        — export Neo4j graph as RDF (round-trip)
4. Cypher + SPARQL hybrid queries via n10s endpoint
"""
from neo4j import GraphDatabase
import requests

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "kycpassword123"))

# ── TECHNIQUE 1: Validate graph against SHACL shapes ─────────────────────────
# SHACL shapes = "rules" about what valid data looks like
# e.g., "every LegalEntity must have a lei property"
print("\n1. SHACL Validation via n10s")

SHACL_SHAPES = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix kyc: <http://kyc-kg.example.org/ontology#> .

kyc:LegalEntityShape
    a sh:NodeShape ;
    sh:targetClass kyc:RegisteredLegalEntity ;
    sh:property [
        sh:path kyc:leiCode ;
        sh:minCount 1 ;
        sh:maxLength 20 ;
        sh:name "LEI Code" ;
    ] ;
    sh:property [
        sh:path kyc:legalName ;
        sh:minCount 1 ;
        sh:name "Legal Name" ;
    ] .
"""

with open("/tmp/shacl_shapes.ttl", "w") as f:
    f.write(SHACL_SHAPES)

with driver.session() as session:
    result = session.run("""
    CALL n10s.validation.shacl.validate()
    YIELD focusNode, nodeType, shapeId, propertyShape, offendingValue, resultPath, severity, resultMessage
    RETURN focusNode, resultMessage, severity LIMIT 10
    """)
    violations = list(result)
    print(f"  SHACL violations found: {len(violations)}")
    for v in violations[:3]:
        print(f"    {v['severity']}: {v['resultMessage']}")

# ── TECHNIQUE 2: Query Neo4j graph as if it were RDF (n10s endpoint) ─────────
print("\n2. Neo4j as RDF endpoint (n10s HTTP API)")
# n10s exposes your Neo4j graph as RDF at: http://localhost:7474/rdf/
# You can then query it with SPARQL from GraphDB (federation)!
# This is the "bidirectional bridge"

neo4j_rdf_url = "http://localhost:7474/rdf/neo4j/describe/node"
print(f"  Neo4j RDF endpoint: {neo4j_rdf_url}")
print("  This endpoint lets GraphDB query Neo4j via SPARQL SERVICE clause")

# ── TECHNIQUE 3: Export Neo4j subgraph as RDF back to GraphDB ─────────────────
print("\n3. Export Neo4j → RDF → GraphDB (round trip)")
with driver.session() as session:
    # Export high-risk entities as RDF
    result = session.run("""
    CALL n10s.rdf.export.cypher(
      'MATCH (e:LegalEntity) WHERE e.riskTier = "high" RETURN e',
      {}
    )
    YIELD data
    RETURN data
    """)
    for r in result:
        rdf_data = r["data"]
        # Load this RDF back into GraphDB for semantic analysis
        requests.post(
            "http://localhost:7200/repositories/kyc-kg/rdf-graphs/service",
            params={"graph": "http://kg/neo4j-export/high-risk"},
            data=rdf_data.encode(),
            headers={"Content-Type": "application/n-triples"}
        )
    print("  High-risk entities exported to GraphDB named graph")

driver.close()
```

---

### Phase 6: GDS — Graph Algorithms for KYC (Week 4, Day 1-3)

#### Script: `scripts/06_gds_kyc_analysis.py`
```python
"""
Graph Data Science for KYC Risk Scoring.

This is WHY NEO4J EXISTS IN THE STACK — GraphDB cannot do any of this.

Algorithms used:
  1. Weakly Connected Components — find isolated entity clusters
  2. Louvain Community Detection — find suspicious rings
  3. PageRank — systemic risk scoring (who is most connected?)
  4. Betweenness Centrality — find "bridge" entities in money flows
  5. Shortest Path — connect investigative leads

All inspired by jbarrasa/goingmeta and Neo4j AML documentation.
"""
from neo4j import GraphDatabase
import pandas as pd

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "kycpassword123"))

def run(cypher, params=None):
    with driver.session() as s:
        return [dict(r) for r in s.run(cypher, params or {})]

# ── Step 1: Create GDS graph projection ───────────────────────────────────────
print("Creating GDS graph projection...")
run("CALL gds.graph.drop('kyc-graph', false) YIELD graphName")
run("""
CALL gds.graph.project(
  'kyc-graph',
  ['LegalEntity', 'NaturalPerson'],
  {
    DIRECTLY_OWNED_BY: {orientation: 'UNDIRECTED', properties: ['percentage']},
    CONTROLLED_BY: {orientation: 'UNDIRECTED'},
    TRANSACTION: {orientation: 'NATURAL', properties: ['amount']}
  }
)
""")
print("  Graph projected into GDS memory")

# ── Algorithm 1: Weakly Connected Components ──────────────────────────────────
print("\nAlgorithm 1: Connected Components")
run("""
CALL gds.wcc.write('kyc-graph', {writeProperty: 'componentId'})
""")
result = run("""
MATCH (n) WHERE exists(n.componentId)
WITH n.componentId AS comp, count(*) AS size
WHERE size > 5
RETURN comp, size ORDER BY size DESC LIMIT 10
""")
print("Largest components:")
for r in result[:5]:
    print(f"  Component {r['comp']}: {r['size']} entities")

# ── Algorithm 2: Louvain Community Detection ──────────────────────────────────
print("\nAlgorithm 2: Louvain Communities (finds suspicious rings)")
run("""
CALL gds.louvain.write('kyc-graph', {writeProperty: 'communityId'})
""")
result = run("""
MATCH (n) WHERE exists(n.communityId)
WITH n.communityId AS comm, collect(n.name) AS members, count(*) AS size
WHERE size BETWEEN 3 AND 10
RETURN comm, size, members[0..5] AS sample_members
ORDER BY size DESC LIMIT 10
""")
print("Suspicious small communities (potential rings):")
for r in result[:5]:
    print(f"  Community {r['comm']}: {r['size']} members — {r['sample_members']}")

# ── Algorithm 3: PageRank (systemic risk) ─────────────────────────────────────
print("\nAlgorithm 3: PageRank (most systemically connected entities)")
run("""
CALL gds.pageRank.write('kyc-graph', {
  writeProperty: 'pageRankScore',
  dampingFactor: 0.85,
  maxIterations: 20
})
""")
result = run("""
MATCH (n) WHERE exists(n.pageRankScore)
RETURN n.name AS name, n.lei AS lei, n.jurisdiction AS juris,
       n.riskTier AS risk, round(n.pageRankScore, 4) AS score
ORDER BY score DESC LIMIT 15
""")
print("Most systemically important entities:")
for r in result[:10]:
    print(f"  {r['name']} ({r['juris']}, {r['risk']} risk): score={r['score']}")

# ── Algorithm 4: Betweenness Centrality ───────────────────────────────────────
print("\nAlgorithm 4: Betweenness Centrality (bridge entities in money flow)")
run("""
CALL gds.betweenness.write('kyc-graph', {writeProperty: 'betweennessScore'})
""")
result = run("""
MATCH (n:LegalEntity) WHERE exists(n.betweennessScore) AND n.betweennessScore > 0
RETURN n.name, n.jurisdiction, n.riskTier, round(n.betweennessScore, 2) as score
ORDER BY score DESC LIMIT 10
""")
print("Bridge entities (high betweenness = money laundering conduits):")
for r in result[:5]:
    print(f"  {r['n.name']} ({r['n.jurisdiction']}): betweenness={r['score']}")

# ── Algorithm 5: Composite KYC Risk Score ────────────────────────────────────
print("\nComputing composite KYC risk scores...")
run("""
MATCH (n:LegalEntity)
WHERE exists(n.pageRankScore) AND exists(n.betweennessScore)
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

# Find highest risk entities
result = run("""
MATCH (n:LegalEntity)
WHERE exists(n.kycRiskScore)
RETURN n.name, n.lei, n.jurisdiction, n.riskTier,
       round(n.kycRiskScore, 2) AS riskScore
ORDER BY riskScore DESC LIMIT 20
""")
print("\nTOP 10 HIGHEST KYC RISK ENTITIES:")
for r in result[:10]:
    print(f"  [{r['riskScore']}] {r['n.name']} — {r['jurisdiction']} ({r['riskTier']})")

driver.close()
```

---

### Phase 7: GraphRAG KYC Agent (Week 4, Day 4-5)

#### Script: `scripts/07_graphrag_kyc_agent.py`
```python
"""
GraphRAG KYC Investigation Agent.
Built with LangGraph + Neo4j + Claude/OpenAI.

Architecture (from Going Meta S02/S03 and Neo4j KYC blog):
  User Question
      ↓
  LangGraph Agent (decides which tool to use)
      ├── tool: cypher_query    → runs Cypher on Neo4j
      ├── tool: sparql_query    → queries GraphDB (FIBO semantics)
      ├── tool: ubo_discovery   → traverses ownership chains
      ├── tool: sanctions_check → checks against OFAC list
      └── tool: risk_score      → returns GDS-computed risk score
      ↓
  LLM synthesizes answer + cites graph evidence
"""
from langchain_neo4j import Neo4jGraph, GraphCypherQAChain
from langchain_anthropic import ChatAnthropic
from langchain.tools import tool
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import create_react_agent
from typing import TypedDict, Annotated
import operator

# Neo4j connection
graph = Neo4jGraph(
    url="bolt://localhost:7687",
    username="neo4j",
    password="kycpassword123"
)

# LLM
llm = ChatAnthropic(model="claude-sonnet-4-20250514")

# ── Tools ────────────────────────────────────────────────────────────────────
@tool
def find_ubo(company_name: str) -> str:
    """Find the Ultimate Beneficial Owner (UBO) of a company by traversing ownership chains."""
    result = graph.query("""
    MATCH (e:LegalEntity)
    WHERE toLower(e.name) CONTAINS toLower($name)
    WITH e LIMIT 1
    MATCH path = (e)-[:DIRECTLY_OWNED_BY*1..10]->(owner)
    WHERE NOT (owner)-[:DIRECTLY_OWNED_BY]->()
    RETURN e.name AS company,
           owner.name AS ultimate_owner,
           length(path) AS hops,
           owner.isSanctioned AS is_sanctioned,
           owner.isPEP AS is_pep,
           owner.nationality AS nationality
    ORDER BY hops
    LIMIT 5
    """, params={"name": company_name})
    
    if not result:
        return f"No UBO found for company matching '{company_name}'"
    
    output = f"Ultimate Beneficial Owners for companies matching '{company_name}':\n"
    for r in result:
        flags = []
        if r.get("is_sanctioned"): flags.append("⚠️ SANCTIONED")
        if r.get("is_pep"): flags.append("🏛️ PEP")
        flag_str = " ".join(flags) or "✅ No flags"
        output += f"\n  Company: {r['company']}\n"
        output += f"  UBO: {r['ultimate_owner']} ({r['nationality']})\n"
        output += f"  Ownership depth: {r['hops']} hops\n"
        output += f"  Status: {flag_str}\n"
    return output

@tool
def check_sanctions_proximity(lei: str, max_hops: int = 3) -> str:
    """Check if an entity is within N hops of any sanctioned entity."""
    result = graph.query("""
    MATCH (e:LegalEntity {lei: $lei})
    MATCH path = (e)-[:DIRECTLY_OWNED_BY|CONTROLLED_BY*1..$hops]-(risky:SanctionedEntity)
    RETURN e.name AS entity,
           risky.name AS sanctioned_entity,
           length(path) AS hops,
           [n IN nodes(path) | coalesce(n.name, 'unknown')] AS path_names
    ORDER BY hops
    LIMIT 5
    """, params={"lei": lei, "hops": max_hops})
    
    if not result:
        return f"No sanctions proximity found within {max_hops} hops for LEI {lei}"
    
    output = f"⚠️ SANCTIONS PROXIMITY ALERT for LEI {lei}:\n"
    for r in result:
        output += f"\n  Entity: {r['entity']}\n"
        output += f"  Connected to sanctioned: {r['sanctioned_entity']}\n"
        output += f"  Hops away: {r['hops']}\n"
        output += f"  Path: {' → '.join(r['path_names'])}\n"
    return output

@tool
def get_risk_score(company_name: str) -> str:
    """Get the KYC risk score and breakdown for a company."""
    result = graph.query("""
    MATCH (e:LegalEntity)
    WHERE toLower(e.name) CONTAINS toLower($name)
    RETURN e.name, e.lei, e.jurisdiction, e.riskTier,
           round(e.kycRiskScore, 2) AS riskScore,
           round(e.pageRankScore, 4) AS systemicImportance,
           round(e.betweennessScore, 2) AS bridgeScore,
           e.isActive, e.hasOperationalAddress
    ORDER BY riskScore DESC LIMIT 3
    """, params={"name": company_name})
    
    if not result:
        return f"No entity found matching '{company_name}'"
    
    r = result[0]
    return (
        f"KYC Risk Assessment for {r['e.name']}:\n"
        f"  LEI: {r['e.lei']}\n"
        f"  Overall Risk Score: {r['riskScore']}/100\n"
        f"  Jurisdiction: {r['e.jurisdiction']} (tier: {r['e.riskTier']})\n"
        f"  Systemic Importance (PageRank): {r['systemicImportance']}\n"
        f"  Bridge Centrality: {r['bridgeScore']}\n"
        f"  Is Active: {r['e.isActive']}\n"
        f"  Has Operational Address: {r['e.hasOperationalAddress']}\n"
    )

@tool
def find_circular_ownership(community_id: str = None) -> str:
    """Detect circular ownership structures (potential shell company rings)."""
    result = graph.query("""
    MATCH (a:LegalEntity)-[:DIRECTLY_OWNED_BY]->(b:LegalEntity)
           -[:DIRECTLY_OWNED_BY]->(c:LegalEntity)
           -[:DIRECTLY_OWNED_BY]->(a)
    RETURN a.name AS entity_a, b.name AS entity_b, c.name AS entity_c,
           a.jurisdiction AS juris_a, b.jurisdiction AS juris_b
    LIMIT 10
    """)
    
    if not result:
        return "No circular ownership detected"
    
    output = f"🔄 CIRCULAR OWNERSHIP DETECTED — {len(result)} rings:\n"
    for r in result:
        output += f"\n  Ring: {r['entity_a']} → {r['entity_b']} → {r['entity_c']} → (back to first)\n"
        output += f"  Jurisdictions: {r['juris_a']}, {r['juris_b']}\n"
    return output

# ── LangGraph Agent ───────────────────────────────────────────────────────────
tools = [find_ubo, check_sanctions_proximity, get_risk_score, find_circular_ownership]
agent = create_react_agent(llm, tools)

def run_investigation(question: str):
    """Run a KYC investigation query through the GraphRAG agent."""
    print(f"\n{'='*60}")
    print(f"INVESTIGATOR: {question}")
    print(f"{'='*60}")
    
    result = agent.invoke({"messages": [("user", question)]})
    
    final_answer = result["messages"][-1].content
    print(f"\nAGENT RESPONSE:\n{final_answer}")
    return final_answer

if __name__ == "__main__":
    # Test investigation queries
    investigations = [
        "Who ultimately owns the company called 'Shell Corp'? Are there any sanctions flags?",
        "Find any circular ownership structures in our database",
        "What are the top 5 highest risk entities and why?",
        "Are there any entities connected to sanctioned persons within 3 hops?",
    ]
    
    for question in investigations:
        run_investigation(question)
```

---

### Phase 8: Streamlit Dashboard (Week 5)

#### File: `dashboard/app.py`
```python
"""
KYC Beneficial Ownership Intelligence Dashboard.
Connects to Neo4j for graph queries and GDS results.
"""
import streamlit as st
from neo4j import GraphDatabase
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

st.set_page_config(page_title="KYC Intelligence", page_icon="🔍", layout="wide")

@st.cache_resource
def get_driver():
    return GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "kycpassword123"))

def query(cypher, params=None):
    with get_driver().session() as s:
        return [dict(r) for r in s.run(cypher, params or {})]

# Header
st.title("🔍 KYC Beneficial Ownership Intelligence")
st.caption("Powered by FIBO · GLEIF · Neo4j GDS · GraphRAG")

# KPIs
col1, col2, col3, col4 = st.columns(4)
stats = query("MATCH (e:LegalEntity) RETURN count(e) as total, "
              "sum(CASE WHEN e.riskTier='high' THEN 1 ELSE 0 END) as high_risk")[0]
sanctions = query("MATCH (p:SanctionedEntity) RETURN count(p) as count")[0]
circular = query("MATCH (a:LegalEntity)-[:DIRECTLY_OWNED_BY]->(b)-[:DIRECTLY_OWNED_BY]->(c)-[:DIRECTLY_OWNED_BY]->(a) "
                 "RETURN count(distinct a) as count")[0]

col1.metric("Total Entities", stats["total"])
col2.metric("High Risk Entities", stats["high_risk"], delta="needs review")
col3.metric("Sanctioned Connections", sanctions["count"], delta_color="inverse")
col4.metric("Circular Ownership Rings", circular["count"], delta_color="inverse")

# Risk Distribution
st.subheader("Entity Risk Distribution by Jurisdiction")
risk_data = query("""
MATCH (e:LegalEntity)
WHERE exists(e.kycRiskScore)
RETURN e.jurisdiction AS jurisdiction, e.riskTier AS tier,
       avg(e.kycRiskScore) AS avg_score, count(*) AS count
ORDER BY avg_score DESC
""")
if risk_data:
    df = pd.DataFrame(risk_data)
    fig = px.scatter(df, x="jurisdiction", y="avg_score", size="count",
                     color="tier", color_discrete_map={"high":"red","medium":"orange","low":"green"},
                     title="Average KYC Risk Score by Jurisdiction")
    st.plotly_chart(fig, use_container_width=True)

# Top risk entities table
st.subheader("⚠️ Highest Risk Entities")
top_risk = query("""
MATCH (e:LegalEntity)
WHERE exists(e.kycRiskScore)
RETURN e.name AS name, e.lei AS lei, e.jurisdiction AS jurisdiction,
       e.riskTier AS tier, round(e.kycRiskScore, 1) AS risk_score,
       round(e.pageRankScore, 4) AS systemic_importance
ORDER BY risk_score DESC LIMIT 20
""")
if top_risk:
    df = pd.DataFrame(top_risk)
    st.dataframe(df, use_container_width=True)

# UBO Search
st.subheader("🔍 UBO Investigation")
company = st.text_input("Enter company name to find Ultimate Beneficial Owner:")
if company:
    ubo_result = query("""
    MATCH (e:LegalEntity)
    WHERE toLower(e.name) CONTAINS toLower($name)
    WITH e LIMIT 1
    MATCH path = (e)-[:DIRECTLY_OWNED_BY*1..10]->(owner)
    WHERE NOT (owner)-[:DIRECTLY_OWNED_BY]->()
    RETURN e.name, owner.name as ubo, length(path) as hops,
           owner.isSanctioned, owner.nationality
    ORDER BY hops LIMIT 5
    """, {"name": company})
    
    if ubo_result:
        for r in ubo_result:
            icon = "⚠️" if r.get("owner.isSanctioned") else "✅"
            st.write(f"{icon} **{r['e.name']}** → (via {r['hops']} hops) → **{r['ubo']}** ({r['owner.nationality']})")
    else:
        st.info("No entity found or no ownership chain detected")
```

---

## PART 4 — PROJECT FOLDER STRUCTURE (Claude Code)

```
kyc-intelligence/
├── docker-compose.yml
├── requirements.txt
├── .env
├── graphdb_config/
│   └── kyc-repo-config.ttl
├── data/
│   ├── fibo/              # Downloaded FIBO TTL modules
│   ├── glei/              # GLEIF API data (RDF)
│   ├── lcc/               # LCC country codes
│   ├── ofac/              # OFAC sanctions list (XML)
│   └── synthetic/         # Generated KYC test dataset
├── scripts/
│   ├── 01_setup_graphdb.py        # Create repo, load FIBO+LCC ontologies
│   ├── 02_load_glei_data.py       # GLEIF API → RDF → GraphDB
│   ├── 03_generate_kyc_dataset.py # Synthetic entities+ownership+txns
│   ├── 04_load_neo4j.py           # Dataset → Neo4j (n10s + Cypher)
│   ├── 05_barrasa_bridge_pattern.py # GraphDB ↔ Neo4j bridge
│   ├── 06_gds_kyc_analysis.py     # GDS algorithms: WCC, Louvain, PageRank
│   └── 07_graphrag_kyc_agent.py   # LangGraph + Claude KYC agent
├── dashboard/
│   ├── Dockerfile
│   └── app.py                     # Streamlit KYC dashboard
├── sparql/
│   ├── 01_explore_fibo_classes.sparql
│   ├── 02_glei_entities.sparql
│   └── 03_cross_ontology_query.sparql
├── cypher/
│   ├── 01_n10s_setup.cypher
│   ├── 02_gds_projections.cypher
│   └── 03_kyc_investigations.cypher
└── notebooks/
    ├── 01_fibo_exploration.ipynb
    ├── 02_gleif_data_analysis.ipynb
    └── 03_gds_risk_scoring.ipynb
```

---

## PART 5 — LEARNING SEQUENCE (5 Weeks)

| Week | Focus | What You'll Understand |
|---|---|---|
| 1 | GraphDB + FIBO ontology | What OWL means, what classes/properties are, SPARQL |
| 2 | GLEI data + RDF instances | Difference between ontology and data; named graphs |
| 3 | Neo4j + n10s bridge | Why Cypher is easier; the Barrasa bridge pattern |
| 4 | GDS algorithms | PageRank, Louvain, community detection on real data |
| 5 | GraphRAG agent | LLM + KG = explainable AI investigation tool |

---

## PART 6 — KEY REFERENCE URLS

| Resource | URL | What it teaches |
|---|---|---|
| Barrasa Going Meta | github.com/jbarrasa/goingmeta | KG + semantics + AI, each session |
| Neo4j KYC GraphRAG | neo4j.com/blog/developer/graphrag-in-action-know-your-customer/ | The KYC agent pattern |
| FIBO GitHub | github.com/edmcouncil/fibo | All 200+ modules, browsable |
| FIB-DM | fib-dm.com | FIBO as data model (for schema reference) |
| GLEIF API | api.gleif.org/api/v1 | Real LEI data |
| GLEIF Golden Copy | gleif.org/en/lei-data/gleif-golden-copy | Full dataset download |
| Neubauten Public | neubauten.ontotext.com:7200 | Live demo to query |
| n10s Manual | neo4j.com/labs/neosemantics/ | All n10s procedures |
| GDS Manual | neo4j.com/docs/graph-data-science/ | All GDS algorithms |
| OFAC SDN | home.treasury.gov/policy-issues/financial-sanctions/specially-designated-nationals | Sanctions list |
```
