# Neubauten Demo Graph — Implementation Plan
## Learning Financial Knowledge Graphs with GraphDB & Neo4j

> **Purpose**: A step-by-step, Claude Code–executable plan to reproduce and deeply understand the Neubauten Demo Graph, which uses FIBO, GLEI, LCC, FIBO2GLEI, LEI2ISIN and Ontological Shortcuts loaded into GraphDB (Ontotext), with a parallel Neo4j + neosemantics exploration track.

---

## 0. What You're Looking At (Plain English Primer)

Before touching any code, understand what each component IS:

| Component | What it is | Analogy |
|---|---|---|
| **FIBO** | Financial Industry Business Ontology — a giant OWL vocabulary of every financial concept (loans, bonds, legal entities, derivatives…) | A "dictionary + grammar" of finance |
| **GLEI / LEI** | Global Legal Entity Identifier — 20-char codes that uniquely identify every company worldwide (like Aadhaar for companies) | PAN card for companies globally |
| **FIBO2GLEI** | A mapping ontology that links FIBO classes to GLEI data model | Translator between two dictionaries |
| **LCC** | Languages, Countries and Codes — ISO country/language codes as RDF | ISO standard as a graph |
| **LEI2ISIN** | Maps LEI (entity identifier) to ISIN (instrument/security identifier) | Links company identity → securities it issues |
| **Ontological Shortcuts** | Inferred "shortcut" relationships about ownership derived by reasoning | Computed edges from reasoning |
| **GraphDB** | An RDF triplestore by Ontotext — natively understands OWL, SPARQL, reasoning | The "right tool" for ontologies |
| **Neo4j** | Property graph database — nodes + relationships + properties, uses Cypher | The "practical tool" for graph analytics |
| **neosemantics (n10s)** | Neo4j plugin to import/export RDF into Neo4j | Bridge between RDF world and Neo4j |
| **APOC** | Neo4j plugin: utilities for loading data, procedures, graph algorithms | Swiss Army knife for Neo4j |
| **GDS** | Graph Data Science library — community detection, centrality, pathfinding | ML on graphs |

---

## 1. Environment Setup

### 1.1 Docker Compose — Both GraphDB and Neo4j

Create `docker-compose.yml`:

```yaml
version: "3.8"
services:

  graphdb:
    image: ontotext/graphdb:10.7.0
    container_name: graphdb
    ports:
      - "7200:7200"
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
    container_name: neo4j
    ports:
      - "7474:7474"   # Browser
      - "7687:7687"   # Bolt
    volumes:
      - neo4j_data:/data
      - neo4j_plugins:/plugins
      - ./neo4j_import:/var/lib/neo4j/import
    environment:
      NEO4J_AUTH: neo4j/password123
      NEO4J_PLUGINS: '["apoc", "graph-data-science", "n10s"]'
      NEO4J_dbms_security_procedures_unrestricted: "apoc.*,n10s.*,gds.*"
      NEO4J_dbms_security_procedures_allowlist: "apoc.*,n10s.*,gds.*"
      NEO4J_server_memory_heap_initial__size: "2G"
      NEO4J_server_memory_heap_max__size: "4G"
    restart: unless-stopped

volumes:
  graphdb_data:
  neo4j_data:
  neo4j_plugins:
```

**Run:**
```bash
docker-compose up -d
# Wait ~60s for both to start
# GraphDB UI: http://localhost:7200
# Neo4j Browser: http://localhost:7474
```

### 1.2 Python Environment

```bash
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate

pip install \
  rdflib==7.0.0 \
  SPARQLWrapper==2.0.0 \
  requests==2.31.0 \
  pandas==2.2.0 \
  neo4j==5.20.0 \
  python-dotenv==1.0.0 \
  tqdm==4.66.0 \
  httpx==0.27.0 \
  jupyter==1.0.0
```

Create `.env`:
```
GRAPHDB_URL=http://localhost:7200
GRAPHDB_REPO=neubauten
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password123
```

---

## 2. GraphDB Repository Creation

### 2.1 Create the Neubauten Repository

Create `graphdb_config/neubauten-config.ttl`:

```turtle
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix rep: <http://www.openrdf.org/config/repository#> .
@prefix sr: <http://www.openrdf.org/config/repository/sail#> .
@prefix sail: <http://www.openrdf.org/config/sail#> .
@prefix graphdb: <http://www.ontotext.com/config/graphdb#> .

[] a rep:Repository ;
   rep:repositoryID "neubauten" ;
   rdfs:label "Neubauten Demo Graph" ;
   rep:repositoryImpl [
      rep:repositoryType "graphdb:FreeSailRepository" ;
      sr:sailImpl [
         sail:sailType "graphdb:FreeSail" ;
         graphdb:base-URL "http://example.org/owlim#" ;
         graphdb:defaultNS "" ;
         graphdb:entity-index-size "10000000" ;
         graphdb:entity-id-size "32" ;
         graphdb:ruleset "owl-horst-optimized" ;
         graphdb:storage-folder "storage" ;
         graphdb:enable-context-index "true" ;
         graphdb:enablePredicateList "true" ;
         graphdb:in-memory-literal-properties "true" ;
         graphdb:enable-literal-index "true" ;
         graphdb:check-for-inconsistencies "false" ;
      ]
   ] .
```

Create via API:
```python
# scripts/01_create_graphdb_repo.py
import requests

GRAPHDB_URL = "http://localhost:7200"
REPO_ID = "neubauten"

# Create repository from config
with open("graphdb_config/neubauten-config.ttl", "rb") as f:
    response = requests.post(
        f"{GRAPHDB_URL}/rest/repositories",
        files={"config": f},
        headers={"Accept": "application/json"}
    )
print(f"Repository created: {response.status_code}")

# Verify
r = requests.get(f"{GRAPHDB_URL}/rest/repositories")
repos = [rep["id"] for rep in r.json()]
print(f"Repositories: {repos}")
```

**Key Learning**: GraphDB uses "rulesets" — `owl-horst-optimized` enables OWL reasoning (inferencing). This is why "Ontological Shortcuts" (ownership chains) appear automatically when you load the data.

---

## 3. Loading Ontologies into GraphDB

### 3.1 Understanding: What is an Ontology File?

Ontologies come as `.ttl` (Turtle), `.owl` (OWL/XML), `.rdf` (RDF/XML), or `.n3` files. Each is just a serialization of RDF triples.

FIBO has **200+ modules** organized hierarchically. You don't need all of them — start with key modules.

### 3.2 FIBO — Financial Industry Business Ontology

**Source**: https://spec.edmcouncil.org/fibo/

```python
# scripts/02_load_fibo.py
"""
FIBO Module Strategy:
- Production full FIBO: ~200MB of TTL files, complex to load
- LEARNING approach: Load specific modules you need
- Neubauten uses: Business Entities, Legal Entities, Ownership & Control

Key FIBO GitHub: https://github.com/edmcouncil/fibo
"""
import requests

GRAPHDB_URL = "http://localhost:7200"
REPO_ID = "neubauten"
SPARQL_UPDATE = f"{GRAPHDB_URL}/repositories/{REPO_ID}/statements"

# FIBO modules relevant to Neubauten demo
# These are the production CDN URLs from FIBO
FIBO_MODULES = [
    # Foundation modules (load first - others depend on these)
    {
        "url": "https://spec.edmcouncil.org/fibo/ontology/FND/Utilities/AnnotationVocabulary/",
        "name": "FIBO Annotation Vocabulary",
        "named_graph": "http://fibo/annotation"
    },
    {
        "url": "https://spec.edmcouncil.org/fibo/ontology/FND/Relations/Relations/",
        "name": "FIBO Relations",
        "named_graph": "http://fibo/relations"
    },
    # Business Entities
    {
        "url": "https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/",
        "name": "FIBO Legal Persons",
        "named_graph": "http://fibo/be/legal-persons"
    },
    {
        "url": "https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/CorporateBodies/",
        "name": "FIBO Corporate Bodies",
        "named_graph": "http://fibo/be/corporate-bodies"
    },
    {
        "url": "https://spec.edmcouncil.org/fibo/ontology/BE/OwnershipAndControl/Ownership/",
        "name": "FIBO Ownership",
        "named_graph": "http://fibo/be/ownership"
    },
    {
        "url": "https://spec.edmcouncil.org/fibo/ontology/BE/OwnershipAndControl/Control/",
        "name": "FIBO Control",
        "named_graph": "http://fibo/be/control"
    },
]

def load_rdf_url(url, named_graph, name, format="text/turtle"):
    """Load a remote RDF URL into GraphDB named graph."""
    sparql_update = f"""
    LOAD <{url}> INTO GRAPH <{named_graph}>
    """
    headers = {"Content-Type": "application/sparql-update"}
    r = requests.post(SPARQL_UPDATE, data=sparql_update, headers=headers)
    print(f"{'✓' if r.status_code == 204 else '✗'} {name}: {r.status_code}")
    return r.status_code == 204

# Alternative: Use n10s REST API approach for GraphDB
def load_via_rest(url, named_graph, name, rdf_format="text/turtle"):
    """Use GraphDB REST Import API."""
    endpoint = f"{GRAPHDB_URL}/repositories/{REPO_ID}/rdf-graphs/service"
    params = {"graph": named_graph}
    # Tell GraphDB to fetch from URL
    sparql = f"LOAD <{url}> INTO GRAPH <{named_graph}>"
    r = requests.post(
        f"{GRAPHDB_URL}/repositories/{REPO_ID}/statements",
        data=sparql,
        headers={"Content-Type": "application/sparql-update"},
        params=params
    )
    print(f"{'✓' if r.ok else '✗'} {name}: {r.status_code} - {r.text[:200]}")

if __name__ == "__main__":
    for module in FIBO_MODULES:
        load_rdf_url(module["url"], module["named_graph"], module["name"])
```

**Alternative: Download and Load Local Files (more reliable)**:

```python
# scripts/02b_download_fibo_local.py
"""
Download FIBO production build locally then load.
This is more reliable than live URL loading.
"""
import subprocess
import os
import requests

FIBO_RELEASE_URL = "https://spec.edmcouncil.org/fibo/ontology/master/latest/"

# Download specific TTL files
FIBO_FILES = {
    "fibo-be-legal-persons.ttl": "https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/",
    "fibo-be-corp-bodies.ttl": "https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/CorporateBodies/",
    "fibo-be-ownership.ttl": "https://spec.edmcouncil.org/fibo/ontology/BE/OwnershipAndControl/Ownership/",
    "fibo-be-control.ttl": "https://spec.edmcouncil.org/fibo/ontology/BE/OwnershipAndControl/Control/",
    "fibo-be-corps-corps.ttl": "https://spec.edmcouncil.org/fibo/ontology/BE/Corporations/Corporations/",
    "fibo-fnd-relations.ttl": "https://spec.edmcouncil.org/fibo/ontology/FND/Relations/Relations/",
}

os.makedirs("data/fibo", exist_ok=True)

for filename, url in FIBO_FILES.items():
    outpath = f"data/fibo/{filename}"
    if os.path.exists(outpath):
        print(f"Already downloaded: {filename}")
        continue
    print(f"Downloading {filename}...")
    r = requests.get(url, headers={"Accept": "text/turtle"}, timeout=30)
    if r.ok:
        with open(outpath, "w") as f:
            f.write(r.text)
        print(f"  Saved {len(r.text):,} chars")
    else:
        print(f"  FAILED: {r.status_code}")

# Load local files into GraphDB
def load_local_file(filepath, named_graph, graphdb_url, repo_id):
    """Upload local RDF file to GraphDB."""
    url = f"{graphdb_url}/repositories/{repo_id}/rdf-graphs/service"
    with open(filepath, "rb") as f:
        r = requests.post(
            url,
            params={"graph": named_graph},
            data=f,
            headers={"Content-Type": "text/turtle"}
        )
    return r.status_code

GRAPHDB_URL = "http://localhost:7200"
REPO_ID = "neubauten"

for filename, _ in FIBO_FILES.items():
    filepath = f"data/fibo/{filename}"
    named_graph = f"http://fibo/{filename.replace('.ttl','')}"
    status = load_local_file(filepath, named_graph, GRAPHDB_URL, REPO_ID)
    print(f"{'✓' if status in [200,204] else '✗'} Loaded {filename}: {status}")
```

### 3.3 LCC — Languages, Countries and Codes

```python
# scripts/03_load_lcc.py
"""
LCC: ISO country codes, language codes as RDF.
Source: https://www.omg.org/spec/LCC/
Also available via FIBO ecosystem.
"""
import requests

GRAPHDB_URL = "http://localhost:7200"
REPO_ID = "neubauten"

LCC_SOURCES = [
    {
        "url": "https://www.omg.org/spec/LCC/Countries/CountryRepresentation/",
        "graph": "http://lcc/countries/representation",
        "name": "LCC Country Representation",
        "format": "application/rdf+xml"
    },
    {
        "url": "https://www.omg.org/spec/LCC/Countries/ISO3166-1-CountryCodes/",
        "graph": "http://lcc/countries/iso3166-1",
        "name": "LCC ISO 3166-1 Country Codes",
        "format": "application/rdf+xml"
    },
    {
        "url": "https://www.omg.org/spec/LCC/Languages/LanguageRepresentation/",
        "graph": "http://lcc/languages/representation",
        "name": "LCC Language Representation",
        "format": "application/rdf+xml"
    },
]

def load_rdf_from_url(source, graphdb_url, repo_id):
    sparql = f"LOAD <{source['url']}> INTO GRAPH <{source['graph']}>"
    r = requests.post(
        f"{graphdb_url}/repositories/{repo_id}/statements",
        data=sparql,
        headers={"Content-Type": "application/sparql-update"}
    )
    print(f"{'✓' if r.ok else '✗'} {source['name']}: {r.status_code}")

for source in LCC_SOURCES:
    load_rdf_from_url(source, GRAPHDB_URL, REPO_ID)
```

### 3.4 GLEI Data — Global Legal Entity Identifiers

GLEI data is NOT an ontology — it's **instance data** (real companies). It's large (~2GB full dataset). For learning, use a sample.

```python
# scripts/04_load_glei.py
"""
GLEI Data Sources:
- Full data: https://www.gleif.org/en/lei-data/gleif-golden-copy/download-the-golden-copy
- API: https://api.gleif.org/api/v1/lei-records
- RDF version: https://data.world/gleif/lei (requires account)

Strategy for learning:
1. Use GLEIF REST API to fetch a small sample (e.g., 100 entities)
2. Convert to RDF/Turtle
3. Load into GraphDB named graph
"""
import requests
import json
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, RDFS, OWL, XSD
import os

GLEIF_API = "https://api.gleif.org/api/v1"
LEI_NAMESPACE = Namespace("https://www.gleif.org/ontology/L1/")
GLEI_NAMESPACE = Namespace("https://www.gleif.org/ontology/")

def fetch_lei_sample(count=50):
    """Fetch sample LEI records from GLEIF API."""
    url = f"{GLEIF_API}/lei-records"
    params = {
        "page[size]": min(count, 100),
        "page[number]": 1,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()["data"]

def lei_records_to_rdf(records):
    """Convert GLEIF API records to RDF graph."""
    g = Graph()
    LEI = Namespace("https://www.gleif.org/ontology/L1/")
    BASE = Namespace("https://www.gleif.org/data/")
    
    g.bind("lei", LEI)
    g.bind("gleif", BASE)
    g.bind("xsd", XSD)
    
    for record in records:
        attrs = record["attributes"]
        lei_code = attrs["lei"]
        entity = attrs.get("entity", {})
        
        lei_uri = URIRef(f"https://www.gleif.org/data/lei/{lei_code}")
        
        # Type assertion
        g.add((lei_uri, RDF.type, LEI.RegisteredEntity))
        
        # Core properties
        g.add((lei_uri, LEI.leiCode, Literal(lei_code)))
        
        legal_name = entity.get("legalName", {}).get("name", "")
        if legal_name:
            g.add((lei_uri, LEI.legalName, Literal(legal_name)))
        
        # Jurisdiction
        jurisdiction = entity.get("jurisdiction")
        if jurisdiction:
            g.add((lei_uri, LEI.legalJurisdiction, Literal(jurisdiction)))
        
        # Address
        address = entity.get("legalAddress", {})
        if address:
            country = address.get("country")
            if country:
                g.add((lei_uri, LEI.hasLegalAddress_Country, Literal(country)))
            city = address.get("city")
            if city:
                g.add((lei_uri, LEI.hasLegalAddress_City, Literal(city)))
        
        # Status
        status = attrs.get("registration", {}).get("status")
        if status:
            g.add((lei_uri, LEI.registrationStatus, Literal(status)))
    
    return g

def load_glei_to_graphdb(g, graphdb_url, repo_id, named_graph="http://glei/data/sample"):
    """Load RDF graph into GraphDB."""
    ttl_data = g.serialize(format="turtle")
    r = requests.post(
        f"{graphdb_url}/repositories/{repo_id}/rdf-graphs/service",
        params={"graph": named_graph},
        data=ttl_data.encode(),
        headers={"Content-Type": "text/turtle"}
    )
    print(f"GLEI data loaded: {r.status_code} ({len(g)} triples)")

os.makedirs("data/glei", exist_ok=True)

# Fetch and save sample
print("Fetching GLEI sample from API...")
records = fetch_lei_sample(100)
print(f"Fetched {len(records)} LEI records")

# Save raw JSON
with open("data/glei/sample_records.json", "w") as f:
    json.dump(records, f, indent=2)

# Convert to RDF
g = lei_records_to_rdf(records)
ttl = g.serialize(format="turtle")

with open("data/glei/sample.ttl", "w") as f:
    f.write(ttl)
print(f"Saved {len(g)} triples to data/glei/sample.ttl")

# Load into GraphDB
GRAPHDB_URL = "http://localhost:7200"
REPO_ID = "neubauten"
load_glei_to_graphdb(g, GRAPHDB_URL, REPO_ID)
```

### 3.5 FIBO2GLEI Mapping Ontology

This is a **mapping** that says "FIBO's `LegalEntity` corresponds to GLEI's `RegisteredEntity`". This is what makes the two datasets interoperable.

```python
# scripts/05_create_fibo2glei_mapping.py
"""
FIBO2GLEI mapping ontology.
The Neubauten demo includes this as a bridge between the FIBO schema
and the GLEI data model.

We create a simplified version that maps key concepts.
"""
FIBO2GLEI_TTL = """
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix fibo-be: <https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/> .
@prefix fibo-fnd: <https://spec.edmcouncil.org/fibo/ontology/FND/Relations/Relations/> .
@prefix lei: <https://www.gleif.org/ontology/L1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix f2g: <http://neubauten.example.org/fibo2glei#> .

# Ontology declaration
<http://neubauten.example.org/fibo2glei>
    a owl:Ontology ;
    rdfs:label "FIBO to GLEI Mapping Ontology" ;
    rdfs:comment "Maps between FIBO and GLEIF LEI ontology concepts." .

# Class equivalences / mappings
fibo-be:LegalPerson owl:equivalentClass lei:RegisteredEntity .

# Property mappings via annotation
lei:legalName rdfs:subPropertyOf fibo-fnd:hasName .

# SKOS concept matching for human-readable alignment
fibo-be:Corporation skos:closeMatch lei:RegisteredEntity .
"""

import requests
GRAPHDB_URL = "http://localhost:7200"
REPO_ID = "neubauten"

r = requests.post(
    f"{GRAPHDB_URL}/repositories/{REPO_ID}/rdf-graphs/service",
    params={"graph": "http://neubauten/fibo2glei"},
    data=FIBO2GLEI_TTL.encode(),
    headers={"Content-Type": "text/turtle"}
)
print(f"FIBO2GLEI mapping loaded: {r.status_code}")
```

### 3.6 LEI2ISIN Mapping

Maps legal entity identifiers (companies) to securities (ISIN codes):

```python
# scripts/06_load_lei2isin.py
"""
LEI2ISIN: Maps between company identifiers (LEI) and security identifiers (ISIN).
ISIN = International Securities Identification Number (12-char code for stocks/bonds).
LEI = 20-char legal entity identifier.

One company (LEI) can issue multiple securities (ISINs).
One ISIN maps to exactly one issuing entity (LEI).

Source: GLEIF provides this mapping.
Download: https://www.gleif.org/en/lei-data/lei-mapping/download-isin-to-lei-relationship-files
"""
import requests
import pandas as pd
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, RDFS, OWL, XSD

def fetch_isin_sample():
    """Fetch sample ISIN-LEI mappings from GLEIF API."""
    url = "https://api.gleif.org/api/v1/lei-records"
    # Note: Full ISIN mapping requires bulk download
    # For learning, construct synthetic sample
    
    sample_mappings = [
        {"lei": "5493001KJTIIGC8Y1R12", "isin": "US0378331005", "entity": "Apple Inc"},
        {"lei": "HWUPKR0MPOU8FGXBT394", "isin": "US5949181045", "entity": "Microsoft Corp"},
        {"lei": "INR2EJN1ERAN0W5ZP974", "isin": "US02079K3059", "entity": "Alphabet Inc"},
    ]
    return sample_mappings

def create_lei2isin_rdf(mappings):
    g = Graph()
    L2I = Namespace("http://neubauten.example.org/lei2isin#")
    LEI = Namespace("https://www.gleif.org/data/lei/")
    ISIN_NS = Namespace("http://example.org/isin/")
    FIN = Namespace("https://spec.edmcouncil.org/fibo/ontology/SEC/Securities/Securities/")
    
    g.bind("l2i", L2I)
    g.bind("lei", LEI)
    g.bind("isin", ISIN_NS)
    
    for m in mappings:
        lei_uri = URIRef(f"https://www.gleif.org/data/lei/{m['lei']}")
        isin_uri = URIRef(f"http://example.org/isin/{m['isin']}")
        
        g.add((isin_uri, RDF.type, FIN.Security))
        g.add((isin_uri, L2I.isinCode, Literal(m["isin"])))
        g.add((isin_uri, L2I.issuedBy, lei_uri))
        g.add((lei_uri, L2I.issues, isin_uri))
    
    return g

mappings = fetch_isin_sample()
g = create_lei2isin_rdf(mappings)

# Load into GraphDB
r = requests.post(
    "http://localhost:7200/repositories/neubauten/rdf-graphs/service",
    params={"graph": "http://neubauten/lei2isin"},
    data=g.serialize(format="turtle").encode(),
    headers={"Content-Type": "text/turtle"}
)
print(f"LEI2ISIN loaded: {r.status_code} ({len(g)} triples)")
```

---

## 4. Exploring GraphDB — SPARQL Queries

### 4.1 Basic Exploration Queries

```python
# scripts/07_graphdb_sparql_exploration.py
"""
Learn SPARQL by querying the loaded data in GraphDB.
Run these in GraphDB Workbench: http://localhost:7200
"""
from SPARQLWrapper import SPARQLWrapper, JSON

GRAPHDB_SPARQL = "http://localhost:7200/repositories/neubauten"
sparql = SPARQLWrapper(GRAPHDB_SPARQL)
sparql.setReturnFormat(JSON)

def run_query(query_name, query):
    print(f"\n{'='*60}")
    print(f"Query: {query_name}")
    print(f"{'='*60}")
    sparql.setQuery(query)
    results = sparql.query().convert()
    for row in results["results"]["bindings"][:10]:
        print({k: v["value"] for k, v in row.items()})
    return results

# Query 1: What named graphs do we have?
run_query("List All Named Graphs", """
SELECT ?graph (COUNT(?s) as ?triples)
WHERE { GRAPH ?graph { ?s ?p ?o } }
GROUP BY ?graph
ORDER BY DESC(?triples)
""")

# Query 2: What classes are defined in FIBO?
run_query("FIBO Classes", """
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?class ?label WHERE {
  GRAPH ?g {
    ?class a owl:Class .
    OPTIONAL { ?class rdfs:label ?label }
    FILTER(CONTAINS(STR(?class), "edmcouncil.org"))
  }
}
LIMIT 20
""")

# Query 3: Legal entities in GLEI data
run_query("Legal Entities (GLEI)", """
PREFIX lei: <https://www.gleif.org/ontology/L1/>
SELECT ?entity ?name ?jurisdiction WHERE {
  GRAPH <http://glei/data/sample> {
    ?entity a lei:RegisteredEntity ;
            lei:legalName ?name ;
            lei:legalJurisdiction ?jurisdiction .
  }
}
LIMIT 20
""")

# Query 4: FIBO class hierarchy (subclass relationships)
run_query("FIBO Class Hierarchy", """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT ?subclass ?superclass ?subLabel ?superLabel WHERE {
  GRAPH ?g {
    ?subclass rdfs:subClassOf ?superclass .
    FILTER(CONTAINS(STR(?subclass), "edmcouncil.org"))
    FILTER(CONTAINS(STR(?superclass), "edmcouncil.org"))
    OPTIONAL { ?subclass rdfs:label ?subLabel }
    OPTIONAL { ?superclass rdfs:label ?superLabel }
  }
}
LIMIT 30
""")

# Query 5: Cross-ontology: Entities with ISIN instruments
run_query("Entities with Securities (LEI2ISIN)", """
PREFIX l2i: <http://neubauten.example.org/lei2isin#>
PREFIX lei: <https://www.gleif.org/ontology/L1/>
SELECT ?entity ?name ?isin WHERE {
  GRAPH <http://neubauten/lei2isin> {
    ?security l2i:isinCode ?isin ;
              l2i:issuedBy ?entity .
  }
  OPTIONAL {
    GRAPH <http://glei/data/sample> {
      ?entity lei:legalName ?name .
    }
  }
}
""")

# Query 6: Property paths (traversal)
run_query("Ownership Chains (Property Paths)", """
PREFIX l2i: <http://neubauten.example.org/lei2isin#>
PREFIX lei: <https://www.gleif.org/ontology/L1/>

# Find entities that issue securities (direct or indirect via inference)
SELECT ?entity ?isin WHERE {
  ?entity l2i:issues+ ?security .
  ?security l2i:isinCode ?isin .
}
LIMIT 20
""")
```

### 4.2 GraphDB Visual Exploration

Open `http://localhost:7200` → Workbench → "Explore" → "Visual Graph":
- Enter a URI of a loaded entity to see its neighborhood
- Click "Expand" on nodes to traverse relationships
- This is how you understand FIBO's class hierarchy visually

---

## 5. Neo4j Track — neosemantics (n10s)

### 5.1 Initialize neosemantics in Neo4j

```cypher
// Run in Neo4j Browser: http://localhost:7474
// Step 1: Initialize n10s configuration
CALL n10s.graphconfig.init({
  handleVocabUris: 'SHORTEN',
  handleMultival: 'ARRAY',
  handleRDFTypes: 'LABELS_AND_NODES',
  keepLangTag: false,
  applyNeo4jNaming: true
});

// Step 2: Create constraint required by n10s
CREATE CONSTRAINT n10s_unique_uri FOR (r:Resource) REQUIRE r.uri IS UNIQUE;
```

### 5.2 Load FIBO Ontology into Neo4j via n10s

```cypher
// Load FIBO Legal Persons ontology
CALL n10s.onto.import.fetch(
  "https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/",
  "Turtle"
);

// Inspect what was loaded
MATCH (c:Class) RETURN c.name, c.uri LIMIT 20;

// Inspect relationships
MATCH (c1:Class)-[r:SCO]->(c2:Class)
RETURN c1.name, c2.name LIMIT 20;

// Properties defined
MATCH (p:Property) RETURN p.name, p.uri LIMIT 20;
```

### 5.3 Load GLEI Data via n10s

```cypher
// Load the GLEI sample TTL we created (mount it to neo4j import folder first)
// docker cp data/glei/sample.ttl neo4j:/var/lib/neo4j/import/

CALL n10s.rdf.import.fetch(
  "file:///var/lib/neo4j/import/sample.ttl",
  "Turtle"
);

// Inspect loaded entities
MATCH (e:RegisteredEntity) 
RETURN e.leiCode, e.legalName, e.legalJurisdiction
LIMIT 20;
```

### 5.4 Cypher Exploration Queries

```cypher
// How many nodes and relationships?
CALL apoc.meta.stats() YIELD labels, relTypesCount
RETURN labels, relTypesCount;

// Node label distribution
CALL db.labels() YIELD label
CALL apoc.cypher.run('MATCH (n:`'+label+'`) RETURN count(n) as count', {})
YIELD value
RETURN label, value.count ORDER BY value.count DESC;

// Find class hierarchy (FIBO)
MATCH path = (child:Class)-[:SCO*1..3]->(parent:Class)
WHERE parent.name = 'LegalPerson'
RETURN path LIMIT 50;

// Entity with its securities
MATCH (e:RegisteredEntity)-[:issues]->(s)
RETURN e.legalName, s.isinCode;

// Shortest path between two entities
MATCH (a {leiCode: 'LEI_CODE_1'}), (b {leiCode: 'LEI_CODE_2'})
CALL gds.shortestPath.dijkstra.stream({
  sourceNode: id(a),
  targetNode: id(b)
}) YIELD path
RETURN path;
```

### 5.5 APOC Procedures for Data Loading

```cypher
// Load CSV data using APOC
// Download GLEIF sample CSV first
CALL apoc.load.csv('file:///var/lib/neo4j/import/lei_sample.csv', {header: true})
YIELD map
MERGE (e:Entity {lei: map.LEI})
SET e.name = map.Entity_LegalName,
    e.jurisdiction = map.Entity_Jurisdiction,
    e.status = map.Registration_Status;

// Load JSON via APOC
CALL apoc.load.json('file:///var/lib/neo4j/import/sample_records.json')
YIELD value
UNWIND value AS record
MERGE (e:Entity {lei: record.attributes.lei})
SET e.name = record.attributes.entity.legalName.name;

// APOC graph export to JSON
CALL apoc.export.json.all('export.json', {useTypes: true});

// APOC schema visualization
CALL apoc.meta.schema() YIELD value RETURN value;
```

---

## 6. Graph Data Science (GDS)

```cypher
// Project a graph for analysis
CALL gds.graph.project(
  'entities-graph',
  ['Entity', 'RegisteredEntity'],
  {
    ISSUES: {orientation: 'UNDIRECTED'},
    OWNS: {orientation: 'UNDIRECTED'}
  }
);

// Community Detection (Louvain)
CALL gds.louvain.stream('entities-graph')
YIELD nodeId, communityId
RETURN gds.util.asNode(nodeId).legalName AS entity, communityId
ORDER BY communityId
LIMIT 50;

// PageRank — find most central/important entities
CALL gds.pageRank.stream('entities-graph')
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).legalName AS entity, score
ORDER BY score DESC LIMIT 20;

// Degree centrality — who has most connections
CALL gds.degree.stream('entities-graph')
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).legalName AS entity, score as degree
ORDER BY score DESC LIMIT 20;

// Node similarity — which entities are similar (same industry/country)
CALL gds.nodeSimilarity.stream('entities-graph')
YIELD node1, node2, similarity
RETURN gds.util.asNode(node1).legalName AS entity1,
       gds.util.asNode(node2).legalName AS entity2,
       similarity
ORDER BY similarity DESC LIMIT 20;
```

---

## 7. Do You Need ALL the Data? Decision Framework

| Question | Answer |
|---|---|
| Do I need ALL 2M+ GLEIF records? | NO — start with 1,000–10,000 via API |
| Do I need ALL FIBO modules? | NO — load only relevant domains (BE, FND) |
| Should I load full GLEI into Neo4j? | NO — use GraphDB for RDF, Neo4j for analytics |
| Is GraphDB or Neo4j better for this? | GraphDB for ontology/SPARQL, Neo4j for graph analytics |
| Can I use both simultaneously? | YES — sync via n10s export from GraphDB, import to Neo4j |

---

## 8. Understanding "Ontological Shortcuts"

This is the most intellectually interesting part of the Neubauten demo.

```python
# scripts/08_understanding_shortcuts.py
"""
Ontological Shortcuts = inferred ownership chains.

Example WITHOUT shortcuts (ground truth triples):
  A owns B (direct assertion)
  B owns C (direct assertion)
  C owns D (direct assertion)

Example WITH shortcuts (inferred by OWL reasoning in GraphDB):
  A transitively-owns C (inferred)
  A transitively-owns D (inferred)
  B transitively-owns D (inferred)

This is what OWL reasoning + SPARQL property paths give you for FREE.
You only load direct relationships, reasoning derives the rest.

In FIBO, ownership is modeled via:
  fibo-be-oac-cown:isDirectlyConsolidatedBy
  fibo-be-oac-cown:isUltimatelyConsolidatedBy

The "shortcut" in Neubauten is SPARQL property path syntax:
  ?a fibo-be-oac-cown:isDirectlyConsolidatedBy+ ?b
                                               ^
                                           This '+' means "one or more hops"
"""
SHORTCUT_SPARQL = """
PREFIX fibo-be-oac: <https://spec.edmcouncil.org/fibo/ontology/BE/OwnershipAndControl/>
PREFIX lei: <https://www.gleif.org/ontology/L1/>

# Find all entities ultimately controlled by a given entity
SELECT ?controlled ?name ?hops WHERE {
  BIND(<https://www.gleif.org/data/lei/5493001KJTIIGC8Y1R12> AS ?controller)
  
  {
    SELECT ?controlled (COUNT(?hop) AS ?hops) WHERE {
      ?controller fibo-be-oac:controls+ ?controlled .
      ?controller fibo-be-oac:controls ?hop .
    }
    GROUP BY ?controlled
  }
  
  OPTIONAL { ?controlled lei:legalName ?name . }
}
ORDER BY ?hops
"""
print("Shortcut SPARQL query ready. Run in GraphDB Workbench for live results.")
```

---

## 9. Python Helper Library

```python
# src/neubauten_client.py
"""
Helper library for working with Neubauten Demo Graph.
"""
import requests
from SPARQLWrapper import SPARQLWrapper, JSON
from neo4j import GraphDatabase
from typing import Optional, Dict, List, Any
import os
from dotenv import load_dotenv

load_dotenv()

class GraphDBClient:
    def __init__(self, base_url: str = None, repo: str = None):
        self.base_url = base_url or os.getenv("GRAPHDB_URL", "http://localhost:7200")
        self.repo = repo or os.getenv("GRAPHDB_REPO", "neubauten")
        self.sparql_endpoint = f"{self.base_url}/repositories/{self.repo}"
        
    def query(self, sparql: str) -> List[Dict]:
        wrapper = SPARQLWrapper(self.sparql_endpoint)
        wrapper.setQuery(sparql)
        wrapper.setReturnFormat(JSON)
        results = wrapper.query().convert()
        return [
            {k: v["value"] for k, v in row.items()}
            for row in results["results"]["bindings"]
        ]
    
    def load_turtle(self, ttl: str, named_graph: str):
        r = requests.post(
            f"{self.base_url}/repositories/{self.repo}/rdf-graphs/service",
            params={"graph": named_graph},
            data=ttl.encode(),
            headers={"Content-Type": "text/turtle"}
        )
        return r.status_code
    
    def load_from_url(self, url: str, named_graph: str):
        sparql = f"LOAD <{url}> INTO GRAPH <{named_graph}>"
        r = requests.post(
            f"{self.base_url}/repositories/{self.repo}/statements",
            data=sparql,
            headers={"Content-Type": "application/sparql-update"}
        )
        return r.status_code
    
    def list_graphs(self) -> List[str]:
        results = self.query("""
            SELECT DISTINCT ?graph WHERE {
                GRAPH ?graph { ?s ?p ?o }
            }
        """)
        return [r["graph"] for r in results]
    
    def count_triples(self, named_graph: Optional[str] = None) -> int:
        if named_graph:
            q = f"SELECT (COUNT(*) as ?c) WHERE {{ GRAPH <{named_graph}> {{ ?s ?p ?o }} }}"
        else:
            q = "SELECT (COUNT(*) as ?c) WHERE { ?s ?p ?o }"
        results = self.query(q)
        return int(results[0]["c"]) if results else 0


class Neo4jClient:
    def __init__(self, uri: str = None, user: str = None, password: str = None):
        self.driver = GraphDatabase.driver(
            uri or os.getenv("NEO4J_URI", "bolt://localhost:7687"),
            auth=(
                user or os.getenv("NEO4J_USER", "neo4j"),
                password or os.getenv("NEO4J_PASSWORD", "password123")
            )
        )
    
    def query(self, cypher: str, params: Dict = None) -> List[Dict]:
        with self.driver.session() as session:
            result = session.run(cypher, params or {})
            return [dict(record) for record in result]
    
    def run(self, cypher: str, params: Dict = None):
        with self.driver.session() as session:
            return session.run(cypher, params or {})
    
    def close(self):
        self.driver.close()


# Usage example
if __name__ == "__main__":
    gdb = GraphDBClient()
    neo = Neo4jClient()
    
    print("GraphDB graphs:", gdb.list_graphs())
    print("GraphDB total triples:", gdb.count_triples())
    
    node_count = neo.query("MATCH (n) RETURN count(n) as count")
    print("Neo4j nodes:", node_count[0]["count"] if node_count else 0)
```

---

## 10. Neubauten Public SPARQL Endpoint

The Neubauten demo is publicly accessible:

```python
# scripts/09_query_public_neubauten.py
"""
Query the LIVE Neubauten demo at http://neubauten.ontotext.com:7200/
This lets you see the real data before building your own.
"""
from SPARQLWrapper import SPARQLWrapper, JSON

# Public Neubauten SPARQL endpoint
NEUBAUTEN_SPARQL = "http://neubauten.ontotext.com:7200/repositories/neubauten"

sparql = SPARQLWrapper(NEUBAUTEN_SPARQL)
sparql.setReturnFormat(JSON)

# Query 1: What's in there?
sparql.setQuery("""
SELECT ?graph (COUNT(*) as ?triples)
WHERE { GRAPH ?graph { ?s ?p ?o } }
GROUP BY ?graph
ORDER BY DESC(?triples)
""")
try:
    results = sparql.query().convert()
    print("Named graphs in public Neubauten:")
    for row in results["results"]["bindings"]:
        print(f"  {row['graph']['value']}: {row['triples']['value']} triples")
except Exception as e:
    print(f"Note: Public endpoint may be offline. Error: {e}")

# Query 2: Sample entities
sparql.setQuery("""
PREFIX lei: <https://www.gleif.org/ontology/L1/>
SELECT ?entity ?name WHERE {
  ?entity a lei:RegisteredEntity ;
          lei:legalName ?name .
} LIMIT 10
""")
try:
    results = sparql.query().convert()
    for row in results["results"]["bindings"]:
        print(f"  {row.get('name', {}).get('value', 'N/A')}")
except Exception as e:
    print(f"Query error: {e}")
```

---

## 11. Learning Path Checklist

Work through these in order:

### Week 1: Foundations
- [ ] Start Docker Compose, verify both services running
- [ ] Open GraphDB Workbench at `http://localhost:7200`
- [ ] Run `scripts/01_create_graphdb_repo.py`
- [ ] Run `scripts/03_load_lcc.py` (LCC is smaller, good to start)
- [ ] Run SPARQL Query 1 (list graphs) in GraphDB Workbench
- [ ] Understand: What is an RDF triple? (subject - predicate - object)
- [ ] Understand: What is a named graph?

### Week 2: FIBO
- [ ] Run `scripts/02b_download_fibo_local.py`
- [ ] Run SPARQL Query 2 (FIBO classes)
- [ ] Run SPARQL Query 4 (FIBO hierarchy)
- [ ] Open GraphDB Visual Graph on a FIBO class URI
- [ ] Understand: OWL Class vs Instance vs Property

### Week 3: GLEI + Integration
- [ ] Run `scripts/04_load_glei.py`
- [ ] Run `scripts/05_create_fibo2glei_mapping.py`
- [ ] Run `scripts/06_load_lei2isin.py`
- [ ] Run cross-ontology SPARQL queries
- [ ] Understand: Why do we need FIBO2GLEI? (data alignment)

### Week 4: Neo4j Track
- [ ] Run n10s setup in Neo4j Browser
- [ ] Load FIBO ontology via n10s
- [ ] Load GLEI sample via n10s
- [ ] Run Cypher exploration queries
- [ ] Run GDS community detection

### Week 5: Advanced
- [ ] Implement SHACL validation in GraphDB
- [ ] Run GDS PageRank/Centrality
- [ ] Query the public Neubauten endpoint
- [ ] Build a simple Streamlit dashboard

---

## 12. Key URLs & Resources

| Resource | URL |
|---|---|
| FIBO Ontology | https://spec.edmcouncil.org/fibo/ |
| FIBO GitHub | https://github.com/edmcouncil/fibo |
| GLEIF API | https://api.gleif.org/api/v1 |
| GLEIF Golden Copy Download | https://www.gleif.org/en/lei-data/gleif-golden-copy |
| LCC (OMG) | https://www.omg.org/spec/LCC/ |
| Neubauten Public Demo | http://neubauten.ontotext.com:7200/ |
| GraphDB Docs | https://graphdb.ontotext.com/documentation/ |
| neosemantics Docs | https://neo4j.com/labs/neosemantics/ |
| APOC Docs | https://neo4j.com/docs/apoc/current/ |
| GDS Docs | https://neo4j.com/docs/graph-data-science/ |
| W3C SPARQL Spec | https://www.w3.org/TR/sparql11-query/ |
| W3C OWL Primer | https://www.w3.org/TR/owl-primer/ |

---

## 13. Project Folder Structure

```
neubauten-graph/
├── docker-compose.yml
├── .env
├── requirements.txt
├── graphdb_config/
│   └── neubauten-config.ttl
├── data/
│   ├── fibo/           # Downloaded FIBO TTL modules
│   ├── glei/           # GLEIF sample data (JSON + TTL)
│   └── lcc/            # LCC country/language codes
├── scripts/
│   ├── 01_create_graphdb_repo.py
│   ├── 02_load_fibo.py
│   ├── 02b_download_fibo_local.py
│   ├── 03_load_lcc.py
│   ├── 04_load_glei.py
│   ├── 05_create_fibo2glei_mapping.py
│   ├── 06_load_lei2isin.py
│   ├── 07_graphdb_sparql_exploration.py
│   ├── 08_understanding_shortcuts.py
│   └── 09_query_public_neubauten.py
├── src/
│   └── neubauten_client.py
├── notebooks/
│   ├── 01_fibo_exploration.ipynb
│   ├── 02_glei_analysis.ipynb
│   └── 03_neo4j_gds.ipynb
└── cypher/
    ├── 01_n10s_setup.cypher
    ├── 02_load_ontology.cypher
    └── 03_gds_analysis.cypher
```

---

## 14. Answers to Your Specific Questions

**Q: Do we need to load ALL data into Neo4j?**
No. Use a "sample-first" strategy: 100–1,000 LEI records is enough for learning. Full GLEIF data (2M+ records) only needed for production financial risk systems.

**Q: GraphDB vs Neo4j — which to use?**
Use BOTH with different purposes: GraphDB for ontology storage, OWL reasoning, SPARQL queries. Neo4j for Cypher queries, graph algorithms (GDS), application development.

**Q: How to download ontologies?**
Most have HTTP URLs that return RDF when you set `Accept: text/turtle` header. FIBO can be fetched module-by-module from `spec.edmcouncil.org`. Use `requests.get(url, headers={"Accept": "text/turtle"})`.

**Q: What about financial domain knowledge?**
FIBO's rdfs:label and rdfs:comment annotations explain each class in plain English. Always query these first: `SELECT ?class ?label ?comment WHERE { ?class rdfs:label ?label ; rdfs:comment ?comment }`. Read the labels — they are written for financial professionals, not engineers.

**Q: What are Ontological Shortcuts in plain terms?**
If Apple owns Goldman Sachs, and Goldman Sachs owns a fund, FIBO reasoning automatically infers Apple "ultimately controls" the fund. You only load direct relationships; reasoning derives transitive ones. GraphDB does this via its OWL-Horst reasoning engine.
