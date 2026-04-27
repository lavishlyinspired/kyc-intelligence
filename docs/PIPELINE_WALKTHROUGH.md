# KYC Pipeline Walkthrough — Scripts 01 to 05

A plain-language explanation of what each setup script does, illustrated with
real data already loaded in your GraphDB `kyc-kg` repository.

---

## The Big Picture

Think of the pipeline as building a library:

| Step | Analogy | Script |
|------|---------|--------|
| 01 | Build the library building | `01_setup_graphdb.py` |
| 02 | Stock the shelves with dictionaries | `02_load_fibo.py` |
| 03 | Add a translation index between dictionaries | `03_load_fibo2glei_mapping.py` |
| 04 | Walk around and read the labels on the shelves | `04_sparql_exploration.py` |
| 05 | Add the first real books (company records) | `05_load_gleif_data.py` |

---

## Script 01 — `01_setup_graphdb.py` — "Build the container"

**What it does:**  
Creates a new GraphDB repository called `kyc-kg` with OWL reasoning enabled.

**Why OWL reasoning matters:**  
When you declare `kyc:directlyOwnedBy` as an `owl:TransitiveProperty`, GraphDB
automatically infers that if:
- Company A owns Company B
- Company B owns Company C

...then A also owns C — without you storing that triple explicitly. This powers
the 6-level deep UBO (Ultimate Beneficial Owner) traversal later.

**Ruleset used:** `owl-horst-optimized`  
This is the sweet-spot for KYC — supports transitive properties (ownership
chains) and `owl:equivalentClass` (cross-vocabulary joins) without being as
slow as full OWL2.

**Verification:** Repository exists at http://localhost:7200 → select `kyc-kg`.

---

## Script 02 — `02_load_fibo.py` — "Stock the ontology shelves"

**What it does:**  
Downloads and uploads 13 ontology modules into separate named graphs. These are
the **vocabulary definitions** — they tell GraphDB what words like
"LegalPerson", "Corporation", or "ownership" mean in a formal, machine-readable
way.

### Named graphs loaded (from your live GraphDB right now):

| Named Graph | Triples | Contents |
|-------------|---------|----------|
| `http://kg/lcc/iso3166` | **8,725** | All ISO 3166-1 country codes (US, GB, DE …) |
| `http://kg/fibo/fbc/fse` | **901** | Financial Services Entities (banks, brokers) |
| `http://kg/lcc/countries` | **385** | Country concept definitions |
| `http://kg/fibo/be/control` | **376** | Corporate control relationships |
| `http://kg/fibo/be/ownership` | **350** | Corporate ownership relationships |
| `http://kg/fibo/be/corp-bodies` | **318** | Corporations, boards, stock |
| `http://kg/fibo/be/legal-persons` | **234** | Legal persons, SPVs, VIEs |
| `http://kg/fibo/fnd/relations` | **195** | Foundational relation properties |
| `http://kg/fibo/be/corporations` | **78** | Specific corporation types |
| `http://kg/fibo/fnd/annotations` | **75** | Metadata/annotation properties |
| `http://kg/kyc/ontology` | **54** | KYC application vocab (see script 03) |
| `http://kg/fibo/fnd/agents` | **46** | Agents and people |
| `http://kg/mapping/fibo2glei` | **6** | Cross-vocabulary bridge (see script 03) |

### Real FIBO classes loaded in your repo:

These are actual `owl:Class` entries you can query right now:

| FIBO Label | URI (short) | What it means in KYC |
|------------|-------------|----------------------|
| `chartered legal person` | `CorporateBodies/CharteredLegalPerson` | A company created by a government charter |
| `business entity` | `CorporateBodies/BusinessEntity` | Any commercial organisation |
| `legally competent natural person` | `LegalPersons/LegallyCompetentNaturalPerson` | A human who can enter contracts (UBO candidate) |
| `special purpose vehicle` | `LegalPersons/SpecialPurposeVehicle` | SPV — classic shell company structure |
| `profit objective` | `CorporateBodies/ProfitObjective` | For-profit entity flag |
| `not for profit objective` | `CorporateBodies/NotForProfitObjective` | Non-profit flag |
| `automated system` | `FND/AutomatedSystem` | Algorithmic/AI trading entity |

**Try this in GraphDB Workbench (SPARQL tab):**
```sparql
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?class ?label WHERE {
    ?class a owl:Class .
    ?class rdfs:label ?label .
    FILTER(CONTAINS(STR(?class), "edmcouncil.org"))
}
ORDER BY ?label
```
→ Returns all 20+ FIBO class definitions.

### Note on Ownership/Control modules:
The original FIBO URLs for `Ownership` and `Control` returned HTTP 404 (FIBO
reorganised these in 2025). The script was updated to load 4 replacement files
from GitHub:
- `CorporateOwnership.rdf` + `OwnershipParties.rdf` → graph `http://kg/fibo/be/ownership` (350 triples)
- `CorporateControl.rdf` + `ControlParties.rdf` → graph `http://kg/fibo/be/control` (376 triples)

---

## Script 03 — `03_load_fibo2glei_mapping.py` — "Add the translation index"

**What it does:**  
Loads two small but critical ontologies — a cross-vocabulary bridge and the
KYC application vocabulary.

### Problem it solves

FIBO and GLEIF both describe companies, but use different words:

| Concept | FIBO says | GLEIF says |
|---------|-----------|------------|
| A company | `fibo-be:LegalPerson` | `lei:RegisteredEntity` |
| Its name | `fibo-fnd:hasName` | `lei:legalName` |

Without a mapping, a SPARQL query using FIBO vocabulary returns 0 results for
GLEIF data, and vice versa.

### Real mapping triples loaded (from your live GraphDB):

| Subject | Predicate | Object | Meaning |
|---------|-----------|--------|---------|
| `fibo-be:LegalPerson` | `owl:equivalentClass` | `lei:RegisteredEntity` | **They are the same concept** — OWL reasoner treats them interchangeably |
| `lei:legalName` | `rdfs:subPropertyOf` | `fibo-fnd:hasName` | GLEIF's name field IS a FIBO name |
| `fibo-be:LegalPerson` | `skos:closeMatch` | `lei:RegisteredEntity` | Soft alignment for SKOS-aware tools |

**What this means in practice:**  
A SPARQL query for `?x a fibo-be:LegalPerson` will automatically also match
GLEIF entities typed as `lei:RegisteredEntity` — the OWL reasoner handles it.

### KYC Application Vocabulary (graph `http://kg/kyc/ontology`, 54 triples):

This is the **custom type system** used by all downstream scripts. Your live
repo has all 12 properties:

**Datatype Properties** (attributes on nodes):
| Property | Type | Used for |
|----------|------|----------|
| `kyc:leiCode` | `xsd:string` | The 20-char LEI identifier |
| `kyc:legalName` | `xsd:string` | Company or person name |
| `kyc:entityStatus` | `xsd:string` | ACTIVE / INACTIVE / LAPSED |
| `kyc:isPEP` | `xsd:boolean` | Politically Exposed Person flag |
| `kyc:isSanctioned` | `xsd:boolean` | On a sanctions list |
| `kyc:nationality` | `xsd:string` | Person's country of citizenship |

**Object Properties** (relationships between nodes):
| Property | Direction | Special | KYC use |
|----------|-----------|---------|---------|
| `kyc:directlyOwnedBy` | Entity → Entity | `owl:TransitiveProperty` | Direct ownership link |
| `kyc:owns` | Entity → Entity | `owl:inverseOf directlyOwnedBy` | Reverse direction |
| `kyc:ultimatelyOwnedBy` | Entity → Person | — | Inferred UBO (reasoner fills this in) |
| `kyc:controlledBy` | Entity → Person | — | Control relationship |
| `kyc:hasJurisdiction` | Entity → Jurisdiction | — | Which country it's registered in |
| `kyc:hasLegalAddress` | Entity → Address | — | Registered address |

**The key insight — `owl:TransitiveProperty` on `directlyOwnedBy`:**
If you load:
```
CorpA kyc:directlyOwnedBy CorpB .
CorpB kyc:directlyOwnedBy CorpC .
CorpC kyc:directlyOwnedBy Vladimir .
```
The OWL reasoner automatically infers:
```
CorpA kyc:directlyOwnedBy Vladimir .   ← inferred, never stored explicitly
```
A single SPARQL query `?entity kyc:directlyOwnedBy+ ?owner` then finds Vladimir
as the UBO of CorpA through any chain depth.

---

## Script 04 — `04_sparql_exploration.py` — "Walk the shelves"

**What it does:**  
Read-only exploration — runs 6 SPARQL queries to show you what's in the repo.
Good for learning SPARQL concepts and verifying the load.

| Query | SPARQL concept taught | What it answers |
|-------|----------------------|-----------------|
| Q1 | `GRAPH ?g {}` + `GROUP BY` | Which named graphs exist and how big? |
| Q2 | `a owl:Class` + `FILTER(CONTAINS(...))` | What FIBO classes are loaded? |
| Q3 | `rdfs:subClassOf*` property path | Full subclass tree under LegalPerson |
| Q4 | `GRAPH <uri> {}` targeted query | Properties in the Ownership graph |
| Q5 | `ASK {}` boolean query | Is kyc:LegalEntity a subclass of fibo-be:LegalPerson? |
| Q6 | `COUNT(*)` | How many total triples including inferred? |

**Bug fixed:** The ASK detection used `query.startswith("ASK")` which failed
when PREFIX declarations appear before the ASK keyword. Fixed to use
`re.search(r'\bASK\b', query)`.

**Run it:**
```bash
python scripts/04_sparql_exploration.py
```

---

## Script 05 — `05_load_gleif_data.py` — "Add the first real books"

**What it does:**  
Hits the **free, live GLEIF REST API** and fetches up to 50 real company
records per jurisdiction, converts them to FIBO-aligned RDF, and uploads to
`http://kg/glei/instances`.

### Jurisdictions fetched:

| Code | Country | Why included |
|------|---------|--------------|
| `US` | United States | World's largest financial market |
| `GB` | United Kingdom | Major financial centre |
| `DE` | Germany | Largest EU economy |
| `JP` | Japan | Asia-Pacific anchor |
| `CH` | Switzerland | Banking secrecy jurisdiction |
| `KY` | Cayman Islands | **High-risk offshore** — shell company hub |
| `VG` | British Virgin Islands | **High-risk offshore** — shell company hub |

KY and VG are intentionally included — they appear in the risk scoring logic
in script 08 (GDS analysis).

### How a GLEIF record becomes RDF:

**Input (GLEIF JSON):**
```json
{
  "attributes": {
    "lei": "5493001KJTIIGC8Y1R12",
    "entity": {
      "legalName": { "name": "Apple Inc." },
      "jurisdiction": "US",
      "status": "ACTIVE",
      "legalAddress": { "city": "Cupertino", "country": "US" }
    }
  }
}
```

**Output (RDF triples — FIBO-aligned):**
```turtle
<https://www.gleif.org/data/lei/5493001KJTIIGC8Y1R12>
    a fibo-be:LegalPerson ;          ← FIBO type (the alignment step)
    a kyc:RegisteredLegalEntity ;    ← KYC vocab type
    kyc:leiCode     "5493001KJTIIGC8Y1R12" ;
    rdfs:label      "Apple Inc." ;
    kyc:legalName   "Apple Inc." ;
    kyc:hasJurisdiction lcc-iso:US ;
    kyc:entityStatus "ACTIVE" ;
    kyc:hasLegalAddress <.../address/5493001KJTIIGC8Y1R12> .
```

**The critical line is `a fibo-be:LegalPerson`.**  
Because of the `owl:equivalentClass` mapping loaded in script 03, this entity
is also simultaneously a `lei:RegisteredEntity` from the GLEIF vocabulary —
the OWL reasoner infers it. Any SPARQL query using either vocabulary will find
this company.

### After script 05 runs:
- ~350 real companies added to `http://kg/glei/instances`
- Raw JSON cached at `data/glei/raw_records.json`
- RDF cached at `data/glei/entities.ttl`

**Verification query (run in GraphDB Workbench after loading):**
```sparql
PREFIX fibo-be: <https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/>
PREFIX kyc: <http://kyc-kg.example.org/ontology#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?name ?status ?jur WHERE {
    GRAPH <http://kg/glei/instances> {
        ?e a fibo-be:LegalPerson .
        ?e rdfs:label ?name .
        OPTIONAL { ?e kyc:entityStatus ?status }
        OPTIONAL { ?e kyc:hasJurisdiction ?jur }
    }
    FILTER(CONTAINS(STR(?jur), "KY") || CONTAINS(STR(?jur), "VG"))
}
LIMIT 20
```
→ Shows Cayman/BVI companies — your first KYC-interesting data.

---

## Where you are now

```
[01] GraphDB repo ──────────────────────────── ✅ kyc-kg created
[02] FIBO/LCC ontologies ───────────────────── ✅ 13 named graphs, ~11,700 triples
[03] FIBO↔GLEIF mapping + KYC vocab ────────── ✅ bridge + 12 properties
[04] SPARQL exploration ────────────────────── ✅ (bug fixed, re-runnable)
[05] Real GLEIF instances ──────────────────── ⬜ run next
[06] Synthetic crime data ──────────────────── ⬜ 06_generate_synthetic_data.py
[07] Load Neo4j ────────────────────────────── ⬜ 07_load_neo4j.py
[08] GDS risk scoring ──────────────────────── ⬜ 08_gds_analysis.py
[10] SHACL validation ──────────────────────── ⬜ 10_shacl_validate.py
[09] GraphRAG agent ────────────────────────── ⬜ needs API key in .env
```

**Next command:**
```bash
python scripts/05_load_gleif_data.py
```
