---
name: load-fibo-ontology
description: "Use when loading FIBO, LCC, or any OWL/Turtle ontology into GraphDB; creating GraphDB repositories with reasoning rulesets; debugging SPARQL LOAD or RDF-graphs/service uploads; deciding which FIBO modules a KYC use case needs. Covers repository config, named graphs, dependency-ordered loading, and troubleshooting 4xx/5xx errors from the GraphDB REST API."
---

# Load FIBO Ontology into GraphDB

## When to use

Triggered by tasks like: "load FIBO into GraphDB", "create a kyc repository with OWL reasoning", "why does SPARQL LOAD return 400", "what FIBO modules do I need for ownership chains".

## Decision: which FIBO modules?

For KYC/AML use the **Tier 1–3** stack below. Skip SEC/DER/LOAN/IND unless your scenario needs securities/derivatives/loans.

| Tier | Modules | Why |
|---|---|---|
| 1 — Foundations | `FND/Utilities/AnnotationVocabulary`, `FND/Relations/Relations`, `FND/AgentsAndPeople/Agents` | Every other module imports these |
| 2 — LCC | `LCC/Countries/CountryRepresentation`, `LCC/Countries/ISO3166-1-CountryCodes` | Jurisdiction codes used by FIBO BE |
| 3 — Business Entities (KYC core) | `BE/LegalEntities/LegalPersons`, `BE/LegalEntities/CorporateBodies`, `BE/OwnershipAndControl/Ownership`, `BE/OwnershipAndControl/Control`, `BE/Corporations/Corporations` | The actual KYC vocabulary |
| 4 — FBC (optional) | `FBC/FunctionalEntities/FinancialServicesEntities` | Banks/regulators if needed |

Always load Tier 1 → Tier 2 → Tier 3 in that order.

## Repository creation

GraphDB repositories are described by Turtle config files. The single most important setting is the **ruleset** (controls OWL inference):

| Ruleset | Use when |
|---|---|
| `empty` | No reasoning (fastest) |
| `rdfs-plus-optimized` | Subclass/subproperty inference only |
| `owl-horst-optimized` | **Default for KYC** — supports `owl:TransitiveProperty` (needed for ownership chains), `owl:equivalentClass` (needed for FIBO↔GLEIF mapping), `owl:sameAs` |
| `owl2-rl-optimized` | Most powerful, slower |

Repo creation via REST: `POST {GRAPHDB_URL}/rest/repositories` with the config TTL as a multipart `config` file.

## Two ways to load RDF data

**Method A — SPARQL `LOAD` from a URL** (one HTTP call, GraphDB fetches the URL):
```
POST /repositories/{repo}/statements
Content-Type: application/sparql-update
LOAD <https://spec.edmcouncil.org/fibo/.../LegalPersons/> INTO GRAPH <http://kg/fibo/be/legal-persons>
```
Returns 204 on success. Fragile if the source URL is slow/throttled.

**Method B — Upload a local TTL file** (more reliable, recommended for FIBO):
```
POST /repositories/{repo}/rdf-graphs/service?graph=<named-graph-uri>
Content-Type: text/turtle
<file body>
```
Returns 200/204. Use this after downloading FIBO TTL files locally.

## Named graphs

Always load each ontology into its **own named graph** (`http://kg/fibo/be/ownership` etc.). This lets you:
- Query a single ontology: `GRAPH <uri> { ... }`
- Drop/replace one without affecting others: `DROP GRAPH <uri>`
- Track provenance of every triple

## Common failures

| Symptom | Cause | Fix |
|---|---|---|
| HTTP 400 on LOAD | Source URL needs `Accept: text/turtle` content negotiation | Download locally first, then upload via Method B |
| HTTP 500 + "ruleset not found" | Typo in `graphdb:ruleset` value | Use exact strings from table above |
| Repo created but queries return 0 results | Forgot to switch repo in workbench, or loaded into wrong named graph | Check with `SELECT ?g (COUNT(*)) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g` |
| Loading hangs forever | FIBO production CDN slow | Use Method B with `data/fibo/*.ttl` cached files |

## Reference implementation

Working scripts:
- `scripts/01_setup_graphdb.py` — repository creation
- `scripts/02_load_fibo.py` — full ontology stack with download-first fallback
- `scripts/03_load_fibo2glei_mapping.py` — FIBO↔GLEIF bridge ontology

## Verification queries

Always run these after loading:
```sparql
# 1. What graphs exist and how big are they?
SELECT ?graph (COUNT(*) AS ?triples)
WHERE { GRAPH ?graph { ?s ?p ?o } }
GROUP BY ?graph ORDER BY DESC(?triples)

# 2. Did FIBO classes load?
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT (COUNT(*) AS ?classes) WHERE {
  ?c a owl:Class . FILTER(CONTAINS(STR(?c), "edmcouncil.org"))
}
```
