---
name: sparql-exploration
description: "Use when writing, debugging, or teaching SPARQL queries against GraphDB; exploring loaded FIBO/GLEIF ontologies; querying across named graphs; using SPARQL property paths for ownership traversal; converting Cypher mental models to SPARQL. Covers SELECT/CONSTRUCT/ASK/UPDATE, OPTIONAL, FILTER, BIND, GRAPH, property paths (+, *, |), SERVICE federation, and common gotchas."
---

# SPARQL Exploration Skill

## When to use

User asks to "write a SPARQL query", "explore what's in GraphDB", "find UBO chains in SPARQL", "query FIBO classes", "do federated query across two endpoints".

## Mental model: SPARQL vs Cypher

| Concept | SPARQL | Cypher |
|---|---|---|
| Match a triple | `?s ?p ?o` | `(s)-[p]->(o)` |
| Filter | `FILTER(?age > 30)` | `WHERE n.age > 30` |
| Optional | `OPTIONAL { ... }` | `OPTIONAL MATCH ...` |
| Multi-hop | `?a :rel+ ?b` (1+ hops) | `(a)-[:rel*1..]->(b)` |
| Aggregation | `GROUP BY ?x` (same syntax) | `WITH x` |
| Subquery | `{ SELECT ... WHERE { ... } }` | `CALL { ... }` |
| Limit | `LIMIT 10` (same) | `LIMIT 10` |

## Query types

| Type | Returns | Use when |
|---|---|---|
| `SELECT` | Tabular bindings | You want rows of data |
| `CONSTRUCT` | RDF triples | Reshaping data, building new graphs |
| `ASK` | Boolean | Yes/no questions |
| `DESCRIBE` | All triples about a resource | Quick exploration |
| `INSERT/DELETE` (Update) | Side effect | Modify data |

## Named graphs (critical for this project)

Every FIBO module is loaded into its own named graph. To target one:
```sparql
SELECT ?class WHERE {
  GRAPH <http://kg/fibo/be/ownership> {
    ?class a owl:Class .
  }
}
```
To query across **all** graphs, omit `GRAPH` (the default — queries the union).

To list all graphs:
```sparql
SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }
```

## Property paths (the killer feature)

These let you traverse arbitrary-length chains in a single query:

| Operator | Meaning | KYC use |
|---|---|---|
| `+` | One or more hops | UBO chain: `?entity :ownedBy+ ?owner` |
| `*` | Zero or more (includes self) | "self-or-ancestor" relations |
| `?` | Zero or one | Optional single hop |
| `\|` | Alternation | `:ownedBy\|:controlledBy` |
| `^` | Inverse | `^:owns` = "is owned by" |
| `/` | Sequence | `:owns/:registeredIn` |

**Example — UBO discovery in SPARQL:**
```sparql
PREFIX kyc: <http://kyc-kg.example.org/ontology#>
SELECT ?entity ?owner WHERE {
  ?entity kyc:ownedBy+ ?owner .
  FILTER NOT EXISTS { ?owner kyc:ownedBy ?_ }   # owner has no further owner
}
```

## Cross-endpoint federation (`SERVICE`)

Query another SPARQL endpoint inline:
```sparql
SELECT ?company ?wikidataId WHERE {
  ?company kyc:legalName ?name .
  SERVICE <https://query.wikidata.org/sparql> {
    ?wikidataId rdfs:label ?name@en .
  }
}
```

## Common gotchas

| Symptom | Cause | Fix |
|---|---|---|
| 0 results, but data is loaded | Querying default graph; data is in named graph | Add `GRAPH ?g { ... }` or omit GRAPH entirely |
| Query times out | Property path `*` on huge graph | Bound it: `:rel{1,5}` or use materialised inference |
| `FILTER(?x = "foo")` doesn't match | Literal language tag mismatch | Use `STR(?x) = "foo"` or `LANGMATCHES(LANG(?x), "en")` |
| OPTIONAL produces unexpected nulls | Order matters with FILTER | Put FILTER inside OPTIONAL block |
| `COUNT(*)` returns 1 with no group | Need explicit `GROUP BY` | Add `GROUP BY ?x` |

## Standard prefixes for this project

```sparql
PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:     <http://www.w3.org/2002/07/owl#>
PREFIX xsd:     <http://www.w3.org/2001/XMLSchema#>
PREFIX skos:    <http://www.w3.org/2004/02/skos/core#>
PREFIX sh:      <http://www.w3.org/ns/shacl#>
PREFIX fibo-be: <https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/>
PREFIX fibo-fnd: <https://spec.edmcouncil.org/fibo/ontology/FND/Relations/Relations/>
PREFIX fibo-oac: <https://spec.edmcouncil.org/fibo/ontology/BE/OwnershipAndControl/Ownership/>
PREFIX lei:     <https://www.gleif.org/ontology/L1/>
PREFIX lcc:     <https://www.omg.org/spec/LCC/Countries/CountryRepresentation/>
PREFIX kyc:     <http://kyc-kg.example.org/ontology#>
```

## Reference query library

See `sparql/` directory:
- `01_explore_graphs.sparql` — list named graphs and triple counts
- `02_fibo_classes.sparql` — browse FIBO class hierarchy
- `03_fibo_ownership_props.sparql` — ownership/control properties
- `04_glei_entities.sparql` — registered entity instances
- `05_ubo_chain.sparql` — UBO discovery via property paths
- `06_cross_ontology.sparql` — joins across FIBO + GLEIF + LCC
