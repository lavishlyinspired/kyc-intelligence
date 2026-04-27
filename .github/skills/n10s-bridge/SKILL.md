---
name: n10s-bridge
description: "Use when bridging GraphDB and Neo4j with neosemantics (n10s); importing OWL ontologies or RDF instance data into Neo4j; exporting Neo4j subgraphs back to RDF; configuring n10s.graphconfig; debugging 'No graph config exists' or 'uri constraint missing' errors. Covers all four Barrasa bridge techniques (onto.import, rdf.import, rdf.export, RDF endpoint)."
---

# neosemantics (n10s) Bridge Skill

## When to use

User asks to "import FIBO into Neo4j", "load RDF from GraphDB", "set up n10s", "convert OWL classes to Neo4j labels", "export Neo4j as RDF", or sees errors like `Failed to invoke procedure 'n10s.rdf.import.fetch': No graph config exists`.

## The four bridge techniques (Barrasa pattern)

| Technique | Procedure | Imports/Exports | Typical use |
|---|---|---|---|
| 1. Ontology import | `n10s.onto.import.fetch(url, format)` | OWL classes/properties → `:Class`, `:Property` nodes with `:SCO`, `:DOMAIN`, `:RANGE` rels | Get FIBO schema into Neo4j |
| 2. Data import | `n10s.rdf.import.fetch(url, format)` | RDF triples → `:Resource` nodes with arbitrary labels & properties | Load GLEIF entities, OFAC sanctions |
| 3. Data export | `n10s.rdf.export.cypher(query, params)` | Cypher query → RDF triples (stream) | Round-trip Neo4j subgraph back to GraphDB |
| 4. RDF endpoint | HTTP `/rdf/<db>/...` | Neo4j becomes a queryable RDF endpoint | GraphDB SPARQL `SERVICE` federation |

## Setup checklist (do ONCE per database)

```cypher
// 1. Initialize n10s — choose handling strategy for URIs and types
CALL n10s.graphconfig.init({
  handleVocabUris: 'SHORTEN',          // long URIs become prefix:localName
  handleMultival: 'ARRAY',             // multi-valued props become arrays
  handleRDFTypes: 'LABELS_AND_NODES',  // both Neo4j label and :Class node
  keepLangTag: false,                  // strip @en, @de etc.
  applyNeo4jNaming: true               // camelCase URIs → Neo4j-friendly
});

// 2. Required uniqueness constraint (n10s tracks RDF origin via .uri)
CREATE CONSTRAINT n10s_unique_uri IF NOT EXISTS
FOR (r:Resource) REQUIRE r.uri IS UNIQUE;
```

If you skip step 1, every `n10s.rdf.*` call returns `No graph config exists`.
If you skip step 2, imports succeed but duplicate nodes appear.

## handleVocabUris options

| Value | Effect on URI `https://example.org/ns#Person` | Use when |
|---|---|---|
| `SHORTEN` | Becomes `ns:Person`, prefix mappings stored | Default — keeps semantics, readable |
| `IGNORE` | Becomes just `Person` | You want clean Neo4j labels (Going Meta S30 pattern) |
| `KEEP` | Full URI preserved | RDF-purist, exporting back to RDF |
| `MAP` | Custom mapping table | Specific URI rewrites |

## Importing FIBO ontology structure

```cypher
// Pulls Class, Property, SCO (subclass), DOMAIN, RANGE nodes/rels
CALL n10s.onto.import.fetch(
  "https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/",
  "Turtle"
);

// Verify
MATCH (c:Class) RETURN c.name LIMIT 20;
MATCH (c:Class)-[:SCO]->(parent) RETURN c.name, parent.name LIMIT 20;
```

## Importing RDF instance data

Three sources possible:

```cypher
// A — From a local file (must be in Neo4j /import dir)
//    docker cp data/glei/entities.ttl kyc_neo4j:/var/lib/neo4j/import/
CALL n10s.rdf.import.fetch(
  "file:///var/lib/neo4j/import/entities.ttl",
  "Turtle"
);

// B — From an HTTP RDF source (e.g. another GraphDB endpoint)
CALL n10s.rdf.import.fetch(
  "http://graphdb:7200/repositories/kyc-kg/statements?infer=true",
  "Turtle",
  { headerParams: {Accept: "text/turtle"} }
);

// C — Inline RDF string (great for testing)
CALL n10s.rdf.import.inline(
  '@prefix ex: <http://example.org/> . ex:apple ex:name "Apple Inc." .',
  "Turtle"
);
```

## Exporting Neo4j → RDF

```cypher
// Stream RDF triples generated from a Cypher query
CALL n10s.rdf.export.cypher(
  'MATCH (e:LegalEntity)-[r:DIRECTLY_OWNED_BY]->(p) RETURN e, r, p LIMIT 100',
  {}
)
YIELD subject, predicate, object
RETURN subject, predicate, object;
```

Then POST those triples back to GraphDB at `/repositories/{repo}/rdf-graphs/service`.

## Neo4j as RDF endpoint (Technique 4)

n10s exposes Neo4j as RDF over HTTP automatically. From GraphDB you can federate:
```sparql
PREFIX neo4j: <http://localhost:7474/rdf/neo4j/>
SELECT * WHERE {
  SERVICE <http://neo4j:7474/rdf/neo4j/cypher> {
    [] ?p ?o .   # query Neo4j as if it were RDF
  }
}
```

## Common pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| `No graph config exists` | Forgot `n10s.graphconfig.init` | Run init once per database |
| Duplicate nodes after import | Missing `:Resource(uri)` constraint | Run the `CREATE CONSTRAINT` |
| All nodes are `:Resource` only | `handleRDFTypes: 'NODES'` | Use `'LABELS_AND_NODES'` or `'LABELS'` |
| Properties are weird `ns0__name` | URI shortening picked auto prefix | Set explicit prefix: `CALL n10s.nsprefixes.add('kyc', 'http://kyc-kg.example.org/ontology#')` |
| Import says "0 triples imported" | Wrong `format` argument | Match file: `Turtle`, `RDF/XML`, `N-Triples`, `JSON-LD` |
| `file:///...` not allowed | APOC import config | Already enabled in our `docker-compose.yml` (`apoc.import.file.enabled=true`) |

## Reference scripts

- `scripts/04b_neo4j_n10s_setup.py` — runs init + constraint + ontology import
- `scripts/07_load_neo4j.py` — uses both n10s and direct Cypher MERGE (hybrid pattern)
