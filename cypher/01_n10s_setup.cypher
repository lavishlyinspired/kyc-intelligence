// === n10s setup (idempotent) ===
// Required: neosemantics plugin loaded in Neo4j (it is, via NEO4J_PLUGINS).

// 1. Unique-URI constraint required by n10s
CREATE CONSTRAINT n10s_unique_uri IF NOT EXISTS
FOR (r:Resource) REQUIRE r.uri IS UNIQUE;

// 2. Configure how n10s converts RDF to property graph
CALL n10s.graphconfig.init({
  handleVocabUris:  'SHORTEN',
  handleMultival:   'ARRAY',
  handleRDFTypes:   'LABELS_AND_NODES',
  keepLangTag:      false,
  applyNeo4jNaming: true
});

// 3. Pull a FIBO module straight from EDM Council
CALL n10s.onto.import.fetch(
  'https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/',
  'Turtle'
);

// 4. Cross-DB import: pull RDF from GraphDB into Neo4j
CALL n10s.rdf.import.fetch(
  'http://graphdb:7200/repositories/kyc-kg/statements?infer=true',
  'Turtle'
);

// 5. Verify
MATCH (c:Class) RETURN count(c) AS imported_classes;
