# Standalone SPARQL queries

These run directly against GraphDB. Either:

1. **GraphDB Workbench** → http://localhost:7200 → SPARQL → paste a file's contents.
2. **Terminal**:
   ```bash
   curl -X POST http://localhost:7200/repositories/kyc-kg \
     -H "Content-Type: application/sparql-query" \
     -H "Accept: application/sparql-results+json" \
     --data-binary @sparql/01_explore_graphs.sparql
   ```

## Files

| File | Purpose |
|------|---------|
| 01_explore_graphs.sparql | What named graphs do we have, and how big are they? |
| 02_fibo_class_hierarchy.sparql | Walk the FIBO `LegalPerson` subclass tree. |
| 03_ownership_properties.sparql | Inspect FIBO ownership/control object properties. |
| 04_inferred_ubo.sparql | Use OWL transitive inference to find UBOs. |
| 05_cross_dataset_join.sparql | Join FIBO ontology + GLEIF instances + KYC types. |
| 06_federated_query.sparql | Federation example (commented, requires SERVICE setup). |
