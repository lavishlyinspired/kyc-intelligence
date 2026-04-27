"""
Script 04 — SPARQL exploration tour of the loaded ontologies.

Run after scripts 01-03. Each query teaches a SPARQL concept:

    Q1 — Named graphs (the data layout)
    Q2 — OWL classes (what FIBO defines)
    Q3 — Subclass hierarchy
    Q4 — Object properties + their domain/range
    Q5 — Property paths (variable-length traversal)
    Q6 — Cross-graph join (FIBO + KYC ontology)

Skill applied: sparql-exploration

    python scripts/04_sparql_exploration.py
"""
from __future__ import annotations

import sys
from pathlib import Path
from textwrap import indent

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.kg_client import GraphDBClient

QUERIES = [
    ("Q1 — Named graphs and their triple counts", """
        SELECT ?graph (COUNT(*) AS ?triples)
        WHERE { GRAPH ?graph { ?s ?p ?o } }
        GROUP BY ?graph
        ORDER BY DESC(?triples)
    """),

    ("Q2 — OWL classes from FIBO", """
        PREFIX owl:  <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?class ?label WHERE {
            ?class a owl:Class .
            OPTIONAL { ?class rdfs:label ?label }
            FILTER(CONTAINS(STR(?class), "edmcouncil.org"))
        }
        ORDER BY ?label
        LIMIT 20
    """),

    ("Q3 — Subclasses of FIBO LegalPerson (transitive via *)", """
        PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX fibo-be: <https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/>
        SELECT DISTINCT ?descendant ?label WHERE {
            ?descendant rdfs:subClassOf* fibo-be:LegalPerson .
            OPTIONAL { ?descendant rdfs:label ?label }
        }
        ORDER BY ?label
    """),

    ("Q4 — FIBO Ownership object properties (with domain & range)", """
        PREFIX owl:  <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?prop ?label ?domain ?range WHERE {
            GRAPH <http://kg/fibo/be/ownership> {
                ?prop a owl:ObjectProperty .
                OPTIONAL { ?prop rdfs:label   ?label }
                OPTIONAL { ?prop rdfs:domain  ?domain }
                OPTIONAL { ?prop rdfs:range   ?range }
            }
        }
        LIMIT 20
    """),

    ("Q5 — Cross-graph: KYC LegalEntity is subclass of FIBO LegalPerson?", """
        PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX kyc:     <http://kyc-kg.example.org/ontology#>
        PREFIX fibo-be: <https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/>
        ASK { kyc:LegalEntity rdfs:subClassOf+ fibo-be:LegalPerson }
    """),

    ("Q6 — Total triple count across all graphs (incl. inferred)", """
        SELECT (COUNT(*) AS ?total) WHERE { ?s ?p ?o }
    """),
]


def run_query(gdb: GraphDBClient, title: str, query: str) -> None:
    print(f"\n{'═' * 78}\n  {title}\n{'═' * 78}")
    print(indent(query.strip(), "    "))
    print()
    if query.strip().upper().startswith("ASK"):
        print(f"  → {gdb.ask(query)}")
        return
    rows = gdb.query(query)
    if not rows:
        print("  (no rows)")
        return
    for row in rows[:15]:
        print("  " + " | ".join(f"{k}={v}" for k, v in row.items()))
    if len(rows) > 15:
        print(f"  ... ({len(rows)-15} more rows)")


def main() -> int:
    gdb = GraphDBClient()
    print(f"Querying {gdb.sparql_endpoint}\n")
    for title, query in QUERIES:
        run_query(gdb, title, query)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
