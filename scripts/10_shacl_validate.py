"""
Script 10 — Validate KYC data against SHACL shapes.

Uses pyshacl locally (no GraphDB plug-in required). Pulls the data + ontology
from GraphDB via SPARQL CONSTRUCT, then validates against `shacl/kyc_shapes.ttl`.

Skill applied: shacl-validation
"""
from __future__ import annotations

import sys
from pathlib import Path

from rdflib import Graph
from pyshacl import validate

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.kg_client import GraphDBClient

SHAPES = Path("shacl/kyc_shapes.ttl")


def fetch_data_graph(gdb: GraphDBClient) -> Graph:
    """Pull all triples from GraphDB into a local rdflib graph."""
    nt = gdb.query_raw("""
        CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }
    """, accept="application/n-triples")
    g = Graph()
    g.parse(data=nt, format="nt")
    return g


def main() -> int:
    if not SHAPES.exists():
        print(f"✗ {SHAPES} not found")
        return 1

    gdb = GraphDBClient()
    print("→ Fetching data graph from GraphDB ...")
    data_g = fetch_data_graph(gdb)
    print(f"  ✓ {len(data_g):,} triples")

    shapes_g = Graph().parse(SHAPES, format="turtle")
    print(f"→ Loaded {len(shapes_g)} shape triples from {SHAPES}")

    print("→ Running SHACL validation ...")
    conforms, results_graph, results_text = validate(
        data_graph=data_g,
        shacl_graph=shapes_g,
        inference="rdfs",
        abort_on_first=False,
        meta_shacl=False,
        debug=False,
    )

    print()
    if conforms:
        print("✓ Data conforms to all SHACL shapes.")
        return 0

    print("✗ Validation FAILED:")
    print(results_text)
    Path("shacl/last_report.txt").write_text(results_text)
    print("\nFull report → shacl/last_report.txt")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
