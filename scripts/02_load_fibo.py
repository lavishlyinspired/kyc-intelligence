"""
Script 02 — Load the FIBO + LCC ontology stack into GraphDB.

Strategy
--------
1. For each module: try to download the TTL file locally to `data/fibo/`.
2. Then upload the local file to GraphDB (more reliable than live SPARQL LOAD).
3. Falls back to direct SPARQL LOAD if download fails.

Each module goes into its own named graph for traceability:
    http://kg/fibo/...
    http://kg/lcc/...

Skill applied: load-fibo-ontology

    python scripts/02_load_fibo.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import requests
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.kg_client import GraphDBClient

# ─── Module list (load order matters — dependencies first) ────────────────────
MODULES = [
    # Tier 1 — Foundations
    {"url": "https://spec.edmcouncil.org/fibo/ontology/FND/Utilities/AnnotationVocabulary/",
     "graph": "http://kg/fibo/fnd/annotations",  "name": "FIBO Annotations"},
    {"url": "https://spec.edmcouncil.org/fibo/ontology/FND/Relations/Relations/",
     "graph": "http://kg/fibo/fnd/relations",    "name": "FIBO Relations"},
    {"url": "https://spec.edmcouncil.org/fibo/ontology/FND/AgentsAndPeople/Agents/",
     "graph": "http://kg/fibo/fnd/agents",       "name": "FIBO Agents"},

    # Tier 2 — LCC
    {"url": "https://www.omg.org/spec/LCC/Countries/CountryRepresentation/",
     "graph": "http://kg/lcc/countries",         "name": "LCC Countries"},
    {"url": "https://www.omg.org/spec/LCC/Countries/ISO3166-1-CountryCodes/",
     "graph": "http://kg/lcc/iso3166",           "name": "LCC ISO 3166-1"},

    # Tier 3 — Business Entities (KYC core)
    {"url": "https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/",
     "graph": "http://kg/fibo/be/legal-persons", "name": "FIBO Legal Persons"},
    {"url": "https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/CorporateBodies/",
     "graph": "http://kg/fibo/be/corp-bodies",   "name": "FIBO Corporate Bodies"},
    # FIBO reorganised Ownership & Control into separate modules (old monolithic
    # URLs return 404 as of 2025). Use GitHub raw RDF/XML files instead.
    {"url": "https://raw.githubusercontent.com/edmcouncil/fibo/master/BE/OwnershipAndControl/CorporateOwnership.rdf",
     "graph": "http://kg/fibo/be/ownership",     "name": "FIBO Corporate Ownership", "fmt": "rdfxml"},
    {"url": "https://raw.githubusercontent.com/edmcouncil/fibo/master/BE/OwnershipAndControl/OwnershipParties.rdf",
     "graph": "http://kg/fibo/be/ownership",     "name": "FIBO Ownership Parties",   "fmt": "rdfxml"},
    {"url": "https://raw.githubusercontent.com/edmcouncil/fibo/master/BE/OwnershipAndControl/CorporateControl.rdf",
     "graph": "http://kg/fibo/be/control",       "name": "FIBO Corporate Control",   "fmt": "rdfxml"},
    {"url": "https://raw.githubusercontent.com/edmcouncil/fibo/master/BE/OwnershipAndControl/ControlParties.rdf",
     "graph": "http://kg/fibo/be/control",       "name": "FIBO Control Parties",     "fmt": "rdfxml"},
    {"url": "https://spec.edmcouncil.org/fibo/ontology/BE/Corporations/Corporations/",
     "graph": "http://kg/fibo/be/corporations",  "name": "FIBO Corporations"},

    # Tier 4 — FBC (optional but useful)
    {"url": "https://spec.edmcouncil.org/fibo/ontology/FBC/FunctionalEntities/FinancialServicesEntities/",
     "graph": "http://kg/fibo/fbc/fse",          "name": "FIBO Financial Services Entities"},
]

DATA_DIR = Path("data/fibo")


def download(url: str, name: str, fmt: str = "turtle") -> tuple[Path, str] | None:
    """Download a module to data/fibo/. Returns (path, content_type) or None."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    slug = name.lower().replace(" ", "-")
    ext = "rdf" if fmt == "rdfxml" else "ttl"
    path = DATA_DIR / f"{slug}.{ext}"
    if path.exists() and path.stat().st_size > 0:
        return path, ("application/rdf+xml" if fmt == "rdfxml" else "text/turtle")
    accept = "application/rdf+xml" if fmt == "rdfxml" else "text/turtle"
    try:
        r = requests.get(url, headers={"Accept": accept}, timeout=60)
        if r.ok and r.text.strip():
            path.write_bytes(r.content)
            return path, accept
    except Exception as e:
        print(f"    download error: {e}")
    return None


def load_module(gdb: GraphDBClient, module: dict) -> tuple[bool, str]:
    """Try local file upload first, fall back to direct SPARQL LOAD."""
    fmt = module.get("fmt", "turtle")
    result = download(module["url"], module["name"], fmt)
    if result is not None:
        path, content_type = result
        size = path.stat().st_size
        with open(path, "rb") as f:
            r = requests.post(
                gdb.graphs_endpoint,
                params={"graph": module["graph"]},
                data=f,
                headers={"Content-Type": content_type},
                timeout=120,
            )
        if r.ok:
            return True, f"local upload ({size:,} bytes)"
        # fall through to URL load on upload failure

    status = gdb.load_url(module["url"], module["graph"])
    return (status in (200, 204), f"SPARQL LOAD HTTP {status}")


def main() -> int:
    gdb = GraphDBClient()
    if not gdb.repository_exists():
        print(f"✗ Repository '{gdb.repo}' does not exist. Run 01_setup_graphdb.py first.")
        return 1

    print(f"Loading {len(MODULES)} ontology modules into '{gdb.repo}'...\n")

    failed = []
    for module in tqdm(MODULES, desc="Modules", ncols=80):
        ok, detail = load_module(gdb, module)
        icon = "✓" if ok else "✗"
        tqdm.write(f"  {icon} {module['name']:<40} → {detail}")
        if not ok:
            failed.append(module["name"])

    print()
    print("Summary:")
    for graph, count in gdb.list_named_graphs():
        print(f"  {count:>10,} triples in {graph}")
    print(f"  Total triples: {gdb.count_triples():,}")

    if failed:
        print(f"\n✗ Failed modules: {failed}")
        return 1
    print("\n✓ All ontology modules loaded.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
