"""
Script 05 — Fetch real entities from the GLEIF API and load into GraphDB as
FIBO-aligned RDF.

Why
---
- FIBO defines what a LegalEntity IS (the schema).
- GLEIF data is who they ARE (the instances — Apple, Google, ...).
- We type each entity with `rdf:type fibo-be:LegalPerson` so SPARQL queries
  using FIBO vocabulary just work.

Skill applied: load-fibo-ontology

    python scripts/05_load_gleif_data.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import requests
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.kg_client import GraphDBClient

GLEIF_API = "https://api.gleif.org/api/v1"

FIBO_BE = Namespace("https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/")
KYC = Namespace("http://kyc-kg.example.org/ontology#")
LCC_ISO = Namespace("https://www.omg.org/spec/LCC/Countries/ISO3166-1-CountryCodes/")

DATA_DIR = Path("data/glei")
NAMED_GRAPH = "http://kg/glei/instances"

JURISDICTIONS = ["US", "GB", "DE", "JP", "CH", "KY", "VG"]


def fetch_entities(country: str, page_size: int = 50) -> list[dict]:
    """Fetch LEI records filtered by jurisdiction (free, no auth)."""
    r = requests.get(
        f"{GLEIF_API}/lei-records",
        params={
            "filter[entity.legalJurisdiction]": country,
            "page[size]": page_size,
            "page[number]": 1,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["data"]


def entities_to_rdf(records: list[dict]) -> Graph:
    """Convert GLEIF JSON records to a FIBO-aligned RDF graph."""
    g = Graph()
    g.bind("fibo-be", FIBO_BE)
    g.bind("kyc", KYC)
    g.bind("lcc-iso", LCC_ISO)

    for record in records:
        attrs = record["attributes"]
        lei = attrs["lei"]
        entity = attrs.get("entity", {}) or {}

        uri = URIRef(f"https://www.gleif.org/data/lei/{lei}")

        # Type with the FIBO class — this is the alignment step
        g.add((uri, RDF.type, FIBO_BE.LegalPerson))
        g.add((uri, RDF.type, KYC.RegisteredLegalEntity))
        g.add((uri, KYC.leiCode, Literal(lei)))

        legal_name = (entity.get("legalName") or {}).get("name")
        if legal_name:
            g.add((uri, RDFS.label, Literal(legal_name)))
            g.add((uri, KYC.legalName, Literal(legal_name)))

        jur = entity.get("jurisdiction")
        if jur and len(jur) == 2:
            g.add((uri, KYC.hasJurisdiction, URIRef(LCC_ISO + jur)))

        status = entity.get("status")
        if status:
            g.add((uri, KYC.entityStatus, Literal(status)))

        addr = entity.get("legalAddress") or {}
        if addr:
            addr_uri = URIRef(f"http://kyc-kg.example.org/address/{lei}")
            g.add((uri, KYC.hasLegalAddress, addr_uri))
            g.add((addr_uri, RDF.type, KYC.Address))
            for prop, key in [("city", "city"), ("country", "country"), ("postalCode", "postalCode")]:
                v = addr.get(key)
                if v:
                    g.add((addr_uri, KYC[prop], Literal(v)))

    return g


def main() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    all_records: list[dict] = []
    for country in JURISDICTIONS:
        try:
            recs = fetch_entities(country)
            print(f"  ✓ {country}: {len(recs)} entities fetched")
            all_records.extend(recs)
        except Exception as e:
            print(f"  ✗ {country}: {e}")

    if not all_records:
        print("\n✗ No entities fetched — is the network up?")
        return 1

    (DATA_DIR / "raw_records.json").write_text(json.dumps(all_records, indent=2))
    print(f"\nSaved raw JSON ({len(all_records)} records) → {DATA_DIR / 'raw_records.json'}")

    g = entities_to_rdf(all_records)
    ttl_path = DATA_DIR / "entities.ttl"
    ttl_path.write_text(g.serialize(format="turtle"))
    print(f"Generated {len(g):,} RDF triples → {ttl_path}")

    gdb = GraphDBClient()
    status = gdb.load_turtle(g.serialize(format="turtle"), NAMED_GRAPH)
    print(f"\nUploaded to GraphDB: HTTP {status} → graph <{NAMED_GRAPH}>")
    print(f"  Triples in this graph now: {gdb.count_triples(NAMED_GRAPH):,}")
    return 0 if status in (200, 204) else 1


if __name__ == "__main__":
    raise SystemExit(main())
