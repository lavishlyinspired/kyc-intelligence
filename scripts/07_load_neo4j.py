"""
Script 07 — Initialise neosemantics (n10s) and load both:
  (a) the FIBO ontology STRUCTURE into Neo4j as :Class / :Property nodes
  (b) the synthetic KYC dataset as actual entity/person/relationship nodes

This is the "hybrid" pattern from Going Meta:
  • Use n10s for the ontology (so semantic structure is queryable in Cypher)
  • Use direct Cypher MERGE for high-volume application data (faster)

Skills applied: n10s-bridge, cypher-kyc-queries

    python scripts/07_load_neo4j.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.kg_client import Neo4jClient, neo4j_healthy

DATASET = Path("data/synthetic/kyc_dataset.json")

INDEXES = [
    "CREATE CONSTRAINT entity_lei_unique IF NOT EXISTS FOR (e:LegalEntity) REQUIRE e.lei IS UNIQUE",
    "CREATE CONSTRAINT entity_id_unique  IF NOT EXISTS FOR (e:LegalEntity) REQUIRE e.id IS UNIQUE",
    "CREATE CONSTRAINT person_id_unique  IF NOT EXISTS FOR (p:NaturalPerson) REQUIRE p.id IS UNIQUE",
    "CREATE INDEX entity_name           IF NOT EXISTS FOR (e:LegalEntity) ON (e.name)",
    "CREATE INDEX entity_jurisdiction   IF NOT EXISTS FOR (e:LegalEntity) ON (e.jurisdiction)",
    "CREATE INDEX person_name           IF NOT EXISTS FOR (p:NaturalPerson) ON (p.name)",
]


def init_n10s(neo: Neo4jClient) -> None:
    """One-time setup. Idempotent — safe to re-run."""
    print("→ Initialising neosemantics ...")

    # The unique-uri constraint is required by n10s
    neo.execute("""
        CREATE CONSTRAINT n10s_unique_uri IF NOT EXISTS
        FOR (r:Resource) REQUIRE r.uri IS UNIQUE
    """)

    # graphconfig.init can only run once per database — wrap in try
    try:
        neo.execute("""
            CALL n10s.graphconfig.init({
              handleVocabUris:  'SHORTEN',
              handleMultival:   'ARRAY',
              handleRDFTypes:   'LABELS_AND_NODES',
              keepLangTag:      false,
              applyNeo4jNaming: true
            })
        """)
        print("  ✓ n10s.graphconfig.init done")
    except Exception as e:
        msg = str(e)
        if "already exists" in msg or "already" in msg:
            print("  ✓ n10s graphconfig already initialised")
        else:
            raise


def import_fibo_ontology(neo: Neo4jClient) -> None:
    """Import FIBO classes & properties so they're queryable in Cypher.

    Prefers local cached files from data/fibo/ (downloaded by script 02).
    Falls back to GitHub raw URLs when the cache is missing.
    Uses n10s.onto.import.inline to avoid fetching unreliable remote URLs.
    """
    print("→ Importing FIBO ontology structure via n10s.onto.import.inline ...")

    # (local_path, rdf_format, display_name)
    FIBO_MODULES = [
        (Path("data/fibo/fibo-legal-persons.ttl"),      "Turtle",  "LegalPersons"),
        (Path("data/fibo/fibo-corporate-ownership.rdf"), "RDF/XML", "CorporateOwnership"),
        (Path("data/fibo/fibo-ownership-parties.rdf"),  "RDF/XML", "OwnershipParties"),
        (Path("data/fibo/fibo-corporate-control.rdf"),  "RDF/XML", "CorporateControl"),
        (Path("data/fibo/fibo-control-parties.rdf"),    "RDF/XML", "ControlParties"),
    ]

    # GitHub raw fallback URLs (confirmed 200) keyed by local filename stem
    FALLBACK_URLS = {
        "fibo-legal-persons":
            "https://raw.githubusercontent.com/edmcouncil/fibo/master/BE/LegalEntities/LegalPersons.rdf",
        "fibo-corporate-ownership":
            "https://raw.githubusercontent.com/edmcouncil/fibo/master/BE/OwnershipAndControl/CorporateOwnership.rdf",
        "fibo-ownership-parties":
            "https://raw.githubusercontent.com/edmcouncil/fibo/master/BE/OwnershipAndControl/OwnershipParties.rdf",
        "fibo-corporate-control":
            "https://raw.githubusercontent.com/edmcouncil/fibo/master/BE/OwnershipAndControl/CorporateControl.rdf",
        "fibo-control-parties":
            "https://raw.githubusercontent.com/edmcouncil/fibo/master/BE/OwnershipAndControl/ControlParties.rdf",
    }

    import requests as _req

    total_triples = 0
    for local_path, fmt, label in FIBO_MODULES:
        rdf_content: str | None = None

        # 1. Try local cache first
        if local_path.exists() and local_path.stat().st_size > 0:
            rdf_content = local_path.read_text(encoding="utf-8")
        else:
            # 2. Download from GitHub raw
            fallback = FALLBACK_URLS.get(local_path.stem)
            if fallback:
                try:
                    r = _req.get(fallback, timeout=60)
                    if r.ok:
                        local_path.parent.mkdir(parents=True, exist_ok=True)
                        local_path.write_bytes(r.content)
                        rdf_content = r.text
                        fmt = "RDF/XML"  # GitHub raw files are always RDF/XML
                except Exception as dl_err:
                    print(f"  ✗ {label}: download failed: {dl_err}")
                    continue

        if not rdf_content:
            print(f"  ✗ {label}: no local file and download failed — skipping")
            continue

        try:
            result = neo.query_one(
                "CALL n10s.onto.import.inline($rdf, $fmt) YIELD triplesLoaded RETURN triplesLoaded",
                {"rdf": rdf_content, "fmt": fmt},
            )
            n = result.get("triplesLoaded", 0) if result else 0
            total_triples += n
            print(f"  ✓ {label:<25} → {n:,} triples")
        except Exception as e:
            print(f"  ✗ {label}: {e}")

    print(f"  Total ontology triples imported: {total_triples:,}")


def create_indexes(neo: Neo4jClient) -> None:
    print("→ Creating indexes & constraints ...")
    for idx in INDEXES:
        neo.execute(idx)
    print(f"  ✓ {len(INDEXES)} created/verified")


def load_data(neo: Neo4jClient) -> None:
    if not DATASET.exists():
        print(f"✗ {DATASET} not found. Run 06_generate_synthetic_data.py first.")
        sys.exit(1)

    dataset = json.loads(DATASET.read_text())
    print(f"→ Loading dataset into Neo4j ({len(dataset['entities'])} entities) ...")

    with neo.driver.session() as s:
        # Entities
        s.run("""
            UNWIND $items AS e
            MERGE (n:LegalEntity {id: e.id})
            SET n.lei = e.lei, n.name = e.name,
                n.jurisdiction = e.jurisdiction, n.jurisdictionName = e.jurisdiction_name,
                n.riskTier = e.risk_tier, n.category = e.category,
                n.incorporatedDate = date(e.incorporated_date),
                n.isActive = e.is_active,
                n.hasOperationalAddress = e.has_operational_address,
                n.isin = e.isin
        """, items=dataset["entities"]).consume()
        print(f"  ✓ {len(dataset['entities'])} LegalEntity nodes")

        # Persons
        s.run("""
            UNWIND $items AS p
            MERGE (n:NaturalPerson {id: p.id})
            SET n.name = p.name, n.nationality = p.nationality,
                n.dob = date(p.dob),
                n.isPEP = p.is_pep, n.isSanctioned = p.is_sanctioned
            WITH n, p
            FOREACH (_ IN CASE WHEN p.is_sanctioned THEN [1] ELSE [] END |
                SET n:SanctionedEntity)
            FOREACH (_ IN CASE WHEN p.is_pep THEN [1] ELSE [] END |
                SET n:PoliticallyExposedPerson)
        """, items=dataset["persons"]).consume()
        print(f"  ✓ {len(dataset['persons'])} NaturalPerson nodes")

        # Relationships (use apoc.create.relationship for dynamic type)
        s.run("""
            UNWIND $items AS r
            MATCH (a {id: r.from})
            MATCH (b {id: r.to})
            CALL apoc.merge.relationship(
                a, r.type,
                {since: r.since},
                {percentage: r.percentage, role: r.role},
                b, {}
            ) YIELD rel
            RETURN count(rel)
        """, items=dataset["relationships"]).consume()
        print(f"  ✓ {len(dataset['relationships'])} ownership/control relationships")

        # Transactions
        s.run("""
            UNWIND $items AS t
            MATCH (a:LegalEntity {id: t.from_entity})
            MATCH (b:LegalEntity {id: t.to_entity})
            CREATE (a)-[:TRANSACTION {
                id: t.id, amount: t.amount, currency: t.currency,
                date: date(t.date), isSuspicious: t.is_suspicious
            }]->(b)
        """, items=dataset["transactions"]).consume()
        print(f"  ✓ {len(dataset['transactions'])} TRANSACTION relationships")

    # Persist ground truth as a singleton node so tests can read it from the DB
    s_run = neo.execute
    s_run("MATCH (gt:GroundTruth) DETACH DELETE gt")
    s_run("CREATE (gt:GroundTruth) SET gt += $props",
          {"props": {
              "sanctioned_person_ids":  dataset["ground_truth"]["sanctioned_person_ids"],
              "pep_person_ids":         dataset["ground_truth"]["pep_person_ids"],
              "sanctioned_chain_starts": dataset["ground_truth"]["sanctioned_chain_starts"],
              "ring_entity_ids":        [",".join(r) for r in dataset["ground_truth"]["ring_entity_ids"]],
          }})
    print("  ✓ GroundTruth node persisted (used by tests)")


def main() -> int:
    if not neo4j_healthy():
        print("✗ Neo4j is not reachable. Did you `docker compose up -d`?")
        return 1

    with Neo4jClient() as neo:
        init_n10s(neo)
        import_fibo_ontology(neo)
        create_indexes(neo)
        load_data(neo)

        print("\n── Final state ──")
        print(f"  Total nodes:         {neo.node_count():,}")
        print(f"  LegalEntity nodes:   {neo.node_count('LegalEntity'):,}")
        print(f"  NaturalPerson nodes: {neo.node_count('NaturalPerson'):,}")
        # n10s v5 stores ontology classes under 'n4sch__Class', not 'Class'
        print(f"  Class nodes (FIBO):  {neo.node_count('n4sch__Class'):,}")
        print(f"  Labels: {neo.list_labels()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
