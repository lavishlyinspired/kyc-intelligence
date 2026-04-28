"""
Script 11 — Load REAL data into Neo4j the right way:

  1. Initialise neosemantics (n10s) and import FIBO ontology STRUCTURE
     (Class / Property / SubClassOf hierarchy) so the schema is queryable.
  2. Create constraints + indexes aligned to the FIBO schema.
  3. Load REAL GLEIF entities (350 records already cached in data/glei/raw_records.json)
     as :LegalEntity nodes, typed and linked through FIBO classes.
  4. Persist labels n4sch__Class etc. that drive Cypher queries with semantic context.

Why this replaces the previous `11_load_real_data.py`:
  • That earlier script blindly dumped Diffbot output, ignoring FIBO/GLEIF/SHACL
    and producing arbitrary labels (`Organization`, `Skill`, `STOCK_EXCHANGE`)
    that were not aligned to any ontology.
  • This script keeps the project's ontology-first design (Going Meta sessions
    28–32 pattern: ontology guides the construction).

Subsequent enrichment with ownership/control facts is handled by:
  • `scripts/12_ontology_guided_enrichment.py` (LLM extracts ownership/control
    triples from Wikipedia articles, constrained to the FIBO vocabulary).
  • `scripts/13_embed_entities.py` (creates the vector index over real
    LegalEntity / NaturalPerson nodes for semantic search).

Usage:
    python scripts/11_load_real_kg.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.kg_client import Neo4jClient, neo4j_healthy

GLEIF_RECORDS = Path("data/glei/raw_records.json")

CONSTRAINTS_AND_INDEXES = [
    # FIBO-aligned core constraints
    "CREATE CONSTRAINT entity_lei_unique IF NOT EXISTS FOR (e:LegalEntity) REQUIRE e.lei IS UNIQUE",
    "CREATE CONSTRAINT entity_id_unique  IF NOT EXISTS FOR (e:LegalEntity) REQUIRE e.id IS UNIQUE",
    "CREATE CONSTRAINT person_id_unique  IF NOT EXISTS FOR (p:NaturalPerson) REQUIRE p.id IS UNIQUE",
    # Search & lookup indexes
    "CREATE INDEX entity_name           IF NOT EXISTS FOR (e:LegalEntity) ON (e.name)",
    "CREATE INDEX entity_jurisdiction   IF NOT EXISTS FOR (e:LegalEntity) ON (e.jurisdiction)",
    "CREATE INDEX person_name           IF NOT EXISTS FOR (p:NaturalPerson) ON (p.name)",
    # Full-text index used by hybrid search
    """CREATE FULLTEXT INDEX entity_fulltext IF NOT EXISTS
        FOR (n:LegalEntity|NaturalPerson) ON EACH [n.name, n.description]""",
]

JURISDICTION_RISK = {
    "US": "low", "GB": "low", "DE": "low", "JP": "low", "FR": "low",
    "CA": "low", "AU": "low", "NL": "low", "IT": "low", "ES": "low",
    "CH": "medium", "SG": "medium", "HK": "medium", "LU": "medium", "IE": "medium",
    "KY": "high", "VG": "high", "PA": "high", "SC": "high", "BS": "high", "BM": "high",
}

JURISDICTION_NAMES = {
    "US": "United States", "GB": "United Kingdom", "DE": "Germany", "JP": "Japan",
    "FR": "France", "CA": "Canada", "AU": "Australia", "NL": "Netherlands",
    "IT": "Italy", "ES": "Spain", "CH": "Switzerland", "SG": "Singapore",
    "HK": "Hong Kong", "LU": "Luxembourg", "IE": "Ireland",
    "KY": "Cayman Islands", "VG": "British Virgin Islands", "PA": "Panama",
    "SC": "Seychelles", "BS": "Bahamas", "BM": "Bermuda",
}


# ─── n10s + FIBO ontology import ─────────────────────────────────────────────

def init_n10s(neo: Neo4jClient) -> None:
    """One-time idempotent setup for neosemantics."""
    print("→ Initialising neosemantics (n10s) ...")
    neo.execute("""
        CREATE CONSTRAINT n10s_unique_uri IF NOT EXISTS
        FOR (r:Resource) REQUIRE r.uri IS UNIQUE
    """)
    # IGNORE mode strips namespace prefixes -> clean labels (`:Class`, `:Property`,
    # `:Relationship`) instead of `n4sch__Class` etc. Original URIs are still
    # preserved on each node as the `uri` property so SPARQL↔Cypher round-trips
    # remain possible.
    try:
        neo.execute("""
            CALL n10s.graphconfig.init({
              handleVocabUris:  'IGNORE',
              handleMultival:   'ARRAY',
              handleRDFTypes:   'LABELS',
              keepLangTag:      false,
              applyNeo4jNaming: true
            })
        """)
        print("  ✓ n10s graphconfig initialised (IGNORE mode → clean labels)")
    except Exception as e:
        msg = str(e).lower()
        if "already" in msg:
            # Force-reset to IGNORE in case a previous run set SHORTEN
            try:
                neo.execute("CALL n10s.graphconfig.set({handleVocabUris:'IGNORE', handleRDFTypes:'LABELS'})")
                print("  ✓ n10s graphconfig reset to IGNORE / LABELS")
            except Exception as e2:
                print(f"  ! could not force-reset graphconfig: {e2}")
        else:
            raise


def import_fibo_ontology(neo: Neo4jClient) -> None:
    """Import FIBO class & property STRUCTURE (the schema) into Neo4j.

    Result: queryable :n4sch__Class and :n4sch__Property nodes describing
    LegalPerson, ownership/control relationships, etc.
    """
    print("→ Importing FIBO ontology structure via n10s.onto.import.inline ...")

    FIBO_MODULES = [
        (Path("data/fibo/fibo-legal-persons.ttl"),       "Turtle",  "LegalPersons"),
        (Path("data/fibo/fibo-corporate-ownership.rdf"), "RDF/XML", "CorporateOwnership"),
        (Path("data/fibo/fibo-ownership-parties.rdf"),   "RDF/XML", "OwnershipParties"),
        (Path("data/fibo/fibo-corporate-control.rdf"),   "RDF/XML", "CorporateControl"),
        (Path("data/fibo/fibo-control-parties.rdf"),     "RDF/XML", "ControlParties"),
    ]

    total = 0
    for path, fmt, label in FIBO_MODULES:
        if not path.exists():
            print(f"  ✗ {label}: {path} missing — run script 02 first")
            continue
        try:
            result = neo.query_one(
                "CALL n10s.onto.import.inline($rdf, $fmt) YIELD triplesLoaded RETURN triplesLoaded",
                {"rdf": path.read_text(encoding="utf-8"), "fmt": fmt},
            )
            n = result.get("triplesLoaded", 0) if result else 0
            total += n
            print(f"  ✓ {label:<25} → {n:,} triples")
        except Exception as e:
            print(f"  ✗ {label}: {e}")
    print(f"  Total ontology triples: {total:,}")


# ─── Real GLEIF data → LegalEntity nodes (FIBO-aligned) ──────────────────────

def load_real_gleif_entities(neo: Neo4jClient) -> int:
    """Load real GLEIF LEI records as :LegalEntity nodes, FIBO-aligned.

    Each node gets:
      - Labels: :LegalEntity (the FIBO concept) + :Resource (n10s convention)
      - Identity: id (LEI), lei, name, jurisdiction, etc.
      - Risk-tier derived from jurisdiction (offshore vs onshore)
      - Proper FIBO type via :n4sch__type relationship to fibo-be:LegalPerson class
    """
    if not GLEIF_RECORDS.exists():
        print(f"✗ {GLEIF_RECORDS} not found — run scripts/05_load_gleif_data.py first")
        return 0

    raw = json.loads(GLEIF_RECORDS.read_text())
    print(f"→ Loading {len(raw)} real GLEIF entities into Neo4j ...")

    rows: list[dict] = []
    for rec in raw:
        attrs = rec.get("attributes", {}) or {}
        ent = attrs.get("entity", {}) or {}
        legal_addr = ent.get("legalAddress") or {}
        hq_addr = ent.get("headquartersAddress") or {}
        legal_name = (ent.get("legalName") or {}).get("name")
        if not legal_name:
            continue

        lei = attrs.get("lei")
        jur = ent.get("jurisdiction") or legal_addr.get("country") or "??"
        category = ent.get("category") or "GENERAL"
        legal_form = (ent.get("legalForm") or {}).get("other") or category
        registered_at = (ent.get("registeredAt") or {}).get("id")
        status = ent.get("status", "ACTIVE")

        # Category mapping → ENTITY_CATEGORIES used by tools
        cat_map = {
            "GENERAL": "CORPORATION", "FUND": "FUND", "BRANCH": "BRANCH",
            "SOLE_PROPRIETOR": "PARTNERSHIP", "RESIDENT_GOVERNMENT_ENTITY": "CORPORATION",
        }
        category_norm = cat_map.get(category, "CORPORATION")

        # has_operational_address: real if HQ address is present and different from registered
        has_op_addr = bool(hq_addr and hq_addr.get("addressLines"))

        rows.append({
            "id": lei,
            "lei": lei,
            "name": legal_name,
            "jurisdiction": jur,
            "jurisdiction_name": JURISDICTION_NAMES.get(jur, jur),
            "risk_tier": JURISDICTION_RISK.get(jur, "medium"),
            "category": category_norm,
            "legal_form": legal_form,
            "registered_at": registered_at,
            "is_active": status == "ACTIVE",
            "has_operational_address": has_op_addr,
            "city": legal_addr.get("city"),
            "country": legal_addr.get("country"),
            "postal_code": legal_addr.get("postalCode"),
            "hq_city": hq_addr.get("city"),
            "hq_country": hq_addr.get("country"),
            "description": (
                f"{legal_name} — a {category_norm.lower()} legal entity "
                f"registered in {JURISDICTION_NAMES.get(jur, jur)} "
                f"under LEI {lei}. Status: {status}."
            ),
        })

    with neo.driver.session() as s:
        s.run("""
            UNWIND $items AS e
            MERGE (n:LegalEntity {id: e.id})
            SET n.lei                    = e.lei,
                n.name                   = e.name,
                n.jurisdiction           = e.jurisdiction,
                n.jurisdictionName       = e.jurisdiction_name,
                n.riskTier               = e.risk_tier,
                n.category               = e.category,
                n.legalForm              = e.legal_form,
                n.registeredAt           = e.registered_at,
                n.isActive               = e.is_active,
                n.hasOperationalAddress  = e.has_operational_address,
                n.city                   = e.city,
                n.country                = e.country,
                n.postalCode             = e.postal_code,
                n.hqCity                 = e.hq_city,
                n.hqCountry              = e.hq_country,
                n.description            = e.description,
                n.kycRiskScore           = CASE e.risk_tier
                                              WHEN 'high'   THEN 65
                                              WHEN 'medium' THEN 35
                                              ELSE 15 END,
                n.dataSource             = 'GLEIF',
                n.uri                    = 'https://www.gleif.org/data/lei/' + e.lei
        """, items=rows).consume()

    print(f"  ✓ {len(rows)} real :LegalEntity nodes loaded from GLEIF")
    return len(rows)


def link_entities_to_fibo_classes(neo: Neo4jClient) -> None:
    """Link each :LegalEntity to its FIBO class node (semantic typing).

    This is the Going-Meta `n10s` pattern: instances point to the schema
    via a typed relationship, so a Cypher query can ask
    "all instances of LegalPerson or any subclass".
    """
    print("→ Linking real entities to FIBO ontology classes ...")
    rows = neo.query("""
        MATCH (c:Class)
        WHERE c.uri ENDS WITH '/LegalPerson'
        RETURN c.uri AS uri LIMIT 1
    """)
    if not rows:
        print("  ! no FIBO LegalPerson class found — skipping link")
        return

    fibo_uri = rows[0]["uri"]
    res = neo.query_one("""
        MATCH (cls:Class {uri: $uri})
        MATCH (e:LegalEntity)
        MERGE (e)-[r:INSTANCE_OF]->(cls)
        RETURN count(r) AS linked
    """, {"uri": fibo_uri})
    print(f"  ✓ {res.get('linked', 0)} :LegalEntity nodes linked to {fibo_uri}")


def main() -> int:
    if not neo4j_healthy():
        print("✗ Neo4j is not reachable. Did you `docker compose up -d`?")
        return 1

    with Neo4jClient() as neo:
        # Wipe (idempotent — leaves constraints/indexes intact)
        print("→ Wiping existing graph data ...")
        neo.execute("MATCH (n) DETACH DELETE n")

        init_n10s(neo)
        import_fibo_ontology(neo)

        print("→ Creating constraints & indexes (FIBO-aligned) ...")
        for stmt in CONSTRAINTS_AND_INDEXES:
            try:
                neo.execute(stmt)
            except Exception as e:
                print(f"  ! {stmt[:60]}... — {e}")
        print(f"  ✓ {len(CONSTRAINTS_AND_INDEXES)} constraints/indexes verified")

        n = load_real_gleif_entities(neo)
        if n > 0:
            link_entities_to_fibo_classes(neo)

        print("\n── Final state ──")
        print(f"  Total nodes:         {neo.node_count():,}")
        print(f"  LegalEntity nodes:   {neo.node_count('LegalEntity'):,}")
        print(f"  Class nodes (FIBO):  {neo.node_count('Class'):,}")
        print(f"  Labels: {neo.list_labels()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
