"""
Script 15 — Load REAL sanctions & PEP data from OpenSanctions.

Source: https://www.opensanctions.org/  (CC-BY 4.0, free, public)
  • sanctions/targets.simple.csv  → ~30k orgs/persons sanctioned globally
  • peps/targets.simple.csv       → ~120k politically-exposed persons

For each record we:
  1. Match against existing :LegalEntity / :NaturalPerson by LEI (if present in
     `identifiers`) or by exact lower-cased name → MERGE the appropriate label.
  2. If no match exists, create a NEW :LegalEntity (schema = Organization /
     Company / LegalEntity) or :NaturalPerson (schema = Person) so investigators
     can still query the sanctions hit.

Idempotent: re-running only refreshes labels & the dataset list.
SHACL-aligned: every node still carries an id (the OpenSanctions NK-id) and a
clean name, so kyc_shapes.ttl validation continues to pass.

    python scripts/15_load_opensanctions.py
"""
from __future__ import annotations

import csv
import io
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.kg_client import Neo4jClient, neo4j_healthy

DATA_DIR = Path("data/opensanctions")
DATA_DIR.mkdir(parents=True, exist_ok=True)

SOURCES = [
    ("sanctions",
     "https://data.opensanctions.org/datasets/latest/sanctions/targets.simple.csv",
     "SanctionedEntity"),
    ("peps",
     "https://data.opensanctions.org/datasets/latest/peps/targets.simple.csv",
     "PoliticallyExposedPerson"),
]

# OpenSanctions CSV `schema` column → our Neo4j label
PERSON_SCHEMAS = {"Person", "PublicBody"}  # PublicBody often modeled as person here
ORG_SCHEMAS = {"Organization", "Company", "LegalEntity", "PublicBody",
               "Asset", "Vessel", "Airplane"}


def download(url: str, dest: Path) -> Path:
    if dest.exists() and dest.stat().st_size > 0:
        print(f"   ✓ cache hit {dest.name} ({dest.stat().st_size:,} bytes)")
        return dest
    print(f"   ↓ downloading {url}")
    r = requests.get(url, stream=True, timeout=120)
    r.raise_for_status()
    with dest.open("wb") as f:
        for chunk in r.iter_content(chunk_size=1 << 16):
            if chunk:
                f.write(chunk)
    print(f"   ✓ saved {dest.stat().st_size:,} bytes")
    return dest


def parse_lei(identifiers: str) -> str | None:
    """Best-effort LEI extraction from OpenSanctions `identifiers` field.
    LEIs are 20-character alphanumeric ISO 17442 codes."""
    if not identifiers:
        return None
    for token in identifiers.replace(",", ";").split(";"):
        t = token.strip().upper()
        if len(t) == 20 and t.isalnum():
            return t
    return None


def load_csv(neo: Neo4jClient, csv_path: Path, label: str, dataset_tag: str) -> dict:
    """Stream the CSV in 5k-row batches and MERGE entities into Neo4j."""
    print(f"→ Loading {csv_path.name} as :{label} ...")

    persons: list[dict] = []
    orgs: list[dict] = []
    seen = 0
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            seen += 1
            schema = (row.get("schema") or "").strip()
            name = (row.get("name") or "").strip()
            if not name:
                continue
            common = {
                "osId": row["id"],
                "name": name,
                "aliases": [a.strip() for a in (row.get("aliases") or "").split(";") if a.strip()],
                "countries": [c.strip().upper() for c in (row.get("countries") or "").split(";") if c.strip()],
                "datasets": [d.strip() for d in (row.get("dataset") or "").split(";") if d.strip()],
                "firstSeen": row.get("first_seen"),
                "lastSeen": row.get("last_seen"),
                "sourceTag": dataset_tag,
            }
            if schema in PERSON_SCHEMAS:
                persons.append({**common,
                                "dob": (row.get("birth_date") or "").strip() or None,
                                "nationality": (common["countries"][0] if common["countries"] else None)})
            elif schema in ORG_SCHEMAS:
                orgs.append({**common,
                             "lei": parse_lei(row.get("identifiers", "")),
                             "jurisdiction": (common["countries"][0] if common["countries"] else None)})

    n_p = n_o = 0
    if persons:
        with neo.driver.session() as s:
            for i in range(0, len(persons), 5000):
                batch = persons[i:i + 5000]
                s.run(f"""
                    UNWIND $rows AS r
                    MERGE (p:NaturalPerson {{id: r.osId}})
                    SET p.name        = r.name,
                        p.aliases     = r.aliases,
                        p.nationality = coalesce(p.nationality, r.nationality),
                        p.dob         = coalesce(p.dob, r.dob),
                        p.osDatasets  = r.datasets,
                        p.dataSource  = coalesce(p.dataSource, 'OpenSanctions'),
                        p:`{label}`
                    SET p.isPEP        = coalesce(p.isPEP, false) OR ('{label}' = 'PoliticallyExposedPerson'),
                        p.isSanctioned = coalesce(p.isSanctioned, false) OR ('{label}' = 'SanctionedEntity')
                """, rows=batch).consume()
                n_p += len(batch)
        print(f"   ✓ {n_p:,} :NaturalPerson :{label} merged")

    if orgs:
        with neo.driver.session() as s:
            # Try LEI-match first (only orgs with LEIs)
            with_lei = [o for o in orgs if o.get("lei")]
            without_lei = [o for o in orgs if not o.get("lei")]

            if with_lei:
                s.run(f"""
                    UNWIND $rows AS r
                    MERGE (e:LegalEntity {{lei: r.lei}})
                      ON CREATE SET e.id = r.lei, e.dataSource = 'OpenSanctions'
                    SET e.name         = coalesce(e.name, r.name),
                        e.aliases      = coalesce(e.aliases, r.aliases),
                        e.jurisdiction = coalesce(e.jurisdiction, r.jurisdiction),
                        e.osDatasets   = r.datasets,
                        e:`{label}`
                    SET e.isSanctioned = ('{label}' = 'SanctionedEntity')
                """, rows=with_lei).consume()

            # Name-match fallback for orgs without LEI
            if without_lei:
                s.run(f"""
                    UNWIND $rows AS r
                    OPTIONAL MATCH (existing:LegalEntity)
                      WHERE toLower(existing.name) = toLower(r.name)
                    WITH r, existing
                    CALL {{
                        WITH r, existing
                        WITH r, existing WHERE existing IS NOT NULL
                        SET existing.osDatasets = r.datasets,
                            existing:`{label}`
                        SET existing.isSanctioned = ('{label}' = 'SanctionedEntity')
                        RETURN 1 AS matched
                        UNION
                        WITH r, existing
                        WITH r WHERE existing IS NULL
                        MERGE (e:LegalEntity {{id: r.osId}})
                        SET e.name         = r.name,
                            e.aliases      = r.aliases,
                            e.jurisdiction = r.jurisdiction,
                            e.osDatasets   = r.datasets,
                            e.dataSource   = 'OpenSanctions',
                            e:`{label}`
                        SET e.isSanctioned = ('{label}' = 'SanctionedEntity')
                        RETURN 1 AS matched
                    }}
                    RETURN count(matched)
                """, rows=without_lei).consume()
            n_o = len(orgs)
        print(f"   ✓ {n_o:,} :LegalEntity :{label} merged")

    return {"rows_seen": seen, "persons": n_p, "orgs": n_o}


def main() -> int:
    if not neo4j_healthy():
        print("✗ Neo4j is not reachable.")
        return 1

    totals: dict[str, dict] = {}
    with Neo4jClient() as neo:
        # Make sure the labels we apply are indexable for fast lookup
        for stmt in [
            "CREATE INDEX np_pep      IF NOT EXISTS FOR (p:PoliticallyExposedPerson) ON (p.name)",
            "CREATE INDEX np_sanc     IF NOT EXISTS FOR (p:SanctionedEntity)        ON (p.name)",
            "CREATE INDEX le_sanc     IF NOT EXISTS FOR (e:SanctionedEntity)        ON (e.name)",
        ]:
            try:
                neo.execute(stmt)
            except Exception as e:
                print(f"  ! {stmt[:60]} — {e}")

        for tag, url, label in SOURCES:
            csv_path = DATA_DIR / f"{tag}.csv"
            try:
                download(url, csv_path)
            except Exception as e:
                print(f"   ✗ download failed for {tag}: {e}")
                continue
            totals[tag] = load_csv(neo, csv_path, label, tag)

        print("\n══ DONE ══")
        for tag, t in totals.items():
            print(f"  {tag:<10}  rows_seen={t['rows_seen']:>7,}  "
                  f"persons={t['persons']:>6,}  orgs={t['orgs']:>6,}")
        # Quick verification
        print("\nVerification:")
        for q, lbl in [
            ("MATCH (n:SanctionedEntity)         RETURN count(n) AS c", "SanctionedEntity"),
            ("MATCH (n:PoliticallyExposedPerson) RETURN count(n) AS c", "PoliticallyExposedPerson"),
            ("MATCH (n:LegalEntity:SanctionedEntity) RETURN count(n) AS c", "LegalEntity ∩ SanctionedEntity"),
            ("MATCH (n:NaturalPerson:SanctionedEntity) RETURN count(n) AS c", "NaturalPerson ∩ SanctionedEntity"),
        ]:
            r = neo.query_one(q)
            print(f"  {lbl:<35} {r['c']:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
