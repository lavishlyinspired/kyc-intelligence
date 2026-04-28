"""
Script 17 — Load REAL ICIJ Offshore Leaks into Neo4j.

Source: https://offshoreleaks.icij.org/pages/database (CC-BY-NC-SA 4.0).
Combined database of Panama, Paradise, Pandora, Bahamas, Offshore leaks etc.

What this loads (per user choice "just relationships, no addresses")
--------------------------------------------------------------------
  * nodes-entities.csv     → :LegalEntity {dataSource:'ICIJ', sourceID, jurisdiction, ...}
  * nodes-officers.csv     → :NaturalPerson {dataSource:'ICIJ'}
  * nodes-intermediaries   → :LegalEntity :Intermediary
  * nodes-others           → :LegalEntity (trusts, foundations etc.)
  * relationships.csv (filtered): excludes 'registered_address' rels
        - 'officer_of'      → (:NaturalPerson)-[:CONTROLLED_BY {role}]->(:LegalEntity)
        - 'intermediary_of' → (:LegalEntity)-[:INTERMEDIARY_FOR]->(:LegalEntity)
        - 'underlying'      → (:LegalEntity)-[:DIRECTLY_OWNED_BY]->(:LegalEntity)
        - 'connected_to'    → (:LegalEntity)-[:CONNECTED_TO]->(:LegalEntity)
        - 'similar' / 'same_id_as' → (:Resource)-[:SAME_AS]->()
        - 'beneficiary_of'  → (:NaturalPerson)-[:CONTROLLED_BY {role:'beneficiary'}]->(:LegalEntity)
        - 'shareholder_of'  → (:LegalEntity)-[:DIRECTLY_OWNED_BY]->()
        - other types are kept verbatim as relationship type (UPPER_SNAKE)

Required: extract data/Offshore Leaks Database/full-oldb.LATEST.zip
into data/offshoreleaks_extracted/

    python scripts/17_load_icij_offshoreleaks.py
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.kg_client import Neo4jClient, neo4j_healthy

OL_DIR = Path("data/offshoreleaks_extracted")
IMPORT_DIR = Path("import")
IMPORT_DIR.mkdir(parents=True, exist_ok=True)

# Map ICIJ rel_type → Neo4j relationship type (FIBO-friendly where possible)
REL_TYPE_MAP = {
    "officer_of":        "CONTROLLED_BY",     # person → entity
    "beneficiary_of":    "CONTROLLED_BY",     # person → entity (role beneficiary)
    "intermediary_of":   "INTERMEDIARY_FOR",  # entity → entity
    "underlying":        "DIRECTLY_OWNED_BY", # entity → entity
    "shareholder_of":    "DIRECTLY_OWNED_BY",
    "connected_to":      "CONNECTED_TO",
    "similar":           "SIMILAR_TO",
    "same_id_as":        "SAME_AS",
    "same_name_and_address_as": "SAME_AS",
    "same_address_as":   "SAME_ADDRESS_AS",
}


def init_constraints(neo: Neo4jClient) -> None:
    print("→ Creating ICIJ id constraints ...")
    for stmt in [
        "CREATE CONSTRAINT icij_entity_id   IF NOT EXISTS FOR (e:LegalEntity)   REQUIRE e.icijNodeId IS UNIQUE",
        "CREATE CONSTRAINT icij_person_id   IF NOT EXISTS FOR (p:NaturalPerson) REQUIRE p.icijNodeId IS UNIQUE",
    ]:
        try:
            neo.execute(stmt)
        except Exception as e:
            print(f"  ! {stmt[:60]} — {e}")


def load_csv(neo: Neo4jClient, name: str, src_path: Path, cypher: str, batch: int = 5000) -> int:
    """Stream a CSV through apoc.periodic.iterate (file must be in import/ dir)."""
    if not src_path.exists():
        print(f"  ✗ missing {src_path}")
        return 0
    # copy into Neo4j's import volume so apoc.load.csv can read it
    dst = IMPORT_DIR / f"icij_{src_path.name}"
    if not dst.exists() or dst.stat().st_size != src_path.stat().st_size:
        import shutil
        print(f"  · copying {src_path.name} → import/{dst.name} ({src_path.stat().st_size/1e6:.0f} MB)")
        shutil.copy2(src_path, dst)
    n_rows = sum(1 for _ in dst.open()) - 1
    print(f"  · {name:<30} ({n_rows:>10,} rows)")
    file_uri = f"file:///icij_{src_path.name}"
    neo.execute(f"""
        CALL apoc.periodic.iterate(
          "LOAD CSV WITH HEADERS FROM '{file_uri}' AS row RETURN row",
          "{cypher}",
          {{batchSize: {batch}, parallel: false}})
    """)
    return n_rows


def load_entities(neo: Neo4jClient) -> None:
    print("→ Loading nodes-entities.csv (ICIJ legal entities) ...")
    load_csv(neo, "entities", OL_DIR / "nodes-entities.csv",
        """MERGE (e:LegalEntity {icijNodeId: row.node_id})
             ON CREATE SET e.id = 'ICIJ-' + row.node_id, e.dataSource = 'ICIJ'
           SET e.name = coalesce(e.name, row.name),
               e.alternateName = row.original_name,
               e.formerName = row.former_name,
               e.jurisdiction = coalesce(e.jurisdiction, row.jurisdiction),
               e.jurisdictionName = coalesce(e.jurisdictionName, row.jurisdiction_description),
               e.companyType = row.company_type,
               e.serviceProvider = row.service_provider,
               e.icijSource = row.sourceID,
               e.entityStatus = row.status,
               e.foundingDate = row.incorporation_date,
               e.dissolutionDate = row.inactivation_date,
               e.struckOffDate = row.struck_off_date,
               e.countryCodes = row.country_codes,
               e.riskTier = coalesce(e.riskTier, CASE
                   WHEN row.jurisdiction IN ['BVI','PMA','BAH','BMU','CAY','SAM','SEY','VGB','PAN'] THEN 'high'
                   ELSE 'medium' END),
               e.kycRiskScore = coalesce(e.kycRiskScore, CASE
                   WHEN row.jurisdiction IN ['BVI','PMA','BAH','BMU','CAY','SAM','SEY','VGB','PAN'] THEN 65
                   ELSE 35 END),
               e.hasOperationalAddress = false""")


def load_intermediaries(neo: Neo4jClient) -> None:
    print("→ Loading nodes-intermediaries.csv (offshore service providers) ...")
    load_csv(neo, "intermediaries", OL_DIR / "nodes-intermediaries.csv",
        """MERGE (e:LegalEntity {icijNodeId: row.node_id})
             ON CREATE SET e.id = 'ICIJ-' + row.node_id, e.dataSource = 'ICIJ_INTERMEDIARY'
           SET e:Intermediary,
               e.name = coalesce(e.name, row.name),
               e.entityStatus = row.status,
               e.countryCodes = row.country_codes,
               e.icijSource = row.sourceID,
               e.riskTier = coalesce(e.riskTier, 'high'),
               e.kycRiskScore = coalesce(e.kycRiskScore, 50)""")


def load_others(neo: Neo4jClient) -> None:
    print("→ Loading nodes-others.csv (trusts, foundations) ...")
    load_csv(neo, "others", OL_DIR / "nodes-others.csv",
        """MERGE (e:LegalEntity {icijNodeId: row.node_id})
             ON CREATE SET e.id = 'ICIJ-' + row.node_id, e.dataSource = 'ICIJ'
           SET e.name = coalesce(e.name, row.name),
               e.legalForm = row.type,
               e.foundingDate = row.incorporation_date,
               e.dissolutionDate = row.struck_off_date,
               e.jurisdiction = coalesce(e.jurisdiction, row.jurisdiction),
               e.jurisdictionName = row.jurisdiction_description,
               e.icijSource = row.sourceID,
               e.riskTier = coalesce(e.riskTier, 'high'),
               e.kycRiskScore = coalesce(e.kycRiskScore, 55)""")


def load_officers(neo: Neo4jClient) -> None:
    print("→ Loading nodes-officers.csv (real beneficial owners / officers) ...")
    load_csv(neo, "officers", OL_DIR / "nodes-officers.csv",
        """MERGE (p:NaturalPerson {icijNodeId: row.node_id})
             ON CREATE SET p.id = 'ICIJ-' + row.node_id, p.dataSource = 'ICIJ'
           SET p.name = coalesce(p.name, row.name),
               p.nationality = row.country_codes,
               p.icijSource = row.sourceID,
               p.role = coalesce(p.role, 'officer')""")


def load_relationships(neo: Neo4jClient) -> dict:
    """Pre-process relationships.csv: drop registered_address; bucket by output rel type."""
    src = OL_DIR / "relationships.csv"

    # If split files already exist in import/, skip the splitting step (resume-safe).
    existing = sorted(IMPORT_DIR.glob("icij_rel_*.csv"))
    if existing:
        print(f"→ Re-using {len(existing)} existing icij_rel_*.csv split files in import/ ...")
        counts: dict[str, int] = {}
        for p in existing:
            rt = p.stem.replace("icij_rel_", "")
            counts[rt] = sum(1 for _ in p.open()) - 1
        for k, v in sorted(counts.items(), key=lambda x: -x[1]):
            print(f"    {k:<25} {v:>10,}")
    else:
        print("→ Splitting relationships.csv by rel type → import/icij_rel_*.csv ...")
        bucket_files: dict[str, "csv.writer"] = {}
        handles: list = []
        counts = {}
        skipped = 0

        with src.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rt = row.get("rel_type", "").strip()
                if rt == "registered_address" or not rt:
                    skipped += 1
                    continue
                mapped = REL_TYPE_MAP.get(rt, rt.upper().replace(" ", "_"))
                if mapped not in bucket_files:
                    fp = (IMPORT_DIR / f"icij_rel_{mapped}.csv").open("w", encoding="utf-8", newline="")
                    handles.append(fp)
                    w = csv.writer(fp)
                    w.writerow(["start", "end", "rel_type_raw", "link", "status",
                                "start_date", "end_date", "sourceID"])
                    bucket_files[mapped] = w
                    counts[mapped] = 0
                bucket_files[mapped].writerow([row["node_id_start"], row["node_id_end"],
                                                rt, row.get("link", ""), row.get("status", ""),
                                                row.get("start_date", ""), row.get("end_date", ""),
                                                row.get("sourceID", "")])
                counts[mapped] += 1
        for h in handles:
            h.close()

        print(f"  · skipped {skipped:,} registered_address rels")
        for k, v in sorted(counts.items(), key=lambda x: -x[1]):
            print(f"    {k:<25} {v:>10,}")

    print("→ Loading each bucket into Neo4j ...")
    for rel_type, n in counts.items():
        file_uri = f"file:///icij_rel_{rel_type}.csv"
        if rel_type == "CONTROLLED_BY":
            # person → entity ; flip start/end to (entity)-[:CONTROLLED_BY]->(person)
            cypher = (
                "MATCH (p:NaturalPerson {icijNodeId: row.start}) "
                "MATCH (e:LegalEntity   {icijNodeId: row.end}) "
                "MERGE (e)-[r:CONTROLLED_BY]->(p) "
                "ON CREATE SET r.dataSource = 'ICIJ', r.role = row.rel_type_raw, "
                "  r.startDate = row.start_date, r.endDate = row.end_date, r.sourceID = row.sourceID"
            )
        elif rel_type == "DIRECTLY_OWNED_BY":
            cypher = (
                "MATCH (a:LegalEntity {icijNodeId: row.start}) "
                "MATCH (b:LegalEntity {icijNodeId: row.end}) "
                "MERGE (a)-[r:DIRECTLY_OWNED_BY]->(b) "
                "ON CREATE SET r.dataSource = 'ICIJ', r.relationshipType = row.rel_type_raw, "
                "  r.startDate = row.start_date, r.sourceID = row.sourceID"
            )
        else:
            # Generic — entity↔entity for everything except PROBABLY_SAME_OFFICER_AS.
            # MUST use a label so Neo4j can use the icijNodeId unique-constraint index;
            # an unlabeled MATCH does a 17M-node scan per row → effectively hangs.
            if rel_type == "PROBABLY_SAME_OFFICER_AS":
                left_label = right_label = "NaturalPerson"
            else:
                left_label = right_label = "LegalEntity"
            cypher = (
                f"MATCH (a:{left_label}  {{icijNodeId: row.start}}) "
                f"MATCH (b:{right_label} {{icijNodeId: row.end}}) "
                f"MERGE (a)-[r:{rel_type}]->(b) "
                f"ON CREATE SET r.dataSource = 'ICIJ', r.startDate = row.start_date, "
                f"  r.endDate = row.end_date, r.sourceID = row.sourceID"
            )
        print(f"  · {rel_type:<25} ({n:>10,} rels)")
        neo.execute(f"""
            CALL apoc.periodic.iterate(
              "LOAD CSV WITH HEADERS FROM '{file_uri}' AS row RETURN row",
              "{cypher}",
              {{batchSize: 5000, parallel: false}})
        """)
    return counts


def main() -> int:
    if not OL_DIR.exists():
        print(f"✗ {OL_DIR} missing. Run: unzip 'data/Offshore Leaks Database/full-oldb.LATEST.zip' "
              f"-d data/offshoreleaks_extracted/")
        return 1
    if not neo4j_healthy():
        print("✗ Neo4j is not reachable.")
        return 1

    with Neo4jClient() as neo:
        init_constraints(neo)
        load_entities(neo)
        load_intermediaries(neo)
        load_others(neo)
        load_officers(neo)
        load_relationships(neo)

        print("\n══ DONE ══")
        for q, lbl in [
            ("MATCH (n:LegalEntity)   WHERE n.dataSource STARTS WITH 'ICIJ' RETURN count(n) AS c", "ICIJ :LegalEntity"),
            ("MATCH (n:NaturalPerson) WHERE n.dataSource = 'ICIJ' RETURN count(n) AS c",          "ICIJ :NaturalPerson"),
            ("MATCH (n:Intermediary)  RETURN count(n) AS c",                                       "ICIJ :Intermediary"),
            ("MATCH ()-[r]->() WHERE r.dataSource = 'ICIJ' RETURN count(r) AS c",                  "ICIJ relationships"),
        ]:
            print(f"  {lbl:<25} {neo.query_one(q)['c']:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
