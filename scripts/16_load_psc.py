"""
Script 16 — Load REAL UK PSC Register (BODS v0.4 CSV format) into Neo4j.

Source: https://bods-data.openownership.org/ -> "UK People with Significant
Control (PSC) Register" (CC0, free, ~3.6 GB compressed).

What this loads
---------------
Every UK company that has filed at least one PSC declaration:
  * :LegalEntity  (uri=GB-COH-{companyNumber}, name, jurisdiction='GB')
  * :NaturalPerson (uri=GB-COH-PER-..., fullName, nationality, dob, role='UBO')
  * (:LegalEntity)<-[:CONTROLLED_BY]-(:NaturalPerson)
       with: interestType (shareholding/votingRights/appointmentOfBoard),
             sharePctMin / sharePctMax (e.g. 25-50, 50-75, 75-100),
             startDate, dataSource='UK_PSC'

Skips
-----
  * recordStatus='closed' / 'isComponent=true' (intermediate statements)
  * Addresses & nationalities tables (use names file only \u2014 saves ~50M rows)

Performance
-----------
Uses LOAD CSV via file:/// + apoc.periodic.iterate so 14M relationships finish
in ~10-20 min on an SSD.

    python scripts/16_load_psc.py
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.kg_client import Neo4jClient, neo4j_healthy

PSC_DIR = Path("data/BODS/UK People with Significant Control (PSC) Register/csv")
# Write compact CSVs into the Neo4j import volume so apoc.load.csv
# can access them via file:///psc_entities.csv inside the container.
IMPORT_DIR = Path("import")
IMPORT_DIR.mkdir(parents=True, exist_ok=True)
WORK_DIR = IMPORT_DIR


# ─────────────────────────────────────────────────────────────────────────────
# Step 1 – flatten the multi-table BODS layout into 3 compact CSVs we can load
# ─────────────────────────────────────────────────────────────────────────────
def flatten_psc() -> dict:
    """Produce three lean CSV files under data/psc_compact/:
        entities.csv    : recordId, name, jurisdiction, foundingDate, status
        persons.csv     : recordId, fullName, nationality, birthYearMonth, status
        relationships.csv: subjectId, interestedPartyId, type, shareMin, shareMax, startDate
    """
    print("→ Flattening PSC BODS CSVs ...")

    # 1a. Persons: join person_statement + person_recordDetails_names + nationalities
    print("  · indexing person names ...")
    name_by_link: dict[str, str] = {}
    with (PSC_DIR / "person_recordDetails_names.csv").open("r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            link = row["_link_person_statement"]
            t = row.get("type", "")
            if t == "legal" and link not in name_by_link:
                name_by_link[link] = row.get("fullName", "").strip()
    print(f"    → {len(name_by_link):,} legal names")

    print("  · indexing nationalities ...")
    nat_by_link: dict[str, str] = {}
    with (PSC_DIR / "person_recordDetails_nationalities.csv").open("r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            link = row["_link_person_statement"]
            if link not in nat_by_link:
                nat_by_link[link] = row.get("code", "").strip().upper()
    print(f"    → {len(nat_by_link):,} nationalities")

    print("  · writing psc_persons.csv ...")
    persons_out = WORK_DIR / "psc_persons.csv"
    n_persons = 0
    with (PSC_DIR / "person_statement.csv").open("r", encoding="utf-8") as f, \
         persons_out.open("w", encoding="utf-8", newline="") as out:
        reader = csv.DictReader(f)
        writer = csv.writer(out)
        writer.writerow(["recordId", "fullName", "nationality", "birthYearMonth", "status"])
        for row in reader:
            if row.get("recordStatus") == "closed":
                continue
            if row.get("recordDetails_isComponent", "").lower() == "true":
                continue
            link = row["_link"]
            rid = row["recordId"]
            fn = name_by_link.get(link, "").strip()
            if not fn:
                continue
            nat = nat_by_link.get(link, "")
            dob = row.get("recordDetails_birthDate", "")
            writer.writerow([rid, fn, nat, dob, row.get("recordStatus", "")])
            n_persons += 1
    print(f"    → {n_persons:,} persons written")
    name_by_link.clear(); nat_by_link.clear()

    # 1b. Entities
    print("  · writing psc_entities.csv ...")
    entities_out = WORK_DIR / "psc_entities.csv"
    n_ents = 0
    with (PSC_DIR / "entity_statement.csv").open("r", encoding="utf-8") as f, \
         entities_out.open("w", encoding="utf-8", newline="") as out:
        reader = csv.DictReader(f)
        writer = csv.writer(out)
        writer.writerow(["recordId", "name", "jurisdiction", "foundingDate", "status"])
        for row in reader:
            if row.get("recordStatus") == "closed":
                continue
            if row.get("recordDetails_isComponent", "").lower() == "true":
                continue
            rid = row["recordId"]
            name = row.get("recordDetails_name", "").strip()
            if not name:
                continue
            jur = row.get("recordDetails_jurisdiction_code", "GB")
            fd = row.get("recordDetails_foundingDate", "")
            writer.writerow([rid, name, jur, fd, row.get("recordStatus", "")])
            n_ents += 1
    print(f"    → {n_ents:,} entities written")

    # 1c. Relationships: join relationship_statement + interests
    print("  · indexing interests ...")
    interests_by_link: dict[str, list[dict]] = {}
    with (PSC_DIR / "relationship_recordDetails_interests.csv").open("r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            link = row["_link_relationship_statement"]
            interests_by_link.setdefault(link, []).append({
                "type": row.get("type", ""),
                "shareMin": row.get("share_minimum", ""),
                "shareMax": row.get("share_maximum", ""),
                "startDate": row.get("startDate", ""),
            })
    print(f"    → {len(interests_by_link):,} relationships have interests")

    print("  · writing psc_relationships.csv ...")
    rels_out = WORK_DIR / "psc_relationships.csv"
    n_rels = 0
    with (PSC_DIR / "relationship_statement.csv").open("r", encoding="utf-8") as f, \
         rels_out.open("w", encoding="utf-8", newline="") as out:
        reader = csv.DictReader(f)
        writer = csv.writer(out)
        writer.writerow(["companyId", "personId", "interestTypes",
                         "shareMin", "shareMax", "startDate"])
        for row in reader:
            if row.get("recordStatus") == "closed":
                continue
            if row.get("recordDetails_isComponent", "").lower() == "true":
                continue
            subj = row.get("recordDetails_subject", "")
            party = row.get("recordDetails_interestedParty", "")
            if not subj or not party:
                continue
            link = row["_link"]
            its = interests_by_link.get(link, [])
            types = ";".join(sorted({i["type"] for i in its if i["type"]})) or "unknown"
            # take widest band across all interests
            mins = [int(i["shareMin"]) for i in its if i["shareMin"].isdigit()]
            maxs = [int(i["shareMax"]) for i in its if i["shareMax"].isdigit()]
            sd = next((i["startDate"] for i in its if i["startDate"]), "")
            writer.writerow([subj, party, types,
                             min(mins) if mins else "",
                             max(maxs) if maxs else "",
                             sd])
            n_rels += 1
    print(f"    → {n_rels:,} relationships written")
    interests_by_link.clear()

    return {"entities": entities_out, "persons": persons_out, "rels": rels_out,
            "n_ents": n_ents, "n_persons": n_persons, "n_rels": n_rels}


# ─────────────────────────────────────────────────────────────────────────────
# Step 2 – ingest into Neo4j via apoc.load.csv + periodic.iterate
# ─────────────────────────────────────────────────────────────────────────────
def load_into_neo4j(neo: Neo4jClient, files: dict) -> None:
    print("→ Loading into Neo4j (apoc.periodic.iterate, batches of 5000) ...")

    # Constraints to make MERGEs O(1)
    for stmt in [
        "CREATE CONSTRAINT psc_entity_uri IF NOT EXISTS FOR (e:LegalEntity) REQUIRE e.uri IS UNIQUE",
        "CREATE CONSTRAINT psc_person_uri IF NOT EXISTS FOR (p:NaturalPerson) REQUIRE p.uri IS UNIQUE",
    ]:
        try:
            neo.execute(stmt)
        except Exception as e:
            print(f"  ! {stmt[:60]} — {e}")

    abs_ents = "file:///psc_entities.csv"
    abs_pers = "file:///psc_persons.csv"
    abs_rels = "file:///psc_relationships.csv"

    # ---- Entities ----
    print(f"  · loading {files['n_ents']:,} :LegalEntity nodes ...")
    neo.execute(f"""
        CALL apoc.periodic.iterate(
          "LOAD CSV WITH HEADERS FROM '{abs_ents}' AS row RETURN row",
          "MERGE (e:LegalEntity {{uri: row.recordId}})
             ON CREATE SET e.id = row.recordId, e.dataSource = 'UK_PSC'
           SET e.name = coalesce(e.name, row.name),
               e.jurisdiction = coalesce(e.jurisdiction, row.jurisdiction),
               e.jurisdictionName = coalesce(e.jurisdictionName, 'United Kingdom'),
               e.foundingDate = coalesce(e.foundingDate, row.foundingDate),
               e.entityStatus = row.status,
               e.riskTier = coalesce(e.riskTier, 'low'),
               e.kycRiskScore = coalesce(e.kycRiskScore, 15),
               e.hasOperationalAddress = true,
               e.isActive = (row.status <> 'closed')",
          {{batchSize: 5000, parallel: false}})
    """)

    # ---- Persons ----
    print(f"  · loading {files['n_persons']:,} :NaturalPerson nodes ...")
    neo.execute(f"""
        CALL apoc.periodic.iterate(
          "LOAD CSV WITH HEADERS FROM '{abs_pers}' AS row RETURN row",
          "MERGE (p:NaturalPerson {{uri: row.recordId}})
             ON CREATE SET p.id = row.recordId, p.dataSource = 'UK_PSC'
           SET p.name = row.fullName,
               p.nationality = row.nationality,
               p.dob = row.birthYearMonth,
               p.role = 'UBO'",
          {{batchSize: 5000, parallel: false}})
    """)

    # ---- CONTROLLED_BY relationships ----
    print(f"  · loading {files['n_rels']:,} :CONTROLLED_BY relationships ...")
    neo.execute(f"""
        CALL apoc.periodic.iterate(
          "LOAD CSV WITH HEADERS FROM '{abs_rels}' AS row RETURN row",
          "MATCH (e:LegalEntity {{uri: row.companyId}})
           MATCH (p:NaturalPerson {{uri: row.personId}})
           MERGE (e)-[r:CONTROLLED_BY]->(p)
             ON CREATE SET r.dataSource = 'UK_PSC'
           SET r.interestTypes = row.interestTypes,
               r.sharePctMin = toInteger(row.shareMin),
               r.sharePctMax = toInteger(row.shareMax),
               r.startDate = row.startDate",
          {{batchSize: 5000, parallel: false}})
    """)

    # Verification
    n_e  = neo.query_one("MATCH (e:LegalEntity)   WHERE e.dataSource='UK_PSC' RETURN count(e) AS c")["c"]
    n_p  = neo.query_one("MATCH (p:NaturalPerson) WHERE p.dataSource='UK_PSC' RETURN count(p) AS c")["c"]
    n_r  = neo.query_one("MATCH ()-[r:CONTROLLED_BY]-() WHERE r.dataSource='UK_PSC' RETURN count(r) AS c")["c"]
    print(f"\nVerification — UK_PSC :LegalEntity={n_e:,}  :NaturalPerson={n_p:,}  :CONTROLLED_BY={n_r:,}")


def main() -> int:
    if not (PSC_DIR.exists()):
        print(f"✗ {PSC_DIR} missing. Download UK PSC BODS v0.4 from "
              "https://bods-data.openownership.org/ and unzip into data/BODS/")
        return 1
    if not neo4j_healthy():
        print("✗ Neo4j is not reachable.")
        return 1

    # Re-flatten only if compact files don't exist
    if all((WORK_DIR / n).exists() for n in ("psc_entities.csv", "psc_persons.csv", "psc_relationships.csv")):
        print("→ Re-using existing compact CSVs in import/")
        files = {
            "entities":  WORK_DIR / "psc_entities.csv",
            "persons":   WORK_DIR / "psc_persons.csv",
            "rels":      WORK_DIR / "psc_relationships.csv",
            "n_ents":    sum(1 for _ in (WORK_DIR / "psc_entities.csv").open()) - 1,
            "n_persons": sum(1 for _ in (WORK_DIR / "psc_persons.csv").open()) - 1,
            "n_rels":    sum(1 for _ in (WORK_DIR / "psc_relationships.csv").open()) - 1,
        }
    else:
        files = flatten_psc()

    with Neo4jClient() as neo:
        load_into_neo4j(neo, files)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
