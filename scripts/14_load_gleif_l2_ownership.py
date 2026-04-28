"""
14_load_gleif_l2_ownership.py — Real GLEIF Level-2 ownership relationships.

Strategy (NO synthetic data, NO LLM, NO hard-coded LEIs):

  1. Take a list of well-known financial / corporate names that are KYC-relevant.
  2. Resolve each name to a LEI via the public GLEIF search API
     `/lei-records?filter[entity.legalName]=...`.
  3. For each resolved parent LEI, fetch /direct-children (real Level-2 GLEIF
     reference data) and MERGE every child as a :LegalEntity aligned to FIBO
     LegalPerson.
  4. For each child create (child)-[:DIRECTLY_OWNED_BY]->(parent) — a real
     GLEIF-published ownership relationship.

Source: https://api.gleif.org/api/v1   (public, no auth required)
"""
from __future__ import annotations
import os, sys, time, json, requests, urllib.parse
from pathlib import Path
from neo4j import GraphDatabase
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]   # kyc-intelligence/
load_dotenv(ROOT.parent.parent / ".env")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "kycpassword123")

GLEIF_BASE = "https://api.gleif.org/api/v1"
HEADERS = {"Accept": "application/vnd.api+json"}

PARENT_NAMES = [
    "Apple Inc.",
    "Microsoft Corporation",
    "JPMorgan Chase & Co.",
    "Goldman Sachs Group, Inc.",
    "BlackRock, Inc.",
    "Berkshire Hathaway Inc.",
    "Deutsche Bank Aktiengesellschaft",
    "HSBC Holdings plc",
    "Citigroup Inc.",
    "Bank of America Corporation",
    "Wells Fargo & Company",
    "UBS Group AG",
    "Credit Suisse Group AG",
    "Morgan Stanley",
    "Banco Santander, S.A.",
    "Société Générale S.A.",
    "Barclays PLC",
    "BNP Paribas",
    "ING Groep N.V.",
    "Mitsubishi UFJ Financial Group, Inc.",
]


def search_lei(name: str) -> str | None:
    q = urllib.parse.quote(name)
    url = f"{GLEIF_BASE}/lei-records?filter[entity.legalName]={q}&page[size]=1"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return None
        data = r.json().get("data") or []
        return data[0]["id"] if data else None
    except Exception:
        return None


def fetch_record(lei: str) -> dict | None:
    try:
        r = requests.get(f"{GLEIF_BASE}/lei-records/{lei}", headers=HEADERS, timeout=15)
        if r.status_code == 200:
            return r.json().get("data")
        return None
    except Exception:
        return None


def fetch_children(lei: str, page_size: int = 100) -> list[dict]:
    out, page = [], 1
    while True:
        url = (f"{GLEIF_BASE}/lei-records/{lei}/direct-children"
               f"?page[size]={page_size}&page[number]={page}")
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
        except Exception:
            break
        if r.status_code != 200:
            break
        body = r.json()
        out.extend(body.get("data") or [])
        meta = (body.get("meta") or {}).get("pagination") or {}
        if page >= int(meta.get("lastPage", 1)):
            break
        page += 1
        time.sleep(0.05)
    return out


def upsert_legal_entity(session, attrs: dict, source: str) -> str:
    ent = attrs.get("entity") or {}
    lei = attrs["lei"]
    name = (ent.get("legalName") or {}).get("name") or lei
    juris = ent.get("jurisdiction")
    cat = ent.get("category", "GENERAL")
    legal_form = (ent.get("legalForm") or {}).get("other")
    status = ent.get("status", "ACTIVE")
    hq = ent.get("headquartersAddress") or {}
    legal_addr = ent.get("legalAddress") or {}

    session.run("""
        MERGE (e:LegalEntity {id: $lei})
        ON CREATE SET e.dataSource = $source
        SET e.lei              = $lei,
            e.name             = coalesce(e.name, $name),
            e.jurisdiction     = coalesce(e.jurisdiction, $jur),
            e.jurisdictionName = coalesce(e.jurisdictionName, $jur),
            e.category         = coalesce(e.category, $cat),
            e.legalForm        = coalesce(e.legalForm, $legal_form),
            e.entityStatus     = coalesce(e.entityStatus, $status),
            e.isActive         = ($status = 'ACTIVE'),
            e.hqCity           = coalesce(e.hqCity, $hq_city),
            e.hqCountry        = coalesce(e.hqCountry, $hq_country),
            e.city             = coalesce(e.city,    $addr_city),
            e.country          = coalesce(e.country, $addr_country),
            e.uri              = coalesce(e.uri, 'https://www.gleif.org/lei/' + $lei),
            e.kycRiskScore     = coalesce(e.kycRiskScore, 30),
            e.riskTier         = coalesce(e.riskTier, 'medium'),
            e.hasOperationalAddress = coalesce(e.hasOperationalAddress, true)
        WITH e
        OPTIONAL MATCH (fibo:Class)
        WHERE fibo.uri ENDS WITH '/LegalPerson'
        FOREACH (_ IN CASE WHEN fibo IS NULL THEN [] ELSE [1] END |
            MERGE (e)-[:INSTANCE_OF]->(fibo))
    """, {
        "lei": lei, "name": name, "jur": juris,
        "cat": cat, "legal_form": legal_form, "status": status,
        "hq_city": hq.get("city"), "hq_country": hq.get("country"),
        "addr_city": legal_addr.get("city"), "addr_country": legal_addr.get("country"),
        "source": source,
    })
    return lei


def link_owned_by(session, child_lei: str, parent_lei: str):
    session.run("""
        MATCH (c:LegalEntity {id: $cid})
        MATCH (p:LegalEntity {id: $pid})
        MERGE (c)-[r:DIRECTLY_OWNED_BY]->(p)
        ON CREATE SET r.dataSource = 'GLEIF_L2',
                      r.relationshipType = 'IS_DIRECTLY_CONSOLIDATED_BY'
    """, {"cid": child_lei, "pid": parent_lei})


def main():
    print(f"→ Resolving {len(PARENT_NAMES)} parent names to LEIs via GLEIF...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    counts = {"parents_resolved": 0, "parents_upserted": 0,
              "children_total": 0, "children_upserted": 0, "rels_created": 0}

    with driver.session() as session:
        for name in PARENT_NAMES:
            parent_lei = search_lei(name)
            if not parent_lei:
                print(f"  ✗ no LEI for '{name}'"); continue
            counts["parents_resolved"] += 1
            print(f"  ✓ {name} → {parent_lei}")

            prec = fetch_record(parent_lei)
            if prec:
                upsert_legal_entity(session, prec["attributes"], "GLEIF_L2_parent")
                counts["parents_upserted"] += 1

            children = fetch_children(parent_lei)
            print(f"     → {len(children)} direct children")
            counts["children_total"] += len(children)
            for ch in children:
                child_lei = ch["id"]
                upsert_legal_entity(session, ch["attributes"], "GLEIF_L2_child")
                counts["children_upserted"] += 1
                link_owned_by(session, child_lei, parent_lei)
                counts["rels_created"] += 1

    driver.close()
    print("\n══ DONE ══")
    for k, v in counts.items():
        print(f"  {k:25s}: {v}")


if __name__ == "__main__":
    main()
