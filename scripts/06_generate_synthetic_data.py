"""
Script 06 — Generate synthetic KYC dataset with PLANTED financial-crime patterns.

Outputs `data/synthetic/kyc_dataset.json` with a `ground_truth` block so tests
can assert exactly what should be detected.

Skill applied: synthetic-kyc-data

    python scripts/06_generate_synthetic_data.py
"""
from __future__ import annotations

import json
import random
import string
import sys
from pathlib import Path

from faker import Faker

random.seed(42)
fake = Faker(["en_US", "en_GB", "de_DE"])
Faker.seed(42)

OUT = Path("data/synthetic/kyc_dataset.json")

JURISDICTIONS = [
    ("US", "United States",         "low"),
    ("GB", "United Kingdom",        "low"),
    ("DE", "Germany",               "low"),
    ("JP", "Japan",                 "low"),
    ("SG", "Singapore",             "medium"),
    ("CH", "Switzerland",           "medium"),
    ("KY", "Cayman Islands",        "high"),
    ("VG", "British Virgin Islands","high"),
    ("PA", "Panama",                "high"),
    ("SC", "Seychelles",            "high"),
]

ENTITY_CATEGORIES = ["BRANCH", "FUND", "TRUST", "PARTNERSHIP", "LIMITED_PARTNERSHIP", "CORPORATION"]
ROLES = ["Director", "CEO", "Shareholder", "Nominee", "Trustee"]


def gen_lei() -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=20))


def gen_isin(country: str = "US") -> str:
    return country + "".join(random.choices(string.ascii_uppercase + string.digits, k=10))


def generate_entities(n: int = 500) -> list[dict]:
    out = []
    for i in range(n):
        juris = random.choice(JURISDICTIONS)
        out.append({
            "id": f"ENTITY_{i:04d}",
            "lei": gen_lei(),
            "name": fake.company(),
            "jurisdiction": juris[0],
            "jurisdiction_name": juris[1],
            "risk_tier": juris[2],
            "category": random.choice(ENTITY_CATEGORIES),
            "incorporated_date": fake.date_between(start_date="-30y", end_date="-1y").isoformat(),
            "is_active": random.random() > 0.1,
            "has_operational_address": random.random() > 0.3,
            "isin": gen_isin(juris[0]) if random.random() > 0.7 else None,
        })
    return out


def generate_persons(n: int = 200) -> list[dict]:
    out = []
    for i in range(n):
        out.append({
            "id": f"PERSON_{i:04d}",
            "name": fake.name(),
            "nationality": random.choice([j[0] for j in JURISDICTIONS]),
            "dob": fake.date_of_birth(minimum_age=30, maximum_age=80).isoformat(),
            "is_pep": i < 10,        # first 10 are PEPs
            "is_sanctioned": i < 3,   # first 3 are sanctioned
        })
    return out


def generate_relationships(entities: list[dict], persons: list[dict]) -> tuple[list[dict], dict]:
    rels: list[dict] = []
    ground_truth = {
        "sanctioned_person_ids": [p["id"] for p in persons if p["is_sanctioned"]],
        "pep_person_ids":        [p["id"] for p in persons if p["is_pep"]],
        "sanctioned_chain_starts": [],   # entities that lead to a sanctioned UBO
        "ring_entity_ids":         [],   # circular ownership rings
    }

    # ── Normal corporate hierarchy ────────────────────────────────────────────
    for i, entity in enumerate(entities[:300]):
        if i > 20 and random.random() > 0.4:
            parent = random.choice(entities[:i])
            rels.append({
                "from": entity["id"], "to": parent["id"], "type": "DIRECTLY_OWNED_BY",
                "percentage": round(random.uniform(50, 100), 2),
                "since": fake.date_between(start_date="-10y", end_date="-1y").isoformat(),
                "role": None,
            })
        if random.random() > 0.5:
            person = random.choice(persons[10:])  # skip planted bad actors
            rels.append({
                "from": entity["id"], "to": person["id"], "type": "CONTROLLED_BY",
                "percentage": None,
                "since": fake.date_between(start_date="-10y", end_date="-1y").isoformat(),
                "role": random.choice(ROLES),
            })

    # ── PLANTED CRIME 1: sanctioned UBO behind 2 shells (3 hops total) ────────
    sanctioned = persons[0]   # PERSON_0000
    for victim in random.sample(entities[100:150], 5):
        shell1 = random.choice(entities[300:350])
        shell2 = random.choice(entities[350:400])
        ground_truth["sanctioned_chain_starts"].append(victim["id"])
        rels.extend([
            {"from": victim["id"],  "to": shell1["id"],     "type": "DIRECTLY_OWNED_BY",
             "percentage": 100.0, "since": "2018-01-01", "role": None},
            {"from": shell1["id"],  "to": shell2["id"],     "type": "DIRECTLY_OWNED_BY",
             "percentage": 100.0, "since": "2018-01-01", "role": None},
            {"from": shell2["id"],  "to": sanctioned["id"], "type": "CONTROLLED_BY",
             "percentage": None,  "since": "2018-01-01", "role": "Ultimate Beneficial Owner"},
        ])

    # ── PLANTED CRIME 2: circular ownership rings (A→B→C→A) ─────────────────
    used = set()
    for _ in range(5):
        candidates = [e for e in entities[400:450] if e["id"] not in used]
        if len(candidates) < 3:
            break
        ring = random.sample(candidates, 3)
        for e in ring:
            used.add(e["id"])
        ground_truth["ring_entity_ids"].append([e["id"] for e in ring])
        for i in range(3):
            rels.append({
                "from": ring[i]["id"], "to": ring[(i + 1) % 3]["id"],
                "type": "DIRECTLY_OWNED_BY",
                "percentage": 51.0, "since": "2020-01-01", "role": None,
            })

    return rels, ground_truth


def generate_transactions(entities: list[dict], n: int = 1000) -> list[dict]:
    out = []
    ids = [e["id"] for e in entities]
    for i in range(n):
        # Bias toward structuring patterns
        amount = random.choice([
            random.uniform(9000, 9999),       # STRUCTURED
            random.uniform(100, 5000),         # normal small
            random.uniform(100000, 5000000),   # normal large
        ])
        out.append({
            "id": f"TXN_{i:05d}",
            "from_entity": random.choice(ids),
            "to_entity":   random.choice(ids),
            "amount":      round(amount, 2),
            "currency":    random.choice(["USD", "EUR", "GBP", "CHF"]),
            "date":        fake.date_between(start_date="-2y", end_date="today").isoformat(),
            "is_suspicious": 9000 < amount < 10000,
        })
    return out


def main() -> int:
    OUT.parent.mkdir(parents=True, exist_ok=True)

    entities = generate_entities(500)
    persons  = generate_persons(200)
    rels, ground_truth = generate_relationships(entities, persons)
    txns = generate_transactions(entities, 1000)

    dataset = {
        "entities":      entities,
        "persons":       persons,
        "relationships": rels,
        "transactions":  txns,
        "ground_truth":  ground_truth,
    }
    OUT.write_text(json.dumps(dataset, indent=2))

    print(f"Generated KYC dataset → {OUT}")
    print(f"  • {len(entities)} legal entities")
    print(f"  • {len(persons)} natural persons")
    print(f"      sanctioned: {len(ground_truth['sanctioned_person_ids'])}  "
          f"PEPs: {len(ground_truth['pep_person_ids'])}")
    print(f"  • {len(rels)} ownership/control relationships")
    print(f"      planted sanctioned-UBO chains: {len(ground_truth['sanctioned_chain_starts'])}")
    print(f"      planted circular rings:        {len(ground_truth['ring_entity_ids'])}")
    print(f"  • {len(txns)} transactions  "
          f"({sum(1 for t in txns if t['is_suspicious'])} suspicious)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
