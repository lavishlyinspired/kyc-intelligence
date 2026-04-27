---
name: synthetic-kyc-data
description: "Use when generating synthetic KYC test datasets with embedded financial-crime patterns — sanctioned UBOs hidden behind shell companies, circular ownership rings, transaction structuring, PEP links, high-risk jurisdictions. Covers reproducible seeding, Faker usage, and the 'ground truth' pattern for testing whether your detection queries actually find what they should."
---

# Synthetic KYC Data Generation Skill

## When to use

User asks: "generate test data", "I need a dataset to validate the queries", "create entities with hidden sanctioned UBOs", "make a graph with circular ownership for SCC testing".

## Why synthetic data with PLANTED crimes?

Real KYC data is restricted (PII, GDPR, regulatory). Synthetic data lets you:
1. **Test detection queries** — you know the planted patterns; if your Cypher doesn't find them, the Cypher is wrong.
2. **Demo without compliance issues** — share, screenshot, blog freely.
3. **Reproduce bugs** — `random.seed(42)` makes the dataset bit-exact.

## Required ingredients

```
500 LegalEntity nodes
  ├── 60% in low-risk jurisdictions (US, GB, DE, JP)
  ├── 20% in medium-risk (SG, CH)
  └── 20% in high-risk (KY, VG, PA, SC)

200 NaturalPerson nodes
  ├── 3 SANCTIONED  (planted ground truth)
  ├── 10 PEPs       (planted ground truth)
  └── rest: clean

Ownership chains:
  ├── Normal hierarchies (60% of entities have a parent)
  └── PLANTED CRIMES:
      ├── 5 chains end in a sanctioned UBO 3 hops deep
      └── 5 circular ownership rings (A→B→C→A)

1000 Transactions
  └── 30% are STRUCTURED (amount in $9000–$9999 range)
```

## Reproducibility checklist

```python
random.seed(42)
fake = Faker(["en_US", "en_GB", "de_DE"])
Faker.seed(42)
```

Always set BOTH `random.seed` AND `Faker.seed`. Otherwise faker output drifts.

## "Ground truth" pattern

Mark planted crimes explicitly so tests can assert on them:

```python
# In the generated entities
sanctioned_persons = persons[:3]   # IDs PERSON_0000, PERSON_0001, PERSON_0002

# In tests
def test_finds_all_planted_sanctioned_ubos():
    results = run_cypher("MATCH ...UBO query...")
    found_ubos = {r["ubo_id"] for r in results}
    assert "PERSON_0000" in found_ubos, "Failed to find planted sanctioned UBO #1"
```

## Pattern: planted sanctioned UBO chain

```python
# entity → shell1 → shell2 → sanctioned_person  (3 hops)
sanctioned = persons[0]  # known
for victim_entity in random.sample(entities[100:150], 5):
    shell1 = random.choice(entities[300:350])
    shell2 = random.choice(entities[350:400])
    rels.extend([
        {"from": victim_entity["id"], "to": shell1["id"], "type": "DIRECTLY_OWNED_BY", "percentage": 100.0},
        {"from": shell1["id"], "to": shell2["id"], "type": "DIRECTLY_OWNED_BY", "percentage": 100.0},
        {"from": shell2["id"], "to": sanctioned["id"], "type": "CONTROLLED_BY", "role": "UBO"},
    ])
```

## Pattern: circular ownership ring

```python
# A → B → C → A
ring = random.sample(entities[400:450], 3)
for i in range(3):
    rels.append({
        "from": ring[i]["id"],
        "to": ring[(i+1) % 3]["id"],
        "type": "DIRECTLY_OWNED_BY",
        "percentage": 51.0,
    })
```

## Pattern: transaction structuring

```python
# Amount distribution biased toward $9k-$10k range
amount = random.choice([
    random.uniform(9000, 9999),       # STRUCTURED
    random.uniform(100, 5000),         # NORMAL small
    random.uniform(100000, 5000000),   # NORMAL large
])
txn["is_suspicious"] = 9000 < amount < 10000
```

## Output schema

```json
{
  "entities":      [{"id", "lei", "name", "jurisdiction", "risk_tier", ...}],
  "persons":       [{"id", "name", "nationality", "is_pep", "is_sanctioned"}],
  "relationships": [{"from", "to", "type", "percentage", "since", "role"}],
  "transactions":  [{"id", "from_entity", "to_entity", "amount", "currency", "date", "is_suspicious"}],
  "ground_truth": {
    "sanctioned_person_ids": ["PERSON_0000", "PERSON_0001", "PERSON_0002"],
    "pep_person_ids":        ["PERSON_0003", ..., "PERSON_0009"],
    "ring_entity_ids":       [["ENTITY_0401", "ENTITY_0402", "ENTITY_0403"], ...],
    "sanctioned_chain_starts": ["ENTITY_0123", ...]
  }
}
```

The `ground_truth` block is the killer feature — your tests can assert exactly what should be found.

## Reference

- `scripts/06_generate_synthetic_data.py` — implementation with ground truth output
- `tests/test_detection.py` — assertions against the ground truth
