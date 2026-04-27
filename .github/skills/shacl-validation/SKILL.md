---
name: shacl-validation
description: "Use when defining SHACL shapes for data quality; validating Neo4j graphs with n10s.validation.shacl; validating RDF in GraphDB; writing shape constraints (cardinality, datatype, regex, sh:in, closed shapes); debugging shape violations or shapes-not-loading. Covers the property-shape vs node-shape distinction, severity levels, and KYC-specific shapes for LEI, jurisdiction, ownership."
---

# SHACL Validation Skill

## When to use

User asks: "validate my graph", "ensure every entity has a LEI", "enforce ownership percentage is 0–100", "data quality checks", "regulatory data validation", "why is SHACL reporting violations".

## SHACL = "regex for graphs"

A SHACL shape says: *"For every node of type X, property Y must satisfy Z."*

Two shape types:
- **NodeShape** — applies to a node (the "focus node")
- **PropertyShape** — applies to a single property of that node (`sh:path :myProp`)

## Anatomy of a shape

```turtle
kyc:LegalEntityShape
    a sh:NodeShape ;
    sh:targetClass kyc:LegalEntity ;        # what nodes does this apply to
    sh:property [                            # property constraint
        sh:path kyc:leiCode ;
        sh:minCount 1 ;                      # cardinality
        sh:maxCount 1 ;
        sh:datatype xsd:string ;             # type
        sh:pattern "^[A-Z0-9]{20}$" ;        # regex
        sh:severity sh:Violation ;           # Violation | Warning | Info
        sh:message "LEI must be 20 alphanumeric chars" ;
    ] .
```

## Constraint cheat sheet

| Constraint | Meaning | Example |
|---|---|---|
| `sh:minCount N` | At least N values | `sh:minCount 1` (required) |
| `sh:maxCount N` | At most N values | `sh:maxCount 1` (single-valued) |
| `sh:datatype xsd:T` | Value must be of type T | `sh:datatype xsd:dateTime` |
| `sh:nodeKind sh:IRI` | Must be a URI | For object properties |
| `sh:pattern "regex"` | String matches regex | `^[A-Z]{2}$` |
| `sh:in (A B C)` | Must be one of these | `("active" "inactive" "lapsed")` |
| `sh:minInclusive N` | ≥ N | `sh:minInclusive 0` |
| `sh:maxInclusive N` | ≤ N | `sh:maxInclusive 100` |
| `sh:class C` | Object must be of class C | enforces relationship target |
| `sh:hasValue v` | Must include this value | |
| `sh:closed true` | No properties besides those declared | strict mode |
| `sh:or ((shape1) (shape2))` | Disjunction | |
| `sh:not [...]` | Negation | |

## Severity levels

| Level | When to use |
|---|---|
| `sh:Violation` | Data is invalid — must be fixed |
| `sh:Warning` | Concerning but allowed (e.g., missing optional address) |
| `sh:Info` | Heads-up only |

## Validating in Neo4j (via n10s)

```cypher
// Load shapes (one of these three)
CALL n10s.validation.shacl.import.fetch("file:///var/lib/neo4j/import/kyc_shapes.ttl", "Turtle");
CALL n10s.validation.shacl.import.inline('@prefix sh: ... .', "Turtle");

// Run validation — returns one row per violation
CALL n10s.validation.shacl.validate()
YIELD focusNode, nodeType, shapeId, propertyShape, offendingValue,
      resultPath, severity, resultMessage
RETURN focusNode, severity, resultMessage
ORDER BY severity LIMIT 50;

// List loaded shapes
CALL n10s.validation.shacl.listShapes();

// Drop all shapes
CALL n10s.validation.shacl.dropShapes();
```

## Validating in GraphDB

GraphDB Free supports SHACL via the workbench: **Setup → Validations → Shapes**.
Or via REST: `POST /repositories/{repo}/shacl` with the shapes as Turtle.

GraphDB will then validate any new data being loaded against the shapes (transactions are rejected on violation).

## KYC SHACL pattern library

| Constraint | Shape pattern |
|---|---|
| LEI must be 20 alphanumerics | `sh:pattern "^[A-Z0-9]{20}$"` |
| Country must be ISO 3166-1 alpha-2 | `sh:pattern "^[A-Z]{2}$"` |
| Ownership % between 0 and 100 | `sh:minInclusive 0 ; sh:maxInclusive 100` |
| Status in fixed list | `sh:in ("ACTIVE" "INACTIVE" "LAPSED")` |
| Every entity must have ≥1 jurisdiction | `sh:path kyc:hasJurisdiction ; sh:minCount 1` |
| ISIN format | `sh:pattern "^[A-Z]{2}[A-Z0-9]{10}$"` |
| Sanctioned entities require source citation | `sh:path kyc:sanctionSource ; sh:minCount 1 ; sh:nodeKind sh:IRI` |

## Common pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| 0 violations even with bad data | Shape's `sh:targetClass` doesn't match Neo4j label | Check that n10s mapped your class to a label (often `kyc__LegalEntity`) |
| Every node violates | `sh:closed true` rejects unknown props | Use `sh:ignoredProperties (rdf:type ...)` or remove `sh:closed` |
| `n10s.validation.shacl.validate` errors | Shapes not loaded | Run `listShapes` first to verify |
| Pattern doesn't match | Anchors needed | Use `^...$` for full-string match |
| Shapes ignore inferred triples | n10s validates stored data only | For inference, validate in GraphDB instead |

## Reference

- `shacl/kyc_shapes.ttl` — full set of KYC shapes
- `scripts/10_shacl_validate.py` — load + validate workflow
