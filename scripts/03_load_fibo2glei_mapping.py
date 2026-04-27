"""
Script 03 — Load the FIBO ↔ GLEIF mapping ontology + the KYC application ontology.

Why
---
- FIBO defines `fibo-be:LegalPerson` (the SCHEMA).
- GLEIF data uses `lei:RegisteredEntity` (different vocabulary for the same thing).
- Without a mapping, queries can't join them.

This script declares `owl:equivalentClass` mappings AND defines our application-
specific `kyc:` vocabulary used by the synthetic data and SHACL shapes.

Skill applied: load-fibo-ontology
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.kg_client import GraphDBClient

FIBO2GLEI_TTL = """\
@prefix owl:     <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:    <http://www.w3.org/2000/01/rdf-schema#> .
@prefix rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix skos:    <http://www.w3.org/2004/02/skos/core#> .
@prefix fibo-be: <https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/> .
@prefix fibo-fnd: <https://spec.edmcouncil.org/fibo/ontology/FND/Relations/Relations/> .
@prefix lei:     <https://www.gleif.org/ontology/L1/> .
@prefix f2g:     <http://kyc-kg.example.org/fibo2glei#> .

<http://kyc-kg.example.org/fibo2glei>
    a owl:Ontology ;
    rdfs:label "FIBO ↔ GLEIF Mapping Ontology" ;
    rdfs:comment "Maps FIBO classes/properties to GLEIF L1 vocabulary so KYC queries can join the two datasets." .

# ── Class mappings ────────────────────────────────────────────────────────────
fibo-be:LegalPerson owl:equivalentClass lei:RegisteredEntity .

# ── Property mappings ─────────────────────────────────────────────────────────
lei:legalName rdfs:subPropertyOf fibo-fnd:hasName .

# ── Soft links (for SKOS-aware tools) ─────────────────────────────────────────
fibo-be:LegalPerson skos:closeMatch lei:RegisteredEntity .
"""

KYC_ONTOLOGY_TTL = """\
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .
@prefix kyc:  <http://kyc-kg.example.org/ontology#> .
@prefix fibo-be: <https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/> .

<http://kyc-kg.example.org/ontology>
    a owl:Ontology ;
    rdfs:label "KYC Application Ontology" ;
    rdfs:comment "Application-level vocabulary used by synthetic data, SHACL shapes, and the GraphRAG agent." .

# ── Classes ───────────────────────────────────────────────────────────────────
kyc:LegalEntity        a owl:Class ; rdfs:label "Legal Entity" ; rdfs:subClassOf fibo-be:LegalPerson .
kyc:NaturalPerson      a owl:Class ; rdfs:label "Natural Person" .
kyc:Address            a owl:Class ; rdfs:label "Address" .
kyc:Jurisdiction       a owl:Class ; rdfs:label "Jurisdiction" .
kyc:SanctionEntry      a owl:Class ; rdfs:label "Sanction List Entry" .
kyc:PEPEntry           a owl:Class ; rdfs:label "Politically Exposed Person Entry" .
kyc:RegisteredLegalEntity a owl:Class ; rdfs:subClassOf kyc:LegalEntity .

# ── Datatype properties ───────────────────────────────────────────────────────
kyc:leiCode      a owl:DatatypeProperty ; rdfs:domain kyc:LegalEntity ; rdfs:range xsd:string ;
                 rdfs:label "LEI Code" .
kyc:legalName    a owl:DatatypeProperty ; rdfs:range xsd:string ; rdfs:label "Legal Name" .
kyc:entityStatus a owl:DatatypeProperty ; rdfs:range xsd:string .
kyc:isPEP        a owl:DatatypeProperty ; rdfs:domain kyc:NaturalPerson ; rdfs:range xsd:boolean .
kyc:isSanctioned a owl:DatatypeProperty ; rdfs:domain kyc:NaturalPerson ; rdfs:range xsd:boolean .
kyc:nationality  a owl:DatatypeProperty ; rdfs:domain kyc:NaturalPerson ; rdfs:range xsd:string .

# ── Object properties ─────────────────────────────────────────────────────────
kyc:hasJurisdiction  a owl:ObjectProperty ;
                     rdfs:domain kyc:LegalEntity ; rdfs:range kyc:Jurisdiction .
kyc:hasLegalAddress  a owl:ObjectProperty ;
                     rdfs:domain kyc:LegalEntity ; rdfs:range kyc:Address .
kyc:directlyOwnedBy  a owl:ObjectProperty , owl:TransitiveProperty ;
                     rdfs:domain kyc:LegalEntity ; rdfs:range kyc:LegalEntity ;
                     rdfs:label "directly owned by" .
kyc:controlledBy     a owl:ObjectProperty ;
                     rdfs:domain kyc:LegalEntity ; rdfs:range kyc:NaturalPerson .
kyc:ultimatelyOwnedBy a owl:ObjectProperty ;
                     rdfs:label "ultimately owned by (inferred via owl:TransitiveProperty)" .

# ── Inverse properties (so queries work both directions) ──────────────────────
kyc:owns             a owl:ObjectProperty ; owl:inverseOf kyc:directlyOwnedBy .
"""


def main() -> int:
    gdb = GraphDBClient()
    print(f"Loading mapping ontologies into '{gdb.repo}'...")

    pairs = [
        (FIBO2GLEI_TTL,    "http://kg/mapping/fibo2glei", "FIBO↔GLEIF mapping"),
        (KYC_ONTOLOGY_TTL, "http://kg/kyc/ontology",       "KYC application ontology"),
    ]
    for ttl, named_graph, label in pairs:
        status = gdb.load_turtle(ttl, named_graph)
        icon = "✓" if status in (200, 204) else "✗"
        print(f"  {icon} {label:<32} → {named_graph}  (HTTP {status})")

    print(f"\nTotal triples in repo: {gdb.count_triples():,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
