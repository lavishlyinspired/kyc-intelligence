"""
Detection tests — assert that planted financial-crime patterns are actually
discovered by our Cypher queries.

Run with:
    pytest tests/test_detection.py -v -m integration
"""
import pytest

pytestmark = pytest.mark.integration


def test_data_loaded(neo):
    assert neo.node_count("LegalEntity") >= 500
    assert neo.node_count("NaturalPerson") >= 200


def test_planted_sanctioned_persons_have_label(neo, ground_truth):
    sanctioned_ids = ground_truth["sanctioned_person_ids"]
    rows = neo.query("""
        MATCH (p:SanctionedEntity) RETURN p.id AS id
    """)
    found = {r["id"] for r in rows}
    for pid in sanctioned_ids:
        assert pid in found, f"Planted sanctioned person {pid} missing :SanctionedEntity label"


def test_pep_label_applied(neo, ground_truth):
    pep_ids = ground_truth["pep_person_ids"]
    rows = neo.query("MATCH (p:PoliticallyExposedPerson) RETURN p.id AS id")
    found = {r["id"] for r in rows}
    for pid in pep_ids:
        assert pid in found, f"Planted PEP {pid} missing :PoliticallyExposedPerson label"


def test_finds_all_planted_sanctioned_ubo_chains(neo, ground_truth):
    """Every planted chain start should have a discoverable sanctioned UBO."""
    chain_starts = ground_truth["sanctioned_chain_starts"]
    rows = neo.query("""
        MATCH (e:LegalEntity)
        WHERE e.id IN $ids
        AND EXISTS {
            MATCH (e)-[:DIRECTLY_OWNED_BY*0..6]->()
                  -[:CONTROLLED_BY]->(p:NaturalPerson {isSanctioned: true})
        }
        RETURN e.id AS id
    """, {"ids": chain_starts})
    found = {r["id"] for r in rows}
    missing = set(chain_starts) - found
    assert not missing, f"Sanctioned UBO chain not detected for: {missing}"


def test_finds_all_planted_circular_rings(neo, ground_truth):
    """Each planted ring should appear as an SCC of size >= 3."""
    expected_rings = [r.split(",") for r in ground_truth["ring_entity_ids"]]

    rows = neo.query("""
        MATCH (e:LegalEntity)
        WHERE e.sccComponentId IS NOT NULL
        WITH e.sccComponentId AS scc, collect(e.id) AS members
        WHERE size(members) > 1
        RETURN members
    """)
    detected_sets = [set(r["members"]) for r in rows]

    for expected in expected_rings:
        expected_set = set(expected)
        assert any(expected_set.issubset(d) for d in detected_sets), \
            f"Planted ring {expected} not detected by SCC"


def test_high_risk_jurisdictions_have_higher_avg_score(neo):
    rows = neo.query("""
        MATCH (e:LegalEntity)
        RETURN e.riskTier AS tier, avg(e.kycRiskScore) AS avg_score
    """)
    by_tier = {r["tier"]: r["avg_score"] for r in rows}
    assert by_tier.get("high", 0) > by_tier.get("low", 0), \
        f"High-risk jurisdictions should average higher score: {by_tier}"


def test_no_orphan_relationships(neo):
    rows = neo.query("""
        MATCH (n) WHERE NOT (n)--() AND NOT n:GroundTruth AND NOT n:Resource
        RETURN labels(n) AS labels, count(*) AS n
    """)
    isolated = {tuple(r["labels"]): r["n"] for r in rows}
    # Some isolated nodes are tolerable (e.g. persons not yet linked)
    assert sum(isolated.values()) < 50, f"Too many isolated nodes: {isolated}"
