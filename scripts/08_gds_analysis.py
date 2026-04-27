"""
Script 08 — Run Graph Data Science algorithms to compute risk signals.

Pipeline
--------
1. Project the in-memory graphs (kyc-graph undirected, kyc-directed natural)
2. Run: WCC → Louvain → PageRank → Betweenness → SCC
3. Compute composite kycRiskScore on each LegalEntity
4. Drop projections (free heap)

Skill applied: gds-analysis

    python scripts/08_gds_analysis.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.kg_client import Neo4jClient

UNDIRECTED = "kyc-graph"
DIRECTED   = "kyc-directed"

ALGOS = [
    ("WCC (Weakly Connected Components)", f"""
        CALL gds.wcc.write('{UNDIRECTED}', {{ writeProperty: 'wccComponentId' }})
        YIELD componentCount, nodePropertiesWritten
        RETURN componentCount AS components, nodePropertiesWritten AS nodes
    """),
    ("Louvain (community detection)", f"""
        CALL gds.louvain.write('{UNDIRECTED}', {{ writeProperty: 'louvainCommunityId' }})
        YIELD communityCount, modularity
        RETURN communityCount AS communities, modularity
    """),
    ("PageRank (influence)", f"""
        CALL gds.pageRank.write('{DIRECTED}', {{
            writeProperty: 'pageRankScore', maxIterations: 20, dampingFactor: 0.85
        }}) YIELD nodePropertiesWritten, ranIterations
        RETURN nodePropertiesWritten AS nodes, ranIterations AS iters
    """),
    ("Betweenness (gatekeepers)", f"""
        CALL gds.betweenness.write('{DIRECTED}', {{ writeProperty: 'betweennessScore' }})
        YIELD nodePropertiesWritten
        RETURN nodePropertiesWritten AS nodes
    """),
    ("SCC (Strongly Connected Components — finds rings)", f"""
        CALL gds.scc.write('{DIRECTED}', {{ writeProperty: 'sccComponentId' }})
        YIELD componentCount
        RETURN componentCount AS components
    """),
]


def drop_if_exists(neo: Neo4jClient, name: str) -> None:
    res = neo.query("CALL gds.graph.exists($name) YIELD exists RETURN exists", {"name": name})
    if res and res[0]["exists"]:
        neo.execute("CALL gds.graph.drop($name)", {"name": name})
        print(f"  · dropped existing projection '{name}'")


def project_graphs(neo: Neo4jClient) -> None:
    print("→ Projecting in-memory graphs ...")
    drop_if_exists(neo, UNDIRECTED)
    drop_if_exists(neo, DIRECTED)

    neo.execute(f"""
        CALL gds.graph.project(
            '{UNDIRECTED}',
            ['LegalEntity', 'NaturalPerson'],
            {{
              DIRECTLY_OWNED_BY: {{ orientation: 'UNDIRECTED' }},
              CONTROLLED_BY:     {{ orientation: 'UNDIRECTED' }}
            }}
        )
    """)
    print(f"  ✓ '{UNDIRECTED}' (undirected, for WCC/Louvain)")

    neo.execute(f"""
        CALL gds.graph.project(
            '{DIRECTED}',
            ['LegalEntity', 'NaturalPerson'],
            {{
              DIRECTLY_OWNED_BY: {{ orientation: 'NATURAL' }},
              CONTROLLED_BY:     {{ orientation: 'NATURAL' }}
            }}
        )
    """)
    print(f"  ✓ '{DIRECTED}' (natural, for PageRank/SCC)")


def run_algos(neo: Neo4jClient) -> None:
    for name, query in ALGOS:
        print(f"\n→ {name}")
        rows = neo.query(query)
        print(f"  ✓ {rows}")


def compute_risk_score(neo: Neo4jClient) -> None:
    """Composite KYC risk score: 0 (clean) to 100 (red flag)."""
    print("\n→ Computing kycRiskScore ...")
    neo.execute("""
        MATCH (e:LegalEntity)
        WITH e,
             // 1. High-risk jurisdiction (35 points)
             CASE e.riskTier WHEN 'high' THEN 35 WHEN 'medium' THEN 15 ELSE 0 END AS jurisdictionRisk,

             // 2. Sanctioned UBO anywhere up the ownership chain (40 points)
             EXISTS {
                 MATCH (e)-[:DIRECTLY_OWNED_BY*1..6]->()-[:CONTROLLED_BY]->(p:NaturalPerson)
                 WHERE p.isSanctioned = true
             } AS sanctionedUBO,

             // 3. PEP control (15 points)
             EXISTS {
                 MATCH (e)-[:CONTROLLED_BY]->(p:NaturalPerson) WHERE p.isPEP = true
             } AS pepControl,

             // 4. In a strongly-connected component (= ring) of size > 1 (20 points)
             COALESCE(e.sccComponentId, -1) AS scc

        OPTIONAL MATCH (other:LegalEntity)
        WHERE other.sccComponentId = scc AND other <> e
        WITH e, jurisdictionRisk, sanctionedUBO, pepControl,
             count(other) > 0 AS inRing

        SET e.kycRiskScore =
            jurisdictionRisk
          + (CASE WHEN sanctionedUBO THEN 40 ELSE 0 END)
          + (CASE WHEN pepControl    THEN 15 ELSE 0 END)
          + (CASE WHEN inRing        THEN 20 ELSE 0 END)
    """)
    rows = neo.query("""
        MATCH (e:LegalEntity)
        RETURN e.kycRiskScore AS score, count(*) AS n
        ORDER BY score DESC
    """)
    print("  Distribution of risk scores:")
    for row in rows:
        print(f"    score={row['score']:>3}  count={row['n']}")


def show_top_risks(neo: Neo4jClient) -> None:
    print("\n→ Top 10 highest-risk entities:")
    rows = neo.query("""
        MATCH (e:LegalEntity)
        WHERE e.kycRiskScore > 0
        RETURN e.id AS id, e.name AS name, e.jurisdiction AS jur,
               e.kycRiskScore AS score
        ORDER BY score DESC, name
        LIMIT 10
    """)
    for r in rows:
        print(f"    [{r['score']:>3}] {r['id']}  {r['name'][:40]:<40} ({r['jur']})")


def cleanup(neo: Neo4jClient) -> None:
    print("\n→ Dropping in-memory projections (free heap) ...")
    drop_if_exists(neo, UNDIRECTED)
    drop_if_exists(neo, DIRECTED)


def main() -> int:
    with Neo4jClient() as neo:
        project_graphs(neo)
        run_algos(neo)
        compute_risk_score(neo)
        show_top_risks(neo)
        cleanup(neo)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
