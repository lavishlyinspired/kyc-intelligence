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
    # NOTE: exact Betweenness is O(V*E) — infeasible on the 13M-node real-data
    # perimeter (Neo4j was OOM-killed). Use sampled approximation instead.
    ("Betweenness (sampled, gatekeepers)", f"""
        CALL gds.betweenness.write('{DIRECTED}', {{
            writeProperty: 'betweennessScore', samplingSize: 1000
        }})
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
        # YIELD graphName suppresses the deprecated 'schema' field warning in GDS 2.5+
        neo.execute("CALL gds.graph.drop($name) YIELD graphName", {"name": name})
        print(f"  · dropped existing projection '{name}'")


def project_graphs(neo: Neo4jClient) -> None:
    print("→ Projecting in-memory graphs (real-data perimeter: GLEIF + UK_PSC + ICIJ) ...")
    drop_if_exists(neo, UNDIRECTED)
    drop_if_exists(neo, DIRECTED)

    # Scope to real ownership graph: GLEIF L1/L2 + UK PSC + ICIJ Offshore Leaks.
    # OpenSanctions screening-list isolates (no graph relationships) are
    # excluded from analytics so PageRank / Louvain etc. stay meaningful.
    SCOPE = "n.dataSource IN ['GLEIF','GLEIF_RR','UK_PSC','ICIJ','ICIJ_INTERMEDIARY']"
    SCOPE_AB = ("(a:LegalEntity AND a.dataSource IN ['GLEIF','GLEIF_RR','UK_PSC','ICIJ','ICIJ_INTERMEDIARY'])"
                " OR (b:LegalEntity AND b.dataSource IN ['GLEIF','GLEIF_RR','UK_PSC','ICIJ','ICIJ_INTERMEDIARY'])")

    node_query = f"""
        MATCH (n:LegalEntity)
        WHERE {SCOPE}
        RETURN id(n) AS id, labels(n) AS labels
        UNION
        MATCH (p:NaturalPerson)<-[:CONTROLLED_BY]-(:LegalEntity)
        RETURN DISTINCT id(p) AS id, labels(p) AS labels
    """

    rel_query_undir = f"""
        MATCH (a)-[r:DIRECTLY_OWNED_BY|CONTROLLED_BY]-(b)
        WHERE {SCOPE_AB}
        RETURN id(a) AS source, id(b) AS target, type(r) AS type
    """
    rel_query_dir = f"""
        MATCH (a)-[r:DIRECTLY_OWNED_BY|CONTROLLED_BY]->(b)
        WHERE {SCOPE_AB}
        RETURN id(a) AS source, id(b) AS target, type(r) AS type
    """

    neo.execute(
        f"CALL gds.graph.project.cypher('{UNDIRECTED}', $nodes, $rels)",
        {"nodes": node_query, "rels": rel_query_undir},
    )
    print(f"  ✓ '{UNDIRECTED}' (undirected, for WCC/Louvain)")

    neo.execute(
        f"CALL gds.graph.project.cypher('{DIRECTED}', $nodes, $rels)",
        {"nodes": node_query, "rels": rel_query_dir},
    )
    print(f"  ✓ '{DIRECTED}' (natural, for PageRank/SCC)")


def run_algos(neo: Neo4jClient) -> None:
    for name, query in ALGOS:
        print(f"\n→ {name}")
        rows = neo.query(query)
        print(f"  ✓ {rows}")


def compute_risk_score(neo: Neo4jClient) -> None:
    """Composite KYC risk score: 0 (clean) to 100 (red flag).

    Scoped to GLEIF perimeter only \u2014 OpenSanctions screening-list isolates
    (which have no relationships in our graph) keep the score they already
    have (typically null) so analytics aren\u2019t skewed by a 26k-row no-op.
    """
    print("\n\u2192 Computing kycRiskScore ...")
    # Index on sccComponentId for the ring-membership lookup
    try:
        neo.execute("CREATE INDEX scc_id IF NOT EXISTS FOR (e:LegalEntity) ON (e.sccComponentId)")
    except Exception:
        pass

    # Batch via apoc.periodic.iterate so the per-transaction memory cap doesn't
    # blow up on the 17M-node real-data perimeter.
    neo.execute("""
        CALL apoc.periodic.iterate(
          "MATCH (e:LegalEntity)
           WHERE e.dataSource IN ['GLEIF','GLEIF_RR','UK_PSC','ICIJ','ICIJ_INTERMEDIARY']
           RETURN e",
          "WITH e,
                CASE e.riskTier WHEN 'high' THEN 60 WHEN 'medium' THEN 30 ELSE 0 END AS jurisdictionRisk,
                EXISTS {
                    MATCH (e)-[:DIRECTLY_OWNED_BY*1..3]->()-[:CONTROLLED_BY]->(p:NaturalPerson)
                    WHERE p.isSanctioned = true
                } AS sanctionedUBO,
                EXISTS {
                    MATCH (e)-[:CONTROLLED_BY]->(p:NaturalPerson) WHERE p.isPEP = true
                } AS pepControl,
                COALESCE(e.sccComponentId, -1) AS scc,
                COALESCE(e.pageRankScore, 0.0) AS pr
           OPTIONAL MATCH (other:LegalEntity)
           WHERE other.sccComponentId = scc AND other <> e AND scc <> -1
           WITH e, jurisdictionRisk, sanctionedUBO, pepControl, pr,
                count(other) > 0 AS inRing
           SET e.kycRiskScore =
               apoc.coll.min([100,
                 jurisdictionRisk
               + (CASE WHEN sanctionedUBO THEN 30 ELSE 0 END)
               + (CASE WHEN pepControl    THEN 15 ELSE 0 END)
               + (CASE WHEN inRing        THEN 20 ELSE 0 END)
               + (CASE WHEN pr > 1.0      THEN 10 ELSE 0 END)
               ])",
          {batchSize: 5000, parallel: false}
        ) YIELD batches, total, errorMessages
        RETURN batches, total, errorMessages
    """)
    rows = neo.query("""
        MATCH (e:LegalEntity)
        WHERE e.dataSource IN ['GLEIF','GLEIF_RR','UK_PSC','ICIJ','ICIJ_INTERMEDIARY']
        RETURN e.kycRiskScore AS score, count(*) AS n
        ORDER BY score DESC
    """)
    print("  Distribution of risk scores (real-data perimeter):")
    for row in rows:
        print(f"    score={row['score']!s:>5}  count={row['n']}")


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
