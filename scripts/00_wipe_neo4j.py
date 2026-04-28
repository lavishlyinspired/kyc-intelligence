"""
Script 00 — Wipe Neo4j completely (data + schema + n10s artefacts).

Use this BEFORE re-running the load pipeline when you want a guaranteed
clean slate. Removes:
  • all nodes & relationships (in batches via apoc.periodic.iterate)
  • all constraints & indexes (apoc.schema.assert)
  • leftover n10s `_GraphConfig` / `_NsPrefDef` / `Resource` nodes
  • any prior `n4sch__*` ontology label residue

After this script the database has 0 nodes / 0 rels / no schema.
The ontology lives in GraphDB (queried via SPARQL); Neo4j only holds
instance data + indexes for it.

    python scripts/00_wipe_neo4j.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.kg_client import Neo4jClient, neo4j_healthy


def main() -> int:
    if not neo4j_healthy():
        print("✗ Neo4j is not reachable. Did you `docker compose up -d`?")
        return 1

    with Neo4jClient() as neo:
        # 1. Drop all data in batches (handles millions of rels safely)
        print("→ Deleting all nodes & relationships ...")
        neo.execute("""
            CALL apoc.periodic.iterate(
              'MATCH (n) RETURN n',
              'DETACH DELETE n',
              {batchSize: 10000, parallel: false}
            )
        """)

        # 2. Drop ALL constraints + indexes (schema reset)
        print("→ Dropping all constraints & indexes (apoc.schema.assert) ...")
        neo.execute("CALL apoc.schema.assert({}, {}, true)")

        # 3. Drop full-text & vector indexes that apoc.schema.assert misses
        print("→ Dropping non-schema indexes (full-text / vector) ...")
        idx_rows = neo.query("SHOW INDEXES YIELD name, type")
        for row in idx_rows:
            name = row["name"]
            try:
                neo.execute(f"DROP INDEX `{name}` IF EXISTS")
            except Exception as e:
                print(f"   ! could not drop {name}: {e}")

        # 4. Verify
        n = neo.node_count()
        rels = neo.query_one("MATCH ()-[r]->() RETURN count(r) AS c") or {"c": 0}
        labels = neo.list_labels()
        print("\n── Wipe complete ──")
        print(f"  nodes:         {n}")
        print(f"  relationships: {rels['c']}")
        print(f"  labels:        {labels}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
