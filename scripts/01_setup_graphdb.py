"""
Script 01 — Create the GraphDB repository for KYC Knowledge Graph.

What this does
--------------
1. Writes a Turtle config file describing a repository with OWL reasoning enabled
   (`owl-horst-optimized` — needed for transitive ownership inference).
2. Creates the repository in GraphDB via REST.
3. Verifies it appears in the list.

Run AFTER `docker compose up -d` and once GraphDB is healthy.

    python scripts/01_setup_graphdb.py

Skill applied: load-fibo-ontology
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# Allow `python scripts/01_*.py` from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.kg_client import GraphDBClient, graphdb_healthy

REPO_CONFIG_TEMPLATE = """\
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix rep:  <http://www.openrdf.org/config/repository#> .
@prefix sr:   <http://www.openrdf.org/config/repository/sail#> .
@prefix sail: <http://www.openrdf.org/config/sail#> .
@prefix graphdb: <http://www.ontotext.com/config/graphdb#> .

[] a rep:Repository ;
   rep:repositoryID "{repo_id}" ;
   rdfs:label "KYC Beneficial Ownership Knowledge Graph" ;
   rep:repositoryImpl [
      rep:repositoryType "graphdb:SailRepository" ;
      sr:sailImpl [
         sail:sailType "graphdb:Sail" ;
         graphdb:ruleset "owl-horst-optimized" ;
         graphdb:entity-index-size "10000000" ;
         graphdb:entity-id-size "32" ;
         graphdb:enable-context-index "true" ;
         graphdb:enablePredicateList "true" ;
         graphdb:in-memory-literal-properties "true" ;
         graphdb:enable-literal-index "true" ;
         graphdb:check-for-inconsistencies "false"
      ]
   ] .
"""


def main() -> int:
    if not graphdb_healthy():
        print("✗ GraphDB is not reachable. Did you `docker compose up -d`?")
        return 1

    gdb = GraphDBClient()
    print(f"GraphDB URL: {gdb.base_url}")
    print(f"Target repository: {gdb.repo}")

    # Already exists?
    if gdb.repository_exists():
        print(f"✓ Repository '{gdb.repo}' already exists — skipping create.")
        return 0

    # Write config
    config_dir = Path("graphdb_config")
    config_dir.mkdir(exist_ok=True)
    config_path = config_dir / f"{gdb.repo}-config.ttl"
    config_path.write_text(REPO_CONFIG_TEMPLATE.format(repo_id=gdb.repo))
    print(f"  → Wrote {config_path}")

    # Create
    status = gdb.create_repository(str(config_path))
    if status not in (200, 201, 204):
        print(f"✗ Repository creation failed: HTTP {status}")
        return 1
    print(f"✓ Repository '{gdb.repo}' created (HTTP {status})")

    # Verify
    repos = gdb.list_repositories()
    print(f"  Available repositories: {repos}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
