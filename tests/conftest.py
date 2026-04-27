"""
pytest configuration & shared fixtures.

Tests are split into:
  • Unit-ish tests        (no Neo4j) — run by default
  • @pytest.mark.integration tests — require docker compose stack + data loaded

Run only fast tests:        pytest -m "not integration"
Run integration tests:      pytest -m integration
Run all:                    pytest
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.kg_client import Neo4jClient, neo4j_healthy


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: requires running Neo4j with data loaded")


@pytest.fixture(scope="session")
def neo() -> Neo4jClient:
    if not neo4j_healthy():
        pytest.skip("Neo4j is not reachable — start `docker compose up -d`")
    client = Neo4jClient()
    if client.node_count("LegalEntity") == 0:
        pytest.skip("No data in Neo4j — run scripts 06/07 first")
    yield client
    client.close()


@pytest.fixture(scope="session")
def ground_truth(neo) -> dict:
    rows = neo.query("MATCH (gt:GroundTruth) RETURN gt LIMIT 1")
    if not rows:
        pytest.skip("No GroundTruth node — run script 07")
    return dict(rows[0]["gt"])
