"""
Shared client helpers for GraphDB and Neo4j.

Use these everywhere instead of recreating connection/query logic.

    from src.kg_client import GraphDBClient, Neo4jClient

    gdb = GraphDBClient()
    rows = gdb.query("SELECT * WHERE { ?s ?p ?o } LIMIT 5")

    neo = Neo4jClient()
    rows = neo.query("MATCH (n:LegalEntity) RETURN n LIMIT 5")
    neo.close()
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Iterable

import requests
from dotenv import load_dotenv
from neo4j import GraphDatabase, Driver
from SPARQLWrapper import SPARQLWrapper, JSON, POST

load_dotenv()


# ─── GraphDB ──────────────────────────────────────────────────────────────────

class GraphDBClient:
    """Thin wrapper over GraphDB's REST/SPARQL endpoints."""

    def __init__(self, base_url: str | None = None, repo: str | None = None):
        self.base_url = (base_url or os.getenv("GRAPHDB_URL", "http://localhost:7200")).rstrip("/")
        self.repo = repo or os.getenv("GRAPHDB_REPO", "kyc-kg")
        self.sparql_endpoint = f"{self.base_url}/repositories/{self.repo}"
        self.update_endpoint = f"{self.sparql_endpoint}/statements"
        self.graphs_endpoint = f"{self.sparql_endpoint}/rdf-graphs/service"

    # ── Read ──────────────────────────────────────────────────────────────────
    def query(self, sparql: str) -> list[dict[str, Any]]:
        """Run a SELECT query, return a list of {var: value} dicts."""
        wrapper = SPARQLWrapper(self.sparql_endpoint)
        wrapper.setQuery(sparql)
        wrapper.setReturnFormat(JSON)
        results = wrapper.query().convert()
        return [
            {k: v["value"] for k, v in row.items()}
            for row in results["results"]["bindings"]
        ]

    def query_raw(self, sparql: str, accept: str = "application/sparql-results+json") -> str:
        """Run any SPARQL query and return the raw response body (CONSTRUCT, etc.)."""
        r = requests.post(
            self.sparql_endpoint,
            data=sparql,
            headers={"Content-Type": "application/sparql-query", "Accept": accept},
            timeout=120,
        )
        r.raise_for_status()
        return r.text

    def ask(self, sparql: str) -> bool:
        """Run an ASK query, return bool."""
        wrapper = SPARQLWrapper(self.sparql_endpoint)
        wrapper.setQuery(sparql)
        wrapper.setReturnFormat(JSON)
        return bool(wrapper.query().convert().get("boolean"))

    # ── Write ─────────────────────────────────────────────────────────────────
    def update(self, sparql_update: str) -> int:
        """Run a SPARQL Update (INSERT/DELETE/LOAD). Returns HTTP status."""
        r = requests.post(
            self.update_endpoint,
            data=sparql_update,
            headers={"Content-Type": "application/sparql-update"},
            timeout=120,
        )
        return r.status_code

    def load_turtle(self, ttl: str | bytes, named_graph: str) -> int:
        """Upload a Turtle string into a named graph. Returns HTTP status."""
        body = ttl.encode("utf-8") if isinstance(ttl, str) else ttl
        r = requests.post(
            self.graphs_endpoint,
            params={"graph": named_graph},
            data=body,
            headers={"Content-Type": "text/turtle"},
            timeout=120,
        )
        return r.status_code

    def load_url(self, url: str, named_graph: str) -> int:
        """Ask GraphDB to LOAD an RDF document from URL into a named graph."""
        return self.update(f"LOAD <{url}> INTO GRAPH <{named_graph}>")

    def drop_graph(self, named_graph: str) -> int:
        return self.update(f"DROP GRAPH <{named_graph}>")

    # ── Repository management ────────────────────────────────────────────────
    def list_repositories(self) -> list[str]:
        r = requests.get(f"{self.base_url}/rest/repositories", timeout=10)
        r.raise_for_status()
        return [repo["id"] for repo in r.json()]

    def repository_exists(self) -> bool:
        return self.repo in self.list_repositories()

    def create_repository(self, config_ttl_path: str) -> int:
        """Create the repo from a Turtle config file. Returns HTTP status."""
        with open(config_ttl_path, "rb") as f:
            r = requests.post(
                f"{self.base_url}/rest/repositories",
                files={"config": f},
                timeout=30,
            )
        return r.status_code

    # ── Convenience ──────────────────────────────────────────────────────────
    def list_named_graphs(self) -> list[tuple[str, int]]:
        rows = self.query("""
            SELECT ?graph (COUNT(*) AS ?triples)
            WHERE { GRAPH ?graph { ?s ?p ?o } }
            GROUP BY ?graph
            ORDER BY DESC(?triples)
        """)
        return [(r["graph"], int(r["triples"])) for r in rows]

    def count_triples(self, named_graph: str | None = None) -> int:
        if named_graph:
            q = f"SELECT (COUNT(*) AS ?c) WHERE {{ GRAPH <{named_graph}> {{ ?s ?p ?o }} }}"
        else:
            q = "SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }"
        rows = self.query(q)
        return int(rows[0]["c"]) if rows else 0


# ─── Neo4j ────────────────────────────────────────────────────────────────────

class Neo4jClient:
    """Thin wrapper over the official Neo4j driver."""

    def __init__(
        self,
        uri: str | None = None,
        user: str | None = None,
        password: str | None = None,
    ):
        self.uri = uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = user or os.getenv("NEO4J_USER", "neo4j")
        self.password = password or os.getenv("NEO4J_PASSWORD", "kycpassword123")
        self._driver: Driver | None = None

    @property
    def driver(self) -> Driver:
        if self._driver is None:
            self._driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        return self._driver

    def close(self) -> None:
        if self._driver is not None:
            self._driver.close()
            self._driver = None

    def __enter__(self) -> "Neo4jClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ── Query ─────────────────────────────────────────────────────────────────
    def query(self, cypher: str, params: dict | None = None) -> list[dict[str, Any]]:
        with self.driver.session() as s:
            return [dict(r) for r in s.run(cypher, params or {})]

    def query_one(self, cypher: str, params: dict | None = None) -> dict[str, Any] | None:
        """Run a query and return the first row, or None if no results."""
        rows = self.query(cypher, params)
        return rows[0] if rows else None

    def execute(self, cypher: str, params: dict | None = None) -> None:
        with self.driver.session() as s:
            s.run(cypher, params or {}).consume()

    def execute_many(self, cyphers: Iterable[str]) -> None:
        with self.driver.session() as s:
            for c in cyphers:
                if c.strip():
                    s.run(c).consume()

    # ── Convenience ──────────────────────────────────────────────────────────
    def node_count(self, label: str | None = None) -> int:
        if label:
            # Use EXISTS to avoid the Neo4j "label not in DB" warning
            rows = self.query(
                "MATCH (n) WHERE $lbl IN labels(n) RETURN count(n) AS c",
                {"lbl": label},
            )
        else:
            rows = self.query("MATCH (n) RETURN count(n) AS c")
        return rows[0]["c"] if rows else 0

    def list_labels(self) -> list[str]:
        return [r["label"] for r in self.query("CALL db.labels() YIELD label RETURN label ORDER BY label")]

    def list_relationship_types(self) -> list[str]:
        return [r["relationshipType"] for r in self.query(
            "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType ORDER BY relationshipType"
        )]

    def schema(self) -> dict:
        return self.query("CALL apoc.meta.schema() YIELD value RETURN value")[0]["value"]


# ─── Health checks ────────────────────────────────────────────────────────────

def graphdb_healthy(timeout: float = 2.0) -> bool:
    try:
        url = (os.getenv("GRAPHDB_URL") or "http://localhost:7200").rstrip("/")
        r = requests.get(f"{url}/rest/repositories", timeout=timeout)
        return r.ok
    except Exception:
        return False


def neo4j_healthy(timeout: float = 2.0) -> bool:
    try:
        with Neo4jClient() as n:
            n.query("RETURN 1 AS ok")
        return True
    except Exception:
        return False


@contextmanager
def neo4j_session():
    """Compat shortcut: `with neo4j_session() as s: s.run(...)`."""
    n = Neo4jClient()
    try:
        with n.driver.session() as s:
            yield s
    finally:
        n.close()


__all__ = [
    "GraphDBClient",
    "Neo4jClient",
    "graphdb_healthy",
    "neo4j_healthy",
    "neo4j_session",
]
