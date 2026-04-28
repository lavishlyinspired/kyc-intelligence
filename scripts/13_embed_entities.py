"""
Script 13 — Create vector embeddings on real :LegalEntity / :NaturalPerson nodes
using Ollama (nomic-embed-text) + Neo4jVector.

Pattern: Going Meta sessions 21–22 ("Vector-based and Graph-based semantic
search", "RAG with Knowledge Graphs").

This builds the `entity_embeddings` vector index that the agent's
`semantic_search_entities` tool queries.

Embeddings are computed over the entity's natural-language description
(name + jurisdiction + category + description) — NOT arbitrary text chunks,
because the entities themselves are the retrieval units in our KYC use case.

Usage:
    python scripts/13_embed_entities.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from src.kg_client import Neo4jClient, neo4j_healthy

VECTOR_INDEX = "entity_embeddings"
KEYWORD_INDEX = "keyword"
EMBED_MODEL  = os.getenv("OLLAMA_EMBED_MODEL", "nomic-embed-text")
EMBED_DIM    = 768  # nomic-embed-text dimension


def get_embeddings():
    from langchain_ollama import OllamaEmbeddings
    base = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1").replace("/v1", "")
    return OllamaEmbeddings(model=EMBED_MODEL, base_url=base)


def build_text_for_entity(node: dict) -> str:
    """Compose a retrieval-friendly description from node properties."""
    parts = [node.get("name") or ""]
    if node.get("category"):       parts.append(f"category={node['category']}")
    if node.get("jurisdiction"):   parts.append(f"jurisdiction={node['jurisdiction']}")
    if node.get("legalForm"):      parts.append(f"legalForm={node['legalForm']}")
    if node.get("riskTier"):       parts.append(f"risk={node['riskTier']}")
    if node.get("hqCity"):         parts.append(f"HQ={node['hqCity']},{node.get('hqCountry','')}")
    if node.get("description"):    parts.append(node["description"])
    return " | ".join(p for p in parts if p)


def build_text_for_person(node: dict) -> str:
    parts = [node.get("name") or ""]
    if node.get("role"):           parts.append(f"role={node['role']}")
    if node.get("nationality"):    parts.append(f"nationality={node['nationality']}")
    if node.get("isPEP"):          parts.append("PEP")
    if node.get("isSanctioned"):   parts.append("SANCTIONED")
    return " | ".join(p for p in parts if p)


def main() -> int:
    if not neo4j_healthy():
        print("✗ Neo4j is not reachable.")
        return 1

    embeddings = get_embeddings()
    print(f"→ Embedding model: {EMBED_MODEL}")

    with Neo4jClient() as neo:
        # Drop & recreate the index (idempotent)
        neo.execute(f"DROP INDEX {VECTOR_INDEX} IF EXISTS")
        neo.execute(f"DROP INDEX {KEYWORD_INDEX} IF EXISTS")

        # Set `text` property on the entities we embed (GLEIF investigation
        # perimeter only — OpenSanctions persons & orgs are searched via the
        # keyword/full-text index on `name`, since 740k+ embeddings is
        # impractical with a local model).
        print("→ Composing retrieval text on GLEIF entities ...")
        entities = neo.query("""
            MATCH (e:LegalEntity)
            WHERE e.dataSource STARTS WITH 'GLEIF'
            RETURN e.id AS id, e.name AS name, e.category AS category,
                   e.jurisdiction AS jurisdiction, e.legalForm AS legalForm,
                   e.riskTier AS riskTier, e.hqCity AS hqCity, e.hqCountry AS hqCountry,
                   e.description AS description
        """)
        # Persons in our investigation graph: those connected via CONTROLLED_BY
        # to a GLEIF entity. (Sanctioned/PEP isolates are searched by name only.)
        persons = neo.query("""
            MATCH (p:NaturalPerson)<-[:CONTROLLED_BY]-(:LegalEntity)
            RETURN DISTINCT p.id AS id, p.name AS name, p.role AS role,
                   p.nationality AS nationality, p.isPEP AS isPEP, p.isSanctioned AS isSanctioned
        """)
        print(f"   {len(entities)} :LegalEntity (GLEIF), {len(persons)} :NaturalPerson (controllers)")

        for e in entities:
            txt = build_text_for_entity(e)
            neo.execute("MATCH (n:LegalEntity {id:$id}) SET n.text = $t", {"id": e["id"], "t": txt})
        for p in persons:
            txt = build_text_for_person(p)
            neo.execute("MATCH (n:NaturalPerson {id:$id}) SET n.text = $t", {"id": p["id"], "t": txt})

        # Compute embeddings (batch)
        print("→ Computing embeddings ...")
        all_nodes = [(e["id"], "LegalEntity", build_text_for_entity(e)) for e in entities] \
                  + [(p["id"], "NaturalPerson", build_text_for_person(p)) for p in persons]

        BATCH = 32
        for i in range(0, len(all_nodes), BATCH):
            batch = all_nodes[i:i+BATCH]
            texts = [b[2] for b in batch]
            try:
                vecs = embeddings.embed_documents(texts)
            except Exception as ex:
                print(f"   ✗ embedding batch {i}: {ex}")
                continue
            for (nid, label, _), vec in zip(batch, vecs):
                neo.execute(
                    f"MATCH (n:`{label}` {{id:$id}}) CALL db.create.setNodeVectorProperty(n,'embedding',$v)",
                    {"id": nid, "v": vec},
                )
            print(f"   ✓ embedded {min(i+BATCH, len(all_nodes))}/{len(all_nodes)}")

        # Create vector index — Neo4j requires one label per vector index, so
        # build separate indexes for LegalEntity and NaturalPerson.
        print(f"→ Creating vector indexes (dim={EMBED_DIM}) ...")
        for label in ("LegalEntity", "NaturalPerson"):
            idx_name = f"{VECTOR_INDEX}_{label.lower()}"
            neo.execute(f"DROP INDEX {idx_name} IF EXISTS")
            neo.execute(f"""
                CREATE VECTOR INDEX {idx_name} IF NOT EXISTS
                FOR (n:{label}) ON (n.embedding)
                OPTIONS {{ indexConfig: {{
                    `vector.dimensions`: {EMBED_DIM},
                    `vector.similarity_function`: 'cosine'
                }}}}
            """)

        # Create full-text keyword index for hybrid search (one per label).
        # Use `name` directly so it covers ALL entities/persons — including the
        # OpenSanctions ones we did not embed.
        print(f"→ Creating fulltext indexes on name ...")
        for label in ("LegalEntity", "NaturalPerson"):
            idx_name = f"{KEYWORD_INDEX}_{label.lower()}"
            neo.execute(f"DROP INDEX {idx_name} IF EXISTS")
            neo.execute(f"""
                CREATE FULLTEXT INDEX {idx_name} IF NOT EXISTS
                FOR (n:{label}) ON EACH [n.name]
            """)

        # Verify
        idx = neo.query(
            "SHOW INDEXES YIELD name, type, state "
            "WHERE name STARTS WITH 'entity_embeddings' OR name STARTS WITH 'keyword' "
            "RETURN name, type, state"
        )
        for i in idx:
            print(f"   ✓ {i['name']} ({i['type']}) — {i['state']}")

        print(f"\n✓ Done. {len(all_nodes)} entities embedded with `{EMBED_MODEL}`")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
