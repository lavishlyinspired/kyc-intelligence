"""
KYC Intelligence — Agentic Chat Engine (LangGraph + LangChain + Neo4j)

Architecture
────────────
• LangGraph ReAct agent with MemorySaver for multi-turn conversation.
• 15+ deterministic tools (Cypher/SPARQL) — results come directly from the
  graph, not from LLM generation, preventing hallucination.
• 1 schema-constrained Text2Cypher fallback for open-ended questions.
• 1 SPARQL tool for ontology/GraphDB questions.
• Session-based memory — supports follow-up questions within a session.
• LLM priority: Anthropic → OpenAI → DeepSeek → Ollama (local).

Anti-hallucination strategy:
  1. All structured tools return data DIRECTLY from Neo4j/GraphDB.
  2. The Text2Cypher fallback is constrained by the full schema definition.
  3. The system prompt instructs the LLM to NEVER fabricate data.
  4. Results are always attributed to the graph query that produced them.
"""
from __future__ import annotations

import json
import os
import sys
import re
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from langchain_core.tools import tool
from langchain_core.documents import Document
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langgraph.checkpoint.memory import MemorySaver
from langgraph.prebuilt import create_react_agent

from src.kg_client import Neo4jClient, GraphDBClient

# ─── Shared Clients ──────────────────────────────────────────────────────────
_neo = Neo4jClient()
_gdb = GraphDBClient()

# ─── Vector Store (lazy init) ─────────────────────────────────────────────────
_vector_store = None


def _get_vector_store():
    """Lazy-init Neo4j vector store with Ollama embeddings."""
    global _vector_store
    if _vector_store is None:
        try:
            from langchain_neo4j import Neo4jVector
            from langchain_ollama import OllamaEmbeddings

            ollama_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1").replace("/v1", "")
            embeddings = OllamaEmbeddings(model="nomic-embed-text", base_url=ollama_url)

            _vector_store = Neo4jVector.from_existing_index(
                embeddings,
                url=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
                username=os.getenv("NEO4J_USER", "neo4j"),
                password=os.getenv("NEO4J_PASSWORD", "kycpassword123"),
                index_name="entity_embeddings",
                text_node_property="text",
                embedding_node_property="embedding",
                search_type="hybrid",
                keyword_index_name="keyword",
            )
        except Exception:
            _vector_store = None
    return _vector_store


# ─── Diffbot (lazy init) ─────────────────────────────────────────────────────
_diffbot_transformer = None


# Diffbot emits free-form labels (Organization / Person / Bank / Country / …).
# The KYC ontology only knows :LegalEntity and :NaturalPerson, so we must
# remap Diffbot output back onto the ontology and merge into existing GLEIF /
# UK-PSC / ICIJ nodes by name. Otherwise enrichment creates orphan clusters.
DIFFBOT_ORG_LABELS = (
    "Organization", "Company", "Corporation", "Business",
    "Bank", "FinancialOrganization", "GovernmentOrganization",
    "EducationalOrganization", "NonProfitOrganization",
    "PoliticalParty", "MusicGroup", "SportsTeam",
)
DIFFBOT_PERSON_LABELS = ("Person",)
# Map Diffbot-style relationship types onto our FIBO-aligned vocabulary.
DIFFBOT_REL_OWNERSHIP = (
    "SUBSIDIARY", "PARENT_ORGANIZATION", "PARENT", "OWNED_BY",
    "ACQUIRED_BY", "ACQUIRER_OF", "AFFILIATE_OF",
)
DIFFBOT_REL_CONTROL = (
    "CEO", "FOUNDED_BY", "FOUNDER", "BOARD_MEMBER", "BOARD_OF_DIRECTORS",
    "EXECUTIVE", "CHAIRMAN", "PRESIDENT", "DIRECTOR", "MANAGING_DIRECTOR",
    "CHIEF_EXECUTIVE", "EMPLOYEE_OF", "MEMBER_OF",
)


def _classify_diffbot_node(node) -> str | None:
    """Return 'org' for Diffbot org-like nodes, 'person' for person-like nodes,
    or None for non-ontology types (Location, Skill, Country, etc.) which
    we deliberately skip to avoid polluting the ontology."""
    t = (getattr(node, "type", "") or "").strip()
    if t in DIFFBOT_ORG_LABELS:
        return "org"
    if t in DIFFBOT_PERSON_LABELS:
        return "person"
    return None


def _classify_diffbot_rel(rel_type: str) -> tuple[str, str] | None:
    """Map a Diffbot relationship type onto our ontology.
    Returns (canonical_rel_type, role_or_kind) or None to drop the rel."""
    if rel_type in DIFFBOT_REL_OWNERSHIP:
        return ("DIRECTLY_OWNED_BY", rel_type)
    if rel_type in DIFFBOT_REL_CONTROL:
        return ("CONTROLLED_BY", rel_type.lower())
    return None  # drop everything else (MENTIONS, INDUSTRY, LOCATION, etc.)


def _load_diffbot_aligned(graph_documents, primary_name: str | None = None) -> dict:
    """Custom Diffbot loader that writes DIRECTLY into the KYC ontology.

    Unlike Neo4jGraph.add_graph_documents(), this DOES NOT create any
    :Organization / :Person / :Location / :Skill / :Document nodes or
    Diffbot-style rel types. Instead:

      * Org-like nodes  → fuzzy-merged into existing :LegalEntity by name
                          (or created as :LegalEntity if no match).
      * Person nodes    → fuzzy-merged into existing :NaturalPerson by name
                          (or created as :NaturalPerson if no match).
      * Location/Skill/Country/etc → SKIPPED (not in ontology).
      * Ownership rels  → :DIRECTLY_OWNED_BY between LegalEntities.
      * Control rels    → :CONTROLLED_BY (LegalEntity)-->(NaturalPerson).
      * Other rels      → DROPPED.

    Returns stats dict.
    """
    stats = {
        "orgs_aligned": 0,                  # # of org nodes processed
        "orgs_merged_into_existing": 0,     # # merged into existing entities
        "orgs_created_new": 0,              # # new :LegalEntity created
        "persons_aligned": 0,
        "persons_merged_into_existing": 0,
        "persons_created_new": 0,
        "ownership_rels_remapped": 0,
        "control_rels_remapped": 0,
        "skipped_nodes": 0,                 # non-ontology Diffbot types
        "skipped_rels": 0,                  # non-ontology rel types
    }

    # node.id (Diffbot) → elementId of the resulting KYC ontology node.
    # If a Diffbot node was skipped (Location etc), it is absent from this map
    # and any rel touching it is dropped.
    diffbot_to_kyc: dict[str, str] = {}

    with Neo4jClient() as neo:
        for gd in graph_documents:
            # ── 1. Process nodes — for each org/person, try to merge into
            #       existing ontology entity, else create new.
            for node in gd.nodes:
                kind = _classify_diffbot_node(node)
                if kind is None:
                    stats["skipped_nodes"] += 1
                    continue

                # Diffbot node id is sometimes a Wikidata URL, sometimes a name.
                raw_id = (node.id or "").strip()
                # Best display name from Diffbot properties.
                props = dict(getattr(node, "properties", {}) or {})
                name = (props.get("name") or props.get("label")
                        or (raw_id if not raw_id.startswith("http") else "")).strip()
                if not name:
                    # Fall back to id when no name was given.
                    name = raw_id
                if not name:
                    stats["skipped_nodes"] += 1
                    continue

                stats[f"{kind}s_aligned"] += 1

                if kind == "org":
                    res = neo.query_one("""
                        // Fuzzy-find an existing non-DIFFBOT LegalEntity by name.
                        OPTIONAL MATCH (existing:LegalEntity)
                        WHERE existing.name IS NOT NULL
                          AND coalesce(existing.dataSource,'') <> 'DIFFBOT'
                          AND size(trim(existing.name)) >= 5
                          AND size(trim($name)) >= 5
                          AND (toLower(trim(existing.name)) = toLower(trim($name))
                               OR toLower(trim(existing.name)) STARTS WITH toLower(trim($name))
                               OR toLower(trim(existing.name)) CONTAINS toLower(trim($name)))
                        WITH existing
                        ORDER BY
                          CASE WHEN toLower(trim(existing.name)) = toLower(trim($name)) THEN 0
                               WHEN toLower(trim(existing.name)) STARTS WITH toLower(trim($name)) THEN 1
                               ELSE 2 END,
                          size(existing.name) ASC
                        LIMIT 1
                        WITH existing
                        CALL {
                            WITH existing
                            WITH existing WHERE existing IS NOT NULL
                            // Tag the existing entity with provenance.
                            SET existing.enrichedFrom = coalesce(existing.enrichedFrom,'') + ';DIFFBOT'
                            RETURN elementId(existing) AS eid, false AS created
                          UNION
                            WITH existing
                            WITH existing WHERE existing IS NULL
                            // No match — create a new :LegalEntity (no extra labels).
                            CREATE (n:LegalEntity {
                                id: $newId,
                                name: $name,
                                dataSource: 'DIFFBOT',
                                needsVerification: true,
                                kycRiskScore: 30,
                                riskTier: 'medium',
                                isActive: true,
                                diffbotId: $rawId
                            })
                            RETURN elementId(n) AS eid, true AS created
                        }
                        RETURN eid, created
                    """, {
                        "name": name,
                        "rawId": raw_id,
                        "newId": f"DIFFBOT_{abs(hash(raw_id or name)) % 10**12}",
                    })
                    if res:
                        diffbot_to_kyc[raw_id] = res["eid"]
                        if res["created"]:
                            stats["orgs_created_new"] += 1
                        else:
                            stats["orgs_merged_into_existing"] += 1

                else:  # kind == "person"
                    res = neo.query_one("""
                        OPTIONAL MATCH (existing:NaturalPerson)
                        WHERE existing.name IS NOT NULL
                          AND coalesce(existing.dataSource,'') <> 'DIFFBOT'
                          AND size(trim(existing.name)) >= 5
                          AND size(trim($name)) >= 5
                          AND (toLower(trim(existing.name)) = toLower(trim($name))
                               OR toLower(trim(existing.name)) CONTAINS toLower(trim($name))
                               OR toLower(trim($name)) CONTAINS toLower(trim(existing.name)))
                        WITH existing
                        ORDER BY
                          CASE WHEN toLower(trim(existing.name)) = toLower(trim($name)) THEN 0 ELSE 1 END,
                          size(existing.name) ASC
                        LIMIT 1
                        WITH existing
                        CALL {
                            WITH existing
                            WITH existing WHERE existing IS NOT NULL
                            SET existing.enrichedFrom = coalesce(existing.enrichedFrom,'') + ';DIFFBOT'
                            RETURN elementId(existing) AS eid, false AS created
                          UNION
                            WITH existing
                            WITH existing WHERE existing IS NULL
                            CREATE (p:NaturalPerson {
                                id: $newId,
                                name: $name,
                                dataSource: 'DIFFBOT',
                                needsVerification: true,
                                diffbotId: $rawId
                            })
                            RETURN elementId(p) AS eid, true AS created
                        }
                        RETURN eid, created
                    """, {
                        "name": name,
                        "rawId": raw_id,
                        "newId": f"DIFFBOT_P_{abs(hash(raw_id or name)) % 10**12}",
                    })
                    if res:
                        diffbot_to_kyc[raw_id] = res["eid"]
                        if res["created"]:
                            stats["persons_created_new"] += 1
                        else:
                            stats["persons_merged_into_existing"] += 1

            # ── 2. Process relationships — only ownership/control mapped, others dropped.
            for rel in gd.relationships:
                src_id = (getattr(rel.source, "id", "") or "").strip()
                tgt_id = (getattr(rel.target, "id", "") or "").strip()
                rt = (getattr(rel, "type", "") or "").strip()
                if src_id not in diffbot_to_kyc or tgt_id not in diffbot_to_kyc:
                    stats["skipped_rels"] += 1
                    continue
                mapped = _classify_diffbot_rel(rt)
                if mapped is None:
                    stats["skipped_rels"] += 1
                    continue
                canonical, role = mapped
                src_eid = diffbot_to_kyc[src_id]
                tgt_eid = diffbot_to_kyc[tgt_id]

                if canonical == "DIRECTLY_OWNED_BY":
                    # (a)-[OWNED_BY]->(b)  means a is owned by b.  We always
                    # write (owned)-[:DIRECTLY_OWNED_BY]->(owner).
                    # Diffbot direction varies — for SUBSIDIARY/PARENT/PARENT_ORGANIZATION
                    # the source is the child and target is the parent.
                    if rt in ("SUBSIDIARY",):
                        # (parent)-[SUBSIDIARY]->(child)  → flip
                        owner_eid, owned_eid = src_eid, tgt_eid
                    else:
                        owner_eid, owned_eid = tgt_eid, src_eid
                    res = neo.query_one("""
                        MATCH (owned), (owner)
                        WHERE elementId(owned) = $owned AND elementId(owner) = $owner
                          AND owned:LegalEntity AND owner:LegalEntity
                        MERGE (owned)-[r:DIRECTLY_OWNED_BY]->(owner)
                          ON CREATE SET r.source = 'DIFFBOT', r.originalType = $orig
                        RETURN count(r) AS c
                    """, {"owned": owned_eid, "owner": owner_eid, "orig": role})
                    if res and res["c"]:
                        stats["ownership_rels_remapped"] += 1

                else:  # CONTROLLED_BY
                    # We need (LegalEntity)-[:CONTROLLED_BY]->(NaturalPerson).
                    # Determine direction by inspecting node labels.
                    res = neo.query_one("""
                        MATCH (a), (b)
                        WHERE elementId(a) = $aid AND elementId(b) = $bid
                        WITH a, b,
                             CASE WHEN a:LegalEntity AND b:NaturalPerson THEN 'ab'
                                  WHEN b:LegalEntity AND a:NaturalPerson THEN 'ba'
                                  ELSE NULL END AS dir
                        WHERE dir IS NOT NULL
                        WITH (CASE dir WHEN 'ab' THEN a ELSE b END) AS entity,
                             (CASE dir WHEN 'ab' THEN b ELSE a END) AS person
                        MERGE (entity)-[r:CONTROLLED_BY]->(person)
                          ON CREATE SET r.source = 'DIFFBOT', r.role = $role
                        RETURN count(r) AS c
                    """, {"aid": src_eid, "bid": tgt_eid, "role": role})
                    if res and res["c"]:
                        stats["control_rels_remapped"] += 1

        # ── 3. Optional: tag the primary searched entity with a provenance note.
        if primary_name:
            neo.execute("""
                MATCH (e:LegalEntity)
                WHERE toLower(trim(e.name)) STARTS WITH toLower(trim($n))
                WITH e ORDER BY size(e.name) ASC LIMIT 1
                SET e.enrichedFrom = coalesce(e.enrichedFrom,'') + ';DIFFBOT_WIKIPEDIA'
            """, {"n": primary_name})

    return stats


def _align_diffbot_to_ontology(primary_name: str | None = None) -> dict:
    """LEGACY post-process cleanup — kept for safety.  The new
    `_load_diffbot_aligned()` writes ontology-correct nodes upfront, so this
    function should normally find nothing to do.  It still cleans up any
    stray Diffbot-style nodes/rels left over from earlier runs.
    """
    stats = {
        "orgs_aligned": 0, "orgs_merged_into_existing": 0,
        "persons_aligned": 0, "persons_merged_into_existing": 0,
        "ownership_rels_remapped": 0, "control_rels_remapped": 0,
    }
    with Neo4jClient() as neo:
        # Gather live schema info.
        live = neo.query_one("""
            CALL db.labels() YIELD label
            WITH collect(label) AS labels
            RETURN labels AS labels
        """) or {"labels": []}
        live_labels = set(live["labels"])
        org_labels   = [l for l in DIFFBOT_ORG_LABELS    if l in live_labels]
        person_labels= [l for l in DIFFBOT_PERSON_LABELS if l in live_labels]
        live_rels = {r["relationshipType"] for r in neo.query("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")}
        own_rels   = [r for r in DIFFBOT_REL_OWNERSHIP if r in live_rels]
        ctrl_rels  = [r for r in DIFFBOT_REL_CONTROL   if r in live_rels]

        # Check for existing DIFFBOT-tagged nodes (may already be :LegalEntity
        # from a prior alignment run — still need merge step).
        diffbot_org_count = (neo.query_one(
            "MATCH (n:LegalEntity {dataSource: 'DIFFBOT'}) RETURN count(n) AS c"
        ) or {"c": 0})["c"]
        diffbot_person_count = (neo.query_one(
            "MATCH (n:NaturalPerson {dataSource: 'DIFFBOT'}) RETURN count(n) AS c"
        ) or {"c": 0})["c"]

        # Fast path: nothing to do if no Diffbot labels, rels, or tagged nodes.
        if not (org_labels or person_labels or own_rels or ctrl_rels
                or diffbot_org_count or diffbot_person_count):
            return stats

        # 1. Promote Diffbot org-like nodes to :LegalEntity (with provenance flags).
        for lbl in org_labels:
            r = neo.query_one(f"""
                MATCH (n:`{lbl}`)
                WHERE NOT n:LegalEntity AND coalesce(n.name, n.id) IS NOT NULL
                WITH n, coalesce(n.name, n.id) AS nm
                SET n:LegalEntity,
                    n.name              = coalesce(n.name, nm),
                    n.dataSource        = coalesce(n.dataSource, 'DIFFBOT'),
                    n.needsVerification = coalesce(n.needsVerification, true),
                    n.id                = coalesce(n.id, 'DIFFBOT_' + apoc.text.slug(toLower(nm)) + '_' + toString(id(n))),
                    n.kycRiskScore      = coalesce(n.kycRiskScore, 30),
                    n.riskTier          = coalesce(n.riskTier, 'medium'),
                    n.isActive          = coalesce(n.isActive, true)
                RETURN count(n) AS c
            """) or {"c": 0}
            stats["orgs_aligned"] += r["c"]

        # 2. Merge Diffbot orgs into existing GLEIF/PSC/ICIJ entities by name.
        #    Uses fuzzy CONTAINS match (minimum 5-char name) to handle cases like
        #    "Deutsche Bank" vs "Deutsche Bank Aktiengesellschaft".
        #    Picks the best candidate: prefer exact match, then STARTS WITH, then
        #    shortest CONTAINS match (avoids single-letter false positives).
        if stats["orgs_aligned"] > 0 or diffbot_org_count > 0:
            merged = neo.query_one("""
                MATCH (new:LegalEntity {dataSource: 'DIFFBOT'})
                WHERE new.name IS NOT NULL AND size(trim(new.name)) >= 5
                OPTIONAL MATCH (exact:LegalEntity)
                WHERE exact.dataSource <> 'DIFFBOT'
                  AND exact.name IS NOT NULL
                  AND toLower(trim(exact.name)) = toLower(trim(new.name))
                  AND elementId(exact) <> elementId(new)
                WITH new, head(collect(exact)) AS exactMatch
                WITH new, exactMatch WHERE exactMatch IS NULL
                OPTIONAL MATCH (fuzzy:LegalEntity)
                WHERE fuzzy.dataSource <> 'DIFFBOT'
                  AND fuzzy.name IS NOT NULL
                  AND size(trim(fuzzy.name)) >= 5
                  AND (toLower(trim(fuzzy.name)) STARTS WITH toLower(trim(new.name))
                       OR toLower(trim(fuzzy.name)) CONTAINS toLower(trim(new.name)))
                  AND elementId(fuzzy) <> elementId(new)
                WITH new, fuzzy ORDER BY
                    CASE WHEN toLower(trim(fuzzy.name)) STARTS WITH toLower(trim(new.name)) THEN 0 ELSE 1 END,
                    size(fuzzy.name) ASC
                WITH new, head(collect(fuzzy)) AS best
                WITH new, best WHERE best IS NOT NULL
                CALL apoc.refactor.mergeNodes(
                    [best, new],
                    {properties: 'discard', mergeRels: true}
                ) YIELD node
                RETURN count(node) AS c
            """) or {"c": 0}
            # Also handle exact matches (separate pass for clarity)
            merged_exact = neo.query_one("""
                MATCH (new:LegalEntity {dataSource: 'DIFFBOT'})
                WHERE new.name IS NOT NULL
                MATCH (existing:LegalEntity)
                WHERE existing.dataSource <> 'DIFFBOT'
                  AND existing.name IS NOT NULL
                  AND toLower(trim(existing.name)) = toLower(trim(new.name))
                  AND elementId(new) <> elementId(existing)
                WITH existing, collect(DISTINCT new) AS dups
                CALL apoc.refactor.mergeNodes(
                    [existing] + dups,
                    {properties: 'discard', mergeRels: true}
                ) YIELD node
                RETURN count(node) AS c
            """) or {"c": 0}
            stats["orgs_merged_into_existing"] = merged["c"] + merged_exact["c"]

        # 3. Promote Diffbot Person nodes to :NaturalPerson.
        for lbl in person_labels:
            r = neo.query_one(f"""
                MATCH (p:`{lbl}`)
                WHERE NOT p:NaturalPerson AND coalesce(p.name, p.id) IS NOT NULL
                WITH p, coalesce(p.name, p.id) AS nm
                SET p:NaturalPerson,
                    p.name              = coalesce(p.name, nm),
                    p.dataSource        = coalesce(p.dataSource, 'DIFFBOT'),
                    p.needsVerification = coalesce(p.needsVerification, true),
                    p.id                = coalesce(p.id, 'DIFFBOT_P_' + apoc.text.slug(toLower(nm)) + '_' + toString(id(p)))
                RETURN count(p) AS c
            """) or {"c": 0}
            stats["persons_aligned"] += r["c"]

        if stats["persons_aligned"] > 0 or diffbot_person_count > 0:
            # Exact match first
            merged_p = neo.query_one("""
                MATCH (new:NaturalPerson {dataSource: 'DIFFBOT'})
                WHERE new.name IS NOT NULL
                MATCH (existing:NaturalPerson)
                WHERE existing.dataSource <> 'DIFFBOT'
                  AND existing.name IS NOT NULL
                  AND toLower(trim(existing.name)) = toLower(trim(new.name))
                  AND elementId(new) <> elementId(existing)
                WITH existing, collect(DISTINCT new) AS dups
                CALL apoc.refactor.mergeNodes(
                    [existing] + dups,
                    {properties: 'discard', mergeRels: true}
                ) YIELD node
                RETURN count(node) AS c
            """) or {"c": 0}
            # Fuzzy CONTAINS fallback for persons (>=5 char names)
            merged_p_fuzzy = neo.query_one("""
                MATCH (new:NaturalPerson {dataSource: 'DIFFBOT'})
                WHERE new.name IS NOT NULL AND size(trim(new.name)) >= 5
                OPTIONAL MATCH (fuzzy:NaturalPerson)
                WHERE fuzzy.dataSource <> 'DIFFBOT'
                  AND fuzzy.name IS NOT NULL
                  AND size(trim(fuzzy.name)) >= 5
                  AND (toLower(trim(fuzzy.name)) = toLower(trim(new.name))
                       OR toLower(trim(fuzzy.name)) CONTAINS toLower(trim(new.name))
                       OR toLower(trim(new.name)) CONTAINS toLower(trim(fuzzy.name)))
                  AND elementId(fuzzy) <> elementId(new)
                WITH new, fuzzy ORDER BY size(fuzzy.name) ASC
                WITH new, head(collect(fuzzy)) AS best
                WITH new, best WHERE best IS NOT NULL
                CALL apoc.refactor.mergeNodes(
                    [best, new],
                    {properties: 'discard', mergeRels: true}
                ) YIELD node
                RETURN count(node) AS c
            """) or {"c": 0}
            stats["persons_merged_into_existing"] = merged_p["c"] + merged_p_fuzzy["c"]

        # 4. Remap ownership-style Diffbot relationships to :DIRECTLY_OWNED_BY.
        for rt in own_rels:
            r = neo.query_one(f"""
                MATCH (a:LegalEntity)-[r:`{rt}`]->(b:LegalEntity)
                MERGE (a)-[nw:DIRECTLY_OWNED_BY]->(b)
                  ON CREATE SET nw.source = 'DIFFBOT', nw.originalType = '{rt}'
                DELETE r
                RETURN count(nw) AS c
            """) or {"c": 0}
            stats["ownership_rels_remapped"] += r["c"]

        # 5. Remap control/officer relationships to :CONTROLLED_BY.
        for rt in ctrl_rels:
            r1 = neo.query_one(f"""
                MATCH (e:LegalEntity)-[r:`{rt}`]->(p:NaturalPerson)
                MERGE (e)-[nw:CONTROLLED_BY]->(p)
                  ON CREATE SET nw.source = 'DIFFBOT', nw.role = '{rt.lower()}'
                DELETE r
                RETURN count(nw) AS c
            """) or {"c": 0}
            r2 = neo.query_one(f"""
                MATCH (p:NaturalPerson)-[r:`{rt}`]->(e:LegalEntity)
                MERGE (e)-[nw:CONTROLLED_BY]->(p)
                  ON CREATE SET nw.source = 'DIFFBOT', nw.role = '{rt.lower()}'
                DELETE r
                RETURN count(nw) AS c
            """) or {"c": 0}
            stats["control_rels_remapped"] += r1["c"] + r2["c"]

        # 6. If the user named a primary entity, force-link the :Document
        #    source to that LegalEntity so provenance is queryable.
        if primary_name:
            neo.execute("""
                MATCH (e:LegalEntity)
                WHERE toLower(trim(e.name)) STARTS WITH toLower(trim($n))
                WITH e ORDER BY size(e.name) ASC LIMIT 1
                MATCH (d:Document) WHERE NOT (e)-[:SOURCED_FROM]->(d)
                WITH e, d ORDER BY d.id DESC LIMIT 5
                MERGE (e)-[:SOURCED_FROM]->(d)
            """, {"n": primary_name})

        # 7. Delete orphan non-ontology nodes (Location, Skill, Document,
        #    Country, City, etc.) that Diffbot creates but are not in our schema.
        ORPHAN_LABELS = ("Location", "Skill", "Country", "City",
                         "Award", "Degree", "Language")
        orphan_labels_present = [l for l in ORPHAN_LABELS if l in live_labels]
        for lbl in orphan_labels_present:
            neo.execute(f"MATCH (x:`{lbl}`) WHERE NOT x:LegalEntity AND NOT x:NaturalPerson DETACH DELETE x")

        # 8. (Intentionally skipped) Do NOT strip Diffbot org/person labels
        #    from aligned nodes — they are needed by graph.add_graph_documents()
        #    MERGE logic on re-enrichment.  The secondary labels are harmless.

        # 9. Delete orphan Diffbot relationship types not in FIBO vocabulary.
        ORPHAN_RELS = ("INDUSTRY", "STOCK_EXCHANGE",
                       "ORGANIZATION_LOCATIONS", "FAMILY_MEMBER",
                       "SOCIAL_RELATIONSHIP", "DOMAIN")
        orphan_rels_present = [r for r in ORPHAN_RELS if r in live_rels]
        for rt in orphan_rels_present:
            neo.execute(f"MATCH ()-[r:`{rt}`]->() DELETE r")

    return stats


def _format_alignment(stats: dict) -> str:
    lines = ["\n  Ontology alignment:"]
    if "orgs_created_new" in stats:
        # New direct-loader stats.
        lines += [
            f"    • {stats['orgs_aligned']:,} orgs processed → "
            f"{stats['orgs_merged_into_existing']:,} merged into existing :LegalEntity, "
            f"{stats['orgs_created_new']:,} new",
            f"    • {stats['persons_aligned']:,} persons processed → "
            f"{stats['persons_merged_into_existing']:,} merged into existing :NaturalPerson, "
            f"{stats['persons_created_new']:,} new",
            f"    • {stats['ownership_rels_remapped']:,} → :DIRECTLY_OWNED_BY",
            f"    • {stats['control_rels_remapped']:,} → :CONTROLLED_BY",
            f"    • {stats.get('skipped_nodes', 0):,} non-ontology nodes skipped (Location/Skill/etc)",
            f"    • {stats.get('skipped_rels', 0):,} non-ontology rels skipped",
        ]
    else:
        lines += [
            f"    • {stats['orgs_aligned']:,} Diffbot orgs labelled :LegalEntity",
            f"    • {stats['orgs_merged_into_existing']:,} merged into existing GLEIF/PSC/ICIJ entities",
            f"    • {stats['persons_aligned']:,} persons labelled :NaturalPerson",
            f"    • {stats['persons_merged_into_existing']:,} merged into existing persons",
            f"    • {stats['ownership_rels_remapped']:,} ownership rels → :DIRECTLY_OWNED_BY",
            f"    • {stats['control_rels_remapped']:,} control rels → :CONTROLLED_BY",
        ]
    return "\n".join(lines)


def _get_diffbot():
    """Lazy-init Diffbot NLP transformer."""
    global _diffbot_transformer
    if _diffbot_transformer is None:
        api_key = os.getenv("DIFFBOT_API_KEY", "").strip()
        if api_key:
            from langchain_experimental.graph_transformers.diffbot import DiffbotGraphTransformer
            _diffbot_transformer = DiffbotGraphTransformer(diffbot_api_key=api_key)
    return _diffbot_transformer


# ─── LLM Selection ───────────────────────────────────────────────────────────
def _get_llm():
    """Auto-select LLM: Anthropic → OpenAI → DeepSeek → Ollama.
    Optimized for fast response with tool calling support."""
    if os.getenv("ANTHROPIC_API_KEY"):
        from langchain_anthropic import ChatAnthropic
        model = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-5-20250929")
        return ChatAnthropic(model=model, temperature=0, max_tokens=2048)

    if os.getenv("OPENAI_API_KEY"):
        from langchain_openai import ChatOpenAI
        model = os.getenv("OPENAI_MODEL", "gpt-4o")
        return ChatOpenAI(model=model, temperature=0, max_tokens=2048)

    if os.getenv("DEEPSEEK_API_KEY"):
        from langchain_openai import ChatOpenAI
        model = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")
        return ChatOpenAI(
            model=model, temperature=0, max_tokens=2048,
            api_key=os.environ["DEEPSEEK_API_KEY"],
            base_url=os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1"),
        )

    if os.getenv("OLLAMA_MODEL"):
        from langchain_openai import ChatOpenAI
        model = os.environ["OLLAMA_MODEL"]
        base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1")
        return ChatOpenAI(
            model=model, temperature=0,
            max_tokens=1024,  # Shorter for speed with local models
            api_key="ollama",
            base_url=base_url,
        )

    raise RuntimeError(
        "No LLM configured. Set ONE of: ANTHROPIC_API_KEY, OPENAI_API_KEY, "
        "DEEPSEEK_API_KEY, or OLLAMA_MODEL in .env"
    )


# ─── Neo4j Schema (for grounding the LLM) ────────────────────────────────────
NEO4J_SCHEMA = """
Node Labels and Properties (FIBO-aligned, populated from real GLEIF + ontology-guided enrichment):
- LegalEntity: id (= LEI for GLEIF entities, or EXT_<slug> for LLM-extracted),
               lei, name, jurisdiction (ISO alpha-2), jurisdictionName,
               category (CORPORATION|FUND|BRANCH|TRUST|PARTNERSHIP|LIMITED_PARTNERSHIP),
               legalForm, isActive, hasOperationalAddress,
               kycRiskScore, riskTier (low|medium|high|critical),
               city, country, postalCode, hqCity, hqCountry,
               description, dataSource (GLEIF | LLM_EXTRACTED), needsVerification,
               sourceArticles, uri (GLEIF URL).
- NaturalPerson: id, name, nationality (ISO alpha-2), role,
                 isPEP (bool), isSanctioned (bool), dataSource, sourceArticles.
- n4sch__Class / n4sch__Property: FIBO ontology structure (loaded via n10s).
- Resource: n10s convention (every URI-typed node).

Relationships (controlled vocabulary derived from FIBO + KYC ontology):
- (LegalEntity)-[:DIRECTLY_OWNED_BY {percentage, since, source}]->(LegalEntity)
     → A is directly held by B with given equity %.
- (LegalEntity)-[:CONTROLLED_BY {role, since, source}]->(NaturalPerson)
     → entity is controlled by an individual (CEO, founder, UBO, trustee, ...).
- (LegalEntity)-[:HAS_JURISDICTION]->(LegalEntity|Resource)  (where applicable)
- (LegalEntity)-[:INSTANCE_OF]->(n4sch__Class)
     → semantic typing link to FIBO class hierarchy.
- (LegalEntity)-[:OWNS]->(LegalEntity)  (inverse of DIRECTLY_OWNED_BY)

Key domain facts:
- All :LegalEntity nodes carry real LEI codes when dataSource='GLEIF'.
- Offshore jurisdictions: KY (Cayman), VG (BVI), PA (Panama), SC (Seychelles), BS (Bahamas), BM (Bermuda).
- Risk tiers: low (0-24), medium (25-49), high (50-74), critical (75-100).
- UBO discovery: traverse DIRECTLY_OWNED_BY chain then CONTROLLED_BY to find ultimate controllers.
- Use semantic_search_entities for fuzzy/similar lookup over real-entity descriptions.
- Use run_custom_cypher for any Cypher query against this schema.
- Use query_ontology to ask FIBO/SHACL/GLEIF questions via SPARQL.
"""

GRAPHDB_SCHEMA = """
GraphDB contains RDF/OWL ontologies:
- FIBO (Financial Industry Business Ontology) classes for legal entities, ownership, control
- GLEIF entity instances mapped to FIBO classes
- KYC application ontology extending FIBO (named graph http://kg/kyc/ontology)
- FIBO↔GLEIF mapping (named graph http://kg/mapping/fibo2glei)
- LCC ISO-3166 country codes
- Named graphs: http://kg/fibo, http://kg/glei/instances, http://kg/kyc/ontology,
  http://kg/mapping/fibo2glei, http://kg/lcc/iso3166
Use SPARQL SELECT queries only. No INSERT/DELETE/UPDATE.
"""


# ─── Tools ────────────────────────────────────────────────────────────────────

@tool
def find_ultimate_beneficial_owners(entity_id: str) -> str:
    """Find the Ultimate Beneficial Owner(s) of a legal entity by traversing
    the ownership chain up to 6 hops. Shows who really controls the entity.

    Args:
        entity_id: The entity ID, e.g. 'ENTITY_0042'.
    """
    rows = _neo.query("""
        MATCH path = (e:LegalEntity {id: $id})
              -[:DIRECTLY_OWNED_BY*0..6]->()
              -[:CONTROLLED_BY]->(p:NaturalPerson)
        RETURN p.id AS person_id, p.name AS name, p.nationality AS nationality,
               p.isPEP AS is_pep, p.isSanctioned AS is_sanctioned,
               length(path) AS chain_length,
               [n IN nodes(path) | coalesce(n.name, n.id)] AS chain
        ORDER BY chain_length, name
        LIMIT 10
    """, {"id": entity_id})
    if not rows:
        return f"No UBO found for {entity_id}. The entity may have no ownership chain to a natural person."
    out = [f"UBO analysis for {entity_id} — {len(rows)} beneficial owner(s):"]
    for r in rows:
        flags = []
        if r["is_sanctioned"]: flags.append("🔴 SANCTIONED")
        if r["is_pep"]: flags.append("🟣 PEP")
        flag = f"  [{', '.join(flags)}]" if flags else ""
        chain_str = " → ".join(str(x) for x in r["chain"])
        out.append(f"  • {r['name']} ({r['nationality']}) — {r['chain_length']} hops{flag}")
        out.append(f"    Chain: {chain_str}")
    return "\n".join(out)


@tool
def check_sanctions_exposure(entity_id: str) -> str:
    """Check if an entity has any sanctioned persons in its ownership/control
    chain at any depth. Critical for KYC compliance.

    Args:
        entity_id: The entity ID to check.
    """
    rows = _neo.query("""
        MATCH (e:LegalEntity {id: $id})
        OPTIONAL MATCH path = (e)-[:DIRECTLY_OWNED_BY*0..6]->()
                              -[:CONTROLLED_BY]->(p:NaturalPerson {isSanctioned: true})
        RETURN e.name AS entity_name, e.kycRiskScore AS score,
               p.name AS sanctioned_person, p.nationality AS nationality,
               length(path) AS hops
        LIMIT 10
    """, {"id": entity_id})
    if not rows:
        return f"Entity {entity_id} not found in the knowledge graph."
    if not rows[0]["sanctioned_person"]:
        return f"✅ NO sanctioned person found in ownership chain of {entity_id} ({rows[0]['entity_name']})."
    ename = rows[0]["entity_name"]
    out = [f"⚠️ SANCTIONS EXPOSURE for {entity_id} ({ename}, risk score {rows[0]['score']}):"]
    for r in rows:
        if r["sanctioned_person"]:
            out.append(f"  🔴 {r['sanctioned_person']} ({r['nationality']}) — {r['hops']} hops away")
    return "\n".join(out)


@tool
def get_entity_risk_profile(entity_id: str) -> str:
    """Get comprehensive risk profile for an entity including KYC score,
    risk tier, GDS analytics (PageRank, betweenness, community), ownership
    structure, and red flags.

    Args:
        entity_id: The entity ID.
    """
    rows = _neo.query("""
        MATCH (e:LegalEntity {id: $id})
        OPTIONAL MATCH (e)-[:CONTROLLED_BY]->(ctrl:NaturalPerson)
        OPTIONAL MATCH (e)-[r:DIRECTLY_OWNED_BY]->(parent:LegalEntity)
        OPTIONAL MATCH (child:LegalEntity)-[:DIRECTLY_OWNED_BY]->(e)
        OPTIONAL MATCH (e)-[t:TRANSACTION]-(other:LegalEntity)
        WITH e, collect(DISTINCT {name: ctrl.name, pep: ctrl.isPEP, sanctioned: ctrl.isSanctioned}) AS controllers,
             count(DISTINCT parent) AS parentCount, count(DISTINCT child) AS childCount,
             count(DISTINCT t) AS txnCount,
             sum(CASE WHEN t.isSuspicious THEN 1 ELSE 0 END) AS suspiciousTxns
        RETURN e {.*} AS entity, controllers, parentCount, childCount, txnCount, suspiciousTxns
    """, {"id": entity_id})
    if not rows:
        return f"Entity {entity_id} not found."
    r = rows[0]
    e = r["entity"]
    ctrls = [c for c in r["controllers"] if c.get("name")]
    flags = []
    if e.get("sccComponentId") is not None: flags.append("In circular ownership ring")
    if not e.get("hasOperationalAddress"): flags.append("No operational address (shell indicator)")
    if e.get("jurisdiction") in ["KY", "VG", "PA", "SC"]: flags.append("Offshore jurisdiction")
    for c in ctrls:
        if c.get("sanctioned"): flags.append(f"Sanctioned controller: {c['name']}")
        if c.get("pep"): flags.append(f"PEP controller: {c['name']}")
    if r["suspiciousTxns"] > 0: flags.append(f"{r['suspiciousTxns']} suspicious transactions")

    out = [
        f"Risk Profile: {e.get('name')} ({entity_id})",
        f"  Jurisdiction: {e.get('jurisdiction')} ({e.get('jurisdictionName', '')})",
        f"  Category: {e.get('category')}",
        f"  Risk Tier: {e.get('riskTier')} | KYC Score: {e.get('kycRiskScore')}/100",
        f"  PageRank: {e.get('pageRankScore', 0):.4f} | Betweenness: {e.get('betweennessScore', 0):.2f}",
        f"  Community: #{e.get('louvainCommunityId')} | SCC Ring: {e.get('sccComponentId', 'None')}",
        f"  Parents: {r['parentCount']} | Children: {r['childCount']} | Transactions: {r['txnCount']}",
        f"  Controllers: {', '.join(c['name'] for c in ctrls) if ctrls else 'None'}",
        f"  Red Flags: {'; '.join(flags) if flags else 'None detected'}",
    ]
    return "\n".join(out)


@tool
def find_circular_ownership_rings() -> str:
    """Find all circular ownership rings (entities owning each other in loops).
    Strong indicator of shell company structures or layered ownership for
    obfuscation purposes."""
    rows = _neo.query("""
        MATCH (e:LegalEntity)
        WHERE e.sccComponentId IS NOT NULL
        WITH e.sccComponentId AS ring, collect(e) AS members
        WHERE size(members) > 1
        RETURN ring,
               [m IN members | {id: m.id, name: m.name, jur: m.jurisdiction, score: m.kycRiskScore}] AS entities,
               size(members) AS ringSize,
               toInteger(reduce(s=0.0, m IN members | s + m.kycRiskScore) / size(members)) AS avgRisk
        ORDER BY avgRisk DESC
    """)
    if not rows:
        return "No circular ownership rings detected in the graph."
    out = [f"Found {len(rows)} circular ownership ring(s):\n"]
    for r in rows:
        ents = r["entities"]
        out.append(f"Ring #{r['ring']} — {r['ringSize']} entities, avg risk: {r['avgRisk']}")
        for e in ents[:5]:
            out.append(f"  • {e['id']} {e['name']} ({e['jur']}) — score {e['score']}")
        if len(ents) > 5:
            out.append(f"  ... and {len(ents) - 5} more")
        out.append("")
    return "\n".join(out)


@tool
def find_shell_companies() -> str:
    """Find entities that appear to be shell companies (no operational address,
    potentially in offshore jurisdictions). Shell companies are high-risk for
    money laundering and ownership obfuscation."""
    rows = _neo.query("""
        MATCH (e:LegalEntity {hasOperationalAddress: false})
        OPTIONAL MATCH (e)-[:CONTROLLED_BY]->(p:NaturalPerson)
        RETURN e.id AS id, e.name AS name, e.jurisdiction AS jur,
               e.kycRiskScore AS score, e.riskTier AS tier,
               e.sccComponentId AS ring,
               collect(DISTINCT {name: p.name, sanctioned: p.isSanctioned, pep: p.isPEP}) AS controllers
        ORDER BY e.kycRiskScore DESC LIMIT 20
    """)
    if not rows:
        return "No shell companies detected."
    out = [f"Found {len(rows)} potential shell companies (no operational address):\n"]
    for r in rows:
        flags = []
        for c in r["controllers"]:
            if c.get("sanctioned"): flags.append(f"🔴 {c['name']}")
            if c.get("pep"): flags.append(f"🟣 {c['name']}")
        ring_note = f" [Ring #{r['ring']}]" if r["ring"] is not None else ""
        out.append(f"  {r['id']} {r['name']} ({r['jur']}) — score {r['score']}, tier {r['tier']}{ring_note}")
        if flags:
            out.append(f"    Controllers: {', '.join(flags)}")
    return "\n".join(out)


@tool
def get_top_risk_entities(limit: int = 15) -> str:
    """Get the entities with highest KYC risk scores. Useful for identifying
    the most concerning entities in the knowledge graph.

    Args:
        limit: Number of results (default 15, max 50).
    """
    limit = min(max(limit, 1), 50)
    rows = _neo.query("""
        MATCH (e:LegalEntity)
        WHERE e.kycRiskScore > 0
        RETURN e.id AS id, e.name AS name, e.jurisdiction AS jur,
               e.kycRiskScore AS score, e.riskTier AS tier, e.category AS category,
               e.sccComponentId AS ring
        ORDER BY e.kycRiskScore DESC LIMIT $limit
    """, {"limit": limit})
    out = [f"Top {len(rows)} riskiest entities:\n"]
    for r in rows:
        ring = f" [Ring #{r['ring']}]" if r["ring"] is not None else ""
        out.append(f"  [{r['score']:>3}] {r['id']}  {r['name']}  ({r['jur']}, {r['category']}){ring}")
    return "\n".join(out)


@tool
def find_pep_connections() -> str:
    """Find all Politically Exposed Persons (PEPs) and the entities they
    control. PEPs require enhanced due diligence."""
    rows = _neo.query("""
        MATCH (p:NaturalPerson {isPEP: true})<-[:CONTROLLED_BY]-(e:LegalEntity)
        RETURN p.id AS pepId, p.name AS pepName, p.nationality AS nationality,
               collect({id: e.id, name: e.name, jur: e.jurisdiction, score: e.kycRiskScore}) AS entities,
               count(e) AS entityCount
        ORDER BY entityCount DESC
    """)
    if not rows:
        return "No PEP connections found."
    out = [f"Found {len(rows)} PEPs controlling entities:\n"]
    for r in rows:
        out.append(f"  🟣 {r['pepName']} ({r['nationality']}) — controls {r['entityCount']} entities:")
        for e in r["entities"][:5]:
            out.append(f"    • {e['id']} {e['name']} ({e['jur']}, score {e['score']})")
    return "\n".join(out)


@tool
def find_sanctioned_persons() -> str:
    """List all sanctioned persons and entities they control/are linked to.
    Critical for compliance — these connections must be reported."""
    rows = _neo.query("""
        MATCH (p:NaturalPerson {isSanctioned: true})
        OPTIONAL MATCH (p)<-[:CONTROLLED_BY]-(e:LegalEntity)
        RETURN p.id AS id, p.name AS name, p.nationality AS nationality,
               collect({id: e.id, name: e.name, jur: e.jurisdiction, score: e.kycRiskScore}) AS entities
        ORDER BY p.name
    """)
    if not rows:
        return "No sanctioned persons in the graph."
    out = [f"Found {len(rows)} sanctioned persons:\n"]
    for r in rows:
        ents = r["entities"]
        ent_str = ", ".join(f"{e['id']} ({e['jur']})" for e in ents if e.get("id")) if ents else "None"
        out.append(f"  🔴 {r['name']} ({r['nationality']}) — controls: {ent_str}")
    return "\n".join(out)


@tool
def analyze_suspicious_transactions(entity_id: str | None = None) -> str:
    """Find suspicious transactions, optionally filtered to a specific entity.
    Shows transaction details including amount, counterparty, and date.

    Args:
        entity_id: Optional entity ID to filter to. If None, shows all suspicious transactions.
    """
    if entity_id:
        rows = _neo.query("""
            MATCH (a:LegalEntity {id: $id})-[t:TRANSACTION {isSuspicious: true}]-(b:LegalEntity)
            RETURN a.id AS from_id, a.name AS from_name,
                   b.id AS to_id, b.name AS to_name,
                   t.amount AS amount, t.currency AS currency, t.date AS date
            ORDER BY t.amount DESC LIMIT 20
        """, {"id": entity_id})
    else:
        rows = _neo.query("""
            MATCH (a:LegalEntity)-[t:TRANSACTION {isSuspicious: true}]->(b:LegalEntity)
            RETURN a.id AS from_id, a.name AS from_name,
                   b.id AS to_id, b.name AS to_name,
                   t.amount AS amount, t.currency AS currency, t.date AS date
            ORDER BY t.amount DESC LIMIT 20
        """)
    if not rows:
        return f"No suspicious transactions found{' for ' + entity_id if entity_id else ''}."
    out = [f"Suspicious transactions{' for ' + entity_id if entity_id else ''} ({len(rows)} found):\n"]
    for r in rows:
        out.append(f"  {r['from_id']} ({r['from_name']}) → {r['to_id']} ({r['to_name']})")
        out.append(f"    Amount: {r['currency']} {r['amount']:,.2f} | Date: {r['date']}")
    return "\n".join(out)


@tool
def find_path_between_entities(entity_id_1: str, entity_id_2: str) -> str:
    """Find the shortest ownership path between two entities. Useful for
    discovering hidden connections between apparently unrelated companies.

    Args:
        entity_id_1: First entity ID (e.g. 'ENTITY_0001').
        entity_id_2: Second entity ID (e.g. 'ENTITY_0050').
    """
    rows = _neo.query("""
        MATCH (a:LegalEntity {id: $id1}), (b:LegalEntity {id: $id2}),
              path = shortestPath((a)-[*..10]-(b))
        RETURN [n IN nodes(path) | coalesce(n.name, n.id)] AS chain,
               [r IN relationships(path) | type(r)] AS relTypes,
               length(path) AS hops
    """, {"id1": entity_id_1, "id2": entity_id_2})
    if not rows:
        return f"No path found between {entity_id_1} and {entity_id_2} within 10 hops."
    r = rows[0]
    chain_parts = []
    for i, name in enumerate(r["chain"]):
        chain_parts.append(str(name))
        if i < len(r["relTypes"]):
            chain_parts.append(f" —[{r['relTypes'][i]}]→ ")
    return f"Path between {entity_id_1} and {entity_id_2} ({r['hops']} hops):\n  {''.join(chain_parts)}"


@tool
def get_entity_detail(entity_id: str) -> str:
    """Get complete details for an entity including all properties and relationships.

    Args:
        entity_id: The entity ID or name.
    """
    # Try by id first, then by name
    rows = _neo.query("""
        MATCH (e) WHERE e.id = $id OR toLower(e.name) = toLower($id)
        OPTIONAL MATCH (e)-[r]->(target)
        WITH e, collect(DISTINCT {type: type(r), target: target.name, targetLabels: labels(target)}) AS outgoing
        OPTIONAL MATCH (source)-[r2]->(e)
        WITH e, outgoing, collect(DISTINCT {type: type(r2), source: source.name, sourceLabels: labels(source)}) AS incoming
        RETURN e {.*} AS entity, labels(e) AS labels, outgoing, incoming
    """, {"id": entity_id})
    if not rows:
        return f"Entity '{entity_id}' not found. Try search_entity_by_name first."
    r = rows[0]
    e = r["entity"]
    lbls = r["labels"]

    out = [f"Entity: {e.get('name', entity_id)} [{', '.join(lbls)}]"]
    # Show all properties
    for k, v in sorted(e.items()):
        if k not in ("embedding",) and v is not None:
            out.append(f"  {k}: {v}")

    # Outgoing relationships
    outgoing = [o for o in r["outgoing"] if o.get("target")]
    if outgoing:
        out.append("\nOutgoing relationships:")
        for o in outgoing:
            out.append(f"  -[{o['type']}]-> {o['target']} [{','.join(o.get('targetLabels',[]))}]")

    # Incoming relationships
    incoming = [i for i in r["incoming"] if i.get("source")]
    if incoming:
        out.append("\nIncoming relationships:")
        for i in incoming:
            out.append(f"  <-[{i['type']}]- {i['source']} [{','.join(i.get('sourceLabels',[]))}]")

    return "\n".join(out)


@tool
def compare_entities(entity_id_1: str, entity_id_2: str) -> str:
    """Compare two entities side by side — risk scores, jurisdictions,
    categories, and key metrics.

    Args:
        entity_id_1: First entity ID.
        entity_id_2: Second entity ID.
    """
    rows = _neo.query("""
        MATCH (e:LegalEntity) WHERE e.id IN [$id1, $id2]
        RETURN e.id AS id, e.name AS name, e.jurisdiction AS jur,
               e.category AS category, e.kycRiskScore AS score,
               e.riskTier AS tier, e.pageRankScore AS pageRank,
               e.betweennessScore AS betweenness,
               e.louvainCommunityId AS community,
               e.sccComponentId AS scc,
               e.hasOperationalAddress AS hasAddr
        ORDER BY e.id
    """, {"id1": entity_id_1, "id2": entity_id_2})
    if len(rows) < 2:
        return f"Could not find both entities. Found: {[r['id'] for r in rows]}"
    a, b = rows[0], rows[1]
    return (
        f"Comparison:\n"
        f"{'Metric':<20} {a['id']:<25} {b['id']:<25}\n"
        f"{'─'*70}\n"
        f"{'Name':<20} {a['name']:<25} {b['name']:<25}\n"
        f"{'Jurisdiction':<20} {a['jur']:<25} {b['jur']:<25}\n"
        f"{'Category':<20} {a['category']:<25} {b['category']:<25}\n"
        f"{'Risk Score':<20} {str(a['score']):<25} {str(b['score']):<25}\n"
        f"{'Risk Tier':<20} {a['tier']:<25} {b['tier']:<25}\n"
        f"{'PageRank':<20} {a['pageRank']:.4f}{'':<20} {b['pageRank']:.4f}\n"
        f"{'Community':<20} {str(a['community']):<25} {str(b['community']):<25}\n"
        f"{'In Ring':<20} {str(a['scc'] is not None):<25} {str(b['scc'] is not None):<25}\n"
        f"{'Has Address':<20} {str(a['hasAddr']):<25} {str(b['hasAddr']):<25}"
    )


@tool
def get_jurisdiction_risk_analysis(jurisdiction: str | None = None) -> str:
    """Analyze risk by jurisdiction. If a specific jurisdiction is given,
    show details for it. Otherwise show a summary across all jurisdictions.

    Args:
        jurisdiction: Optional 2-letter country code (e.g. 'KY', 'US', 'DE'). If None, shows all.
    """
    if jurisdiction:
        rows = _neo.query("""
            MATCH (e:LegalEntity {jurisdiction: $jur})
            RETURN e.jurisdiction AS jur, count(e) AS cnt,
                   toInteger(avg(e.kycRiskScore)) AS avgRisk,
                   max(e.kycRiskScore) AS maxRisk,
                   collect(CASE WHEN e.kycRiskScore >= 70 THEN e.id END) AS criticalEntities
        """, {"jur": jurisdiction.upper()})
    else:
        rows = _neo.query("""
            MATCH (e:LegalEntity)
            WITH e.jurisdiction AS jur, collect(e) AS entities
            RETURN jur, size(entities) AS cnt,
                   toInteger(reduce(s=0.0, e IN entities | s + e.kycRiskScore) / size(entities)) AS avgRisk,
                   max([e IN entities | e.kycRiskScore]) AS maxRisk
            ORDER BY avgRisk DESC
        """)
    if not rows:
        return f"No data for jurisdiction {jurisdiction}." if jurisdiction else "No jurisdictions found."
    out = ["Jurisdiction Risk Analysis:\n"]
    for r in rows:
        critical = f" | Critical: {', '.join(str(e) for e in r.get('criticalEntities', []) if e)}" if r.get("criticalEntities") else ""
        out.append(f"  {r['jur']}: {r['cnt']} entities, avg risk {r['avgRisk']}, max {r['maxRisk']}{critical}")
    return "\n".join(out)


@tool
def get_community_analysis(community_id: int | None = None) -> str:
    """Analyze Louvain community clusters. If a community ID is given, show
    its members. Otherwise show summary of all communities.

    Args:
        community_id: Optional community number. If None, shows summary of all communities.
    """
    if community_id is not None:
        rows = _neo.query("""
            MATCH (e:LegalEntity {louvainCommunityId: $cid})
            RETURN e.id AS id, e.name AS name, e.jurisdiction AS jur,
                   e.kycRiskScore AS score, e.category AS category
            ORDER BY e.kycRiskScore DESC
        """, {"cid": community_id})
        if not rows:
            return f"Community #{community_id} not found."
        out = [f"Community #{community_id} — {len(rows)} members:\n"]
        for r in rows:
            out.append(f"  {r['id']} {r['name']} ({r['jur']}, {r['category']}) — score {r['score']}")
        return "\n".join(out)
    else:
        rows = _neo.query("""
            MATCH (e:LegalEntity) WHERE e.louvainCommunityId IS NOT NULL
            WITH e.louvainCommunityId AS cid, collect(e) AS members
            RETURN cid,
                   size(members) AS cnt,
                   toInteger(reduce(s=0.0, m IN members | s + m.kycRiskScore) / size(members)) AS avgRisk,
                   collect(DISTINCT [m IN members | m.jurisdiction][0]) AS jurisdictions
            ORDER BY avgRisk DESC
        """)
        out = [f"Found {len(rows)} Louvain communities:\n"]
        for r in rows:
            out.append(f"  Community #{r['cid']}: {r['cnt']} members, avg risk {r['avgRisk']}")
        return "\n".join(out)


@tool
def get_graph_statistics() -> str:
    """Get overall statistics of the knowledge graph — node counts, relationship
    counts, and label distribution."""
    label_counts = _neo.query("""
        CALL db.labels() YIELD label
        CALL apoc.cypher.run('MATCH (n:`' + label + '`) RETURN count(n) AS c', {}) YIELD value
        RETURN label, value.c AS count ORDER BY value.c DESC
    """)
    if not label_counts:
        # Fallback without APOC
        label_counts = _neo.query("""
            MATCH (n) WITH labels(n) AS lbls UNWIND lbls AS label
            RETURN label, count(*) AS count ORDER BY count DESC
        """)
    rel_counts = _neo.query("""
        MATCH ()-[r]->() RETURN type(r) AS rel_type, count(r) AS count
        ORDER BY count DESC LIMIT 15
    """)
    total_nodes = _neo.query("MATCH (n) RETURN count(n) AS c")[0]["c"]
    total_rels = _neo.query("MATCH ()-[r]->() RETURN count(r) AS c")[0]["c"]
    labels_str = "\n".join(f"  {r['label']}: {r['count']}" for r in label_counts)
    rels_str = "\n".join(f"  {r['rel_type']}: {r['count']}" for r in rel_counts)
    return (
        f"Knowledge Graph Statistics:\n"
        f"  Total Nodes: {total_nodes}\n"
        f"  Total Relationships: {total_rels}\n\n"
        f"Node Labels:\n{labels_str}\n\n"
        f"Relationship Types:\n{rels_str}"
    )


@tool
def find_money_laundering_indicators(entity_id: str | None = None) -> str:
    """Detect potential money laundering indicators: entities combining multiple
    red flags (high risk + suspicious txns + shell + offshore + sanctions links).

    Args:
        entity_id: Optional entity to check. If None, scans all entities for multi-flag patterns.
    """
    if entity_id:
        rows = _neo.query("""
            MATCH (e:LegalEntity {id: $id})
            OPTIONAL MATCH (e)-[t:TRANSACTION {isSuspicious: true}]-(other)
            WITH e, count(t) AS suspTxns
            OPTIONAL MATCH (e)-[:DIRECTLY_OWNED_BY*0..4]->()-[:CONTROLLED_BY]->(p:NaturalPerson)
            WHERE p.isSanctioned = true OR p.isPEP = true
            WITH e, suspTxns,
                 collect(DISTINCT CASE WHEN p.isSanctioned THEN p.name END) AS sanctioned,
                 collect(DISTINCT CASE WHEN p.isPEP THEN p.name END) AS peps
            RETURN e.id AS id, e.name AS name, e.jurisdiction AS jur,
                   e.kycRiskScore AS score,
                   e.hasOperationalAddress AS hasAddr,
                   e.sccComponentId AS ring,
                   suspTxns,
                   [s IN sanctioned WHERE s IS NOT NULL] AS sanctionedLinks,
                   [p IN peps WHERE p IS NOT NULL] AS pepLinks
        """, {"id": entity_id})
    else:
        rows = _neo.query("""
            MATCH (e:LegalEntity) WHERE e.kycRiskScore >= 35
            OPTIONAL MATCH (e)-[t:TRANSACTION {isSuspicious: true}]-(other)
            WITH e, count(t) AS suspTxns
            WHERE suspTxns > 0 OR e.hasOperationalAddress = false OR e.sccComponentId IS NOT NULL
            RETURN e.id AS id, e.name AS name, e.jurisdiction AS jur,
                   e.kycRiskScore AS score,
                   e.hasOperationalAddress AS hasAddr,
                   e.sccComponentId AS ring,
                   suspTxns
            ORDER BY e.kycRiskScore DESC LIMIT 15
        """)
    if not rows:
        return f"No money laundering indicators found{' for ' + entity_id if entity_id else ''}."
    out = ["Money Laundering Risk Indicators:\n"]
    for r in rows:
        flags = []
        if r["score"] >= 50: flags.append("HIGH RISK SCORE")
        if not r.get("hasAddr", True): flags.append("SHELL (no address)")
        if r.get("ring") is not None: flags.append("CIRCULAR OWNERSHIP")
        if r.get("jur") in ["KY", "VG", "PA", "SC"]: flags.append("OFFSHORE")
        if r.get("suspTxns", 0) > 0: flags.append(f"{r['suspTxns']} SUSP. TXNS")
        if r.get("sanctionedLinks"): flags.append(f"SANCTIONED: {', '.join(r['sanctionedLinks'])}")
        if r.get("pepLinks"): flags.append(f"PEP: {', '.join(r['pepLinks'])}")
        out.append(f"  {r['id']} {r['name']} ({r['jur']}, score {r['score']})")
        out.append(f"    Flags: {' | '.join(flags)}")
    return "\n".join(out)


@tool
def query_ontology(question: str) -> str:
    """Query the FIBO/GLEIF/KYC ontology in GraphDB using SPARQL. Use this for
    questions about how the ontology models ownership, what classes exist,
    how ontologies relate, entity types, etc.

    Args:
        question: Natural language question about the ontology.
    """
    # Map common ontology questions to SPARQL queries
    q_lower = question.lower()

    if "class" in q_lower and ("fibo" in q_lower or "ontology" in q_lower or "what" in q_lower):
        sparql = """PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?class ?label WHERE {
  ?class a owl:Class . ?class rdfs:label ?label .
} ORDER BY ?label LIMIT 30"""
    elif "ownership" in q_lower or "control" in q_lower:
        sparql = """PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?class ?label ?parent ?parentLabel WHERE {
  ?class a owl:Class . ?class rdfs:label ?label .
  OPTIONAL { ?class rdfs:subClassOf ?parent . ?parent rdfs:label ?parentLabel . FILTER(isIRI(?parent)) }
  FILTER(CONTAINS(STR(?class), "Ownership") || CONTAINS(STR(?class), "Control") ||
         CONTAINS(STR(?class), "Owner") || CONTAINS(STR(?class), "Shareholder"))
} ORDER BY ?label"""
    elif "named graph" in q_lower or "what graph" in q_lower:
        sparql = """SELECT ?graph (COUNT(*) AS ?triples) WHERE {
  GRAPH ?graph { ?s ?p ?o }
} GROUP BY ?graph ORDER BY DESC(?triples)"""
    elif "gleif" in q_lower and ("type" in q_lower or "classif" in q_lower):
        sparql = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?type (COUNT(?entity) AS ?count) ?typeLabel WHERE {
  GRAPH <http://kg/glei/instances> { ?entity rdf:type ?type . }
  OPTIONAL { ?type rdfs:label ?typeLabel }
  FILTER(?type != <http://www.w3.org/2002/07/owl#NamedIndividual>)
} GROUP BY ?type ?typeLabel ORDER BY DESC(?count)"""
    elif "cross" in q_lower and "ontology" in q_lower:
        sparql = """PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?subject ?predicate ?object WHERE {
  { ?subject owl:equivalentClass ?object . BIND(owl:equivalentClass AS ?predicate) }
  UNION {
    ?subject rdfs:subClassOf ?object . FILTER(isIRI(?object))
    FILTER((CONTAINS(STR(?subject), "kyc-kg") && CONTAINS(STR(?object), "edmcouncil")) ||
           (CONTAINS(STR(?subject), "edmcouncil") && CONTAINS(STR(?object), "kyc-kg")))
    BIND(rdfs:subClassOf AS ?predicate)
  }
} LIMIT 30"""
    elif "country" in q_lower or "iso" in q_lower:
        sparql = """PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?country ?label WHERE {
  GRAPH <http://kg/lcc/iso3166> {
    ?country a ?type . ?country rdfs:label ?label .
    FILTER(LANG(?label) = "en" || LANG(?label) = "")
  }
} ORDER BY ?label LIMIT 25"""
    else:
        # Generic ontology exploration
        sparql = """PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?class ?label ?comment WHERE {
  ?class a owl:Class .
  OPTIONAL { ?class rdfs:label ?label }
  OPTIONAL { ?class rdfs:comment ?comment }
  FILTER(CONTAINS(STR(?class), "edmcouncil") || CONTAINS(STR(?class), "kyc-kg"))
} ORDER BY ?label LIMIT 20"""

    try:
        rows = _gdb.query(sparql)
        if not rows:
            return "No results from ontology query."
        # Format results as text
        out = [f"SPARQL results ({len(rows)} rows):\n"]
        for r in rows[:20]:
            parts = []
            for k, v in r.items():
                # Shorten URIs for readability
                val = str(v)
                if val.startswith("http"):
                    val = val.split("/")[-1].split("#")[-1]
                parts.append(f"{k}: {val}")
            out.append(f"  {' | '.join(parts)}")
        if len(rows) > 20:
            out.append(f"  ... and {len(rows) - 20} more rows")
        return "\n".join(out)
    except Exception as e:
        return f"SPARQL query failed: {e}"


@tool
def run_custom_cypher(cypher_query: str) -> str:
    """Execute a custom read-only Cypher query against Neo4j. Use this ONLY when
    no other tool can answer the question. The query MUST be read-only (no CREATE,
    DELETE, SET, MERGE, REMOVE operations).

    IMPORTANT: Only generate Cypher that uses the known schema:
    - Node labels: LegalEntity, NaturalPerson
    - Relationships: DIRECTLY_OWNED_BY, CONTROLLED_BY, TRANSACTION
    - Use properties exactly as documented in the schema.

    Args:
        cypher_query: A valid read-only Cypher query.
    """
    # Security: block write operations
    upper = cypher_query.upper()
    write_ops = ["CREATE", "DELETE", "SET ", "MERGE", "REMOVE", "DROP", "DETACH"]
    for op in write_ops:
        if op in upper:
            return f"ERROR: Write operation '{op.strip()}' not allowed. Only read queries permitted."

    try:
        rows = _neo.query(cypher_query)
        if not rows:
            return "Query returned no results."
        # Format up to 20 rows
        out = [f"Query returned {len(rows)} row(s):\n"]
        for r in rows[:20]:
            parts = []
            for k, v in r.items():
                if isinstance(v, float):
                    parts.append(f"{k}: {v:.4f}")
                elif isinstance(v, list):
                    parts.append(f"{k}: [{', '.join(str(x) for x in v[:5])}{'...' if len(v) > 5 else ''}]")
                else:
                    parts.append(f"{k}: {v}")
            out.append(f"  {' | '.join(parts)}")
        if len(rows) > 20:
            out.append(f"  ... and {len(rows) - 20} more rows")
        return "\n".join(out)
    except Exception as e:
        return f"Cypher execution error: {e}"


@tool
def find_hidden_controllers() -> str:
    """Find persons who exert control over many entities through indirect
    ownership chains (multiple hops). These 'shadow controllers' may be
    using corporate layers to obscure their influence."""
    rows = _neo.query("""
        MATCH path = (e:LegalEntity)-[:DIRECTLY_OWNED_BY*1..6]->()
              -[:CONTROLLED_BY]->(p:NaturalPerson)
        WHERE length(path) >= 3
        WITH p, collect(DISTINCT e) AS indirectEntities,
             collect(DISTINCT e.jurisdiction) AS jurisdictions
        WHERE size(indirectEntities) >= 2
        OPTIONAL MATCH (direct:LegalEntity)-[:CONTROLLED_BY]->(p)
        WITH p, indirectEntities, jurisdictions,
             collect(DISTINCT direct) AS directEntities
        RETURN p.name AS name, p.nationality AS nationality,
               p.isPEP AS isPEP, p.isSanctioned AS isSanctioned,
               size(directEntities) AS directControl,
               size(indirectEntities) AS indirectControl,
               jurisdictions
        ORDER BY size(indirectEntities) + size(directEntities) DESC LIMIT 10
    """)
    if not rows:
        return "No hidden controllers found with significant indirect influence."
    out = ["Hidden Controllers (shadow ownership via multi-layer chains):\n"]
    for r in rows:
        flags = []
        if r["isSanctioned"]: flags.append("🔴 SANCTIONED")
        if r["isPEP"]: flags.append("🟣 PEP")
        flag_str = f" {' '.join(flags)}" if flags else ""
        juris = ", ".join(str(j) for j in r["jurisdictions"][:5])
        out.append(f"  {r['name']} ({r['nationality']}){flag_str}")
        out.append(f"    Direct: {r['directControl']} | Indirect: {r['indirectControl']} | Jurisdictions: {juris}")
    return "\n".join(out)


@tool
def search_entity_by_name(name: str) -> str:
    """Search for legal entities or natural persons by name (case-insensitive partial match).
    Use this when the user mentions an entity by name instead of ID.

    Args:
        name: Full or partial name to search for.
    """
    rows = _neo.query("""
        MATCH (n)
        WHERE (n:LegalEntity OR n:NaturalPerson)
          AND toLower(n.name) CONTAINS toLower($name)
        RETURN labels(n) AS labels, n.id AS id, n.name AS name,
               n.lei AS lei, n.jurisdiction AS jurisdiction,
               n.nationality AS nationality, n.kycRiskScore AS riskScore,
               n.dataSource AS dataSource
        ORDER BY n.name LIMIT 10
    """, {"name": name})
    if not rows:
        return f"No legal entities or natural persons found matching '{name}'."
    out = [f"Search results for '{name}' ({len(rows)} found):\n"]
    for r in rows:
        lbls = r["labels"]
        is_le = "LegalEntity" in lbls
        label = "LegalEntity" if is_le else "NaturalPerson"
        extras = []
        if is_le:
            if r.get("lei"):          extras.append(f"LEI={r['lei']}")
            if r.get("jurisdiction"): extras.append(f"jur={r['jurisdiction']}")
            if r.get("riskScore") is not None: extras.append(f"risk={r['riskScore']}")
        else:
            if r.get("nationality"):  extras.append(f"nat={r['nationality']}")
        if r.get("dataSource"):       extras.append(f"src={r['dataSource']}")
        extra_str = f"  ({', '.join(extras)})" if extras else ""
        out.append(f"  [{label}] {r['id']} — {r['name']}{extra_str}")
    return "\n".join(out)


# ─── Vector Search Tools ─────────────────────────────────────────────────────

@tool
def semantic_search_entities(query: str, k: int = 5) -> str:
    """Search entities using semantic similarity (vector embeddings).
    This finds entities based on meaning, not just exact name match.
    Useful when you don't know the exact entity name or want related entities.

    Args:
        query: Natural language description of what to find (e.g. 'banks involved in money laundering').
        k: Number of results to return (default 5).
    """
    vs = _get_vector_store()
    if vs is None:
        return "Vector search not available. Use search_entity_by_name for text search."

    try:
        results = vs.similarity_search_with_score(query, k=min(k, 10))
        if not results:
            return f"No semantic matches found for: '{query}'"

        out = [f"Semantic search results for '{query}' ({len(results)} matches):\n"]
        for doc, score in results:
            out.append(f"  • [{score:.3f}] {doc.page_content}")
            if doc.metadata:
                meta_str = ", ".join(f"{k}={v}" for k, v in doc.metadata.items()
                                    if k not in ("embedding", "nodeId", "text"))
                if meta_str:
                    out.append(f"    Metadata: {meta_str}")
        return "\n".join(out)
    except Exception as e:
        return f"Vector search error: {e}. Try search_entity_by_name instead."


@tool
def extract_entities_from_url(url: str) -> str:
    """Extract entities and relationships from a web page or article URL
    using Diffbot NLP and load them into the knowledge graph.
    Use this to enrich the graph with real-time data from news articles,
    company pages, or Wikipedia.

    Args:
        url: The URL to extract entities from (e.g. Wikipedia article URL).
    """
    diffbot = _get_diffbot()
    if diffbot is None:
        return "Diffbot not configured. Set DIFFBOT_API_KEY in .env to enable web extraction."

    try:
        from langchain_community.document_loaders import WebBaseLoader

        # Load the page
        loader = WebBaseLoader([url])
        docs = loader.load()
        if not docs:
            return f"Could not load content from: {url}"

        # Truncate for Diffbot limits
        doc = docs[0]
        if len(doc.page_content) > 90000:
            doc.page_content = doc.page_content[:90000]

        # Extract graph structure
        graph_documents = diffbot.convert_to_graph_documents([doc])
        if not graph_documents:
            return f"No entities/relationships extracted from: {url}"

        n_nodes = sum(len(gd.nodes) for gd in graph_documents)
        n_rels = sum(len(gd.relationships) for gd in graph_documents)

        # Load DIRECTLY into the KYC ontology (no orphan :Organization /
        # :Document / :Location nodes created).
        align = _load_diffbot_aligned(graph_documents)

        return (
            f"✅ Extracted and loaded from {url}:\n"
            f"  • {n_nodes} entities (Diffbot)\n"
            f"  • {n_rels} relationships (Diffbot)\n"
            f"  Data is now queryable in the knowledge graph."
            + _format_alignment(align)
        )
    except Exception as e:
        return f"Extraction error: {e}"


@tool
def extract_entities_from_text(text: str) -> str:
    """Extract entities and relationships from raw text using Diffbot NLP
    and load them into the knowledge graph. Use for pasting news articles,
    regulatory filings, or any unstructured text.

    Args:
        text: Raw text to extract entities from (company descriptions, news, filings).
    """
    diffbot = _get_diffbot()
    if diffbot is None:
        return "Diffbot not configured. Set DIFFBOT_API_KEY in .env to enable text extraction."

    try:
        doc = Document(page_content=text[:90000], metadata={"source": "user_input"})
        graph_documents = diffbot.convert_to_graph_documents([doc])
        if not graph_documents:
            return "No entities/relationships extracted from the provided text."

        n_nodes = sum(len(gd.nodes) for gd in graph_documents)
        n_rels = sum(len(gd.relationships) for gd in graph_documents)

        # Load DIRECTLY into the KYC ontology.
        align = _load_diffbot_aligned(graph_documents)

        return (
            f"✅ Extracted and loaded from text:\n"
            f"  • {n_nodes} entities (Diffbot)\n"
            f"  • {n_rels} relationships (Diffbot)\n"
            f"  Data is now queryable in the knowledge graph."
            + _format_alignment(align)
        )
    except Exception as e:
        return f"Extraction error: {e}"


@tool
def enrich_entity_from_web(entity_name: str) -> str:
    """Enrich the knowledge graph with real-world information about an entity
    by searching Wikipedia and extracting structured data via Diffbot.

    Args:
        entity_name: Name of entity to research (e.g. 'Deutsche Bank', 'BlackRock').
    """
    diffbot = _get_diffbot()
    if diffbot is None:
        return "Diffbot not configured. Set DIFFBOT_API_KEY in .env to enable enrichment."

    try:
        from langchain_community.document_loaders import WikipediaLoader

        raw_docs = WikipediaLoader(query=entity_name, load_max_docs=1).load()
        if not raw_docs:
            return f"No Wikipedia article found for '{entity_name}'."

        doc = raw_docs[0]
        if len(doc.page_content) > 90000:
            doc.page_content = doc.page_content[:90000]

        graph_documents = diffbot.convert_to_graph_documents([doc])
        if not graph_documents:
            return f"No structured data extracted for '{entity_name}'."

        n_nodes = sum(len(gd.nodes) for gd in graph_documents)
        n_rels = sum(len(gd.relationships) for gd in graph_documents)

        # Load DIRECTLY into the KYC ontology — no orphan :Organization /
        # :Document / :Location nodes are created.
        align = _load_diffbot_aligned(graph_documents, primary_name=entity_name)

        return (
            f"✅ Enriched graph with data about '{entity_name}':\n"
            f"  • Source: Wikipedia\n"
            f"  • {n_nodes} entities extracted (Diffbot)\n"
            f"  • {n_rels} relationships extracted (Diffbot)\n"
            f"  You can now query about {entity_name} and its connections."
            + _format_alignment(align)
        )
    except Exception as e:
        return f"Enrichment error: {e}"


# ─── System Prompt ────────────────────────────────────────────────────────────
SYSTEM_PROMPT = f"""You are a KYC/AML Investigation Intelligence Assistant with access to a financial
knowledge graph containing legal entities, natural persons, ownership chains, control
relationships, and transaction data.

YOUR ROLE:
- Answer questions about beneficial ownership, sanctions exposure, risk assessment,
  money laundering indicators, corporate structures, and regulatory compliance.
- Use the available tools to query the knowledge graph for EVERY factual question.
- NEVER fabricate or hallucinate data. If a tool returns no results, say so clearly.
- Support follow-up questions — remember the conversation context.

GROUNDING RULES (CRITICAL):
1. ALL factual claims MUST come from tool results. Never invent entity IDs, names, scores, or relationships.
2. If you don't know an entity ID, use search_entity_by_name first.
3. If no tool fits perfectly, use run_custom_cypher with a valid read-only Cypher query.
4. For ontology/FIBO/GLEIF questions, use query_ontology.
5. Always cite the entity IDs and data source in your answer.
6. If the graph doesn't contain the answer, say "The knowledge graph does not contain this information."
7. Use AT MOST 3 tool calls per question. After gathering data, synthesize your answer immediately.
8. For follow-up questions, use conversation context and only call tools for NEW information needed.

SCHEMA CONTEXT:
{NEO4J_SCHEMA}

{GRAPHDB_SCHEMA}

When the user asks about:
- "who owns X" or "UBO" → use find_ultimate_beneficial_owners
- "sanctions" or "compliance" → use check_sanctions_exposure
- "risk" or "risk score" → use get_entity_risk_profile
- "shell companies" → use find_shell_companies
- "circular" or "rings" → use find_circular_ownership_rings
- "PEP" or "politically exposed" → use find_pep_connections
- "transactions" or "suspicious" → use analyze_suspicious_transactions
- "path between" or "connection" → use find_path_between_entities
- "compare" → use compare_entities
- "jurisdiction" → use get_jurisdiction_risk_analysis
- "community" or "cluster" → use get_community_analysis
- "statistics" or "overview" → use get_graph_statistics
- "money laundering" or "red flags" → use find_money_laundering_indicators
- "hidden controller" or "shadow" → use find_hidden_controllers
- "ontology" or "FIBO" or "SPARQL" → use query_ontology
- "search" or "find similar" or vague entity reference → use semantic_search_entities
- "enrich" or "add data about" or "research" → use enrich_entity_from_web
- "extract from URL" or "load from" → use extract_entities_from_url
- "extract from text" or user pastes text → use extract_entities_from_text
- Any other graph question → use run_custom_cypher

Be concise but thorough. Format output with clear structure. Flag any sanctions
or PEP exposure prominently with ⚠️ warnings.
"""


# ─── Agent Factory ────────────────────────────────────────────────────────────
ALL_TOOLS = [
    find_ultimate_beneficial_owners,
    check_sanctions_exposure,
    get_entity_risk_profile,
    find_circular_ownership_rings,
    find_shell_companies,
    get_top_risk_entities,
    find_pep_connections,
    find_sanctioned_persons,
    analyze_suspicious_transactions,
    find_path_between_entities,
    get_entity_detail,
    compare_entities,
    get_jurisdiction_risk_analysis,
    get_community_analysis,
    get_graph_statistics,
    find_money_laundering_indicators,
    find_hidden_controllers,
    search_entity_by_name,
    query_ontology,
    run_custom_cypher,
    # Vector search
    semantic_search_entities,
    # Diffbot integration
    extract_entities_from_url,
    extract_entities_from_text,
    enrich_entity_from_web,
]

# Module-level agent singleton (lazy init)
_agent = None
_memory = None


def _get_agent():
    """Lazy-initialize the LangGraph agent with memory."""
    global _agent, _memory
    if _agent is None:
        llm = _get_llm()
        _memory = MemorySaver()
        _agent = create_react_agent(
            llm,
            ALL_TOOLS,
            checkpointer=_memory,
            prompt=SYSTEM_PROMPT,
        )
    return _agent


def process_agent_chat(message: str, session_id: str = "default") -> dict:
    """Process a chat message through the agentic system.

    Args:
        message: User's natural language question.
        session_id: Session ID for conversation memory (supports follow-ups).

    Returns:
        dict with keys: reply, tools_used, session_id
    """
    agent = _get_agent()
    config = {"configurable": {"thread_id": session_id}, "recursion_limit": 25}

    try:
        result = agent.invoke(
            {"messages": [("user", message)]},
            config=config,
        )

        # Extract the final AI response
        ai_message = result["messages"][-1]
        reply = ai_message.content if hasattr(ai_message, "content") else str(ai_message)

        # Track which tools were called
        tools_used = []
        for msg in result["messages"]:
            if hasattr(msg, "tool_calls") and msg.tool_calls:
                for tc in msg.tool_calls:
                    tools_used.append(tc.get("name", "unknown"))

        return {
            "reply": reply,
            "tools_used": tools_used,
            "session_id": session_id,
        }
    except Exception as e:
        return {
            "reply": f"Error processing your question: {str(e)}. Please try rephrasing.",
            "tools_used": [],
            "session_id": session_id,
        }


def reset_session(session_id: str = "default") -> None:
    """Clear conversation memory for a session."""
    global _agent, _memory
    # Reset by recreating the agent (MemorySaver is in-memory)
    _agent = None
    _memory = None
