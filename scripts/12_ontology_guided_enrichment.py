"""
Script 12 — Ontology-guided KG enrichment from unstructured text.

Pattern: Going Meta sessions 28–32 (jbarrasa/goingmeta).
Uses the same `getNLOntology` approach as session30/python/utils.py.

Key design principles (per user requirement):
  • NO hardcoded ontology — schema is pulled from existing assets:
        a) KYC application ontology stored in GraphDB (named graph
           http://kg/kyc/ontology), loaded by script 03 — fetched via SPARQL
           CONSTRUCT.
        b) SHACL shapes from shacl/kyc_shapes.ttl — used to constrain the
           extraction prompt and to validate output.
  • NO synthetic data — only real Wikipedia text + the GLEIF entities that
    script 11 already loaded.
  • Output is SHACL-validated per batch (pyshacl, same library as script 10).
  • Named-Entity Disambiguation (NED) resolves extracted names to existing
    :LegalEntity nodes by name match before insertion.

Flow:
   1. Pull KYC ontology TTL from GraphDB                       [no hardcode]
   2. Read SHACL shapes from project file                      [no hardcode]
   3. Build NL ontology + constraint description programmatically
      (getNLOntology — direct port of jbarrasa/goingmeta utils)
   4. For each KYC topic article:
         - Fetch text from Wikipedia
         - LLM extracts entities + relationships using ONLY ontology vocab
         - NED resolves entities against GLEIF
         - Cypher MERGE into Neo4j
         - SHACL-validate the batch's RDF view
   5. Print summary + SHACL report

Usage:
    python scripts/12_ontology_guided_enrichment.py
"""
from __future__ import annotations

import os
import re
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from rdflib import Graph, Namespace, RDF, RDFS, OWL, URIRef, Literal
from rdflib.namespace import SH

from src.kg_client import Neo4jClient, GraphDBClient, neo4j_healthy, graphdb_healthy

# ─── KYC topics: real Wikipedia articles, no synthetic data ───────────────────
KYC_TOPICS = [
    "Deutsche Bank",
    "HSBC",
    "Wirecard",
    "BlackRock",
    "Berkshire Hathaway",
]

KYC      = Namespace("http://kyc-kg.example.org/ontology#")
FIBO_BE  = Namespace("https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/")
LCC      = Namespace("https://www.omg.org/spec/LCC/Countries/ISO3166-1-CountryCodes/")

SHACL_FILE           = Path("shacl/kyc_shapes.ttl")
ONTOLOGY_NAMED_GRAPH = "http://kg/kyc/ontology"


# ─── Ontology + SHACL loading (from authoritative sources, not hardcoded) ────
def fetch_kyc_ontology(gdb: GraphDBClient) -> Graph:
    """CONSTRUCT the KYC application ontology out of GraphDB."""
    sparql = f"""
        CONSTRUCT {{ ?s ?p ?o }}
        FROM <{ONTOLOGY_NAMED_GRAPH}>
        WHERE   {{ ?s ?p ?o }}
    """
    turtle = gdb.query_raw(sparql, accept="text/turtle")
    g = Graph()
    g.parse(data=turtle, format="turtle")
    return g


def load_shacl(path: Path = SHACL_FILE) -> Graph:
    g = Graph()
    g.parse(path.as_posix(), format="turtle")
    return g


# ─── getNLOntology — port of goingmeta/session30/python/utils.py ──────────────
def _local(uri) -> str:
    s = str(uri)
    for sep in ("#", "/", ":"):
        i = s.rfind(sep)
        if i >= 0:
            s = s[i + 1:]
    return s


def get_nl_ontology(g: Graph) -> str:
    """Render an OWL ontology graph as natural-language description."""
    out: list[str] = ["CATEGORIES:"]
    for cat in g.subjects(RDF.type, OWL.Class):
        line = f"  • {_local(cat)}"
        for desc in g.objects(cat, RDFS.comment):
            line += f": {desc}"
        for sup in g.objects(cat, RDFS.subClassOf):
            line += f" (subClassOf {_local(sup)})"
        out.append(line)

    out.append("\nATTRIBUTES (datatype properties):")
    for att in g.subjects(RDF.type, OWL.DatatypeProperty):
        line = f"  • {_local(att)}"
        for dom in g.objects(att, RDFS.domain):
            line += f" — applies to {_local(dom)}"
        for ran in g.objects(att, RDFS.range):
            line += f", value type {_local(ran)}"
        for desc in g.objects(att, RDFS.comment):
            line += f". {desc}"
        out.append(line)

    out.append("\nRELATIONSHIPS (object properties):")
    for prop in g.subjects(RDF.type, OWL.ObjectProperty):
        line = f"  • {_local(prop)}"
        for dom in g.objects(prop, RDFS.domain):
            line += f" — connects {_local(dom)}"
        for ran in g.objects(prop, RDFS.range):
            line += f" → {_local(ran)}"
        for desc in g.objects(prop, RDFS.comment):
            line += f". {desc}"
        for inv in g.objects(prop, OWL.inverseOf):
            line += f". (inverseOf {_local(inv)})"
        out.append(line)

    return "\n".join(out)


def get_nl_shacl(g: Graph) -> str:
    """Render SHACL NodeShapes as NL constraints."""
    out = ["DATA QUALITY CONSTRAINTS (SHACL):"]
    for shape in g.subjects(RDF.type, SH.NodeShape):
        target = next(g.objects(shape, SH.targetClass), None) \
                 or next(g.objects(shape, SH.targetSubjectsOf), None) \
                 or "(generic)"
        label = next(g.objects(shape, RDFS.label), Literal(""))
        out.append(f"\n  Shape on {_local(target)}: {label}")
        for prop in g.objects(shape, SH.property):
            path     = next(g.objects(prop, SH.path), None)
            datatype = next(g.objects(prop, SH.datatype), None)
            pattern  = next(g.objects(prop, SH.pattern), None)
            mn       = next(g.objects(prop, SH.minCount), None)
            mx       = next(g.objects(prop, SH.maxCount), None)
            mininc   = next(g.objects(prop, SH.minInclusive), None)
            maxinc   = next(g.objects(prop, SH.maxInclusive), None)
            msg      = next(g.objects(prop, SH.message), Literal(""))
            line = f"     - {_local(path)}"
            if datatype: line += f" ({_local(datatype)})"
            if pattern:  line += f" matching /{pattern}/"
            if mn:       line += f", minCount={mn}"
            if mx:       line += f", maxCount={mx}"
            if mininc is not None or maxinc is not None:
                line += f", range=[{mininc}..{maxinc}]"
            if msg: line += f"  → {msg}"
            out.append(line)
    return "\n".join(out)


# ─── LLM (same priority as agent.py) ──────────────────────────────────────────
def get_llm():
    if os.getenv("ANTHROPIC_API_KEY"):
        from langchain_anthropic import ChatAnthropic
        return ChatAnthropic(
            model=os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-5-20250929"),
            temperature=0, max_tokens=4096,
        )
    if os.getenv("OPENAI_API_KEY"):
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(
            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            temperature=0, max_tokens=4096,
        )
    if os.getenv("DEEPSEEK_API_KEY"):
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(
            model=os.getenv("DEEPSEEK_MODEL", "deepseek-chat"),
            api_key=os.environ["DEEPSEEK_API_KEY"],
            base_url=os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1"),
            temperature=0, max_tokens=4096,
        )
    if os.getenv("OLLAMA_MODEL"):
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(
            model=os.environ["OLLAMA_MODEL"],
            api_key="ollama",
            base_url=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1"),
            temperature=0, max_tokens=2048,
            timeout=120, max_retries=1,
        )
    raise RuntimeError("No LLM configured")


# ─── Build extraction prompt programmatically (from ontology + SHACL) ─────────
def build_prompt(ontology_nl: str, shacl_nl: str, topic: str, text: str) -> str:
    return f"""Given the ontology below run your best entity extraction over the content.
The extracted entities and relationships MUST be described using exclusively the
terms in the ontology and in the way they are defined. Respect domain/range
constraints. NEVER use terms not defined in the ontology.

The data quality constraints (SHACL) are MANDATORY — entities you extract must
satisfy them or be omitted. Codes (jurisdiction, nationality) must be ISO 3166-1
alpha-2 (e.g. US, GB, DE, KY, VG). Ownership percentages must be in [0, 100].

KEY EXTRACTION GOALS — the output is useless without these:
  1. Always include the article's main subject (e.g. "{topic}") as a LegalEntity.
  2. Extract every named subsidiary, parent company, joint venture, acquired
     firm, or affiliate as a LegalEntity.
  3. Extract every named board member, founder, CEO, chairman, beneficial
     owner, controlling shareholder as a NaturalPerson.
  4. **You MUST emit relationships connecting these entities.** A submission
     with zero relationships will be rejected. Use ONLY the predicates listed
     in the ontology (directlyOwnedBy, owns, controlledBy, ultimatelyOwnedBy,
     hasJurisdiction, hasLegalAddress).
  5. When the text says "X is a subsidiary of Y" / "X was acquired by Y" /
     "Y owns X" → emit {{"source_name":"X","target_name":"Y","predicate":"directlyOwnedBy"}}.
  6. When the text says "Z founded X" / "Z is CEO of X" / "Z chairs X" →
     emit {{"source_name":"X","target_name":"Z","predicate":"controlledBy"}}.
  7. When the company is registered/headquartered in a country → emit
     hasJurisdiction; when an address is given → emit hasLegalAddress.

────────────────  ONTOLOGY  ────────────────
{ontology_nl}

────────────  SHACL CONSTRAINTS  ───────────
{shacl_nl}

──────────  WORKED EXAMPLE (study, then apply) ──────────
TEXT: "Acme Holdings Plc is a British investment firm headquartered in
London. It owns 80% of Acme Bank Ltd. The company was founded by Jane Smith,
who continues to chair the board."
EXPECTED OUTPUT:
{{
  "entities": [
    {{"class":"LegalEntity","name":"Acme Holdings Plc","properties":{{"legalName":"Acme Holdings Plc"}}}},
    {{"class":"LegalEntity","name":"Acme Bank Ltd","properties":{{"legalName":"Acme Bank Ltd"}}}},
    {{"class":"Jurisdiction","name":"GB","properties":{{}}}},
    {{"class":"Address","name":"London","properties":{{}}}},
    {{"class":"NaturalPerson","name":"Jane Smith","properties":{{"nationality":"GB"}}}}
  ],
  "relationships": [
    {{"source_name":"Acme Bank Ltd","target_name":"Acme Holdings Plc","predicate":"directlyOwnedBy","properties":{{"ownershipPercentage":80}}}},
    {{"source_name":"Acme Holdings Plc","target_name":"GB","predicate":"hasJurisdiction"}},
    {{"source_name":"Acme Holdings Plc","target_name":"London","predicate":"hasLegalAddress"}},
    {{"source_name":"Acme Holdings Plc","target_name":"Jane Smith","predicate":"controlledBy"}}
  ]
}}

OUTPUT FORMAT — strict JSON, no markdown, no commentary:
{{
  "entities":      [ {{"class":"...","name":"...","properties":{{...}}}}, ... ],
  "relationships": [ {{"source_name":"...","target_name":"...","predicate":"...","properties":{{...}}}}, ... ]
}}

TOPIC: {topic}

TEXT:
{text}

Now extract entities AND relationships as strict JSON. Remember: a response
with empty "relationships" is a failure.
"""


def extract(llm, ontology_nl: str, shacl_nl: str, topic: str, text: str) -> dict:
    from langchain_core.messages import SystemMessage, HumanMessage
    system = (
        "You are an expert in extracting structured information out of natural "
        "language text using a given ontology. You output ONLY valid JSON that "
        "respects the ontology classes, attributes, relationships, and SHACL "
        "constraints. You never invent new vocabulary."
    )
    resp = llm.invoke([
        SystemMessage(content=system),
        HumanMessage(content=build_prompt(ontology_nl, shacl_nl, topic, text[:4000])),
    ])
    raw = resp.content if isinstance(resp.content, str) else str(resp.content)
    raw = raw.strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
    m = re.search(r"\{.*\}", raw, re.DOTALL)
    if not m:
        return {"entities": [], "relationships": []}
    try:
        data = json.loads(m.group(0))
    except json.JSONDecodeError as e:
        print(f"      ! JSON parse error: {e}")
        return {"entities": [], "relationships": []}
    return {
        "entities":      data.get("entities") or [],
        "relationships": data.get("relationships") or [],
    }


# ─── Helpers ──────────────────────────────────────────────────────────────────
def slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")[:60]


def discover_classes(g: Graph) -> set[str]:
    return {_local(c) for c in g.subjects(RDF.type, OWL.Class)}


def discover_predicates(g: Graph) -> set[str]:
    return {_local(p) for p in g.subjects(RDF.type, OWL.ObjectProperty)}


def discover_attributes(g: Graph) -> set[str]:
    return {_local(p) for p in g.subjects(RDF.type, OWL.DatatypeProperty)}


def predicate_to_rel(pred: str) -> str:
    """`directlyOwnedBy` → `DIRECTLY_OWNED_BY`. Camel → SNAKE_UPPER, generic."""
    return re.sub(r"([A-Z])", r"_\1", pred).upper().lstrip("_")


# ─── NED against existing GLEIF :LegalEntity nodes ────────────────────────────
def resolve(neo: Neo4jClient, name: str, person: bool = False) -> str | None:
    if person:
        rows = neo.query("""
            MATCH (p:NaturalPerson)
            WHERE toLower(p.name) = toLower($n)
            RETURN p.id AS id LIMIT 1
        """, {"n": name})
        return rows[0]["id"] if rows else None
    rows = neo.query("""
        MATCH (e:LegalEntity)
        WHERE toLower(e.name) = toLower($n)
           OR toLower(e.name) CONTAINS toLower($n)
           OR toLower($n) CONTAINS toLower(e.name)
        RETURN e.id AS id, e.name AS name
        ORDER BY size(e.name) ASC
        LIMIT 1
    """, {"n": name})
    return rows[0]["id"] if rows else None


# ─── Persist into Neo4j + build RDF view for SHACL ────────────────────────────
def persist(neo: Neo4jClient, extraction: dict, source: str,
            allowed_classes: set[str], allowed_preds: set[str]) -> tuple[dict, Graph]:
    rdf = Graph()
    rdf.bind("kyc",     KYC)
    rdf.bind("fibo-be", FIBO_BE)

    name_to_id: dict[str, str] = {}
    person_names: set[str] = set()
    counts = {"resolved_to_gleif": 0, "new_entities": 0, "new_persons": 0,
              "new_relationships": 0, "skipped_off_ontology": 0}

    # Entities
    for e in extraction["entities"]:
        cls   = e.get("class") or e.get("type") or e.get("category")
        # Some LLMs return name nested in `properties.legalName`
        props = e.get("properties") or {}
        name  = ((e.get("name") or e.get("legalName")
                  or props.get("legalName") or props.get("name")) or "").strip()
        if not name or cls not in allowed_classes:
            counts["skipped_off_ontology"] += 1
            continue

        if cls in ("LegalEntity", "RegisteredLegalEntity"):
            existing = resolve(neo, name)
            if existing:
                name_to_id[name] = existing
                counts["resolved_to_gleif"] += 1
                node_id = existing
            else:
                node_id = "EXT_" + slugify(name)
                neo.execute("""
                    MERGE (n:LegalEntity {id: $id})
                    ON CREATE SET n.name = $name,
                                  n.dataSource = 'LLM_EXTRACTED',
                                  n.needsVerification = true
                    SET n.lei              = coalesce(n.lei, $lei),
                        n.jurisdiction     = coalesce(n.jurisdiction, $jur),
                        n.category         = coalesce(n.category, $cat),
                        n.description      = coalesce(n.description, $desc),
                        n.kycRiskScore     = coalesce(n.kycRiskScore, 30),
                        n.riskTier         = coalesce(n.riskTier, 'medium'),
                        n.isActive         = coalesce(n.isActive, true),
                        n.hasOperationalAddress = coalesce(n.hasOperationalAddress, true),
                        n.sourceArticles   = coalesce(n.sourceArticles, []) + $source
                """, {
                    "id": node_id, "name": name,
                    "lei":  props.get("leiCode") or props.get("lei"),
                    "jur":  props.get("hasJurisdiction") or props.get("jurisdiction"),
                    "cat":  props.get("category", "CORPORATION"),
                    "desc": props.get("description") or f"{name} (from {source})",
                    "source": source,
                })
                name_to_id[name] = node_id
                counts["new_entities"] += 1

            # RDF view (for SHACL)
            uri = URIRef(f"http://kyc-kg.example.org/entity/{slugify(name)}")
            rdf.add((uri, RDF.type, FIBO_BE.LegalPerson))
            rdf.add((uri, KYC.legalName, Literal(name)))
            lei_val = props.get("leiCode") or props.get("lei")
            if lei_val:
                rdf.add((uri, KYC.leiCode, Literal(lei_val)))
            jur = props.get("hasJurisdiction") or props.get("jurisdiction")
            if jur:
                rdf.add((uri, KYC.hasJurisdiction, URIRef(LCC + jur)))

        elif cls == "NaturalPerson":
            node_id = "PER_" + slugify(name)
            neo.execute("""
                MERGE (p:NaturalPerson {id: $id})
                ON CREATE SET p.name = $name, p.dataSource = 'LLM_EXTRACTED'
                SET p.nationality   = coalesce(p.nationality, $nat),
                    p.role          = coalesce(p.role, $role),
                    p.isPEP         = coalesce(p.isPEP, $pep),
                    p.isSanctioned  = coalesce(p.isSanctioned, $sanct),
                    p.sourceArticles = coalesce(p.sourceArticles, []) + $source
            """, {
                "id": node_id, "name": name,
                "nat":   props.get("nationality"),
                "role":  props.get("role"),
                "pep":   bool(props.get("isPEP", False)),
                "sanct": bool(props.get("isSanctioned", False)),
                "source": source,
            })
            name_to_id[name] = node_id
            person_names.add(name)
            counts["new_persons"] += 1

            uri = URIRef(f"http://kyc-kg.example.org/person/{slugify(name)}")
            rdf.add((uri, RDF.type, KYC.NaturalPerson))
            if props.get("nationality"):
                rdf.add((uri, KYC.nationality, Literal(props["nationality"])))
            if props.get("isPEP"):
                rdf.add((uri, KYC.isPEP, Literal(True)))
            if props.get("isSanctioned"):
                rdf.add((uri, KYC.isSanctioned, Literal(True)))

        elif cls == "Jurisdiction":
            node_id = "JUR_" + slugify(name)
            neo.execute("""
                MERGE (j:Jurisdiction {id: $id})
                ON CREATE SET j.name = $name, j.code = $code, j.dataSource = 'LLM_EXTRACTED'
            """, {"id": node_id, "name": name,
                  "code": name.upper() if len(name) <= 3 else None})
            name_to_id[name] = node_id

        elif cls == "Address":
            node_id = "ADR_" + slugify(name)
            neo.execute("""
                MERGE (a:Address {id: $id})
                ON CREATE SET a.name = $name, a.dataSource = 'LLM_EXTRACTED'
            """, {"id": node_id, "name": name})
            name_to_id[name] = node_id

    # Relationships
    # Build forgiving lookup: 'directly_owned_by','DIRECTLY_OWNED_BY','directlyOwnedBy' all → directlyOwnedBy
    pred_lookup = {p.lower().replace("_", ""): p for p in allowed_preds}
    for r in extraction["relationships"]:
        raw_pred = (r.get("predicate") or r.get("type") or r.get("relationship") or "").strip()
        pred = pred_lookup.get(raw_pred.lower().replace("_", ""))
        if not pred:
            counts["skipped_off_ontology"] += 1
            continue
        sname = (r.get("source_name") or r.get("from_name") or r.get("source") or r.get("from") or "").strip()
        tname = (r.get("target_name") or r.get("to_name")   or r.get("target") or r.get("to")   or "").strip()
        if not sname or not tname:
            continue
        sid = name_to_id.get(sname) or resolve(neo, sname)
        # Person target if it was created as a person OR if not resolvable as entity
        is_person_target = tname in person_names
        tid = name_to_id.get(tname) or resolve(neo, tname, person=is_person_target)
        if not tid and not is_person_target:
            tid = resolve(neo, tname, person=True)
        if not sid or not tid:
            continue

        rel_type = predicate_to_rel(pred)
        rprops = r.get("properties") or {}
        # Coerce numeric percentage if present
        if "ownershipPercentage" in rprops or "percentage" in rprops:
            try:
                pct = float(rprops.get("ownershipPercentage") or rprops.get("percentage"))
                rprops["percentage"] = pct
            except Exception:
                rprops.pop("percentage", None)

        neo.execute(f"""
            MATCH (a {{id: $sid}}), (b {{id: $tid}})
            MERGE (a)-[rr:`{rel_type}`]->(b)
            SET rr += $props,
                rr.source = $source
        """, {"sid": sid, "tid": tid, "props": rprops, "source": source})
        counts["new_relationships"] += 1

        # RDF triple for SHACL
        s_uri = URIRef(f"http://kyc-kg.example.org/entity/{slugify(sname)}")
        t_uri = URIRef(
            f"http://kyc-kg.example.org/{'person' if is_person_target else 'entity'}/{slugify(tname)}"
        )
        rdf.add((s_uri, KYC[pred], t_uri))
        if "ownershipPercentage" in rprops or "percentage" in rprops:
            rdf.add((s_uri, KYC.ownershipPercentage,
                     Literal(rprops.get("percentage"))))

    return counts, rdf


def shacl_validate(data_g: Graph, shapes_g: Graph) -> tuple[bool, str]:
    from pyshacl import validate
    conforms, _, report = validate(
        data_graph=data_g,
        shacl_graph=shapes_g,
        inference="rdfs",
        abort_on_first=False,
    )
    return conforms, report


def load_wikipedia(topic: str, max_chars: int = 4000) -> str:
    from langchain_community.document_loaders import WikipediaLoader
    docs = WikipediaLoader(query=topic, load_max_docs=1).load()
    return docs[0].page_content[:max_chars] if docs else ""


def main() -> int:
    if not neo4j_healthy():
        print("✗ Neo4j is not reachable.")
        return 1
    if not graphdb_healthy():
        print("✗ GraphDB is not reachable.")
        return 1

    gdb = GraphDBClient()

    print("→ Pulling KYC ontology from GraphDB ...")
    ont_g = fetch_kyc_ontology(gdb)
    print(f"   ✓ {len(ont_g):,} ontology triples from <{ONTOLOGY_NAMED_GRAPH}>")
    if len(ont_g) == 0:
        print("   ✗ Ontology empty — run scripts/03_load_fibo2glei_mapping.py first.")
        return 2

    print("→ Loading SHACL shapes ...")
    shapes_g = load_shacl()
    print(f"   ✓ {len(shapes_g):,} shape triples from {SHACL_FILE}")

    ontology_nl = get_nl_ontology(ont_g)
    shacl_nl    = get_nl_shacl(shapes_g)
    allowed_classes = discover_classes(ont_g)
    allowed_preds   = discover_predicates(ont_g)

    print(f"\n→ Allowed classes:    {sorted(allowed_classes)}")
    print(f"→ Allowed predicates: {sorted(allowed_preds)}")
    print(f"→ Allowed attributes: {sorted(discover_attributes(ont_g))}")

    print("\n──────── NL ONTOLOGY (sent to LLM, derived from GraphDB) ────────")
    print(ontology_nl)
    print("\n──────── NL SHACL (sent to LLM, derived from kyc_shapes.ttl) ────")
    print(shacl_nl)

    llm = get_llm()
    print(f"\n→ LLM: {type(llm).__name__} model="
          f"{getattr(llm, 'model', getattr(llm, 'model_name', '?'))}\n")

    summary = {"resolved_to_gleif": 0, "new_entities": 0, "new_persons": 0,
               "new_relationships": 0, "skipped_off_ontology": 0,
               "shacl_violations": 0}

    with Neo4jClient() as neo:
        for topic in KYC_TOPICS:
            print(f"══ {topic} ══")
            text = load_wikipedia(topic)
            if not text:
                print("   ✗ no Wikipedia content"); continue
            print(f"   ✓ fetched {len(text):,} chars")

            try:
                extraction = extract(llm, ontology_nl, shacl_nl, topic, text)
            except Exception as ex:
                print(f"   ✗ LLM call failed: {ex}")
                continue
            print(f"   ✓ extracted {len(extraction['entities'])} entities, "
                  f"{len(extraction['relationships'])} relationships")
            # Persist raw extraction for debugging/inspection
            dump_dir = Path("data/extractions"); dump_dir.mkdir(parents=True, exist_ok=True)
            (dump_dir / f"{slugify(topic)}.json").write_text(json.dumps(extraction, indent=2))
            if extraction["entities"]:
                first = extraction["entities"][0]
                print(f"      first entity sample: {json.dumps(first)[:200]}")
            if extraction["relationships"]:
                first_r = extraction["relationships"][0]
                print(f"      first rel sample:    {json.dumps(first_r)[:200]}")

            counts, batch_rdf = persist(neo, extraction,
                                        f"Wikipedia: {topic}",
                                        allowed_classes, allowed_preds)
            for k, v in counts.items():
                summary[k] += v
            print(f"      resolved-to-GLEIF: {counts['resolved_to_gleif']}, "
                  f"new entities: {counts['new_entities']}, "
                  f"new persons: {counts['new_persons']}, "
                  f"new rels: {counts['new_relationships']}, "
                  f"skipped: {counts['skipped_off_ontology']}")

            if len(batch_rdf) > 0:
                conforms, report = shacl_validate(batch_rdf, shapes_g)
                if conforms:
                    print("      ✓ SHACL: batch conforms")
                else:
                    n = report.count("Constraint Violation")
                    summary["shacl_violations"] += n
                    print(f"      ⚠ SHACL: {n} violation(s)")
            print()

        print("══ Summary ══")
        for k, v in summary.items():
            print(f"   {k}: {v}")

        print("\n══ Final graph ══")
        print(f"   :LegalEntity        {neo.node_count('LegalEntity'):,}")
        print(f"   :NaturalPerson      {neo.node_count('NaturalPerson'):,}")
        for rel in sorted({predicate_to_rel(p) for p in allowed_preds}):
            r = neo.query(f"MATCH ()-[r:`{rel}`]->() RETURN count(r) AS c")
            if r and r[0]["c"]:
                print(f"   :{rel}  {r[0]['c']:,}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
