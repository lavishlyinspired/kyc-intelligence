import { useState } from "react";

const COLORS = {
  graphdb: "#1a6b4a",
  neo4j: "#008CC1",
  fibo: "#7B2D8B",
  glei: "#C0392B",
  lcc: "#E67E22",
  gds: "#27AE60",
  rag: "#2C3E50",
  bridge: "#F39C12",
  bg: "#0D1117",
  card: "#161B22",
  border: "#30363D",
  text: "#E6EDF3",
  muted: "#8B949E",
};

const Box = ({ color, label, sub, children, onClick, selected }) => (
  <div
    onClick={onClick}
    style={{
      background: selected ? `${color}22` : `${color}11`,
      border: `1.5px solid ${selected ? color : color + "55"}`,
      borderRadius: 10,
      padding: "10px 14px",
      cursor: onClick ? "pointer" : "default",
      transition: "all 0.2s",
      boxShadow: selected ? `0 0 12px ${color}44` : "none",
    }}
  >
    <div style={{ color, fontWeight: 700, fontSize: 13, letterSpacing: 0.5 }}>{label}</div>
    {sub && <div style={{ color: COLORS.muted, fontSize: 10, marginTop: 2 }}>{sub}</div>}
    {children}
  </div>
);

const Arrow = ({ label, color = COLORS.bridge }) => (
  <div style={{ display: "flex", flexDirection: "column", alignItems: "center", margin: "4px 0", gap: 2 }}>
    <div style={{ color, fontSize: 9, fontWeight: 600, letterSpacing: 0.5, opacity: 0.85 }}>{label}</div>
    <div style={{ color, fontSize: 18, lineHeight: 1 }}>↓</div>
  </div>
);

const HArrow = ({ label }) => (
  <div style={{ display: "flex", alignItems: "center", gap: 4, margin: "0 6px" }}>
    <div style={{ height: 1.5, width: 20, background: COLORS.bridge, opacity: 0.7 }} />
    <div style={{ color: COLORS.bridge, fontSize: 16 }}>→</div>
    {label && <div style={{ color: COLORS.muted, fontSize: 9 }}>{label}</div>}
  </div>
);

const Tag = ({ text, color }) => (
  <span style={{
    background: `${color}22`, color, border: `1px solid ${color}44`,
    borderRadius: 4, fontSize: 9, padding: "1px 5px", fontWeight: 600, marginRight: 4,
    display: "inline-block", marginTop: 3
  }}>{text}</span>
);

const DETAIL_PANELS = {
  graphdb: {
    title: "GraphDB — Ontology Authority",
    color: COLORS.graphdb,
    points: [
      "Stores FIBO OWL as RDF triples (subject-predicate-object)",
      "OWL-Horst reasoning: automatically infers transitive ownership (UBO shortcuts)",
      "SPARQL 1.1: queries across all named graphs simultaneously",
      "SHACL validation: enforces that data conforms to ontology rules",
      "Named graphs: FIBO, GLEI, LCC, FIB-DM each in separate graph",
      "Neubauten public endpoint: neubauten.ontotext.com:7200",
      "WHY: Source of semantic truth — what 'LegalEntity' MEANS"
    ]
  },
  neo4j: {
    title: "Neo4j — Analytics & Application Engine",
    color: COLORS.neo4j,
    points: [
      "Property graph: nodes + relationships + typed properties",
      "Cypher: developer-friendly, LLM-friendly query language",
      "neosemantics (n10s): imports FIBO OWL + GLEI RDF from GraphDB",
      "APOC: 300+ procedures — LOAD CSV, path finding, graph export",
      "GDS: Louvain, PageRank, Betweenness, WCC — runs in-memory",
      "GraphRAG: LangChain/LangGraph connect LLMs to graph",
      "WHY: Where analysis, applications, and AI agents run"
    ]
  },
  fibo: {
    title: "FIBO — Financial Industry Business Ontology",
    color: COLORS.fibo,
    points: [
      "2,437+ OWL classes as of Q4/2025 (EDM Council)",
      "Domains: FND, BE, FBC, SEC, DER, LOAN, BP, IND",
      "Key modules: LegalPersons, Ownership, Control, CorporateBodies",
      "Download: spec.edmcouncil.org/fibo (module-by-module TTL files)",
      "FIB-DM: derived ER model with 3,173 entities — use as Neo4j schema reference",
      "Used to type GLEI entities: entity rdf:type fibo-be:LegalPerson",
      "URL pattern: spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/"
    ]
  },
  glei: {
    title: "GLEIF / GLEI — Legal Entity Instance Data",
    color: COLORS.glei,
    points: [
      "Global LEI Foundation: 2M+ legal entity identifiers (LEIs)",
      "20-char alphanumeric code uniquely identifies every company worldwide",
      "Level 1 data: who is who (company name, address, jurisdiction)",
      "Level 2 data: who owns whom (direct + ultimate parents)",
      "API: api.gleif.org/api/v1/lei-records — free, no auth required",
      "Golden Copy: full bulk download at gleif.org (XML/CSV/RDF)",
      "FIBO alignment: LEI holders = fibo-be:LegalPerson instances"
    ]
  },
  gds: {
    title: "Graph Data Science — Why Neo4j Wins for KYC",
    color: COLORS.gds,
    points: [
      "WCC (Weakly Connected Components): find isolated ownership clusters",
      "Louvain: community detection — finds suspicious corporate rings",
      "PageRank: systemic risk scoring — most connected entities",
      "Betweenness Centrality: bridge entities in money flows",
      "Shortest Path: connect investigative leads between entities",
      "Node Similarity: find entities that look like known bad actors",
      "ALL IMPOSSIBLE in GraphDB — this is the core Neo4j value prop"
    ]
  },
  bridge: {
    title: "neosemantics (n10s) — The Barrasa Bridge",
    color: COLORS.bridge,
    points: [
      "Created by Jesús Barrasa (Director of Solutions, Neo4j)",
      "goingmeta series: github.com/jbarrasa/goingmeta",
      "n10s.onto.import.fetch: import OWL ontology structure into Neo4j",
      "n10s.rdf.import.fetch: import RDF instance data (GLEI) into Neo4j",
      "n10s.rdf.export: export Neo4j graph as RDF back to GraphDB",
      "n10s.validation.shacl: validate Neo4j graph against SHACL shapes",
      "n10s HTTP endpoint: makes Neo4j queryable via SPARQL SERVICE"
    ]
  },
  rag: {
    title: "GraphRAG KYC Agent — The AI Layer",
    color: COLORS.rag,
    points: [
      "LangGraph: orchestrates multi-step investigation workflows",
      "Tool: find_ubo — traverses ownership chains to find UBO",
      "Tool: sanctions_check — N-hop proximity to sanctioned entities",
      "Tool: gds_risk_score — returns computed PageRank/community score",
      "Tool: circular_ownership — detects ring structures",
      "LLM (Claude): synthesizes findings into natural-language reports",
      "Pattern from: Neo4j KYC GraphRAG blog + Going Meta S03"
    ]
  }
};

export default function App() {
  const [selected, setSelected] = useState("graphdb");
  const detail = DETAIL_PANELS[selected];

  return (
    <div style={{
      background: COLORS.bg, color: COLORS.text,
      fontFamily: "'JetBrains Mono', 'Fira Code', 'Courier New', monospace",
      minHeight: "100vh", padding: 24
    }}>
      <div style={{ maxWidth: 1100, margin: "0 auto" }}>
        {/* Header */}
        <div style={{ textAlign: "center", marginBottom: 28 }}>
          <div style={{
            fontSize: 22, fontWeight: 800, letterSpacing: 1,
            background: `linear-gradient(90deg, ${COLORS.fibo}, ${COLORS.neo4j}, ${COLORS.gds})`,
            WebkitBackgroundClip: "text", WebkitTextFillColor: "transparent"
          }}>
            KYC / AML BENEFICIAL OWNERSHIP INTELLIGENCE SYSTEM
          </div>
          <div style={{ color: COLORS.muted, fontSize: 11, marginTop: 6 }}>
            FIBO · GLEIF · LCC · FIB-DM → GraphDB ↔ neosemantics ↔ Neo4j → GDS → GraphRAG
          </div>
          <div style={{ color: COLORS.muted, fontSize: 10, marginTop: 3 }}>
            Click any component to learn what it does and why it's here
          </div>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20 }}>
          {/* Left: Architecture Flow */}
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>

            {/* Layer 0: Data Sources */}
            <div style={{ color: COLORS.muted, fontSize: 10, textTransform: "uppercase", letterSpacing: 1, marginBottom: 2 }}>
              LAYER 0 — Ontologies & Open Data
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 6 }}>
              <Box color={COLORS.fibo} label="FIBO" sub="OWL Ontology" onClick={() => setSelected("fibo")} selected={selected === "fibo"}>
                <Tag text="EDM Council" color={COLORS.fibo} />
                <Tag text="2,437 classes" color={COLORS.fibo} />
              </Box>
              <Box color={COLORS.glei} label="GLEIF" sub="LEI Instance Data" onClick={() => setSelected("glei")} selected={selected === "glei"}>
                <Tag text="2M+ entities" color={COLORS.glei} />
                <Tag text="Free API" color={COLORS.glei} />
              </Box>
              <Box color={COLORS.lcc} label="LCC + FIB-DM" sub="Codes & Schema" onClick={() => setSelected("fibo")} selected={false}>
                <Tag text="ISO 3166" color={COLORS.lcc} />
                <Tag text="3173 entities" color={COLORS.lcc} />
              </Box>
            </div>

            <Arrow label="RDF triples (Turtle/OWL) loaded via SPARQL LOAD" color={COLORS.graphdb} />

            {/* Layer 1: GraphDB */}
            <div style={{ color: COLORS.muted, fontSize: 10, textTransform: "uppercase", letterSpacing: 1 }}>
              LAYER 1 — Ontology Store & Reasoner
            </div>
            <Box color={COLORS.graphdb} label="GraphDB (Ontotext)" sub="RDF Triplestore + OWL Reasoner" onClick={() => setSelected("graphdb")} selected={selected === "graphdb"}>
              <div style={{ marginTop: 8, display: "flex", gap: 6, flexWrap: "wrap" }}>
                <Tag text="SPARQL 1.1" color={COLORS.graphdb} />
                <Tag text="OWL-Horst" color={COLORS.graphdb} />
                <Tag text="SHACL" color={COLORS.graphdb} />
                <Tag text="Named Graphs" color={COLORS.graphdb} />
                <Tag text="Inference" color={COLORS.graphdb} />
              </div>
              <div style={{ marginTop: 8, fontSize: 9, color: COLORS.muted }}>
                ► Infers UBO chains from direct ownership triples automatically<br />
                ► Validates that every LEI entity conforms to FIBO shapes<br />
                ► Runs cross-ontology SPARQL (FIBO + GLEI + LCC in one query)
              </div>
            </Box>

            <Arrow label="n10s.onto.import.fetch + n10s.rdf.import.fetch" color={COLORS.bridge} />

            {/* Bridge */}
            <Box color={COLORS.bridge} label="neosemantics (n10s)" sub="The Barrasa Bridge — RDF ↔ Neo4j" onClick={() => setSelected("bridge")} selected={selected === "bridge"}>
              <div style={{ marginTop: 6, display: "flex", gap: 6, flexWrap: "wrap" }}>
                <Tag text="onto.import" color={COLORS.bridge} />
                <Tag text="rdf.import" color={COLORS.bridge} />
                <Tag text="rdf.export" color={COLORS.bridge} />
                <Tag text="SHACL.validate" color={COLORS.bridge} />
              </div>
            </Box>

            <Arrow label="Ontology + instances materialized as property graph" color={COLORS.neo4j} />

            {/* Layer 2: Neo4j */}
            <div style={{ color: COLORS.muted, fontSize: 10, textTransform: "uppercase", letterSpacing: 1 }}>
              LAYER 2 — Analytics & Application Engine
            </div>
            <Box color={COLORS.neo4j} label="Neo4j" sub="Property Graph — Cypher + APOC + GDS" onClick={() => setSelected("neo4j")} selected={selected === "neo4j"}>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 6, marginTop: 8 }}>
                <Box color={COLORS.gds} label="GDS Algorithms" sub="" onClick={() => setSelected("gds")} selected={selected === "gds"}>
                  <div style={{ fontSize: 9, color: COLORS.muted, marginTop: 4 }}>
                    Louvain · PageRank<br />Betweenness · WCC
                  </div>
                </Box>
                <Box color={COLORS.neo4j} label="APOC Utilities" sub="">
                  <div style={{ fontSize: 9, color: COLORS.muted, marginTop: 4 }}>
                    Load CSV/JSON<br />Path finding · Export
                  </div>
                </Box>
              </div>
              <div style={{ marginTop: 8, fontSize: 9, color: COLORS.muted }}>
                ► 500 entities · 200 persons · 1000 txns loaded<br />
                ► KYC risk score = PageRank + jurisdiction + GDS community<br />
                ► Circular ownership · Shell company detection
              </div>
            </Box>

            <Arrow label="LangChain Neo4jGraph + Cypher tool calls" color={COLORS.rag} />

            {/* Layer 3: GraphRAG */}
            <div style={{ color: COLORS.muted, fontSize: 10, textTransform: "uppercase", letterSpacing: 1 }}>
              LAYER 3 — GraphRAG Investigation Agent
            </div>
            <Box color={COLORS.rag} label="LangGraph KYC Agent + Claude" sub="GraphRAG · Going Meta S03 pattern" onClick={() => setSelected("rag")} selected={selected === "rag"}>
              <div style={{ marginTop: 8, display: "flex", gap: 6, flexWrap: "wrap" }}>
                <Tag text="find_ubo" color={COLORS.rag} />
                <Tag text="sanctions_check" color={COLORS.rag} />
                <Tag text="risk_score" color={COLORS.rag} />
                <Tag text="circular_detect" color={COLORS.rag} />
              </div>
              <div style={{ marginTop: 8, fontSize: 9, color: COLORS.muted }}>
                Natural language → Cypher → Graph traversal → LLM explanation
              </div>
            </Box>

            <Arrow label="Streamlit dashboard" color={COLORS.muted} />
            <Box color={COLORS.muted} label="Streamlit Dashboard" sub="KYC risk scoring · UBO search · community visualization">
              <div style={{ fontSize: 9, color: COLORS.muted, marginTop: 4 }}>
                Risk heatmaps · Ownership trees · Alert queue
              </div>
            </Box>
          </div>

          {/* Right: Detail Panel */}
          <div>
            <div style={{ color: COLORS.muted, fontSize: 10, textTransform: "uppercase", letterSpacing: 1, marginBottom: 8 }}>
              COMPONENT DETAIL
            </div>
            {detail && (
              <div style={{
                background: COLORS.card, border: `1.5px solid ${detail.color}`,
                borderRadius: 12, padding: 20,
                boxShadow: `0 0 20px ${detail.color}22`
              }}>
                <div style={{ color: detail.color, fontWeight: 800, fontSize: 15, marginBottom: 16 }}>
                  {detail.title}
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                  {detail.points.map((pt, i) => (
                    <div key={i} style={{ display: "flex", gap: 10, alignItems: "flex-start" }}>
                      <div style={{
                        background: detail.color, color: "#fff",
                        borderRadius: "50%", width: 18, height: 18,
                        display: "flex", alignItems: "center", justifyContent: "center",
                        fontSize: 9, fontWeight: 700, flexShrink: 0, marginTop: 1
                      }}>{i + 1}</div>
                      <div style={{ fontSize: 11, color: COLORS.text, lineHeight: 1.5 }}>{pt}</div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* The Key Question Box */}
            <div style={{
              marginTop: 16, background: "#0D1117",
              border: `1px solid ${COLORS.border}`, borderRadius: 10, padding: 16
            }}>
              <div style={{ color: COLORS.bridge, fontWeight: 700, fontSize: 12, marginBottom: 12 }}>
                THE CORE QUESTION: GraphDB vs Neo4j?
              </div>
              <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                {[
                  ["GraphDB answers:", "What does this class MEAN? What can we INFER? Is this data VALID?", COLORS.graphdb],
                  ["Neo4j answers:", "Who is CONNECTED to whom? What PATTERNS exist? What is the RISK SCORE?", COLORS.neo4j],
                  ["neosemantics bridges:", "Makes Neo4j ontology-aware. Makes GraphDB app-connectable.", COLORS.bridge],
                  ["GDS delivers:", "Louvain rings, PageRank risk, Betweenness conduits — only in Neo4j.", COLORS.gds],
                  ["GraphRAG completes:", "Natural language KYC investigations over the entire graph.", COLORS.rag],
                ].map(([label, text, color]) => (
                  <div key={label}>
                    <div style={{ color, fontSize: 10, fontWeight: 700 }}>{label}</div>
                    <div style={{ color: COLORS.muted, fontSize: 10, marginTop: 2, lineHeight: 1.4 }}>{text}</div>
                  </div>
                ))}
              </div>
            </div>

            {/* Execution Order */}
            <div style={{
              marginTop: 16, background: "#0D1117",
              border: `1px solid ${COLORS.border}`, borderRadius: 10, padding: 16
            }}>
              <div style={{ color: COLORS.text, fontWeight: 700, fontSize: 12, marginBottom: 10 }}>
                EXECUTION ORDER (Claude Code)
              </div>
              {[
                ["01", "docker-compose up -d", COLORS.muted],
                ["02", "01_setup_graphdb.py → load FIBO+LCC", COLORS.graphdb],
                ["03", "02_load_glei_data.py → GLEIF API → RDF", COLORS.glei],
                ["04", "03_generate_kyc_dataset.py → synthetic data", COLORS.fibo],
                ["05", "04_load_neo4j.py → n10s + Cypher MERGE", COLORS.neo4j],
                ["06", "05_barrasa_bridge_pattern.py → SHACL + export", COLORS.bridge],
                ["07", "06_gds_kyc_analysis.py → algorithms + risk scores", COLORS.gds],
                ["08", "07_graphrag_kyc_agent.py → LangGraph + Claude", COLORS.rag],
                ["09", "streamlit run dashboard/app.py", COLORS.muted],
              ].map(([num, cmd, color]) => (
                <div key={num} style={{ display: "flex", gap: 8, marginBottom: 5, alignItems: "center" }}>
                  <div style={{
                    background: color, color: "#fff", borderRadius: 3,
                    width: 20, height: 16, display: "flex", alignItems: "center",
                    justifyContent: "center", fontSize: 8, fontWeight: 700, flexShrink: 0
                  }}>{num}</div>
                  <div style={{ fontSize: 10, color: COLORS.muted, fontFamily: "monospace" }}>{cmd}</div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
