"""
KYC Intelligence — Streamlit dashboard.

Run with:
    streamlit run dashboard/app.py
"""
import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.kg_client import Neo4jClient

st.set_page_config(page_title="KYC Intelligence", page_icon="🔎", layout="wide")
st.title("🔎 KYC Beneficial Ownership Intelligence")


@st.cache_resource
def get_neo() -> Neo4jClient:
    return Neo4jClient()


neo = get_neo()


@st.cache_data(ttl=60)
def kpis() -> dict:
    rows = neo.query("""
        MATCH (e:LegalEntity)
        RETURN count(e) AS entities,
               sum(CASE WHEN e.kycRiskScore >= 70 THEN 1 ELSE 0 END) AS high_risk,
               avg(e.kycRiskScore)                                     AS avg_score
    """)
    p = neo.query("MATCH (p:NaturalPerson) RETURN count(p) AS persons, "
                  "sum(CASE WHEN p.isSanctioned THEN 1 ELSE 0 END) AS sanctioned, "
                  "sum(CASE WHEN p.isPEP        THEN 1 ELSE 0 END) AS peps")[0]
    return {**rows[0], **p}


@st.cache_data(ttl=60)
def top_risky(n: int = 25) -> pd.DataFrame:
    rows = neo.query("""
        MATCH (e:LegalEntity) WHERE e.kycRiskScore > 0
        RETURN e.id AS id, e.name AS name, e.jurisdiction AS jurisdiction,
               e.riskTier AS tier, e.kycRiskScore AS score
        ORDER BY score DESC LIMIT $n
    """, {"n": n})
    return pd.DataFrame(rows)


@st.cache_data(ttl=60)
def jurisdiction_breakdown() -> pd.DataFrame:
    rows = neo.query("""
        MATCH (e:LegalEntity)
        RETURN e.jurisdiction AS jurisdiction, e.riskTier AS tier,
               count(e) AS entities, avg(e.kycRiskScore) AS avg_score
        ORDER BY avg_score DESC
    """)
    return pd.DataFrame(rows)


# ─── KPI row ─────────────────────────────────────────────────────────────────
k = kpis()
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Legal entities",   f"{k['entities']:,}")
c2.metric("Natural persons",  f"{k['persons']:,}")
c3.metric("Sanctioned",       k["sanctioned"])
c4.metric("PEPs",             k["peps"])
c5.metric("High-risk (≥70)",  k["high_risk"])

st.divider()

# ─── Two-column layout ───────────────────────────────────────────────────────
left, right = st.columns(2)

with left:
    st.subheader("📊 Risk by jurisdiction")
    df_jur = jurisdiction_breakdown()
    st.dataframe(df_jur, hide_index=True, use_container_width=True)
    st.bar_chart(df_jur.set_index("jurisdiction")["avg_score"])

with right:
    st.subheader("🚨 Top-risk entities")
    st.dataframe(top_risky(25), hide_index=True, use_container_width=True)

st.divider()

# ─── UBO lookup ──────────────────────────────────────────────────────────────
st.subheader("🔍 UBO lookup")
entity_id = st.text_input("Entity ID (e.g. ENTITY_0042)", value="ENTITY_0042")
if st.button("Find UBO"):
    rows = neo.query("""
        MATCH path = (e:LegalEntity {id: $id})
              -[:DIRECTLY_OWNED_BY*0..6]->()
              -[:CONTROLLED_BY]->(p:NaturalPerson)
        RETURN p.name AS name, p.nationality AS nationality,
               p.isPEP AS is_pep, p.isSanctioned AS is_sanctioned,
               length(path) AS hops
        ORDER BY hops
    """, {"id": entity_id})

    if not rows:
        st.warning(f"No UBO found for {entity_id}.")
    else:
        for r in rows:
            badge = []
            if r["is_sanctioned"]: badge.append("⚠️ SANCTIONED")
            if r["is_pep"]:        badge.append("🏛️ PEP")
            st.markdown(f"**{r['name']}** ({r['nationality']}) — "
                        f"{r['hops']} hop(s) {'  '.join(badge)}")

# ─── Circular ownership ──────────────────────────────────────────────────────
st.subheader("🔄 Circular ownership rings")
rings = neo.query("""
    MATCH (e:LegalEntity) WHERE e.sccComponentId IS NOT NULL
    WITH e.sccComponentId AS scc, collect(e) AS members
    WHERE size(members) > 1
    RETURN scc, [m IN members | m.id + ' (' + m.name + ')'] AS members
    LIMIT 20
""")
if rings:
    for r in rings:
        st.write(f"Ring #{r['scc']}: " + " ↔ ".join(r["members"]))
else:
    st.info("No circular ownership detected.")
