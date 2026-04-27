// === GDS projections used by 08_gds_analysis.py ===

// UNDIRECTED — for WCC, Louvain (community detection)
CALL gds.graph.project(
  'kyc-graph',
  ['LegalEntity', 'NaturalPerson'],
  {
    DIRECTLY_OWNED_BY: { orientation: 'UNDIRECTED' },
    CONTROLLED_BY:     { orientation: 'UNDIRECTED' }
  }
);

// NATURAL — for PageRank, SCC (direction matters)
CALL gds.graph.project(
  'kyc-directed',
  ['LegalEntity', 'NaturalPerson'],
  {
    DIRECTLY_OWNED_BY: { orientation: 'NATURAL' },
    CONTROLLED_BY:     { orientation: 'NATURAL' }
  }
);

// Run algorithms
CALL gds.wcc.write       ('kyc-graph',    {writeProperty: 'wccComponentId'});
CALL gds.louvain.write   ('kyc-graph',    {writeProperty: 'louvainCommunityId'});
CALL gds.pageRank.write  ('kyc-directed', {writeProperty: 'pageRankScore', maxIterations: 20});
CALL gds.betweenness.write('kyc-directed',{writeProperty: 'betweennessScore'});
CALL gds.scc.write       ('kyc-directed', {writeProperty: 'sccComponentId'});

// Cleanup
CALL gds.graph.drop('kyc-graph');
CALL gds.graph.drop('kyc-directed');
