// === Data quality checks ===

// 1. Entities missing required fields
MATCH (e:LegalEntity)
WHERE e.lei IS NULL OR e.name IS NULL OR e.jurisdiction IS NULL
RETURN e.id, e.lei, e.name, e.jurisdiction;

// 2. Invalid LEI format (must be 20 uppercase alphanumerics)
MATCH (e:LegalEntity)
WHERE NOT e.lei =~ '^[A-Z0-9]{20}$'
RETURN e.id, e.lei;

// 3. Duplicate LEIs (must be unique)
MATCH (e:LegalEntity)
WITH e.lei AS lei, count(*) AS n
WHERE n > 1
RETURN lei, n ORDER BY n DESC;

// 4. Ownership percentages outside [0, 100]
MATCH ()-[r:DIRECTLY_OWNED_BY]->()
WHERE r.percentage < 0 OR r.percentage > 100
RETURN r;

// 5. Cumulative ownership > 100% (over-attribution)
MATCH (e:LegalEntity)<-[r:DIRECTLY_OWNED_BY]-()
WITH e, sum(r.percentage) AS total
WHERE total > 100.01
RETURN e.id, e.name, total ORDER BY total DESC;

// 6. Orphan persons (declared but never linked)
MATCH (p:NaturalPerson)
WHERE NOT (p)<-[:CONTROLLED_BY]-()
RETURN p.id, p.name LIMIT 25;
