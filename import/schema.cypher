:begin
CREATE RANGE INDEX FOR (n:LegalEntity) ON (n.jurisdiction);
CREATE RANGE INDEX FOR (n:LegalEntity) ON (n.name);
CREATE RANGE INDEX FOR (n:NaturalPerson) ON (n.name);
CREATE CONSTRAINT entity_id_unique FOR (node:LegalEntity) REQUIRE (node.id) IS UNIQUE;
CREATE CONSTRAINT entity_lei_unique FOR (node:LegalEntity) REQUIRE (node.lei) IS UNIQUE;
CREATE CONSTRAINT n10s_unique_uri FOR (node:Resource) REQUIRE (node.uri) IS UNIQUE;
CREATE CONSTRAINT person_id_unique FOR (node:NaturalPerson) REQUIRE (node.id) IS UNIQUE;
:commit
CALL db.awaitIndexes(300);
