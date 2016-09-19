# STEP 4: Extract and create parent Category nodes

import time

from neo4j.v1 import GraphDatabase, basic_auth, TRUST_ON_FIRST_USE, CypherError

driver = GraphDatabase.driver("bolt://localhost",
                              auth=basic_auth("neo4j", "neo4j"),
                              encrypted=False,
                              trust=TRUST_ON_FIRST_USE)

session = driver.session()

cleanup1 = '''
MATCH (n:Country) DETACH DELETE n
'''

# extract from a 5% random sample
extractCategory1 = '''
MATCH (n:Organization) WHERE rand() < 0.05
WITH COLLECT(DISTINCT n.country) AS names
FOREACH (name IN names |
 MERGE (:Country {countryName: name}))
RETURN names
'''


# or extract from a full scan
extractCategory2 = '''
MATCH (n:Organization)
WITH COLLECT(DISTINCT n.country) AS names
FOREACH (name IN names |
 MERGE (:Country {countryName: name}))
RETURN names
'''


session = driver.session()
t0 = time.time()
print("processing...")
result = session.run(cleanup1)
summary = result.consume()
counters = summary.counters
print(summary)
print(counters)
print(round((time.time() - t0)*1000,1), " ms elapsed time")
print('-----------------')
session.close()


session = driver.session()
t0 = time.time()
print("processing...")
result = session.run(extractCategory1)
summary = result.consume()
counters = summary.counters
print(summary)
print(counters)
print(round((time.time() - t0)*1000,1), " ms elapsed time")
print('-----------------')
session.close()
