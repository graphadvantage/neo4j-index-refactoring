#STEP 2:  Set index on child property and constraint on parent category

#!pip install neo4j-driver

import time

from neo4j.v1 import GraphDatabase, basic_auth, TRUST_ON_FIRST_USE, CypherError

driver = GraphDatabase.driver("bolt://localhost",
                              auth=basic_auth("neo4j", "neo4j"),
                              encrypted=False,
                              trust=TRUST_ON_FIRST_USE)

session = driver.session()


index1 = '''
CREATE INDEX ON :Organization(country)
'''

constraint1 = '''
CREATE CONSTRAINT ON (n:Country) ASSERT n.countryName IS UNIQUE;
'''

session = driver.session()
t0 = time.time()
print("processing...")
result = session.run(index1)
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
result = session.run(constraint1)
summary = result.consume()
counters = summary.counters
print(summary)
print(counters)
print(round((time.time() - t0)*1000,1), " ms elapsed time")
print('-----------------')
session.close()
