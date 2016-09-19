#STEP 3: (Optional)  Warmup Page Cache, helpful for large graphs
# requires APOC procedures
# https://neo4j-contrib.github.io/neo4j-apoc-procedures/

#!pip install neo4j-driver

import time

from neo4j.v1 import GraphDatabase, basic_auth, TRUST_ON_FIRST_USE, CypherError

driver = GraphDatabase.driver("bolt://localhost",
                              auth=basic_auth("neo4j", "neo4j"),
                              encrypted=False,
                              trust=TRUST_ON_FIRST_USE)

session = driver.session()


warmpup1 = '''
CALL apoc.warmup.run();
'''

t0 = time.time()
print("processing...")

result = session.run(warmup1)

for record in result:
    print("%s" % (record))

summary = result.consume()
counters = summary.counters
print(counters)

print(round((time.time() - t0)*1000,1), " ms elapsed time")
print('-----------------')

session.close()
