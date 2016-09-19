# STEP 5: Refactor Children to Parent Category, in batches by indexes on both to restrict cartesian

import time

from neo4j.v1 import GraphDatabase, basic_auth, TRUST_ON_FIRST_USE, CypherError

driver = GraphDatabase.driver("bolt://localhost",
                              auth=basic_auth("neo4j", "neo4j"),
                              encrypted=False,
                              trust=TRUST_ON_FIRST_USE)
session = driver.session()

childProps1 = '''
MATCH (n:Organization)
WHERE NOT ((n)-[:HAS_LOCATION]-())
RETURN n.country AS pname, count(n) AS ncount
'''

indexRefactor1 = '''
MATCH (c:Country {countryName:{pname}}), (n:Organization {country:{pname}})
WHERE NOT ((n)-[:HAS_LOCATION]-())
WITH c,n LIMIT {limit} MERGE (c)<-[r:HAS_LOCATION]-(n)
'''
ntotal = 0
batchSize = 2000


try:
    tjob = time.time()
    print("processing ---> getting child property list for refactoring")

    propertyList = session.run(childProps1)

    print('-----------------')
    print("processing ---> starting refactoring to parent category")
    print ("%s %s" % ("using batch size: ", batchSize))
    print('-----------------')

    for property in propertyList:
        print("%s %s %s" % (property["pname"], "nodes:", property["ncount"]))
        t0 = time.time()
        btotal=0
        pname = (property["pname"])
        ncount = (property["ncount"])

        while True:
            result = session.run(indexRefactor1, {"pname": pname, "limit": batchSize})
            btotal = btotal + batchSize
            summary = result.consume()
            counters = summary.counters
            print(counters)

            if btotal < ncount:
                print('--next batch--')

            else:
                print('-----done-----')
                break

        print(round((time.time() - t0)*1000,1), " ms elapsed time")
        ntotal = ntotal + ncount
        print ("%s %s" % ("------------------------------> total refactored nodes: ", ntotal))


except Exception as e:
    print('*** Got exception',e)
    if not isinstance(e, CypherError):
        print('*** Rolling back')
        session.rollback()
    else:
        print('*** Not rolling back')

finally:
    print('Done!')
    print(round((time.time() - tjob)/60,1), " minutes elapsed time")
