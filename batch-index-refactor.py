# STEP 5: Refactor Children to Parent Category, in batches by indexes on both
# Bolt variant: using session, implicit transactions

import time

from neo4j.v1 import GraphDatabase, basic_auth, TRUST_ON_FIRST_USE, CypherError

driver = GraphDatabase.driver("bolt://localhost",
                              auth=basic_auth("neo4j", "neo4j"),
                              encrypted=False,
                              trust=TRUST_ON_FIRST_USE)
session = driver.session()

# get counts of child nodes by property that needs refactoring
childProps1 = '''
MATCH (n:Organization)
WHERE NOT ((n)-[:HAS_LOCATION]-())
RETURN n.country AS pname, count(n) AS ncount
'''

# refactor, passing in the property value to both parent and child as paramters to restrict cartesian
indexRefactor1 = '''
MATCH (c:Country {countryName:{pname}}), (n:Organization {country:{pname}})
WHERE NOT ((n)-[:HAS_LOCATION]-())
WITH c,n LIMIT {limit} MERGE (c)<-[r:HAS_LOCATION]-(n)
'''

ntotal = 0

# configure batch size
batchSize = 2000


try:
    tjob = time.time()

    #get category and child node counts
    print("processing ---> getting child property list for refactoring")

    propertyList = session.run(childProps1)

    print('-----------------')

    # refactor, stepping through category values in batches per child node counts
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
            # refactor, passing in the property value to match both parent and child on indexes
            result = session.run(indexRefactor1, {"pname": pname, "limit": batchSize})
            btotal = btotal + batchSize
            summary = result.consume()
            counters = summary.counters
            print(counters)

            # compare running total of batched nodes to total nodes
            if btotal < ncount:
                print('--next batch--')

            # end batches, go to next parent category value
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
