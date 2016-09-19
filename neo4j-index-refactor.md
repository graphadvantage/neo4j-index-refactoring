## Neo4j Category Refactoring for Large Graphs using Indexes & Batching

# Avoiding Massive Cartesians: Refactoring to Dense Nodes in a Large Neo4j Graph

##Introduction

Sometimes it is necessary to refactor a node property as a new hierarchy- extracting the unique values of the property from child nodes, creating the new parent category nodes, and the setting the relationship between the matching parent and child nodes.

In small graphs this is a trivial operation, and thanks to the Michael Hunger and APOC team there are even procedures available to perform refactoring with a single statement.

Let's suppose we have an (:Organization) node that has a {country: "..."} property, and we want to refactor to a (:Country) parent category, and create a [:HAS_LOCATION] relationship between the two:

```
(:Organization)-[:HAS_LOCATION]->(:Country)
```

The first step is to find the child values that need to be extracted, and create the parent nodes:

```
//Find the child values that need to be extracted, and create the parent nodes

MATCH (n:Organization)
WITH COLLECT(DISTINCT n.country) AS names
FOREACH (name IN names |
 MERGE (:Country {countryName: name}))
RETURN names

```

Next, match the parents and children and set the relationship as appropriate:

```
// Match parent and child nodes, create hierarchy relationship

MATCH (c:Country), (n:Organization)
WHERE c.countryName = n.country
AND NOT ((n)-[:HAS_LOCATION]-())
WITH c,n MERGE (c)<-[r:HAS_LOCATION]-(n)
```

In small graphs this is a trivial operation, and thanks to the Michael Hunger and the APOC team you perform refactoring using just single statement, which will also delete the extracted property from the children, and copy over any other needed hierarchy properties from children to the parent (such as `['area']`), in configurable batches:

```
MATCH (n:Organization)
CALL apoc.refactor.categorize('country','HAS_LOCATION',true,'Country','countryName',['area'],1000)
RETURN 'Done!'
```

However, for large graphs both of these approaches can be computationally expensive and v e r y   s l o w.

I recently had the opportunity to build a large graph with 450M nodes and over 2B relationships. The size of the graph on disk exceeded the server's available memory page cache so any refactoring required a lot of disk reads. Plus, as in this exercise, I needed to refactor :Organizations to :Country, and there were 260M Organization records and only 244 Countries, so each (:Country) node would be a dense node with an average of about 1M [:HAS_LOCATION] relationships per node.  (There are several approaches to managing dense nodes, but we're going to set aside that discussion for now...)

The major cost comes from this statement in refactoring:

```
MATCH (c:Country), (n:Organization)
```

This is of course a dreaded cartesian join -- unavoidable if we want to set the new parent child relationship.

The secret to managing these kind of cartesian joins in a large graph is to use indexes...

##  Graph Gist: Index based Category Refactoring with Batches

In this Gist, I'll show you how to leverage some newer Neo4j capabilities to efficiently refactor a large graph.

## TopLine:

To control the scope of the refactor cartesian, we'll pass the property values as parameters to match on index for both parent and child, and then loop through the indexed result set creating the relationships in smaller batches.  These two approaches in combination provide for efficient memory management during refactoring.

**Make a Test Graph**

So let's start by creating a graph, here we'll use the GraphAware GraphGen plugin (https://github.com/graphaware/neo4j-graphgen-procedure) to make 1M Organization nodes, and give them a randomly assigned country property.

You'll need a running Neo4j instance, and you'll need to compile the graphgen .jar file and add it to Neo4j/plugins and restart Neo4j

This python script uses the Bolt driver.

```
#STEP 1 : Generate fake data using GraphAware graphgen
# https://github.com/graphaware/neo4j-graphgen-procedure
# you will need to compile the graphgen .jar file and add it to Neo4j/plugins and restart Neo4j
# (tip: update to JDK 8)

#!pip install neo4j-driver

import time

from neo4j.v1 import GraphDatabase, basic_auth, TRUST_ON_FIRST_USE, CypherError

driver = GraphDatabase.driver("bolt://localhost",
                              auth=basic_auth("neo4j", "neo4j"),
                              encrypted=False,
                              trust=TRUST_ON_FIRST_USE)

session = driver.session()


generate1 = '''
CALL generate.nodes('Organization', '{name: companyName, country: country}', 1000000)
'''

session = driver.session()
t0 = time.time()
print("processing...")
result = session.run(generate1)
summary = result.consume()
counters = summary.counters
print(summary)
print(counters)
print(round((time.time() - t0)*1000,1), " ms elapsed time")
print('-----------------')
session.close()

```

**Set Indexes and Constraints**

The way that we can control the scope of the cartesian join is to make sure we have access to indexes for both the child property that needs refactoring and the new parent category.  

This script sets an index on the {country} property of (:Organization) and sets a constraint on our new parent category (:Country) which also set an index.

```
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

```

**Extract Parent Category Nodes from Child Properties**

Next, we need to create as many parent category nodes as there are unique property values in the child nodes.

In a large graph it may not be necessary to scan all of the child nodes to extract the parent set.

This script uses a random number to sample the graph (a nice trick courtesy of Michael Hunger).

```
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

```


**Refactoring***

So now we are set for refactoring - this script has two parts, first we are going to gather some statistics about the child nodes, second we'll refactor using the property value as a parameter for doing an index-based match for both parent and nodes.  Batching makes the commits managable and fast.

A key aspect is that we are matching all the children to a single category parent node constrained by the indexed results. I did fairly extensive testing and found that this method is much faster compared to joining children to several parents at once.

In the large graph I set fairly large batch sizes ~ 100K or more

```
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

```

Here's some example output

```

```
