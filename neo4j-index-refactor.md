##Neo4j Category Refactoring for Large Graphs using Bolt, Indexes & Batching

#Refactor 1M Nodes in 30 Seconds with Neo4j Indexes

<img width="888" alt="refactor" src="https://cloud.githubusercontent.com/assets/5991751/18645738/3982a61a-7e63-11e6-8720-7f670dca3378.png">

##Introduction

Sometimes it is necessary to refactor a node property as a new hierarchy. This involves extracting the unique values of the property from child nodes, creating the new parent category nodes, and then the setting the relationship between the matching parent and child nodes.


Let's suppose we have an (:Organization) node that has a {country: "..."} property, and we want to refactor to a (:Country) parent category, and create a new [:HAS_LOCATION] relationship between the two:

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

In small graphs this is a trivial operation.  And thanks to the Michael Hunger and the APOC team, you can even perform refactoring using just a single statement, which will also delete the extracted property from the children, and copy over any other needed properties from children to the parent (such as `['area']`), and run in configurable batches:

```
MATCH (n:Organization)
CALL apoc.refactor.categorize('country','HAS_LOCATION',true,'Country','countryName',['area'],1000)
RETURN 'Done!'
```

However, for large graphs both of these approaches can be computationally expensive and  v-e-r-y s-l-o-w.

I recently built a large graph that had 450 million nodes and 2 billion relationships. The size of the graph on disk exceeded the server's available memory page cache so any refactoring required a lot of disk reads.

Just as in this exercise, I needed to refactor (:Organizations) to (:Country), and with 260M Organization nodes and only 244 Country nodes, each (:Country) node would be a dense node with an average of 1M [:HAS_LOCATION] relationships per node.

***(Note: There are several approaches to managing dense nodes, but we're going to set that discussion aside for now...)***

The major cost comes from this statement in refactoring:

```
MATCH (c:Country), (n:Organization)
```

This is, of course, the dreaded cartesian join -- painful, but unavoidable if we want to set the new parent-child relationship.

The secret to managing these kind of cartesian joins in a large graph is to use Neo4j indexes...


##GraphGist: Neo4j Category Refactoring for Large Graphs using Bolt, Indexes & Batching

In this Gist, I'll show you how to leverage some newer Neo4j capabilities to efficiently refactor a large graph.

We'll make a graph with 1M child nodes and using Python Bolt, refactor it to a parent category in under 30 sec (which is what I got on my MacBook).


##TopLine

To control the scope of cartesian joins in refactoring, we'll pass the property values as parameters to match on index for both parent and child, and then loop through the indexed result set creating the relationships in smaller batches.  These combined approaches provide efficient memory management and disk reads/writes during refactoring, maximizing throughput.


##Step 1. Make the Test Graph

So let's start by creating a graph, here we'll use the GraphAware GraphGen plugin to make 1M (:Organization) nodes, and give them a randomly assigned {country: "..."} property (https://github.com/graphaware/neo4j-graphgen-procedure).

You'll need a running Neo4j instance, and you'll need to compile the graphgen .jar file and add it to Neo4j/plugins and restart Neo4j.

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

Example output:

```
processing...
<neo4j.v1.session.ResultSummary object at 0x17fb0c048>
{}
94686.1  ms elapsed time
-----------------
```


##Step 2. Set Indexes and Constraints

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

Example output:

```
processing...
<neo4j.v1.session.ResultSummary object at 0x1078017f0>
{'indexes_added': 1}
42.2  ms elapsed time
-----------------
processing...
<neo4j.v1.session.ResultSummary object at 0x107801b00>
{'constraints_added': 1}
990.2  ms elapsed time
-----------------
```


##Step 3. Graph Warmup

This is an optional step for this Gist, but in a large graph you can see better results prior to refactoring if you can load some of the graph into memory.

The output of this script will show you how much of the graph is actually in memory per your neo4j.conf page cache settings.

This script require the APOC procedures plugin:

https://neo4j-contrib.github.io/neo4j-apoc-procedures/


```
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


warmup1 = '''
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

```

Example output:

```
processing...
<Record pageSize=8192 nodesPerPage=546 nodesTotal=1000000 nodesLoaded=1832 nodesTime=0 relsPerPage=240 relsTotal=0 relsLoaded=0 relsTime=0 totalTime=0>
{}
245.3  ms elapsed time
-----------------
```


##Step 4. Extract Parent Category Nodes from Child Properties

Next, we need to create as many parent category nodes as there are unique property values in the child nodes.

In a large graph it may not be necessary to scan all of the child nodes to extract the parent set.

This script uses a random number to sample the graph (a nice trick courtesy of Michael Hunger).

***(Note: I've included a cleanup query to delete the (:Country) nodes and relationships so you can run Steps 4 & 5 multiple times.)***

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

Example output:

```
processing...
<neo4j.v1.session.ResultSummary object at 0x1093abda0>
{}
42.3  ms elapsed time
-----------------
processing...
<neo4j.v1.session.ResultSummary object at 0x109360588>
{'labels_added': 244, 'properties_set': 244, 'nodes_created': 244}
751.6  ms elapsed time
-----------------
```


## Step 5. Fast Refactoring

So now we are set for refactoring - this script has two parts, first we are going to gather some statistics about the child nodes, second we'll refactor using the property value as a parameter for doing an index-based match for both parent and nodes.  Batching makes the commits managable and fast.

The key aspect here is that we are matching all the children to a ***single category parent node*** with all matched nodes constrained by the indexed results.

This produces the smallest cartesian possible, and is quite fast due to the single parent.

I did fairly extensive testing and found that this method is much faster compared to joining children to several parents at once - i.e. using batching but not constraining the batches to single index value.

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


##Summary##

Using indexes greatly improves performance of Neo4j operations like refactoring where cartesian joins are unavoidable.

With Python scripting, we can leverage Neo4j's parameter support and Bolt interface to more closely control exactly how memory and disk are being used, which pays off for doing these kinds of heavy-lift operations in a large graph.

All the scripts used here are included, and are consolidated in a Jupyter notebook as well.


##Thanks

Special thanks to Michael Kilgore of InfoClear Consulting, who helped with this work and to Michael Hunger, who got me thinking about refactoring...


##Refactor Example Output

```
processing ---> getting child property list for refactoring
-----------------
processing ---> starting refactoring to parent category
using batch size:  2000
-----------------
Azerbaijan nodes: 3948
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1948}
-----done-----
523.9  ms elapsed time
------------------------------> total refactored nodes:  3948
Saint Kitts and Nevis nodes: 3987
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1987}
-----done-----
195.0  ms elapsed time
------------------------------> total refactored nodes:  7935
Egypt nodes: 4167
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 167}
-----done-----
213.1  ms elapsed time
------------------------------> total refactored nodes:  12102
Saudi Arabia nodes: 4088
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 88}
-----done-----
137.9  ms elapsed time
------------------------------> total refactored nodes:  16190
Virgin Islands, U.S. nodes: 4162
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 162}
-----done-----
151.0  ms elapsed time
------------------------------> total refactored nodes:  20352
Bouvet Island (Bouvetoya) nodes: 4237
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 237}
-----done-----
130.0  ms elapsed time
------------------------------> total refactored nodes:  24589
Turkmenistan nodes: 4039
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 39}
-----done-----
134.8  ms elapsed time
------------------------------> total refactored nodes:  28628
Christmas Island nodes: 4130
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 130}
-----done-----
123.7  ms elapsed time
------------------------------> total refactored nodes:  32758
Brazil nodes: 4129
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 129}
-----done-----
144.2  ms elapsed time
------------------------------> total refactored nodes:  36887
Senegal nodes: 4071
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 71}
-----done-----
155.5  ms elapsed time
------------------------------> total refactored nodes:  40958
Kuwait nodes: 4041
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 41}
-----done-----
159.3  ms elapsed time
------------------------------> total refactored nodes:  44999
Austria nodes: 4017
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 17}
-----done-----
164.3  ms elapsed time
------------------------------> total refactored nodes:  49016
South Africa nodes: 4067
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 67}
-----done-----
139.3  ms elapsed time
------------------------------> total refactored nodes:  53083
Vietnam nodes: 3990
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1990}
-----done-----
135.5  ms elapsed time
------------------------------> total refactored nodes:  57073
Canada nodes: 4037
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 37}
-----done-----
138.7  ms elapsed time
------------------------------> total refactored nodes:  61110
Tokelau nodes: 4047
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 47}
-----done-----
142.0  ms elapsed time
------------------------------> total refactored nodes:  65157
Niue nodes: 4030
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 30}
-----done-----
140.8  ms elapsed time
------------------------------> total refactored nodes:  69187
Afghanistan nodes: 4198
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 198}
-----done-----
141.8  ms elapsed time
------------------------------> total refactored nodes:  73385
Eritrea nodes: 4060
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 60}
-----done-----
119.4  ms elapsed time
------------------------------> total refactored nodes:  77445
Saint Pierre and Miquelon nodes: 4047
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 47}
-----done-----
126.3  ms elapsed time
------------------------------> total refactored nodes:  81492
Lao People's Democratic Republic nodes: 4049
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 49}
-----done-----
128.4  ms elapsed time
------------------------------> total refactored nodes:  85541
Western Sahara nodes: 4121
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 121}
-----done-----
122.2  ms elapsed time
------------------------------> total refactored nodes:  89662
Switzerland nodes: 4060
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 60}
-----done-----
128.4  ms elapsed time
------------------------------> total refactored nodes:  93722
Qatar nodes: 3970
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1970}
-----done-----
117.0  ms elapsed time
------------------------------> total refactored nodes:  97692
Slovenia nodes: 4164
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 164}
-----done-----
125.0  ms elapsed time
------------------------------> total refactored nodes:  101856
Australia nodes: 4119
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 119}
-----done-----
131.4  ms elapsed time
------------------------------> total refactored nodes:  105975
Taiwan nodes: 3976
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1976}
-----done-----
115.9  ms elapsed time
------------------------------> total refactored nodes:  109951
Svalbard & Jan Mayen Islands nodes: 4064
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 64}
-----done-----
115.1  ms elapsed time
------------------------------> total refactored nodes:  114015
Sao Tome and Principe nodes: 4039
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 39}
-----done-----
118.2  ms elapsed time
------------------------------> total refactored nodes:  118054
Mongolia nodes: 4007
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 7}
-----done-----
131.7  ms elapsed time
------------------------------> total refactored nodes:  122061
Reunion nodes: 4208
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 208}
-----done-----
120.2  ms elapsed time
------------------------------> total refactored nodes:  126269
Belarus nodes: 4080
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 80}
-----done-----
131.2  ms elapsed time
------------------------------> total refactored nodes:  130349
Central African Republic nodes: 4165
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 165}
-----done-----
118.7  ms elapsed time
------------------------------> total refactored nodes:  134514
Gibraltar nodes: 4265
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 265}
-----done-----
132.3  ms elapsed time
------------------------------> total refactored nodes:  138779
Comoros nodes: 4090
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 90}
-----done-----
123.2  ms elapsed time
------------------------------> total refactored nodes:  142869
Uganda nodes: 4129
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 129}
-----done-----
124.2  ms elapsed time
------------------------------> total refactored nodes:  146998
Tanzania nodes: 4005
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 5}
-----done-----
118.2  ms elapsed time
------------------------------> total refactored nodes:  151003
Macedonia nodes: 4074
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 74}
-----done-----
129.1  ms elapsed time
------------------------------> total refactored nodes:  155077
Republic of Korea nodes: 4197
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 197}
-----done-----
128.4  ms elapsed time
------------------------------> total refactored nodes:  159274
Panama nodes: 4089
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 89}
-----done-----
136.3  ms elapsed time
------------------------------> total refactored nodes:  163363
Pitcairn Islands nodes: 3979
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1979}
-----done-----
121.3  ms elapsed time
------------------------------> total refactored nodes:  167342
Costa Rica nodes: 4091
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 91}
-----done-----
121.4  ms elapsed time
------------------------------> total refactored nodes:  171433
Luxembourg nodes: 4150
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 150}
-----done-----
117.9  ms elapsed time
------------------------------> total refactored nodes:  175583
Belgium nodes: 3969
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1969}
-----done-----
117.9  ms elapsed time
------------------------------> total refactored nodes:  179552
French Guiana nodes: 4147
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 147}
-----done-----
122.8  ms elapsed time
------------------------------> total refactored nodes:  183699
French Polynesia nodes: 4085
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 85}
-----done-----
122.0  ms elapsed time
------------------------------> total refactored nodes:  187784
Norfolk Island nodes: 4251
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 251}
-----done-----
124.9  ms elapsed time
------------------------------> total refactored nodes:  192035
Timor-Leste nodes: 4057
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 57}
-----done-----
133.3  ms elapsed time
------------------------------> total refactored nodes:  196092
Heard Island and McDonald Islands nodes: 4088
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 88}
-----done-----
123.7  ms elapsed time
------------------------------> total refactored nodes:  200180
Romania nodes: 4062
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 62}
-----done-----
122.8  ms elapsed time
------------------------------> total refactored nodes:  204242
Brunei Darussalam nodes: 4073
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 73}
-----done-----
126.5  ms elapsed time
------------------------------> total refactored nodes:  208315
Saint Helena nodes: 4171
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 171}
-----done-----
118.5  ms elapsed time
------------------------------> total refactored nodes:  212486
Tunisia nodes: 3981
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1981}
-----done-----
117.3  ms elapsed time
------------------------------> total refactored nodes:  216467
Niger nodes: 4108
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 108}
-----done-----
119.9  ms elapsed time
------------------------------> total refactored nodes:  220575
Morocco nodes: 4174
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 174}
-----done-----
121.1  ms elapsed time
------------------------------> total refactored nodes:  224749
Congo nodes: 8086
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 86}
-----done-----
192.5  ms elapsed time
------------------------------> total refactored nodes:  232835
Mauritius nodes: 4202
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 202}
-----done-----
122.0  ms elapsed time
------------------------------> total refactored nodes:  237037
Gambia nodes: 4080
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 80}
-----done-----
124.5  ms elapsed time
------------------------------> total refactored nodes:  241117
Guam nodes: 4033
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 33}
-----done-----
120.0  ms elapsed time
------------------------------> total refactored nodes:  245150
Thailand nodes: 4017
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 17}
-----done-----
124.7  ms elapsed time
------------------------------> total refactored nodes:  249167
Ghana nodes: 4063
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 63}
-----done-----
138.0  ms elapsed time
------------------------------> total refactored nodes:  253230
Monaco nodes: 3942
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1942}
-----done-----
121.4  ms elapsed time
------------------------------> total refactored nodes:  257172
Argentina nodes: 4162
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 162}
-----done-----
115.1  ms elapsed time
------------------------------> total refactored nodes:  261334
Faroe Islands nodes: 3976
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1976}
-----done-----
114.2  ms elapsed time
------------------------------> total refactored nodes:  265310
Solomon Islands nodes: 4098
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 98}
-----done-----
127.1  ms elapsed time
------------------------------> total refactored nodes:  269408
Aruba nodes: 4202
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 202}
-----done-----
121.9  ms elapsed time
------------------------------> total refactored nodes:  273610
Malawi nodes: 4068
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 68}
-----done-----
114.6  ms elapsed time
------------------------------> total refactored nodes:  277678
Sudan nodes: 4062
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 62}
-----done-----
122.5  ms elapsed time
------------------------------> total refactored nodes:  281740
Saint Martin nodes: 4014
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 14}
-----done-----
120.3  ms elapsed time
------------------------------> total refactored nodes:  285754
Papua New Guinea nodes: 4197
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 197}
-----done-----
125.5  ms elapsed time
------------------------------> total refactored nodes:  289951
Lesotho nodes: 4012
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 12}
-----done-----
120.4  ms elapsed time
------------------------------> total refactored nodes:  293963
Mali nodes: 3960
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1960}
-----done-----
118.9  ms elapsed time
------------------------------> total refactored nodes:  297923
Antarctica (the territory South of 60 deg S) nodes: 4052
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 52}
-----done-----
114.6  ms elapsed time
------------------------------> total refactored nodes:  301975
Tonga nodes: 4134
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 134}
-----done-----
122.7  ms elapsed time
------------------------------> total refactored nodes:  306109
Saint Barthelemy nodes: 3940
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1940}
-----done-----
117.3  ms elapsed time
------------------------------> total refactored nodes:  310049
Dominica nodes: 4005
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 5}
-----done-----
123.2  ms elapsed time
------------------------------> total refactored nodes:  314054
Peru nodes: 4128
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 128}
-----done-----
131.9  ms elapsed time
------------------------------> total refactored nodes:  318182
Cape Verde nodes: 4060
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 60}
-----done-----
118.1  ms elapsed time
------------------------------> total refactored nodes:  322242
Honduras nodes: 4105
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 105}
-----done-----
129.1  ms elapsed time
------------------------------> total refactored nodes:  326347
Virgin Islands, British nodes: 4209
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 209}
-----done-----
120.4  ms elapsed time
------------------------------> total refactored nodes:  330556
Zimbabwe nodes: 4072
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 72}
-----done-----
122.9  ms elapsed time
------------------------------> total refactored nodes:  334628
Syrian Arab Republic nodes: 4119
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 119}
-----done-----
120.7  ms elapsed time
------------------------------> total refactored nodes:  338747
Myanmar nodes: 4051
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 51}
-----done-----
137.0  ms elapsed time
------------------------------> total refactored nodes:  342798
Kiribati nodes: 4030
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 30}
-----done-----
120.3  ms elapsed time
------------------------------> total refactored nodes:  346828
Maldives nodes: 4155
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 155}
-----done-----
129.0  ms elapsed time
------------------------------> total refactored nodes:  350983
Seychelles nodes: 4164
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 164}
-----done-----
120.5  ms elapsed time
------------------------------> total refactored nodes:  355147
Vanuatu nodes: 4117
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 117}
-----done-----
126.4  ms elapsed time
------------------------------> total refactored nodes:  359264
Yemen nodes: 4063
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 63}
-----done-----
122.7  ms elapsed time
------------------------------> total refactored nodes:  363327
Bahamas nodes: 4023
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 23}
-----done-----
115.3  ms elapsed time
------------------------------> total refactored nodes:  367350
Norway nodes: 4025
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 25}
-----done-----
120.9  ms elapsed time
------------------------------> total refactored nodes:  371375
Guadeloupe nodes: 4028
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 28}
-----done-----
128.1  ms elapsed time
------------------------------> total refactored nodes:  375403
Montserrat nodes: 4053
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 53}
-----done-----
116.1  ms elapsed time
------------------------------> total refactored nodes:  379456
United States Minor Outlying Islands nodes: 4140
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 140}
-----done-----
120.7  ms elapsed time
------------------------------> total refactored nodes:  383596
Greenland nodes: 4113
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 113}
-----done-----
123.2  ms elapsed time
------------------------------> total refactored nodes:  387709
Madagascar nodes: 4128
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 128}
-----done-----
123.9  ms elapsed time
------------------------------> total refactored nodes:  391837
San Marino nodes: 4045
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 45}
-----done-----
123.1  ms elapsed time
------------------------------> total refactored nodes:  395882
Guatemala nodes: 3993
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1993}
-----done-----
117.1  ms elapsed time
------------------------------> total refactored nodes:  399875
Malta nodes: 4069
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 69}
-----done-----
125.7  ms elapsed time
------------------------------> total refactored nodes:  403944
Nigeria nodes: 4123
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 123}
-----done-----
131.2  ms elapsed time
------------------------------> total refactored nodes:  408067
Germany nodes: 4036
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 36}
-----done-----
120.5  ms elapsed time
------------------------------> total refactored nodes:  412103
France nodes: 4232
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 232}
-----done-----
120.0  ms elapsed time
------------------------------> total refactored nodes:  416335
New Caledonia nodes: 4126
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 126}
-----done-----
127.9  ms elapsed time
------------------------------> total refactored nodes:  420461
Ireland nodes: 4043
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 43}
-----done-----
123.1  ms elapsed time
------------------------------> total refactored nodes:  424504
Northern Mariana Islands nodes: 4022
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 22}
-----done-----
134.9  ms elapsed time
------------------------------> total refactored nodes:  428526
Cyprus nodes: 4093
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 93}
-----done-----
150.2  ms elapsed time
------------------------------> total refactored nodes:  432619
Italy nodes: 3954
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1954}
-----done-----
110.2  ms elapsed time
------------------------------> total refactored nodes:  436573
Venezuela nodes: 4059
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 59}
-----done-----
124.9  ms elapsed time
------------------------------> total refactored nodes:  440632
Kenya nodes: 4210
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 210}
-----done-----
120.2  ms elapsed time
------------------------------> total refactored nodes:  444842
Pakistan nodes: 4050
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 50}
-----done-----
120.1  ms elapsed time
------------------------------> total refactored nodes:  448892
Bhutan nodes: 4007
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 7}
-----done-----
118.5  ms elapsed time
------------------------------> total refactored nodes:  452899
Netherlands Antilles nodes: 4126
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 126}
-----done-----
128.9  ms elapsed time
------------------------------> total refactored nodes:  457025
Czech Republic nodes: 4090
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 90}
-----done-----
128.3  ms elapsed time
------------------------------> total refactored nodes:  461115
Bangladesh nodes: 3990
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1990}
-----done-----
115.7  ms elapsed time
------------------------------> total refactored nodes:  465105
Palau nodes: 4051
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 51}
-----done-----
126.1  ms elapsed time
------------------------------> total refactored nodes:  469156
Benin nodes: 4139
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 139}
-----done-----
127.2  ms elapsed time
------------------------------> total refactored nodes:  473295
Marshall Islands nodes: 4061
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 61}
-----done-----
124.4  ms elapsed time
------------------------------> total refactored nodes:  477356
Falkland Islands (Malvinas) nodes: 4169
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 169}
-----done-----
120.6  ms elapsed time
------------------------------> total refactored nodes:  481525
Democratic People's Republic of Korea nodes: 4060
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 60}
-----done-----
128.2  ms elapsed time
------------------------------> total refactored nodes:  485585
Poland nodes: 4136
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 136}
-----done-----
126.7  ms elapsed time
------------------------------> total refactored nodes:  489721
Martinique nodes: 4105
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 105}
-----done-----
122.4  ms elapsed time
------------------------------> total refactored nodes:  493826
Holy See (Vatican City State) nodes: 4155
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 155}
-----done-----
128.8  ms elapsed time
------------------------------> total refactored nodes:  497981
Estonia nodes: 4085
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 85}
-----done-----
125.0  ms elapsed time
------------------------------> total refactored nodes:  502066
Uruguay nodes: 4089
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 89}
-----done-----
118.6  ms elapsed time
------------------------------> total refactored nodes:  506155
Burundi nodes: 4140
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 140}
-----done-----
123.1  ms elapsed time
------------------------------> total refactored nodes:  510295
Ecuador nodes: 4063
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 63}
-----done-----
123.7  ms elapsed time
------------------------------> total refactored nodes:  514358
Ukraine nodes: 4154
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 154}
-----done-----
123.8  ms elapsed time
------------------------------> total refactored nodes:  518512
Cayman Islands nodes: 4123
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 123}
-----done-----
137.9  ms elapsed time
------------------------------> total refactored nodes:  522635
Haiti nodes: 4152
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 152}
-----done-----
124.0  ms elapsed time
------------------------------> total refactored nodes:  526787
Dominican Republic nodes: 4084
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 84}
-----done-----
122.4  ms elapsed time
------------------------------> total refactored nodes:  530871
Guinea-Bissau nodes: 4009
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 9}
-----done-----
129.8  ms elapsed time
------------------------------> total refactored nodes:  534880
Denmark nodes: 4041
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 41}
-----done-----
121.3  ms elapsed time
------------------------------> total refactored nodes:  538921
Saint Lucia nodes: 4082
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 82}
-----done-----
123.9  ms elapsed time
------------------------------> total refactored nodes:  543003
Sri Lanka nodes: 4064
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 64}
-----done-----
125.0  ms elapsed time
------------------------------> total refactored nodes:  547067
Guernsey nodes: 4125
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 125}
-----done-----
126.1  ms elapsed time
------------------------------> total refactored nodes:  551192
China nodes: 3964
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1964}
-----done-----
121.2  ms elapsed time
------------------------------> total refactored nodes:  555156
South Georgia and the South Sandwich Islands nodes: 4058
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 58}
-----done-----
118.6  ms elapsed time
------------------------------> total refactored nodes:  559214
Isle of Man nodes: 4077
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 77}
-----done-----
123.6  ms elapsed time
------------------------------> total refactored nodes:  563291
Zambia nodes: 4080
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 80}
-----done-----
129.2  ms elapsed time
------------------------------> total refactored nodes:  567371
Grenada nodes: 4080
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 80}
-----done-----
119.4  ms elapsed time
------------------------------> total refactored nodes:  571451
Puerto Rico nodes: 4116
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 116}
-----done-----
129.1  ms elapsed time
------------------------------> total refactored nodes:  575567
Kazakhstan nodes: 4091
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 91}
-----done-----
121.0  ms elapsed time
------------------------------> total refactored nodes:  579658
Turks and Caicos Islands nodes: 4022
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 22}
-----done-----
121.5  ms elapsed time
------------------------------> total refactored nodes:  583680
Liberia nodes: 4010
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 10}
-----done-----
116.4  ms elapsed time
------------------------------> total refactored nodes:  587690
El Salvador nodes: 4044
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 44}
-----done-----
123.5  ms elapsed time
------------------------------> total refactored nodes:  591734
Bahrain nodes: 4173
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 173}
-----done-----
126.4  ms elapsed time
------------------------------> total refactored nodes:  595907
Suriname nodes: 4061
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 61}
-----done-----
118.1  ms elapsed time
------------------------------> total refactored nodes:  599968
Liechtenstein nodes: 4143
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 143}
-----done-----
121.4  ms elapsed time
------------------------------> total refactored nodes:  604111
Greece nodes: 4149
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 149}
-----done-----
124.1  ms elapsed time
------------------------------> total refactored nodes:  608260
Portugal nodes: 4027
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 27}
-----done-----
130.8  ms elapsed time
------------------------------> total refactored nodes:  612287
Bosnia and Herzegovina nodes: 4083
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 83}
-----done-----
120.6  ms elapsed time
------------------------------> total refactored nodes:  616370
Bulgaria nodes: 4167
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 167}
-----done-----
119.6  ms elapsed time
------------------------------> total refactored nodes:  620537
Somalia nodes: 4027
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 27}
-----done-----
123.5  ms elapsed time
------------------------------> total refactored nodes:  624564
Sweden nodes: 4035
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 35}
-----done-----
124.2  ms elapsed time
------------------------------> total refactored nodes:  628599
Iceland nodes: 4032
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 32}
-----done-----
124.3  ms elapsed time
------------------------------> total refactored nodes:  632631
United Arab Emirates nodes: 4095
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 95}
-----done-----
120.9  ms elapsed time
------------------------------> total refactored nodes:  636726
Netherlands nodes: 4175
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 175}
-----done-----
124.6  ms elapsed time
------------------------------> total refactored nodes:  640901
Ethiopia nodes: 4135
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 135}
-----done-----
119.1  ms elapsed time
------------------------------> total refactored nodes:  645036
Philippines nodes: 4093
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 93}
-----done-----
124.2  ms elapsed time
------------------------------> total refactored nodes:  649129
Colombia nodes: 4191
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 191}
-----done-----
124.8  ms elapsed time
------------------------------> total refactored nodes:  653320
Oman nodes: 4159
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 159}
-----done-----
114.7  ms elapsed time
------------------------------> total refactored nodes:  657479
Swaziland nodes: 4060
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 60}
-----done-----
123.6  ms elapsed time
------------------------------> total refactored nodes:  661539
Turkey nodes: 4114
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 114}
-----done-----
119.5  ms elapsed time
------------------------------> total refactored nodes:  665653
British Indian Ocean Territory (Chagos Archipelago) nodes: 4100
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 100}
-----done-----
123.0  ms elapsed time
------------------------------> total refactored nodes:  669753
Samoa nodes: 4007
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 7}
-----done-----
119.6  ms elapsed time
------------------------------> total refactored nodes:  673760
Wallis and Futuna nodes: 4113
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 113}
-----done-----
116.2  ms elapsed time
------------------------------> total refactored nodes:  677873
Trinidad and Tobago nodes: 4062
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 62}
-----done-----
123.7  ms elapsed time
------------------------------> total refactored nodes:  681935
Lithuania nodes: 4061
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 61}
-----done-----
121.9  ms elapsed time
------------------------------> total refactored nodes:  685996
India nodes: 4060
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 60}
-----done-----
120.8  ms elapsed time
------------------------------> total refactored nodes:  690056
Anguilla nodes: 4168
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 168}
-----done-----
121.9  ms elapsed time
------------------------------> total refactored nodes:  694224
Libyan Arab Jamahiriya nodes: 4058
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 58}
-----done-----
126.5  ms elapsed time
------------------------------> total refactored nodes:  698282
Djibouti nodes: 4094
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 94}
-----done-----
146.6  ms elapsed time
------------------------------> total refactored nodes:  702376
Nepal nodes: 4027
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 27}
-----done-----
118.6  ms elapsed time
------------------------------> total refactored nodes:  706403
French Southern Territories nodes: 3965
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1965}
-----done-----
121.3  ms elapsed time
------------------------------> total refactored nodes:  710368
Cook Islands nodes: 4029
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 29}
-----done-----
120.8  ms elapsed time
------------------------------> total refactored nodes:  714397
Latvia nodes: 4055
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 55}
-----done-----
118.9  ms elapsed time
------------------------------> total refactored nodes:  718452
Hungary nodes: 4246
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 246}
-----done-----
119.5  ms elapsed time
------------------------------> total refactored nodes:  722698
Antigua and Barbuda nodes: 3981
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1981}
-----done-----
117.2  ms elapsed time
------------------------------> total refactored nodes:  726679
Chad nodes: 4046
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 46}
-----done-----
119.5  ms elapsed time
------------------------------> total refactored nodes:  730725
Algeria nodes: 4058
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 58}
-----done-----
116.0  ms elapsed time
------------------------------> total refactored nodes:  734783
Jordan nodes: 3952
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1952}
-----done-----
131.4  ms elapsed time
------------------------------> total refactored nodes:  738735
Paraguay nodes: 4076
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 76}
-----done-----
123.8  ms elapsed time
------------------------------> total refactored nodes:  742811
Gabon nodes: 4098
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 98}
-----done-----
126.5  ms elapsed time
------------------------------> total refactored nodes:  746909
Lebanon nodes: 4107
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 107}
-----done-----
113.6  ms elapsed time
------------------------------> total refactored nodes:  751016
Togo nodes: 4044
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 44}
-----done-----
121.9  ms elapsed time
------------------------------> total refactored nodes:  755060
Russian Federation nodes: 4108
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 108}
-----done-----
127.6  ms elapsed time
------------------------------> total refactored nodes:  759168
Fiji nodes: 4155
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 155}
-----done-----
121.2  ms elapsed time
------------------------------> total refactored nodes:  763323
Sierra Leone nodes: 4127
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 127}
-----done-----
119.4  ms elapsed time
------------------------------> total refactored nodes:  767450
Mauritania nodes: 3997
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1997}
-----done-----
124.4  ms elapsed time
------------------------------> total refactored nodes:  771447
Macao nodes: 3982
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1982}
-----done-----
109.5  ms elapsed time
------------------------------> total refactored nodes:  775429
American Samoa nodes: 4008
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 8}
-----done-----
123.7  ms elapsed time
------------------------------> total refactored nodes:  779437
Slovakia (Slovak Republic) nodes: 4067
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 67}
-----done-----
123.4  ms elapsed time
------------------------------> total refactored nodes:  783504
Uzbekistan nodes: 4091
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 91}
-----done-----
120.1  ms elapsed time
------------------------------> total refactored nodes:  787595
Guyana nodes: 4102
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 102}
-----done-----
120.7  ms elapsed time
------------------------------> total refactored nodes:  791697
Saint Vincent and the Grenadines nodes: 4181
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 181}
-----done-----
138.4  ms elapsed time
------------------------------> total refactored nodes:  795878
Tuvalu nodes: 4042
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 42}
-----done-----
125.8  ms elapsed time
------------------------------> total refactored nodes:  799920
Nauru nodes: 4156
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 156}
-----done-----
119.3  ms elapsed time
------------------------------> total refactored nodes:  804076
Spain nodes: 4138
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 138}
-----done-----
121.8  ms elapsed time
------------------------------> total refactored nodes:  808214
Albania nodes: 4113
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 113}
-----done-----
126.0  ms elapsed time
------------------------------> total refactored nodes:  812327
Namibia nodes: 4027
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 27}
-----done-----
112.4  ms elapsed time
------------------------------> total refactored nodes:  816354
Mexico nodes: 4164
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 164}
-----done-----
123.0  ms elapsed time
------------------------------> total refactored nodes:  820518
Serbia nodes: 4019
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 19}
-----done-----
120.3  ms elapsed time
------------------------------> total refactored nodes:  824537
Cameroon nodes: 4018
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 18}
-----done-----
132.0  ms elapsed time
------------------------------> total refactored nodes:  828555
Iraq nodes: 4107
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 107}
-----done-----
117.7  ms elapsed time
------------------------------> total refactored nodes:  832662
Guinea nodes: 3967
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1967}
-----done-----
112.8  ms elapsed time
------------------------------> total refactored nodes:  836629
New Zealand nodes: 4049
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 49}
-----done-----
123.7  ms elapsed time
------------------------------> total refactored nodes:  840678
Chile nodes: 4163
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 163}
-----done-----
124.7  ms elapsed time
------------------------------> total refactored nodes:  844841
Belize nodes: 3982
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1982}
-----done-----
117.1  ms elapsed time
------------------------------> total refactored nodes:  848823
Armenia nodes: 4053
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 53}
-----done-----
124.2  ms elapsed time
------------------------------> total refactored nodes:  852876
Botswana nodes: 4174
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 174}
-----done-----
125.1  ms elapsed time
------------------------------> total refactored nodes:  857050
Cocos (Keeling) Islands nodes: 4112
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 112}
-----done-----
123.2  ms elapsed time
------------------------------> total refactored nodes:  861162
United States of America nodes: 4192
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 192}
-----done-----
124.6  ms elapsed time
------------------------------> total refactored nodes:  865354
Hong Kong nodes: 4052
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 52}
-----done-----
119.6  ms elapsed time
------------------------------> total refactored nodes:  869406
Croatia nodes: 4045
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 45}
-----done-----
113.7  ms elapsed time
------------------------------> total refactored nodes:  873451
Israel nodes: 4104
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 104}
-----done-----
119.7  ms elapsed time
------------------------------> total refactored nodes:  877555
Cuba nodes: 4029
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 29}
-----done-----
123.8  ms elapsed time
------------------------------> total refactored nodes:  881584
Rwanda nodes: 4045
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 45}
-----done-----
133.9  ms elapsed time
------------------------------> total refactored nodes:  885629
United Kingdom nodes: 4183
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 183}
-----done-----
117.1  ms elapsed time
------------------------------> total refactored nodes:  889812
Malaysia nodes: 3949
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1949}
-----done-----
118.2  ms elapsed time
------------------------------> total refactored nodes:  893761
Jersey nodes: 4032
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 32}
-----done-----
118.3  ms elapsed time
------------------------------> total refactored nodes:  897793
Burkina Faso nodes: 4077
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 77}
-----done-----
124.6  ms elapsed time
------------------------------> total refactored nodes:  901870
Singapore nodes: 4045
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 45}
-----done-----
117.4  ms elapsed time
------------------------------> total refactored nodes:  905915
Mozambique nodes: 4022
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 22}
-----done-----
116.9  ms elapsed time
------------------------------> total refactored nodes:  909937
Equatorial Guinea nodes: 4216
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 216}
-----done-----
118.6  ms elapsed time
------------------------------> total refactored nodes:  914153
Cambodia nodes: 4107
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 107}
-----done-----
131.4  ms elapsed time
------------------------------> total refactored nodes:  918260
Palestinian Territory nodes: 4097
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 97}
-----done-----
117.1  ms elapsed time
------------------------------> total refactored nodes:  922357
Montenegro nodes: 4042
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 42}
-----done-----
125.9  ms elapsed time
------------------------------> total refactored nodes:  926399
Bermuda nodes: 4027
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 27}
-----done-----
123.3  ms elapsed time
------------------------------> total refactored nodes:  930426
Georgia nodes: 4079
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 79}
-----done-----
124.2  ms elapsed time
------------------------------> total refactored nodes:  934505
Japan nodes: 3993
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1993}
-----done-----
113.2  ms elapsed time
------------------------------> total refactored nodes:  938498
Kyrgyz Republic nodes: 4090
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 90}
-----done-----
123.5  ms elapsed time
------------------------------> total refactored nodes:  942588
Mayotte nodes: 4087
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 87}
-----done-----
114.3  ms elapsed time
------------------------------> total refactored nodes:  946675
Finland nodes: 4010
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 10}
-----done-----
130.7  ms elapsed time
------------------------------> total refactored nodes:  950685
Cote d'Ivoire nodes: 4134
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 134}
-----done-----
118.2  ms elapsed time
------------------------------> total refactored nodes:  954819
Moldova nodes: 4131
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 131}
-----done-----
118.7  ms elapsed time
------------------------------> total refactored nodes:  958950
Tajikistan nodes: 4053
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 53}
-----done-----
120.2  ms elapsed time
------------------------------> total refactored nodes:  963003
Andorra nodes: 4100
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 100}
-----done-----
119.8  ms elapsed time
------------------------------> total refactored nodes:  967103
Nicaragua nodes: 4006
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 6}
-----done-----
120.2  ms elapsed time
------------------------------> total refactored nodes:  971109
Barbados nodes: 4306
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 306}
-----done-----
144.4  ms elapsed time
------------------------------> total refactored nodes:  975415
Micronesia nodes: 4109
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 109}
-----done-----
122.3  ms elapsed time
------------------------------> total refactored nodes:  979524
Indonesia nodes: 4104
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 104}
-----done-----
121.6  ms elapsed time
------------------------------> total refactored nodes:  983628
Jamaica nodes: 4160
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 160}
-----done-----
122.7  ms elapsed time
------------------------------> total refactored nodes:  987788
Iran nodes: 3986
{'relationships_created': 2000}
--next batch--
{'relationships_created': 1986}
-----done-----
113.6  ms elapsed time
------------------------------> total refactored nodes:  991774
Bolivia nodes: 4061
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 61}
-----done-----
120.3  ms elapsed time
------------------------------> total refactored nodes:  995835
Angola nodes: 4165
{'relationships_created': 2000}
--next batch--
{'relationships_created': 2000}
--next batch--
{'relationships_created': 165}
-----done-----
124.1  ms elapsed time
------------------------------> total refactored nodes:  1000000
Done!
0.5  minutes elapsed time
```
