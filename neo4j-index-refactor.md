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

In small graphs this is a trivial operation, and thanks to the Michael Hunger and the APOC team you perform refactoring using single statement, which will also delete the extracted property from the children, and copy over any other needed hierarchy properties from children to the parent (such as `['area']`), in batches:

```
MATCH (n:Organization)
CALL apoc.refactor.categorize('country','HAS_LOCATION',true,'Country','countryName',['area'],1000)
```

However, for large graphs a naive approach is computationally expensive and v e r y   s l o w.
