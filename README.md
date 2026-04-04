# Château by Catenary

Château is a system to group data ingest feeds by a shared super-agency, to prevent weird lookup N+1 patterns.

Each Château is computed by performing depth first search on all feeds and their associated operators and their associated feeds and so forth. Thus, if all data feed and operator entities are defined as verticies in a graph, and the links defined as edges, each château is defined as a connected component.

Example of problems:
![image](https://github.com/catenarytransit/community/assets/7539174/0e44a6b8-3777-45c2-aa90-637e817d291d)

The idea is to group all the feeds under an agency group under a single name like "newyorkcity", thus the name château.
