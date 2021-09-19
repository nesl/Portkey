# Portkey: Adaptive Key-Value Placement over Dynamic Edge Networks
This repository offers an adaptive datastore implementation built atop **Redis Cluster** to dynamically reconfigure data placement to reduce client access latency.

## client
To enable adaptive data placement, Portkey clients need to track their access patterns and network perspective. This modified version of the redis-clustr npm package has this added functionality. client.js offers a basic trace replay.

## engine
This folder hosts the core system intelligence, consisting of a placement solver with a migration handler. 

## redis-src
This folder hosts a slightly modified redis command-line client used for migration. This can also be done with a node.js implementation of migration.

## traces
This folder contains client traces with varying degrees of locality to test edge KV performance