Cassandra Accord
================
Distributed consensus protocol for Apache Cassandra. Accord is the first protocol to achieve the same steady-state performance as leader-based protocols under important conditions such as contention and failure, while delivering the benefits of leaderless approaches to scaling, transaction isolation and geo-distributed client latency. See [the Accord whitepaper](https://cwiki.apache.org/confluence/download/attachments/188744725/Accord.pdf?version=2&modificationDate=1637000779000&api=v2).

Accord Protocol
---------------
Key Features:
- Leaderless - There is no designated leader node. Any node can coordinate transactions. This removes bottlenecks and improves scalability.
- Single round-trip commits - Most transactions can commit in a single message round-trip between coordinating node and replicas. This minimizes latency.
- High contention performance - Special techniques allow it to avoid slowed performance when many transactions conflict.
- Configurable failure tolerance - The protocol can be dynamically reconfigured to maintain fast performance even after node failures.
- Strong consistency guarantees - Provides strict serializability, the strongest isolation level. Transactions behave as if they execute one at a time.
- General purpose transactions - Supports cross-shard multi-statement ACID transactions, not just single partition reads/writes.

At a high level, it works as follows:
1. A coordinator node is chosen to handle each transaction (typically nearby the client for low latency).
2. The coordinator gets votes from replica nodes on an execution timestamp and set of conflicts for the transaction.
3. With enough votes, the transaction can commit in a single round-trip.
4. The coordinator waits for conflicting transactions, then tells the replicas to execute and persist the changes.

Code structure
--------------
`accord-core` is the implementation of the Accord protocol that is imported in Cassandra. See [cep-15-accord branch](https://github.com/apache/cassandra/tree/cep-15-accord) in Cassandra.

`accord-maelstrom` is a wrapper for running Accord within [Jepsen Maelstrom](https://github.com/jepsen-io/maelstrom) which uses STDIN for ingress and STDOUT for egress. 

Build
-----
This repo is used as a submodule for Cassandra, see [C*/CONTRIBUTING.md](https://github.com/apache/cassandra/blob/607302aaa8c1816a75a70173ae39a7d96ce1b18a/CONTRIBUTING.md#working-with-submodules) for instructions on how to include it.

To build this repo:
```bash
./gradlew dependencies
./gradlew check
```

Maelstrom
---------
Jepsen Maelstrom is a workbench for writing toy implementations of distributed systems. It's used for running dtests (distributed tests) against Accord. 

First, build `accord-maelstrom`:
```bash
cd ./accord-maelstrom
./build.sh
```
Save the path to the server script in an environment variable:
```bash
cd ./accord-maelstrom
export ACCORD_MAELSTROM_SERVER=$(pwd)/server.sh
```

Clone Maelstrom repo or get the binary, see [Maelstrom installation](https://github.com/jepsen-io/maelstrom/blob/main/doc/01-getting-ready/index.md#installation) for more. If cloned, use `lein run` instead of `./maelstrom` below.
```bash
# Single-node KV store test
./maelstrom test -w lin-kv --bin $ACCORD_MAELSTROM_SERVER --time-limit 10 --rate 10 --node-count 1 --concurrency 2n
# Multi-node KV store test
./maelstrom test -w lin-kv --bin $ACCORD_MAELSTROM_SERVER --time-limit 10 --rate 10 --node-count 3 --concurrency 2n
# Multi-node with partitions
./maelstrom test -w lin-kv --bin $ACCORD_MAELSTROM_SERVER --time-limit 60 --rate 30 --node-count 3 --concurrency 4n --nemesis partition --nemesis-interval 10 --test-count 10
```
