Cassandra Accord
================
Distributed consensus protocol for Apache Cassandra. Accord is the first protocol to achieve the same steady-state performance as leader-based protocols under important conditions such as contention and failure, while delivering the benefits of leaderless approaches to scaling, transaction isolation and geo-distributed client latency. See [the Accord whitepaper](https://cwiki.apache.org/confluence/download/attachments/188744725/Accord.pdf?version=2&modificationDate=1637000779000&api=v2).

Accord Protocol
---------------
Key Features:
- Leaderless Design: Accord operates without a designated leader node, allowing any node to coordinate transactions. This approach eliminates single points of failure and enhances the system's scalability.
- Fast Consensus Mechanism: It utilizes a special initial round for faster consensus, enabling most transactions to reach agreement quickly, often within two message round-trips.
- Efficient Handling of High Contention: Accord's design reduces the incidence of contention by determining a timestamp for each transaction’s execution order. This feature is particularly beneficial in scenarios where many transactions conflict.
- Partial State-Machine Replication for Scalability: The protocol can be extended to partial state-machine replication (PSMR) scenarios, improving scalability by allowing shards to replicate only a part of the global state-machine.
- Strong Consistency and Isolation Guarantees: Accord provides optimal baseline characteristics for consistency and isolation, ensuring that conflicting transactions are applied in the same order on all participating replicas.
- Support for General-Purpose Transactions: The protocol is capable of handling general-purpose transactions that combine cross-shard state, facilitating multi-statement ACID transactions across different partitions.
- Robust Recovery Mechanism: Accord includes a comprehensive recovery protocol to handle failures effectively. This protocol ensures the continuation and completion of transactions even in the event of coordinator failure.
- Minimal Latency with Fast-Path Option: For transactions where a fast-path quorum of replicas unanimously accept a proposed timestamp, the decision is made immediately, significantly reducing latency.
- Dependency and Order Safety: The protocol ensures that all dependencies of a transaction are accounted for before its execution, maintaining strict order safety and atomicity.

At a high level, it works as follows:
1. Consensus Phase:
- Accord assigns each conflicting transaction a unique execution timestamp, forming a total order.
- Timestamps are tuples of time, sequence, and identifier values. These timestamps are used to assign execution times and impose a total order on conflicting transactions.
- A transaction coordinator proposes a timestamp for execution. If a fast-path quorum of replicas unanimously accepts this timestamp, it is immediately decided. Otherwise, a slow path using classic Paxos is used to agree on one of the possible timestamps.
- Execution proceeds after all transactions with earlier timestamps have been committed and executed.
2. Execution Phase:
- Once the execution timestamp is decided and logically committed, it is disseminated to all shards.
- The coordinator sends Read messages to at least one process in each shard to gather responses.
- Execution awaits the completion of dependencies with lower execution timestamps before computing the transaction result.
- The result is then applied to all replicas.

Recovery protocol:
- If a transaction coordinator fails, a weak failure detector invokes a recovery protocol.
- The recovery protocol contacts a recovery quorum to ensure the transaction is pre-accepted, maintaining the properties of normal execution.
- For fast-path decisions, the protocol may propose a slow-path solution based on the dependencies of superseding transactions. This ensures that all necessary properties are maintained during the recovery of a transaction.

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
