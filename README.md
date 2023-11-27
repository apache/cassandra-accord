Cassandra Accord
----------------
Distributed consensus protocol for Apache Cassandra. Accord is the first protocol to achieve the same steady-state performance as leader-based protocols under important conditions such as contention and failure, while delivering the benefits of leaderless approaches to scaling, transaction isolation and geo-distributed client latency. See [the Accord whitepaper](https://cwiki.apache.org/confluence/download/attachments/188744725/Accord.pdf?version=2&modificationDate=1637000779000&api=v2).

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
