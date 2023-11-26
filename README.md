Cassandra Accord
----------------

Distributed consensus protocol for Apache Cassandra. Accord is the first protocol to achieve the same steady-state performance as leader-based protocols under important conditions such as contention and failure, while delivering the benefits of leaderless approaches to scaling, transaction isolation and geo-distributed client latency. See [the Accord whitepaper](https://cwiki.apache.org/confluence/download/attachments/188744725/Accord.pdf?version=2&modificationDate=1637000779000&api=v2).

Build
-----
This repo is used as a git submodule in Cassandra, see [C*/CONTRIBUTING.md](https://github.com/apache/cassandra/blob/607302aaa8c1816a75a70173ae39a7d96ce1b18a/CONTRIBUTING.md#working-with-submodules) for instructions.

To build this repo:
```bash
./gradlew dependencies
./gradlew check
```

Maelstrom testing
-----------------

