/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package accord.local;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.api.TestableConfigurationService;
import accord.burn.random.FrequentLargeRange;
import accord.coordinate.Preempted;
import accord.coordinate.Timeout;
import accord.coordinate.TopologyMismatch;
import accord.impl.InMemoryCommandStore;
import accord.impl.MessageListener;
import accord.impl.PrefixedIntHashKey;
import accord.impl.TopologyFactory;
import accord.impl.basic.Cluster;
import accord.impl.basic.Packet;
import accord.impl.basic.PendingRunnable;
import accord.impl.basic.PropagatingPendingQueue;
import accord.impl.basic.RandomDelayQueue;
import accord.impl.basic.SimulatedDelayedExecutorService;
import accord.impl.list.ListAgent;
import accord.messages.MessageType;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.primitives.Timestamp;
import accord.topology.TopologyUtils;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static accord.Utils.listWriteTxn;
import static accord.utils.Property.qt;
import static accord.utils.Utils.addAll;

class CommandsTest
{
    private static final Logger logger = LoggerFactory.getLogger(CommandsTest.class);

    @Test
    void removeRangesValidate()
    {
        // could use AccordGens.nodes() but to make debugging easier limit the node ids to small values
        Gen<List<Node.Id>> nodeGen = Gens.ints().between(1, 10).map(size -> IntStream.range(1, size + 1).mapToObj(Node.Id::new).collect(Collectors.toList()));
        qt().check(rs -> {
            List<Node.Id> nodes = nodeGen.next(rs);
            int rf = nodes.size() <= 3 ? nodes.size() : rs.nextInt(3, nodes.size());
            logger.info("Running with {} nodes and rf={}", nodes.size(), rf);
            Range[] prefix0 = PrefixedIntHashKey.ranges(0, nodes.size());
            Range[] prefix1 = PrefixedIntHashKey.ranges(1, nodes.size());
            Range[] allRanges = addAll(prefix0, prefix1);

            Topology initialTopology = TopologyUtils.topology(1, nodes, Ranges.of(allRanges), rf);
            Topology updatedTopology = TopologyUtils.topology(2, nodes, Ranges.of(prefix0), rf); // drop prefix1

            cluster(rs::fork, nodes, initialTopology, nodeMap -> new Request()
            {
                @Override
                public void preProcess(Node on, Node.Id from, ReplyContext replyContext)
                {
                    // no-op
                }

                @Override
                public void process(Node node, Node.Id from, ReplyContext replyContext)
                {
                    Ranges localRange = Ranges.ofSortedAndDeoverlapped(prefix1); // make sure to use the range removed

                    Gen<Key> keyGen = AccordGens.prefixedIntHashKeyInsideRanges(localRange);
                    Keys keys = Keys.of(Gens.lists(keyGen).unique().ofSizeBetween(1, 10).next(rs));
                    Txn txn = listWriteTxn(from, keys);

                    TxnId txnId = node.nextTxnId(Txn.Kind.Write, Routable.Domain.Key);

                    for (Node n : nodeMap.values())
                        ((TestableConfigurationService) n.configService()).reportTopology(updatedTopology);

                    node.coordinate(txnId, txn).addCallback((success, failure) -> {
                        if (failure == null)
                        {
                            node.agent().onUncaughtException(new AssertionError("Expected TopologyMismatch exception, but txn was success"));
                        }
                        else if (!(failure instanceof TopologyMismatch))
                        {
                            if (failure instanceof Timeout || failure instanceof Preempted)
                            {
                                logger.warn("{} seen...", failure.getClass().getSimpleName());
                            }
                            else
                            {
                                node.agent().onUncaughtException(new AssertionError("Expected TopologyMismatch exception, but failed with different exception", failure));
                            }
                        }
                    });
                }

                @Override
                public MessageType type()
                {
                    return null;
                }
            });
        });
    }

    static void cluster(Supplier<RandomSource> randomSupplier, List<Node.Id> nodes, Topology initialTopology, Function<Map<Node.Id, Node>, Request> init)
    {
        List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
        PropagatingPendingQueue queue = new PropagatingPendingQueue(failures, new RandomDelayQueue(randomSupplier.get()));
        RandomSource retryRandom = randomSupplier.get();
        Consumer<Runnable> retryBootstrap = retry -> {
            long delay = retryRandom.nextInt(1, 15);
            queue.add((PendingRunnable) retry::run, delay, TimeUnit.SECONDS);
        };
        Function<BiConsumer<Timestamp, Ranges>, ListAgent> agentSupplier = onStale -> new ListAgent(1000L, failures::add, retryBootstrap, onStale);
        RandomSource nowRandom = randomSupplier.get();
        Supplier<LongSupplier> nowSupplier = () -> {
            RandomSource forked = nowRandom.fork();
            // TODO (now): meta-randomise scale of clock drift
            return FrequentLargeRange.builder(forked)
                                     .ratio(1, 5)
                                     .small(50, 5000, TimeUnit.MICROSECONDS)
                                     .large(1, 10, TimeUnit.MILLISECONDS)
                                     .build()
                                     .mapAsLong(j -> Math.max(0, queue.nowInMillis() + TimeUnit.NANOSECONDS.toMillis(j)))
                                     .asLongSupplier(forked);
        };
        SimulatedDelayedExecutorService globalExecutor = new SimulatedDelayedExecutorService(queue, new ListAgent(1000L, failures::add, retryBootstrap, (i1, i2) -> {
            throw new IllegalAccessError("Global executor should enver get a stale event");
        }));
        TopologyFactory topologyFactory = new TopologyFactory(initialTopology.maxRf(), initialTopology.ranges().stream().toArray(Range[]::new))
        {
            @Override
            public Topology toTopology(Node.Id[] cluster)
            {
                return initialTopology;
            }
        };
        AtomicInteger counter = new AtomicInteger();
        AtomicReference<Map<Node.Id, Node>> nodeMap = new AtomicReference<>();
        Cluster.run(nodes.toArray(Node.Id[]::new),
                    MessageListener.get(),
                    () -> queue,
                    (id, onStale) -> globalExecutor.withAgent(agentSupplier.apply(onStale)),
                    queue::checkFailures,
                    ignore -> {
                    },
                    randomSupplier,
                    nowSupplier,
                    topologyFactory,
                    new Supplier<>()
                    {
                        private Iterator<Request> requestIterator = null;
                        private final RandomSource rs = randomSupplier.get();
                        @Override
                        public Packet get()
                        {
                            // ((Cluster) node.scheduler()).onDone(() -> checkOnResult(homeKey, txnId, 0, null));
                            if (requestIterator == null)
                            {
                                Map<Node.Id, Node> nodes = nodeMap.get();
                                requestIterator = Collections.singleton(init.apply(nodes)).iterator();
                                // the node selected does not matter and should not impact determinism, they all share the same scheduler
                                ((Cluster) nodes.values().stream().findFirst().get().scheduler()).onDone(() -> checkState(0, nodes.values()));
                            }
                            if (!requestIterator.hasNext())
                                return null;
                            Node.Id id = rs.pick(nodes);
                            return new Packet(id, id, counter.incrementAndGet(), requestIterator.next());
                        }
                    },
                    Runnable::run,
                    nodeMap::set);
        if (!failures.isEmpty())
        {
            AssertionError error = new AssertionError("Unexpected errors detected");
            failures.forEach(error::addSuppressed);
            throw error;
        }
    }

    private static void checkState(int attempt, Collection<Node> values)
    {
        List<String> faults = new ArrayList<>();
        for (Node node : values)
        {
            CommandStores stores = node.commandStores();
            for (int id : stores.ids())
            {
                InMemoryCommandStore store = (InMemoryCommandStore) stores.forId(id);
                for (InMemoryCommandStore.GlobalCommand cmd : store.unsafeCommands().values())
                {
                    if (cmd.value() == null) // empty isn't public
                        continue;
                    Command command = cmd.value();
                    if (command.status() == Status.Invalidated || command.status() == Status.AcceptedInvalidate)
                        continue;
                    // current limitation of SimpleProgressLog is that only the home shard will attempt to recover, so
                    // non-home shards may stay PreAccepted!
                    RoutingKey key = command.homeKey();
                    if (key != null && !store.rangesForEpoch.allAt(command.txnId().epoch()).contains(key))
                    {
                        // non-home shard... make sure the state is as expected
                        if (command.status() == Status.PreAccepted)
                            continue;
                    }
                    faults.add(String.format("Node %s and store %d had command with incorrect status; expected Invalidated or AcceptedInvalidate but was %s", node.id(), id, command));
                }
            }
        }
        if (faults.isEmpty())
            return;
        if (attempt == 10)
            throw new AssertionError("Cluster did not converge on time:\n" + String.join("\n", faults));
        values.stream().findFirst().get().scheduler().once(() -> checkState(attempt + 1, values), 10, TimeUnit.HOURS);
    }
}