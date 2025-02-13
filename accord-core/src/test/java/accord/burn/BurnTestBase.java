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

package accord.burn;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.Journal;
import accord.api.Key;
import accord.api.ProtocolModifiers.Toggles;
import accord.burn.random.FrequentLargeRange;
import accord.impl.MessageListener;
import accord.impl.PrefixedIntHashKey;
import accord.impl.TopologyFactory;
import accord.impl.basic.Cluster;
import accord.impl.basic.Cluster.Stats;
import accord.impl.basic.InMemoryJournal;
import accord.impl.basic.MonitoredPendingQueue;
import accord.impl.basic.NodeSink;
import accord.impl.basic.Packet;
import accord.impl.basic.PendingQueue;
import accord.impl.basic.PendingRunnable;
import accord.impl.basic.RandomDelayQueue;
import accord.impl.basic.RandomDelayQueue.Factory;
import accord.impl.basic.SimulatedDelayedExecutorService;
import accord.impl.list.ListAgent;
import accord.impl.list.ListQuery;
import accord.impl.list.ListRead;
import accord.impl.list.ListRequest;
import accord.impl.list.ListResult;
import accord.impl.list.ListUpdate;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.TimeService;
import accord.messages.MessageType;
import accord.messages.Reply;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.Txn.Kind;
import accord.primitives.TxnId;
import accord.primitives.TxnId.FastPath;
import accord.primitives.TxnId.MediumPath;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.DefaultRandom;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import accord.utils.TriFunction;
import accord.utils.UnhandledEnum;
import accord.utils.Utils;
import accord.utils.async.AsyncExecutor;
import accord.utils.async.TimeoutUtils;
import accord.verify.StrictSerializabilityVerifier;
import accord.verify.Verifier;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntHashSet;

import static accord.impl.PrefixedIntHashKey.forHash;
import static accord.impl.PrefixedIntHashKey.range;
import static accord.impl.PrefixedIntHashKey.ranges;
import static accord.impl.list.ListResult.Status.Applied;
import static accord.impl.list.ListResult.Status.Failure;
import static accord.impl.list.ListResult.Status.Invalidated;
import static accord.impl.list.ListResult.Status.Lost;
import static accord.impl.list.ListResult.Status.RecoveryApplied;
import static accord.impl.list.ListResult.Status.Truncated;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.primitives.Txn.Kind.Write;
import static accord.utils.Utils.toArray;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class BurnTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(BurnTestBase.class);

    /**
     * Min hash value for the test domain, this value must be respected by the hash function
     * @see {@link BurnTest#hash(int)}
     */
    public static final int HASH_RANGE_START = 0;
    /**
     * Max hash value for the test domain, this value must be respected by the hash function
     * @see {@link BurnTest#hash(int)}
     */
    public static final int HASH_RANGE_END = 1 << 16;
    private static final Range[] EMPTY_RANGES = new Range[0];

    static List<Packet> generate(RandomSource random, MessageListener listener, Function<? super CommandStore, AsyncExecutor> executor, List<Id> clients, List<Id> nodes, int[] keys, int operations)
    {
        List<Packet> packets = new ArrayList<>();
        Int2ObjectHashMap<int[]> prefixKeyUpdates = new Int2ObjectHashMap<>();
        double readInCommandStore = random.nextDouble();
        Function<int[], Range> nextRange = randomRanges(random);

        {
            Kind[] kinds = Kind.values();
            kinds[Write.ordinal()] = kinds[0];
            kinds[0] = Write;
            int count = 1 + random.nextInt(kinds.length - 1);
            for (int i = 1 ; i < count ; ++i)
            {
                int j = random.nextInt(i, kinds.length);
                Kind tmp = kinds[i];
                kinds[i] = kinds[j];
                kinds[j] = tmp;
            }
            kinds = Arrays.copyOf(kinds, count);
            Toggles.setMarkStaleIfCannotExecute(kinds);
        }

        FastPath[] fastPaths;
        switch (random.nextInt(0, 2))
        {
            default: throw new IllegalStateException();
            case 0: fastPaths = new FastPath[] { FastPath.Unoptimised }; break;
            case 1: fastPaths = new FastPath[] { FastPath.Unoptimised, random.nextBoolean() ? FastPath.PrivilegedCoordinatorWithDeps : FastPath.PrivilegedCoordinatorWithoutDeps }; break;
            case 2: fastPaths = new FastPath[] { FastPath.Unoptimised, FastPath.PrivilegedCoordinatorWithoutDeps, FastPath.PrivilegedCoordinatorWithDeps };
        }

        MediumPath[] mediumPaths;
        {
            MediumPath[] values = MediumPath.values();
            int size = random.nextInt(1, values.length);
            if (size == values.length)
            {
                mediumPaths = values;
            }
            else
            {
                mediumPaths = new MediumPath[size];
                int sourceSize = values.length;
                while (--size >= 0)
                {
                    int index = random.nextInt(0, sourceSize);
                    mediumPaths[size] = values[index];
                    values[index] = values[--sourceSize];
                }
                Arrays.sort(mediumPaths);
            }
        }

        RandomSource txnIdRandom = random.fork();
        BiFunction<Node, Txn, TxnId> txnIdGenerator = (node, txn) -> node.nextTxnId(txn, txnIdRandom.pick(fastPaths), txnIdRandom.pick(mediumPaths));

        for (int count = 0 ; count < operations ; ++count)
        {
            int finalCount = count;
            Id client = clients.get(random.nextInt(clients.size()));
            Id node = nodes.get(random.nextInt(nodes.size()));

            boolean isRangeQuery = random.nextBoolean();
            String description;
            Function<Node, Txn> txnGenerator;
            if (isRangeQuery)
            {
                description = "range";
                txnGenerator = n -> {
                    int[] prefixes = prefixes(n.topology().current());

                    int rangeCount = 1 + random.nextInt(2);
                    List<Range> requestRanges = new ArrayList<>();
                    while (--rangeCount >= 0)
                        requestRanges.add(nextRange.apply(prefixes));
                    Ranges ranges = Ranges.of(requestRanges.toArray(EMPTY_RANGES));
                    ListRead read = new ListRead(random.decide(readInCommandStore) ? Function.identity() : executor, false, ranges, ranges);
                    ListQuery query = new ListQuery(client, finalCount, false);
                    return new Txn.InMemory(ranges, read, query);
                };
            }
            else
            {
                description = "key";
                txnGenerator = n -> {
                    int[] prefixes = prefixes(n.topology().current());

                    boolean isWrite = random.nextBoolean();
                    int readCount = 1 + random.nextInt(2);
                    int writeCount = isWrite ? 1 + random.nextInt(2) : 0;
                    Kind kind = isWrite ? Kind.Write : readCount == 1 ? EphemeralRead : Kind.Read;

                    TreeSet<Key> requestKeys = new TreeSet<>();
                    IntHashSet readValues = new IntHashSet();
                    while (readCount-- > 0)
                        requestKeys.add(randomKey(random, prefixes, keys, readValues));

                    ListUpdate update = isWrite ? new ListUpdate(executor) : null;
                    IntHashSet writeValues = isWrite ? new IntHashSet() : null;
                    while (writeCount-- > 0)
                    {
                        PrefixedIntHashKey.Key key = randomKey(random, prefixes, keys, writeValues);
                        int i = Arrays.binarySearch(keys, key.key);
                        int[] keyUpdateCounter = prefixKeyUpdates.computeIfAbsent(key.prefix, ignore -> new int[keys.length]);
                        update.put(key, ++keyUpdateCounter[i]);
                    }

                    Keys readKeys = new Keys(requestKeys);
                    if (isWrite)
                        requestKeys.addAll(update.keySet());
                    ListRead read = new ListRead(random.decide(readInCommandStore) ? Function.identity() : executor, kind == EphemeralRead, readKeys, new Keys(requestKeys));
                    ListQuery query = new ListQuery(client, finalCount, kind == EphemeralRead);
                    return new Txn.InMemory(kind, new Keys(requestKeys), read, query, update);
                };
            }
            packets.add(new Packet(client, node, Long.MAX_VALUE, count, new ListRequest(description, txnGenerator, txnIdGenerator, listener)));
        }

        return packets;
    }

    private static int[] prefixes(Topology topology)
    {
        IntHashSet uniq = new IntHashSet();
        for (Shard shard : topology.shards())
            uniq.add(((PrefixedIntHashKey) shard.range.start()).prefix);
        int[] prefixes = new int[uniq.size()];
        IntHashSet.IntIterator it = uniq.iterator();
        for (int i = 0; it.hasNext(); i++)
            prefixes[i] = it.nextValue();
        Arrays.sort(prefixes);
        return prefixes;
    }

    private static Function<int[], Range> randomRanges(RandomSource rs)
    {
        int selection = rs.nextInt(0, 2);
        switch (selection)
        {
            case 0: // uniform
                return (prefixes) -> randomRange(rs, prefixes, () -> rs.nextInt(0, 1 << 13) + 1);
            case 1: // zipf
                int domain = HASH_RANGE_END - HASH_RANGE_START + 1;
                int splitSize = 100;
                int interval = domain / splitSize;
                int[] splits = new int[6];
                for (int i = 0; i < splits.length; i++)
                    splits[i] = i == 0 ? interval : splits[i - 1] * 2;
                int[] splitsToPick = splits;
                int bias = rs.nextInt(0, 3); // small, large, random
                if (bias != 0)
                    splitsToPick = Arrays.copyOf(splits, splits.length);
                if (bias == 1)
                    Utils.reverse(splitsToPick);
                else if (bias == 2)
                    Utils.shuffle(splitsToPick, rs);
                Gen.IntGen zipf = Gens.pickZipf(splitsToPick);
                return (prefixes) -> randomRange(rs, prefixes, () -> {
                    int value = zipf.nextInt(rs);
                    int idx = Arrays.binarySearch(splits, value);
                    int min = idx == 0 ? 0 : splits[idx - 1];
                    return rs.nextInt(min, value) + 1;
                });
            default:
                throw new AssertionError("Unexpected value: " + selection);
        }
    }

    private static Range randomRange(RandomSource random, int[] prefixes, IntSupplier rangeSizeFn)
    {
        int prefix = random.pickInt(prefixes);
        int i = random.nextInt(HASH_RANGE_START, HASH_RANGE_END);
        int rangeSize = rangeSizeFn.getAsInt();
        int j = i + rangeSize;
        if (j > HASH_RANGE_END)
        {
            int delta = j - HASH_RANGE_END;
            j = HASH_RANGE_END;
            i -= delta;
            // saftey check, this shouldn't happen unless the configs were changed in an unsafe way
            if (i < HASH_RANGE_START)
                i = HASH_RANGE_START;
        }
        return range(forHash(prefix, i), forHash(prefix, j));
    }

    private static PrefixedIntHashKey.Key randomKey(RandomSource random, int[] prefixes, int[] keys, Set<Integer> seen)
    {
        int prefix = random.pickInt(prefixes);
        int key;
        do
        {
            key = random.pickInt(keys);
        }
        while (!seen.add(key));
        return PrefixedIntHashKey.key(prefix, key, hash(key));
    }

    /**
     * This class uses a limited range than the default for the following reasons:
     *
     * 1) easier to debug smaller numbers
     * 2) adds hash collisions (multiple keys map to the same hash)
     */
    private static int hash(int key)
    {
        CRC32 crc32c = new CRC32();
        crc32c.update(key);
        crc32c.update(key >> 8);
        crc32c.update(key >> 16);
        crc32c.update(key >> 24);
        return (int) crc32c.getValue() & 0xffff;
    }

    @SuppressWarnings("unused")
    static void reconcile(long seed, TopologyFactory topologyFactory, List<Id> clients, List<Id> nodes, int keyCount, int prefixCount, int operations, int concurrency) throws ExecutionException, InterruptedException
    {
        RandomSource random1 = new DefaultRandom(), random2 = new DefaultRandom();

        random1.setSeed(seed);
        random2.setSeed(seed);
        ExecutorService exec = Executors.newFixedThreadPool(2);
        RandomDelayQueue.ReconcilingQueueFactory factory = new RandomDelayQueue.ReconcilingQueueFactory(seed);
        Future<?> f1 = exec.submit(() -> burn(random1, topologyFactory, clients, nodes, keyCount, prefixCount, operations, concurrency, factory.get(true), InMemoryJournal::new));
        Future<?> f2 = exec.submit(() -> burn(random2, topologyFactory, clients, nodes, keyCount, prefixCount, operations, concurrency, factory.get(false), InMemoryJournal::new));
        exec.shutdown();
        f1.get();
        f2.get();
    }

    public static void burn(RandomSource random, TopologyFactory topologyFactory, List<Id> clients, List<Id> nodes, int keyCount, int prefixCount, int operations, int concurrency, PendingQueue pendingQueue, TriFunction<Id, Agent, RandomSource, Journal> journalFactory)
    {
        List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
        Thread.currentThread().setUncaughtExceptionHandler((th, fail) -> {
            failures.add(fail);
            logger.error("Uncaught exception", fail);
        });
        AtomicLong progress = new AtomicLong();
        MonitoredPendingQueue queue = new MonitoredPendingQueue(failures, progress, 5L, TimeUnit.MINUTES, pendingQueue);
        long startNanos = System.nanoTime();
        long startLogicalMillis = queue.nowInMillis();
        Consumer<Runnable> retryBootstrap;
        {
            RandomSource retryRandom = random.fork();
            retryBootstrap = retry -> {
                long delay = retryRandom.nextInt(1, 15);
                queue.add(PendingRunnable.create(retry::run), delay, TimeUnit.SECONDS);
            };
        }
        IntSupplier coordinationDelays, progressDelays, timeoutDelays;
        {
            RandomSource rnd = random.fork();
            coordinationDelays = delayGenerator(rnd, 1, 100, 100, 1000);
            progressDelays = delayGenerator(rnd, 1, 100, 100, 1000);
            timeoutDelays = delayGenerator(rnd, 500, 800, 1000, 10000);
        }

        Supplier<LongSupplier> nowSupplier = () -> {
            RandomSource forked = random.fork();
            // TODO (testing): meta-randomise scale of clock drift
            return FrequentLargeRange.builder(forked)
                                     .ratio(1, 5)
                                     .small(50, 5000, TimeUnit.MICROSECONDS)
                                     .large(1, 10, MILLISECONDS)
                                     .build()
                                     .mapAsLong(j -> Math.max(0, queue.nowInMillis() + TimeUnit.NANOSECONDS.toMillis(j)))
                                     .asLongSupplier(forked);
        };
        Supplier<TimeService> timeServiceSupplier = () -> TimeService.ofNonMonotonic(nowSupplier.get(), MILLISECONDS);
        BiFunction<BiConsumer<Timestamp, Ranges>, NodeSink.TimeoutSupplier, ListAgent> agentSupplier = (onStale, timeoutSupplier) -> new ListAgent(random.fork(), 1000L, failures::add, retryBootstrap, onStale, coordinationDelays, progressDelays, timeoutDelays, pendingQueue::nowInMillis, timeServiceSupplier.get(), timeoutSupplier);
        SimulatedDelayedExecutorService globalExecutor = new SimulatedDelayedExecutorService(queue, new ListAgent(random.fork(), 1000L, failures::add, retryBootstrap, (i1, i2) -> {
            throw new IllegalAccessError("Global executor should enver get a stale event");
        }, coordinationDelays, progressDelays, timeoutDelays, queue::nowInMillis, timeServiceSupplier.get(), null), null);
        Verifier verifier = createVerifier(keyCount * prefixCount);

        MessageListener listener = MessageListener.get();

        int[] prefixes = IntStream.concat(IntStream.of(0), IntStream.generate(() -> random.nextInt(1024))).distinct().limit(prefixCount).toArray();
        int[] newPrefixes = Arrays.copyOfRange(prefixes, 1, prefixes.length);
        Arrays.sort(prefixes);

        int[] keys = IntStream.range(0, keyCount).toArray();
        Packet[] requests = toArray(generate(random, listener, Function.identity(), clients, nodes, keys, operations), Packet[]::new);
        int[] starts = new int[requests.length];
        Packet[] replies = new Packet[requests.length];

        EnumMap<ListResult.Status, AtomicInteger> counters = new EnumMap<>(ListResult.Status.class);
        for (ListResult.Status status : ListResult.Status.values())
            counters.put(status, new AtomicInteger());
        AtomicInteger clock = new AtomicInteger();
        AtomicInteger requestIndex = new AtomicInteger();
        Queue<Packet> initialRequests = new ArrayDeque<>();
        for (int max = Math.min(concurrency, requests.length) ; requestIndex.get() < max ; )
        {
            int i = requestIndex.getAndIncrement();
            starts[i] = clock.incrementAndGet();
            initialRequests.add(requests[i]);
        }

        // not used for atomicity, just for encapsulation
        AtomicReference<Runnable> onSubmitted = new AtomicReference<>();
        Consumer<Packet> responseSink = packet -> {
            if (replies[(int)packet.replyId] != null)
                return;

            if (requestIndex.get() < requests.length)
            {
                int i = requestIndex.getAndIncrement();
                starts[i] = clock.incrementAndGet();
                queue.addNoDelay(requests[i]);
                if (i == requests.length - 1)
                    onSubmitted.get().run();
            }
            if (packet.message instanceof Reply.FailureReply)
            {
                failures.add(new AssertionError("Unexpected failure in list reply", ((Reply.FailureReply) packet.message).failure));
                return;
            }
            ListResult reply = (ListResult) packet.message;

            try
            {
                if (!reply.isSuccess() && reply.status() == ListResult.Status.HeartBeat)
                    return; // interrupted; will fetch our actual reply once rest of simulation is finished (but wanted to send another request to keep correct number in flight)

                int start = starts[(int)packet.replyId];
                int end = clock.incrementAndGet();
                logger.debug("{} at [{}, {}]", reply, start, end);

                switch (reply.status)
                {
                    default: throw new UnhandledEnum(reply.status);
                    case Lost:
                    case Truncated:
                    case Failure:
                    case Invalidated:
                        replies[(int)packet.replyId] = packet;
                        counters.get(reply.status).incrementAndGet();

                    case Other:
                        return;

                    case Applied:
                    case RecoveryApplied:
                        replies[(int)packet.replyId] = packet;
                        counters.get(reply.status).incrementAndGet();
                }


                progress.incrementAndGet();
                // TODO (correctness): when a keyspace is removed, the history/validator isn't cleaned up...
                // the current logic for add keyspace only knows what is there, so a ABA problem exists where keyspaces
                // may come back... logically this is a problem as the history doesn't get reset, but practically that
                // is fine as the backing map and the validator are consistent
                Verifier.Checker check = verifier.witness(reply, start, end);
                for (int i = 0 ; i < reply.read.length ; ++i)
                {
                    Key key = reply.responseKeys.get(i);
                    int prefix = prefix(key);
                    int keyValue = key(key);
                    int k = Arrays.binarySearch(keys, keyValue);

                    int[] read = reply.read[i];
                    int write = reply.update == null ? -1 : reply.update.getOrDefault(key, -1);

                    int prefixIndex = Arrays.binarySearch(prefixes, prefix);
                    int index = keyCount * prefixIndex + k;
                    if (read != null)
                        check.read(index, read);
                    if (write >= 0)
                        check.write(index, write);
                }
                check.close();
            }
            catch (Throwable t)
            {
                failures.add(t);
            }
        };

        Map<MessageType, Stats> messageStatsMap;
        try
        {
            messageStatsMap = Cluster.run(toArray(nodes, Id[]::new), newPrefixes, listener, () -> queue,
                                          (id, onStale, timeoutSupplier) -> globalExecutor.withAgent(agentSupplier.apply(onStale, timeoutSupplier)),
                                          queue::checkFailures,
                                          responseSink, random::fork, timeServiceSupplier,
                                          topologyFactory, initialRequests::poll,
                                          onSubmitted::set,
                                          ignore -> {},
                                          journalFactory);
            verifier.close();
        }
        catch (Throwable t)
        {
            logger.info("Keys: " + Arrays.toString(keys));
            logger.info("Prefixes: " + Arrays.toString(prefixes));
            for (int i = 0 ; i < requests.length ; ++i)
            {
                logger.info("{}", requests[i]);
                logger.info("\t\t" + replies[i]);
            }
            throw t;
        }

        int observedOperations = counters.get(Applied).get() + counters.get(RecoveryApplied).get() + counters.get(Invalidated).get()
                                 + counters.get(Lost).get() + counters.get(Truncated).get() + counters.get(Failure).get();
        logger.info("nodes: {}, rf: {}. Received {} acks, {} recovered, {} nacks, {} lost, {} truncated ({} total) to {} operations", nodes.size(), topologyFactory.rf,
                    counters.get(Applied).get(), counters.get(RecoveryApplied).get(), counters.get(Invalidated).get(), counters.get(Lost).get(), counters.get(Truncated).get(),
                    observedOperations, operations);
        logger.info("Message counts: {}", statsInDescOrder(messageStatsMap));
        logger.info("Took {} and in logical time of {}", Duration.ofNanos(System.nanoTime() - startNanos), Duration.ofMillis(queue.nowInMillis() - startLogicalMillis));
        if (clock.get() != operations * 2 || observedOperations != operations)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0 ; i < requests.length ; ++i)
            {
                // since this only happens when operations are lost, only log the ones without a reply to lower the amount of noise
                if (replies[i] == null)
                {
                    sb.setLength(0);
                    sb.append(requests[i]).append("\n\t\t").append(replies[i]);
                    logger.info(sb.toString());
                }
            }
            if (clock.get() != operations * 2) throw new AssertionError("Incomplete set of responses; clock=" + clock.get() + ", expected operations=" + (operations * 2));
            else throw new AssertionError("Incomplete set of responses; ack+recovered+other+nacks+lost+truncated=" + observedOperations + ", expected operations=" + operations);
        }
    }

    private static IntSupplier delayGenerator(RandomSource rnd, int absoluteMin, int absoluteMaxMin, int absoluteMinMax, int asoluteMax)
    {
        int minDelay = rnd.nextInt(absoluteMin, absoluteMaxMin);
        int maxDelay = rnd.nextInt(Math.max(absoluteMinMax, minDelay), asoluteMax);
        if (rnd.nextBoolean())
        {
            int medianDelay = rnd.nextInt(minDelay, maxDelay);
            return () -> rnd.nextBiasedInt(minDelay, medianDelay, maxDelay);
        }
        return () -> rnd.nextInt(minDelay, maxDelay);
    }

    private static String statsInDescOrder(Map<MessageType, Stats> statsMap)
    {
        List<Stats> stats = new ArrayList<>(statsMap.values());
        stats.sort(Comparator.comparingInt(s -> -s.count()));
        return stats.toString();
    }

    protected static Verifier createVerifier(int keyCount)
    {
        return new StrictSerializabilityVerifier("", keyCount);
    }

    protected static void run(long seed)
    {
        Duration timeout = Duration.ofMinutes(3);
        try
        {
            TimeoutUtils.runBlocking(timeout, "BurnTest with timeout", () -> run(seed, 1000));
        }
        catch (Throwable thrown)
        {
            Throwable cause = thrown;
            if (cause instanceof ExecutionException)
                cause = cause.getCause();
            if (cause instanceof TimeoutException)
            {
                TimeoutException override = new TimeoutException("test did not complete within " + timeout);
                override.setStackTrace(new StackTraceElement[0]);
                cause = override;
            }
            logger.error("Exception running burn test for seed {}:", seed, cause);
            throw SimulationException.wrap(seed, cause);
        }
    }

    protected static void run(long seed, int operations)
    {
        logger.info("Seed: {}", seed);
        Cluster.trace.trace("Seed: {}", seed);
        RandomSource random = new DefaultRandom(seed);
        try
        {
            List<Id> clients = generateIds(true, 1 + random.nextInt(4));
            int rf;
            float chance = random.nextFloat();
            if (chance < 0.2f)      { rf = random.nextInt(2, 9); }
            else if (chance < 0.4f) { rf = 3; }
            else if (chance < 0.7f) { rf = 5; }
            else if (chance < 0.8f) { rf = 7; }
            else                    { rf = 9; }

            List<Id> nodes = generateIds(false, random.nextInt(rf, rf * 3));

            burn(random, new TopologyFactory(rf, ranges(0, HASH_RANGE_START, HASH_RANGE_END, random.nextInt(Math.max(nodes.size() + 1, rf), nodes.size() * 3))),
                 clients,
                 nodes,
                 5 + random.nextInt(15),
                 5 + random.nextInt(15),
                 operations,
                 10 + random.nextInt(30),
                 new Factory(random).get(),
                 InMemoryJournal::new);
        }
        catch (Throwable t)
        {
            logger.error("Exception running burn test for seed {}:", seed, t);
            throw SimulationException.wrap(seed, t);
        }
    }

    protected static void reconcile(long seed, int operations)
    {
        logger.info("Seed: {}", seed);
        Cluster.trace.trace("Seed: {}", seed);
        RandomSource random = new DefaultRandom(seed);
        try
        {
            List<Id> clients = generateIds(true, 1 + random.nextInt(4));
            int rf;
            float chance = random.nextFloat();
            if (chance < 0.2f)      { rf = random.nextInt(2, 9); }
            else if (chance < 0.4f) { rf = 3; }
            else if (chance < 0.7f) { rf = 5; }
            else if (chance < 0.8f) { rf = 7; }
            else                    { rf = 9; }

            List<Id> nodes = generateIds(false, random.nextInt(rf, rf * 3));

            reconcile(seed, new TopologyFactory(rf, ranges(0, HASH_RANGE_START, HASH_RANGE_END, random.nextInt(Math.max(nodes.size() + 1, rf), nodes.size() * 3))),
                      clients,
                      nodes,
                      5 + random.nextInt(15),
                      5 + random.nextInt(15),
                      operations,
                      10 + random.nextInt(30));
        }
        catch (Throwable t)
        {
            logger.error("Exception running burn test for seed {}:", seed, t);
            throw SimulationException.wrap(seed, t);
        }
    }

    public static List<Id> generateIds(boolean clients, int count)
    {
        List<Id> ids = new ArrayList<>();
        for (int i = 1; i <= count ; ++i)
            ids.add(new Id(clients ? -i : i));
        return ids;
    }

    private static int key(Key key)
    {
        return ((PrefixedIntHashKey) key).key;
    }

    private static int prefix(Key key)
    {
        return ((PrefixedIntHashKey) key).prefix;
    }
}
