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
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.zip.CRC32;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Key;
import accord.burn.random.FrequentLargeRange;
import accord.impl.MessageListener;
import accord.impl.PrefixedIntHashKey;
import accord.impl.TopologyFactory;
import accord.impl.basic.Cluster;
import accord.impl.basic.Cluster.Stats;
import accord.impl.basic.Packet;
import accord.impl.basic.PendingRunnable;
import accord.impl.basic.PropagatingPendingQueue;
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
import accord.messages.MessageType;
import accord.messages.Reply;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.DefaultRandom;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import accord.utils.Utils;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncExecutor;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import accord.verify.CompositeVerifier;
import accord.verify.ElleVerifier;
import accord.verify.StrictSerializabilityVerifier;
import accord.verify.Verifier;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntHashSet;

import static accord.impl.PrefixedIntHashKey.forHash;
import static accord.impl.PrefixedIntHashKey.range;
import static accord.impl.PrefixedIntHashKey.ranges;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.utils.Invariants.illegalArgument;
import static accord.utils.Utils.toArray;

public class BurnTest
{
    private static final Logger logger = LoggerFactory.getLogger(BurnTest.class);

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
                    Txn.Kind kind = isWrite ? Txn.Kind.Write : readCount == 1 ? EphemeralRead : Txn.Kind.Read;

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
            packets.add(new Packet(client, node, count, new ListRequest(description, txnGenerator, listener)));
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
    static void reconcile(long seed, TopologyFactory topologyFactory, List<Id> clients, List<Id> nodes, int keyCount, int operations, int concurrency) throws ExecutionException, InterruptedException
    {
        ReconcilingLogger logReconciler = new ReconcilingLogger(logger);

        RandomSource random1 = new DefaultRandom(), random2 = new DefaultRandom();
        random1.setSeed(seed);
        random2.setSeed(seed);
        ExecutorService exec = Executors.newFixedThreadPool(2);
        Future<?> f1;
        try (@SuppressWarnings("unused") ReconcilingLogger.Session session = logReconciler.nextSession())
        {
            f1 = exec.submit(() -> burn(random1, topologyFactory, clients, nodes, keyCount, operations, concurrency));
        }

        Future<?> f2;
        try (@SuppressWarnings("unused") ReconcilingLogger.Session session = logReconciler.nextSession())
        {
            f2 = exec.submit(() -> burn(random2, topologyFactory, clients, nodes, keyCount, operations, concurrency));
        }
        exec.shutdown();
        f1.get();
        f2.get();

        assert logReconciler.reconcile();
    }

    static void burn(RandomSource random, TopologyFactory topologyFactory, List<Id> clients, List<Id> nodes, int keyCount, int operations, int concurrency)
    {
        List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
        RandomDelayQueue delayQueue = new Factory(random).get();
        PropagatingPendingQueue queue = new PropagatingPendingQueue(failures, delayQueue);
        long startNanos = System.nanoTime();
        long startLogicalMillis = queue.nowInMillis();
        Consumer<Runnable> retryBootstrap;
        {
            RandomSource retryRandom = random.fork();
            retryBootstrap = retry -> {
                long delay = retryRandom.nextInt(1, 15);
                queue.add((PendingRunnable) retry::run, delay, TimeUnit.SECONDS);
            };
        }
        IntSupplier coordinationDelays, progressDelays, timeoutDelays;
        {
            RandomSource rnd = random.fork();
            coordinationDelays = delayGenerator(rnd, 1, 100, 100, 1000);
            progressDelays = delayGenerator(rnd, 1, 100, 100, 1000);
            timeoutDelays = delayGenerator(rnd, 500, 800, 1000, 10000);
        }
        Function<BiConsumer<Timestamp, Ranges>, ListAgent> agentSupplier = onStale -> new ListAgent(random.fork(), 1000L, failures::add, retryBootstrap, onStale, coordinationDelays, progressDelays, timeoutDelays);

        Supplier<LongSupplier> nowSupplier = () -> {
            RandomSource forked = random.fork();
            // TODO (now): meta-randomise scale of clock drift
            return FrequentLargeRange.builder(forked)
                                                   .ratio(1, 5)
                                                   .small(50, 5000, TimeUnit.MICROSECONDS)
                                                   .large(1, 10, TimeUnit.MILLISECONDS)
                                                   .build()
                                                   .mapAsLong(j -> Math.max(0, queue.nowInMillis() + TimeUnit.NANOSECONDS.toMillis(j)))
                    .asLongSupplier(forked);
        };

        SimulatedDelayedExecutorService globalExecutor = new SimulatedDelayedExecutorService(queue, new ListAgent(random.fork(), 1000L, failures::add, retryBootstrap, (i1, i2) -> {
            throw new IllegalAccessError("Global executor should enver get a stale event");
        }, coordinationDelays, progressDelays, timeoutDelays));
        Int2ObjectHashMap<Verifier> validators = new Int2ObjectHashMap<>();
        Function<CommandStore, AsyncExecutor> executor = ignore -> globalExecutor;

        MessageListener listener = MessageListener.get();
        
        int[] keys = IntStream.range(0, keyCount).toArray();
        Packet[] requests = toArray(generate(random, listener, executor, clients, nodes, keys, operations), Packet[]::new);
        int[] starts = new int[requests.length];
        Packet[] replies = new Packet[requests.length];

        AtomicInteger acks = new AtomicInteger();
        AtomicInteger nacks = new AtomicInteger();
        AtomicInteger lost = new AtomicInteger();
        AtomicInteger truncated = new AtomicInteger();
        AtomicInteger recovered = new AtomicInteger();
        AtomicInteger failedToCheck = new AtomicInteger();
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
                replies[(int)packet.replyId] = packet;

                if (!reply.isSuccess())
                {
                    switch (reply.status())
                    {
                        case Lost:          lost.incrementAndGet();             break;
                        case Invalidated:   nacks.incrementAndGet();            break;
                        case Failure:       failedToCheck.incrementAndGet();    break;
                        case Truncated:     truncated.incrementAndGet();        break;
                        // txn was applied?, but client saw a timeout, so response isn't known
                        case Other:                                             break;
                        default:            throw new AssertionError("Unexpected fault: " + reply.status());
                    }
                    return;
                }

                switch (reply.status())
                {
                    default: throw new AssertionError("Unhandled status: " + reply.status());
                    case Applied: acks.incrementAndGet(); break;
                    case RecoveryApplied: recovered.incrementAndGet(); // NOTE: technically this might have been applied by the coordinator and it simply timed out
                }
                // TODO (correctness): when a keyspace is removed, the history/validator isn't cleaned up...
                // the current logic for add keyspace only knows what is there, so a ABA problem exists where keyspaces
                // may come back... logically this is a problem as the history doesn't get reset, but practically that
                // is fine as the backing map and the validator are consistent
                Int2ObjectHashMap<Verifier.Checker> seen = new Int2ObjectHashMap<>();
                for (int i = 0 ; i < reply.read.length ; ++i)
                {
                    Key key = reply.responseKeys.get(i);
                    int prefix = prefix(key);
                    int keyValue = key(key);
                    int k = Arrays.binarySearch(keys, keyValue);
                    Verifier verifier = validators.computeIfAbsent(prefix, ignore -> createVerifier(Integer.toString(prefix), keyCount));
                    Verifier.Checker check = seen.computeIfAbsent(prefix, ignore -> verifier.witness(start, end));

                    int[] read = reply.read[i];
                    int write = reply.update == null ? -1 : reply.update.getOrDefault(key, -1);

                    if (read != null)
                        check.read(k, read);
                    if (write >= 0)
                        check.write(k, write);
                }
                for (Verifier.Checker check : seen.values())
                {
                    check.close();
                }
            }
            catch (Throwable t)
            {
                failures.add(t);
            }
        };

        Map<MessageType, Stats> messageStatsMap;
        try
        {
            messageStatsMap = Cluster.run(toArray(nodes, Id[]::new), listener, () -> queue,
                                          (id, onStale) -> globalExecutor.withAgent(agentSupplier.apply(onStale)),
                                          queue::checkFailures,
                                          responseSink, random::fork, nowSupplier,
                                          topologyFactory, initialRequests::poll,
                                          onSubmitted::set,
                                          ignore -> {}
            );
            for (Verifier verifier : validators.values())
                verifier.close();
        }
        catch (Throwable t)
        {
            for (int i = 0 ; i < requests.length ; ++i)
            {
                logger.info("{}", requests[i]);
                logger.info("\t\t" + replies[i]);
            }
            throw t;
        }

        int observedOperations = acks.get() + recovered.get() + nacks.get() + lost.get() + truncated.get();
        logger.info("nodes: {}, rf: {}. Received {} acks, {} recovered, {} nacks, {} lost, {} truncated ({} total) to {} operations", nodes.size(), topologyFactory.rf, acks.get(), recovered.get(), nacks.get(), lost.get(), truncated.get(), observedOperations, operations);
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
            else throw new AssertionError("Incomplete set of responses; ack+recovered+other+nacks+lost+truncated=" + observedOperations + ", expected operations=" + (operations * 2));
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

    private static Verifier createVerifier(String prefix, int keyCount)
    {
        if (!ElleVerifier.Support.allowed())
            return new StrictSerializabilityVerifier(prefix, keyCount);
        return CompositeVerifier.create(new StrictSerializabilityVerifier(prefix, keyCount),
                                        new ElleVerifier());
    }

    public static void main(String[] args)
    {
        int count = 1;
        int operations = 1000;
        Long overrideSeed = null;
        LongSupplier seedGenerator = ThreadLocalRandom.current()::nextLong;
        boolean hasOverriddenSeed = false;
        for (int i = 0 ; i < args.length ; i += 2)
        {
            switch (args[i])
            {
                default: throw illegalArgument("Invalid option: " + args[i]);
                case "-c":
                    count = Integer.parseInt(args[i + 1]);
                    if (hasOverriddenSeed)
                        throw illegalArgument("Cannot override both seed (-s) and number of seeds to run (-c)");
                    overrideSeed = null;
                    break;
                case "-s":
                    overrideSeed = Long.parseLong(args[i + 1]);
                    hasOverriddenSeed = true;
                    count = 1;
                    break;
                case "-o":
                    operations = Integer.parseInt(args[i + 1]);
                    break;
                case "--loop-seed":
                    seedGenerator = new DefaultRandom(Long.parseLong(args[i + 1]))::nextLong;
            }
        }
        while (count-- > 0)
        {
            run(overrideSeed != null ? overrideSeed : seedGenerator.getAsLong(), operations);
        }
    }

    @Test
    public void testOne()
    {
        run(System.nanoTime());
    }

    private static void run(long seed)
    {
        Duration timeout = Duration.ofMinutes(3);
        Runnable fn = () -> run(seed, 1000);
        AsyncResult.Settable<?> promise = AsyncResults.settable();
        Thread t = new Thread(() -> {
            try
            {
                fn.run();
                promise.setSuccess(null);
            }
            catch (Throwable e)
            {
                promise.setFailure(e);
            }
        });
        t.setName("BurnTest with timeout");
        t.setDaemon(true);
        try
        {
            t.start();
            AsyncChains.getBlocking(promise, timeout.toNanos(), TimeUnit.NANOSECONDS);
        }
        catch (Throwable thrown)
        {
            Throwable cause = thrown;
            if (cause instanceof ExecutionException)
                cause = cause.getCause();
            if (cause instanceof InterruptedException || cause instanceof TimeoutException)
                t.interrupt();
            if (cause instanceof TimeoutException)
            {
                TimeoutException override = new TimeoutException("test did not complete within " + timeout);
                override.setStackTrace(new StackTraceElement[0]);
                cause = override;
            }
            logger.error("Exception running burn test for seed {}:", seed, t);
            throw SimulationException.wrap(seed, cause);
        }
    }

    private static void run(long seed, int operations)
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
                    operations,
                    10 + random.nextInt(30));
        }
        catch (Throwable t)
        {
            logger.error("Exception running burn test for seed {}:", seed, t);
            throw SimulationException.wrap(seed, t);
        }
    }

    private static List<Id> generateIds(boolean clients, int count)
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
