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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Key;
import accord.impl.IntHashKey;
import accord.impl.basic.Cluster;
import accord.impl.basic.PendingRunnable;
import accord.impl.basic.PropagatingPendingQueue;
import accord.impl.basic.RandomDelayQueue;
import accord.impl.basic.RandomDelayQueue.Factory;
import accord.impl.TopologyFactory;
import accord.impl.basic.Packet;
import accord.impl.basic.PendingQueue;
import accord.impl.basic.SimulatedDelayedExecutorService;
import accord.impl.list.ListAgent;
import accord.impl.list.ListQuery;
import accord.impl.list.ListRead;
import accord.impl.list.ListRequest;
import accord.impl.list.ListResult;
import accord.impl.list.ListUpdate;
import accord.local.CommandStore;
import accord.local.Node.Id;
import accord.messages.MessageType;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Txn;
import accord.utils.DefaultRandom;
import accord.utils.RandomSource;
import accord.utils.async.AsyncExecutor;
import accord.verify.StrictSerializabilityVerifier;

import static accord.impl.IntHashKey.forHash;
import static accord.utils.Utils.toArray;

public class BurnTest
{
    private static final Logger logger = LoggerFactory.getLogger(BurnTest.class);

    static List<Packet> generate(RandomSource random, Function<? super CommandStore, AsyncExecutor> executor, List<Id> clients, List<Id> nodes, int keyCount, int operations)
    {
        List<Key> keys = new ArrayList<>();
        for (int i = 0 ; i < keyCount ; ++i)
            keys.add(IntHashKey.key(i));

        List<Packet> packets = new ArrayList<>();
        int[] next = new int[keyCount];
        double readInCommandStore = random.nextDouble();

        for (int count = 0 ; count < operations ; ++count)
        {
            Id client = clients.get(random.nextInt(clients.size()));
            Id node = nodes.get(random.nextInt(clients.size()));

            boolean isRangeQuery = random.nextBoolean();
            if (isRangeQuery)
            {
                int rangeCount = 1 + random.nextInt(2);
                List<Range> requestRanges = new ArrayList<>();
                while (--rangeCount >= 0)
                {
                    int j = 1 + random.nextInt(0xffff), i = Math.max(0, j - (1 + random.nextInt(0x1ffe)));
                    requestRanges.add(IntHashKey.range(forHash(i), forHash(j)));
                }
                Ranges ranges = Ranges.of(requestRanges.toArray(new Range[0]));
                ListRead read = new ListRead(random.decide(readInCommandStore) ? Function.identity() : executor, ranges, ranges);
                ListQuery query = new ListQuery(client, count);
                ListRequest request = new ListRequest(new Txn.InMemory(ranges, read, query, null));
                packets.add(new Packet(client, node, count, request));


            }
            else
            {
                boolean isWrite = random.nextBoolean();
                int readCount = 1 + random.nextInt(2);
                int writeCount = isWrite ? random.nextInt(3) : 0;

                TreeSet<Key> requestKeys = new TreeSet<>();
                while (readCount-- > 0)
                    requestKeys.add(randomKey(random, keys, requestKeys));

                ListUpdate update = isWrite ? new ListUpdate(executor) : null;
                while (writeCount-- > 0)
                {
                    int i = randomKeyIndex(random, keys, update.keySet());
                    update.put(keys.get(i), ++next[i]);
                }

                Keys readKeys = new Keys(requestKeys);
                if (isWrite)
                    requestKeys.addAll(update.keySet());
                ListRead read = new ListRead(random.decide(readInCommandStore) ? Function.identity() : executor, readKeys, new Keys(requestKeys));
                ListQuery query = new ListQuery(client, count);
                ListRequest request = new ListRequest(new Txn.InMemory(new Keys(requestKeys), read, query, update));
                packets.add(new Packet(client, node, count, request));
            }
        }

        return packets;
    }

    private static Key randomKey(RandomSource random, List<Key> keys, Set<Key> notIn)
    {
        return keys.get(randomKeyIndex(random, keys, notIn));
    }

    private static int randomKeyIndex(RandomSource random, List<Key> keys, Set<Key> notIn)
    {
        return randomKeyIndex(random, keys, notIn::contains);
    }

    private static int randomKeyIndex(RandomSource random, List<Key> keys, Predicate<Key> notIn)
    {
        int i;
        //noinspection StatementWithEmptyBody
        while (notIn.test(keys.get(i = random.nextInt(keys.size()))));
        return i;
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
        PendingQueue queue = new PropagatingPendingQueue(failures, delayQueue);
        RandomSource retryRandom = random.fork();
        ListAgent agent = new ListAgent(1000L, failures::add, retry -> {
            long delay = retryRandom.nextInt(1, 15);
            queue.add((PendingRunnable)retry::run, delay, TimeUnit.SECONDS);
        });

        Supplier<LongSupplier> nowSupplier = () -> {
            RandomSource jitter = random.fork();
            // TODO (expected): jitter should be random walk with intermittent long periods
            return () -> Math.max(0, delayQueue.nowInMillis() + (jitter.nextInt(10) - 5));
        };

        StrictSerializabilityVerifier strictSerializable = new StrictSerializabilityVerifier(keyCount);
        SimulatedDelayedExecutorService globalExecutor = new SimulatedDelayedExecutorService(queue, agent, random.fork());
        Function<CommandStore, AsyncExecutor> executor = ignore -> globalExecutor;

        Packet[] requests = toArray(generate(random, executor, clients, nodes, keyCount, operations), Packet[]::new);
        int[] starts = new int[requests.length];
        Packet[] replies = new Packet[requests.length];

        AtomicInteger acks = new AtomicInteger();
        AtomicInteger nacks = new AtomicInteger();
        AtomicInteger lost = new AtomicInteger();
        AtomicInteger truncated = new AtomicInteger();
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
            ListResult reply = (ListResult) packet.message;
            if (replies[(int)packet.replyId] != null)
                return;

            if (requestIndex.get() < requests.length)
            {
                int i = requestIndex.getAndIncrement();
                starts[i] = clock.incrementAndGet();
                queue.add(requests[i]);
                if (i == requests.length - 1)
                    onSubmitted.get().run();
            }

            try
            {
                if (reply.responseKeys == null && reply.read != null && reply.read.length == 0)
                    return; // interrupted; will fetch our actual reply once rest of simulation is finished (but wanted to send another request to keep correct number in flight)

                int start = starts[(int)packet.replyId];
                int end = clock.incrementAndGet();
                logger.debug("{} at [{}, {}]", reply, start, end);
                replies[(int)packet.replyId] = packet;

                if (reply.responseKeys == null)
                {
                    if (reply.read == null) nacks.incrementAndGet();
                    else if (reply.read.length == 1) lost.incrementAndGet();
                    else if (reply.read.length == 2) truncated.incrementAndGet();
                    else if (reply.read.length == 3) failedToCheck.incrementAndGet();
                    else throw new AssertionError();
                    return;
                }

                acks.incrementAndGet();
                strictSerializable.begin();

                for (int i = 0 ; i < reply.read.length ; ++i)
                {
                    Key key = reply.responseKeys.get(i);
                    int k = key(key);

                    int[] read = reply.read[i];
                    int write = reply.update == null ? -1 : reply.update.getOrDefault(key, -1);

                    if (read != null)
                        strictSerializable.witnessRead(k, read);
                    if (write >= 0)
                        strictSerializable.witnessWrite(k, write);
                }

                strictSerializable.apply(start, end);
            }
            catch (Throwable t)
            {
                failures.add(t);
            }
        };

        EnumMap<MessageType, Cluster.Stats> messageStatsMap;
        try
        {
            messageStatsMap = Cluster.run(toArray(nodes, Id[]::new), () -> queue,
                                          responseSink, globalExecutor,
                                          random::fork, nowSupplier,
                                          topologyFactory, initialRequests::poll,
                                          onSubmitted::set
            );
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

        logger.info("Received {} acks, {} nacks, {} lost, {} truncated ({} total) to {} operations", acks.get(), nacks.get(), lost.get(), truncated.get(), acks.get() + nacks.get() + lost.get() + truncated.get(), operations);
        logger.info("Message counts: {}", messageStatsMap.entrySet());
        if (clock.get() != operations * 2)
        {
            for (int i = 0 ; i < requests.length ; ++i)
            {
                logger.info("{}", requests[i]);
                logger.info("\t\t" + replies[i]);
            }
            throw new AssertionError("Incomplete set of responses");
        }
    }

    public static void main(String[] args)
    {
        int count = 1;
        int operations = 1000;
        Long overrideSeed = -3734695499364004280L;
        LongSupplier seedGenerator = ThreadLocalRandom.current()::nextLong;
        for (int i = 0 ; i < args.length ; i += 2)
        {
            switch (args[i])
            {
                default: throw new IllegalArgumentException("Invalid option: " + args[i]);
                case "-c":
                    count = Integer.parseInt(args[i + 1]);
                    overrideSeed = null;
                    break;
                case "-s":
                    overrideSeed = Long.parseLong(args[i + 1]);
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
        run(ThreadLocalRandom.current().nextLong(), 1000);
    }

    private static void run(long seed, int operations)
    {
        logger.info("Seed: {}", seed);
        Cluster.trace.trace("Seed: {}", seed);
        RandomSource random = new DefaultRandom(seed);
        try
        {
            List<Id> clients = generateIds(true, 1 + random.nextInt(4));
            List<Id> nodes = generateIds(false, 5 + random.nextInt(5));
            burn(random, new TopologyFactory(nodes.size() == 5 ? 3 : (2 + random.nextInt(3)), IntHashKey.ranges(4 + random.nextInt(12))),
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
        return ((IntHashKey) key).key;
    }
}
