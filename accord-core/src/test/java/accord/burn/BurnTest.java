package accord.burn;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import accord.impl.IntHashKey;
import accord.impl.basic.Cluster;
import accord.impl.basic.PropagatingPendingQueue;
import accord.impl.basic.RandomDelayQueue.Factory;
import accord.impl.IntKey;
import accord.impl.TopologyFactory;
import accord.impl.basic.Packet;
import accord.impl.basic.PendingQueue;
import accord.impl.list.ListQuery;
import accord.impl.list.ListRead;
import accord.impl.list.ListRequest;
import accord.impl.list.ListResult;
import accord.impl.list.ListUpdate;
import accord.verify.SerializabilityVerifier;
import accord.verify.LinearizabilityVerifier;
import accord.verify.LinearizabilityVerifier.Observation;
import accord.local.Node.Id;
import accord.api.Key;
import accord.txn.Txn;
import accord.txn.Keys;
import accord.verify.StrictSerializabilityVerifier;
import ch.qos.logback.classic.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BurnTest
{
    private static final Logger logger = LoggerFactory.getLogger(BurnTest.class);

    static List<Packet> generate(Random random, List<Id> clients, List<Id> nodes, int keyCount, int operations)
    {
        List<Key> keys = new ArrayList<>();
        for (int i = 0 ; i < keyCount ; ++i)
            keys.add(IntHashKey.key(i));

        List<Packet> packets = new ArrayList<>();
        int[] next = new int[keyCount];

        for (int count = 0 ; count < operations ; ++count)
        {
            Id client = clients.get(random.nextInt(clients.size()));
            Id node = nodes.get(random.nextInt(clients.size()));

            int readCount = 1 + random.nextInt(2);
            int writeCount = random.nextInt(3);

            TreeSet<Key> requestKeys = new TreeSet<>();
            while (readCount-- > 0)
                requestKeys.add(randomKey(random, keys, requestKeys));

            ListUpdate update = new ListUpdate();
            while (writeCount-- > 0)
            {
                int i = randomKeyIndex(random, keys, update.keySet());
                update.put(keys.get(i), ++next[i]);
            }

            requestKeys.addAll(update.keySet());
            ListRead read = new ListRead(new Keys(requestKeys));
            ListQuery query = new ListQuery(client, count, read.keys, update);
            ListRequest request = new ListRequest(new Txn(new Keys(requestKeys), read, query, update));
            packets.add(new Packet(client, node, count, request));
        }

        return packets;
    }

    private static Key randomKey(Random random, List<Key> keys, Set<Key> notIn)
    {
        return keys.get(randomKeyIndex(random, keys, notIn));
    }

    private static int randomKeyIndex(Random random, List<Key> keys, Set<Key> notIn)
    {
        int i;
        while (notIn.contains(keys.get(i = random.nextInt(keys.size()))));
        return i;
    }

    static void burn(TopologyFactory topologyFactory, List<Id> clients, List<Id> nodes, int keyCount, int operations, int concurrency) throws IOException
    {
        Random random = new Random();
        long seed = random.nextLong();
        System.out.println(seed);
        random.setSeed(seed);
        burn(random, topologyFactory, clients, nodes, keyCount, operations, concurrency);
    }

    static void burn(long seed, TopologyFactory topologyFactory, List<Id> clients, List<Id> nodes, int keyCount, int operations, int concurrency) throws IOException
    {
        Random random = new Random();
        System.out.println(seed);
        random.setSeed(seed);
        burn(random, topologyFactory, clients, nodes, keyCount, operations, concurrency);
    }

    static void reconcile(long seed, TopologyFactory topologyFactory, List<Id> clients, List<Id> nodes, int keyCount, int operations, int concurrency) throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        ReconcilingLogger logReconciler = new ReconcilingLogger(logger);

        Random random1 = new Random(), random2 = new Random();
        random1.setSeed(seed);
        random2.setSeed(seed);
        ExecutorService exec = Executors.newFixedThreadPool(2);
        Future<?> f1;
        try (ReconcilingLogger.Session session = logReconciler.nextSession())
        {
            f1 = exec.submit(() -> burn(random1, topologyFactory, clients, nodes, keyCount, operations, concurrency));
        }

        Future<?> f2;
        try (ReconcilingLogger.Session session = logReconciler.nextSession())
        {
            f2 = exec.submit(() -> burn(random2, topologyFactory, clients, nodes, keyCount, operations, concurrency));
        }
        exec.shutdown();
        f1.get();
        f2.get();

        assert logReconciler.reconcile();
    }

    static void burn(Random random, TopologyFactory topologyFactory, List<Id> clients, List<Id> nodes, int keyCount, int operations, int concurrency)
    {
        List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
        PendingQueue queue = new PropagatingPendingQueue(failures, new Factory(random).get());

        StrictSerializabilityVerifier strictSerializable = new StrictSerializabilityVerifier(keyCount);

        Packet[] requests = generate(random, clients, nodes, keyCount, operations).toArray(Packet[]::new);
        int[] starts = new int[requests.length];
        Packet[] replies = new Packet[requests.length];

        AtomicInteger clock = new AtomicInteger();
        AtomicInteger requestIndex = new AtomicInteger();
        for (int max = Math.min(concurrency, requests.length) ; requestIndex.get() < max ; )
        {
            int i = requestIndex.getAndIncrement();
            starts[i] = clock.incrementAndGet();
            queue.add(requests[i]);
        }

        // not used for atomicity, just for encapsulation
        Consumer<Packet> responseSink = packet -> {
            ListResult reply = (ListResult) packet.message;
            if (replies[(int)packet.replyId] != null)
                return;

            if (requestIndex.get() < requests.length)
            {
                int i = requestIndex.getAndIncrement();
                starts[i] = clock.incrementAndGet();
                queue.add(requests[i]);
            }

            try
            {

                int start = starts[(int)packet.replyId];
                int end = clock.incrementAndGet();

                logger.debug("{} at [{}, {}]", reply, start, end);

                replies[(int)packet.replyId] = packet;
                strictSerializable.begin();

                for (int i = 0 ; i < reply.read.length ; ++i)
                {
                    Key key = reply.keys.get(i);
                    int k = key(key);

                    int[] read = reply.read[i];
                    int write = reply.update.getOrDefault(key, -1);

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

        Cluster.run(nodes.toArray(Id[]::new), () -> queue,
                    responseSink, () -> new Random(random.nextLong()), () -> new AtomicLong()::incrementAndGet,
                    topologyFactory, () -> null);

        logger.info("Received {} acks to {} operations\n", clock.get() - operations, operations);
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

    public static void main(String[] args) throws Exception
    {
        Long overrideSeed = null;
//        Long overrideSeed = 3400709779888757087L;
        do
        {
            long seed = overrideSeed != null ? overrideSeed : ThreadLocalRandom.current().nextLong();
            logger.info("Seed: {}", seed);
            Random random = new Random(seed);
            try
            {
                List<Id> clients =  generateIds(true, 1 + random.nextInt(4));
                List<Id> nodes =  generateIds(false, 5 + random.nextInt(5));
                burn(random, new TopologyFactory<>(nodes.size() == 5 ? 3 : (2 + random.nextInt(3)), IntHashKey.ranges(4 + random.nextInt(12))),
                     clients,
                     nodes,
                     5 + random.nextInt(15),
                     200,
                     10 + random.nextInt(30));
            }
            catch (Throwable t)
            {
                logger.error("Exception running burn test for seed {}:", seed, t);
                throw t;
            }
        } while (overrideSeed == null);
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

    private static int[] append(int[] to, int append)
    {
        to = Arrays.copyOf(to, to.length + 1);
        to[to.length - 1] = append;
        return to;
    }
}
