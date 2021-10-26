package accord.burn;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
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

public class BurnTest
{
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
        burn(seed, topologyFactory, clients, nodes, keyCount, operations, concurrency, System.out, System.err);
    }

    static void burn(long seed, TopologyFactory topologyFactory, List<Id> clients, List<Id> nodes, int keyCount, int operations, int concurrency, PrintStream out, PrintStream err) throws IOException
    {
        Random random = new Random();
        System.out.println(seed);
        random.setSeed(seed);
        burn(random, topologyFactory, clients, nodes, keyCount, operations, concurrency, out, err);
    }

    static void burn(Random random, TopologyFactory topologyFactory, List<Id> clients, List<Id> nodes, int keyCount, int operations, int concurrency)
    {
        burn(random, topologyFactory, clients, nodes, keyCount, operations, concurrency, System.out, System.err);
    }

    static void reconcile(long seed, TopologyFactory topologyFactory, List<Id> clients, List<Id> nodes, int keyCount, int operations, int concurrency) throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        ReconcilingOutputStreams streams = new ReconcilingOutputStreams(System.out, System.err, 2);
        Random random1 = new Random(), random2 = new Random();
        random1.setSeed(seed);
        random2.setSeed(seed);
        PrintStream out1 = new PrintStream(streams.get(0));
        PrintStream out2 = new PrintStream(streams.get(1));
        ExecutorService exec = Executors.newFixedThreadPool(2);
        Future<?> f1 = exec.submit(() -> burn(random1, topologyFactory, clients, nodes, keyCount, operations, concurrency, out1, out1));
        Future<?> f2 = exec.submit(() -> burn(random2, topologyFactory, clients, nodes, keyCount, operations, concurrency, out2, out2));
        exec.shutdown();
        f1.get();
        f2.get();
    }

    static void burn(Random random, TopologyFactory topologyFactory, List<Id> clients, List<Id> nodes, int keyCount, int operations, int concurrency, PrintStream stdout, PrintStream stderr)
    {
        PendingQueue queue = new Factory(random).get();

        SerializabilityVerifier serializable = new SerializabilityVerifier(keyCount);
        Map<Integer, LinearizabilityVerifier> linearizableMap = new HashMap<>();

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

            stdout.println(reply);
            serializable.begin();

            ListUpdate update = (ListUpdate) ((ListRequest) requests[(int)packet.replyId].message).txn.update;
            int start = starts[(int)packet.replyId];
            int end = clock.incrementAndGet();
            replies[(int)packet.replyId] = packet;

            for (int i = 0 ; i < reply.read.length ; ++i)
            {
                Key key = reply.keys.get(i);
                int k = key(key);

                int[] read = reply.read[i];
                int write = reply.update.getOrDefault(key, -1);

                if (read != null)
                {
                    // TODO: standardise read to include/exclude write
                    serializable.witnessRead(k, read);
                    if (write >= 0)
                        read = append(read, write);
                    linearizableMap.computeIfAbsent(k, LinearizabilityVerifier::new)
                              .witnessRead(new Observation(read, start, end));
                }
                if (write >= 0)
                {
                    serializable.witnessWrite(k, write);
                    linearizableMap.computeIfAbsent(k, LinearizabilityVerifier::new)
                                   .witnessWrite(write, start, end, true);
                }
            }

            serializable.apply();
        };

        Cluster.run(nodes.toArray(Id[]::new), () -> queue,
                    responseSink, () -> new Random(random.nextLong()), () -> new AtomicLong()::incrementAndGet,
                    topologyFactory, () -> null, stderr);

        stdout.printf("Received %d acks to %d operations\n", clock.get() - operations, operations);
        if (clock.get() != operations * 2)
        {
            for (int i = 0 ; i < requests.length ; ++i)
            {
                stdout.println(requests[i]);
                stdout.println("\t\t" + replies[i]);
            }
            throw new AssertionError("Incomplete set of responses");
        }
    }

    public static void main(String[] args) throws Exception
    {
        PrintStream devnull = new PrintStream(new OutputStream()
        {
            @Override
            public void write(int b) throws IOException
            {
            }
        });

        while (true)
        {
            long seed = ThreadLocalRandom.current().nextLong();
            System.out.println("Seed " + seed);
            Random random = new Random(seed);
            List<Id> clients =  generateIds(true, 1 + random.nextInt(4));
            List<Id> nodes =  generateIds(false, 5 + random.nextInt(5));
            burn(random, new TopologyFactory<>(nodes.size() == 5 ? 3 : (2 + random.nextInt(3)), IntHashKey.ranges(4 + random.nextInt(12))),
                 clients,
                 nodes,
                 5 + random.nextInt(15),
                 100,
                 10 + random.nextInt(30),
//                 System.out,
//                 System.err
                 devnull, devnull
            );
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
        return ((IntKey) key).key;
    }

    private static int[] append(int[] to, int append)
    {
        to = Arrays.copyOf(to, to.length + 1);
        to[to.length - 1] = append;
        return to;
    }
}
