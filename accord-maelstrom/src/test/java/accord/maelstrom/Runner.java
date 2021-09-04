package accord.maelstrom;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import accord.local.Node.Id;
import accord.maelstrom.Cluster.Queue;
import accord.maelstrom.Cluster.QueueSupplier;

public class Runner
{
    static class StandardQueue<T> implements Queue<T>
    {
        static class Factory implements QueueSupplier
        {
            final Random seeds;

            Factory(Random seeds)
            {
                this.seeds = seeds;
            }

            @Override
            public <T> Queue<T> get()
            {
                return new StandardQueue<>(new Random(seeds.nextLong()));
            }
        }

        static class Item<T> implements Comparable<Item<T>>
        {
            final long time;
            final int seq;
            final T item;

            Item(long time, int seq, T item)
            {
                this.time = time;
                this.seq = seq;
                this.item = item;
            }

            @Override
            public int compareTo(Item<T> that)
            {
                int c = Long.compare(this.time, that.time);
                if (c == 0) c = Integer.compare(this.seq, that.seq);
                return c;
            }
        }

        final PriorityQueue<Item<T>> queue = new PriorityQueue<>();
        final Random random;
        long now;
        int seq;

        StandardQueue(Random random)
        {
            this.random = random;
        }

        @Override
        public void add(T item)
        {
            add(item, random.nextInt(500), TimeUnit.MILLISECONDS);
        }

        @Override
        public void add(T item, long delay, TimeUnit units)
        {
            queue.add(new Item<>(now + units.toMillis(delay), seq++, item));
        }

        @Override
        public T poll()
        {
            Item<T> item = queue.poll();
            if (item == null)
                return null;
            now = item.time;
            return item.item;
        }

        @Override
        public int size()
        {
            return queue.size();
        }
    }

    static class RandomQueue<T> implements Queue<T>
    {
        static class Factory implements QueueSupplier
        {
            final Random seeds;

            Factory(Random seeds)
            {
                this.seeds = seeds;
            }

            @Override
            public <T> Queue<T> get()
            {
                return new RandomQueue<>(new Random(seeds.nextLong()));
            }
        }

        static class Entry<T> implements Comparable<Entry<T>>
        {
            final double priority;
            final T value;

            Entry(double priority, T value)
            {
                this.priority = priority;
                this.value = value;
            }

            @Override
            public int compareTo(Entry<T> that)
            {
                return Double.compare(this.priority, that.priority);
            }
        }

        final PriorityQueue<Entry<T>> queue = new PriorityQueue<>();
        final Random random;

        public RandomQueue(Random random)
        {
            this.random = random;
        }

        @Override
        public int size()
        {
            return queue.size();
        }

        @Override
        public void add(T item)
        {
            queue.add(new Entry<>(random.nextDouble(), item));
        }

        @Override
        public void add(T item, long delay, TimeUnit units)
        {
            queue.add(new Entry<>(random.nextDouble(), item));
        }

        @Override
        public T poll()
        {
            return unwrap(queue.poll());
        }

        private static <T> T unwrap(Entry<T> e)
        {
            return e == null ? null : e.value;
        }
    }

    static <T> Supplier<T> parseOutput(boolean delay, String output, Function<String, T> parse)
    {
        return parseOutput(delay, output.split("\n"), parse);
    }

    static <T> Supplier<T> parseOutput(boolean delay, BufferedReader output, Function<String, T> parse)
    {
        return parseOutput(delay, output.lines().toArray(String[]::new), parse);
    }

    static <T> Supplier<T> parseOutput(boolean delay, String[] output, Function<String, T> parse)
    {
        long[] nanos = new long[output.length];
        String[] commands = new String[output.length];

        for (int i = 0 ; i < output.length ; ++i)
        {
            String command = output[i];
            long at = TimeUnit.MILLISECONDS.toNanos(Long.parseLong(command.substring(0, command.indexOf(' '))));
            command = command.substring(command.indexOf(' ') + 1);
            if (i > 0 && at <= nanos[i-1]) at = nanos[i-1] + 1;
            nanos[i] = at;
            commands[i] = command;
        }

        long start = System.nanoTime();
        return new Supplier<>()
        {
            int i = 0;
            @Override
            public T get()
            {
                if (i == commands.length)
                    return null;

                while (delay)
                {
                    long wait = start + nanos[i] - System.nanoTime();
                    if (wait <= 0) break;
                    try
                    {
                        TimeUnit.NANOSECONDS.sleep(wait);
                    }
                    catch (InterruptedException e)
                    {
                        throw new IllegalStateException(e);
                    }
                }

                return parse.apply(commands[i++]);
            }
        };
    }

    static void parseNode(TopologyFactory factory, boolean delay, String output) throws IOException
    {
        Main.listen(factory, parseOutput(delay, output, Function.identity()), System.out, System.err);
    }

    // TODO: we need to align response ids with the input; for now replies are broken
    static void replay(int nodeCount, TopologyFactory factory, boolean delay, Supplier<Packet> input) throws IOException
    {
        run(nodeCount, new QueueSupplier()
        {
            @Override
            public <T> Queue<T> get()
            {
                return new Queue<>()
                {
                    @Override
                    public void add(T t)
                    {
                    }

                    @Override
                    public void add(T item, long delay, TimeUnit units)
                    {
                    }

                    @Override
                    public T poll()
                    {
                        return (T)input.get();
                    }

                    @Override
                    public int size()
                    {
                        return 0;
                    }
                };
            }
        }, Random::new, factory, () -> null);
    }

    static void run(TopologyFactory factory, String ... commands) throws IOException
    {
        run(3, factory, commands);
    }

    static void run(int nodeCount, TopologyFactory factory, String ... commands) throws IOException
    {
        run(nodeCount, new StandardQueue.Factory(new Random()), Random::new, factory, commands);
    }

    static void run(int nodeCount, QueueSupplier queueSupplier, Supplier<Random> randomSupplier, TopologyFactory factory, String ... commands) throws IOException
    {
        run(nodeCount, queueSupplier, randomSupplier, factory, new Supplier<>()
        {
            int i = 0;
            @Override
            public Packet get()
            {
                return i == commands.length ? null : Packet.parse(commands[i++]);
            }
        });
    }

    static void run(int nodeCount, QueueSupplier queueSupplier, Supplier<Random> randomSupplier, TopologyFactory factory, Supplier<Packet> commands) throws IOException
    {
        List<Id> nodes = new ArrayList<>();
        for (int i = 1 ; i <= nodeCount ; ++i)
            nodes.add(new Id(i));

        Cluster.run(nodes.toArray(Id[]::new), queueSupplier, ignore -> {}, randomSupplier, () -> new AtomicLong()::incrementAndGet, factory, commands, System.err);
    }
}
