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

package accord.maelstrom;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import accord.local.Node.Id;
import accord.maelstrom.Cluster.Queue;
import accord.maelstrom.Cluster.QueueSupplier;
import accord.utils.DefaultRandom;
import accord.utils.RandomSource;

import static accord.utils.Utils.toArray;

public class Runner
{
    static class StandardQueue<T> implements Queue<T>
    {
        static class Factory implements QueueSupplier
        {
            final RandomSource seeds;

            Factory(RandomSource seeds)
            {
                this.seeds = seeds;
            }

            @Override
            public <T> Queue<T> get()
            {
                return new StandardQueue<>(seeds.fork());
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
        final RandomSource random;
        long now;
        int seq;

        StandardQueue(RandomSource random)
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
            final RandomSource seeds;

            Factory(RandomSource seeds)
            {
                this.seeds = seeds;
            }

            @Override
            public <T> Queue<T> get()
            {
                return new RandomQueue<>(seeds.fork());
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
        final RandomSource random;

        public RandomQueue(RandomSource random)
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
        return new Supplier<T>()
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

    // TODO (low priority, maelstrom): we need to align response ids with the input; for now replies are broken
    static void replay(int nodeCount, TopologyFactory factory, boolean delay, Supplier<Packet> input) throws IOException
    {
        run(nodeCount, new QueueSupplier()
        {
            @Override
            public <T> Queue<T> get()
            {
                return new Queue<T>()
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
        }, DefaultRandom::new, factory, () -> null);
    }

    static void run(TopologyFactory factory, String ... commands) throws IOException
    {
        run(3, factory, commands);
    }

    static void run(int nodeCount, TopologyFactory factory, String ... commands) throws IOException
    {
        run(nodeCount, new StandardQueue.Factory(new DefaultRandom()), DefaultRandom::new, factory, commands);
    }

    static void run(int nodeCount, QueueSupplier queueSupplier, Supplier<RandomSource> randomSupplier, TopologyFactory factory, String ... commands) throws IOException
    {
        run(nodeCount, queueSupplier, randomSupplier, factory, new Supplier<Packet>()
        {
            int i = 0;
            @Override
            public Packet get()
            {
                return i == commands.length ? null : Packet.parse(commands[i++]);
            }
        });
    }

    static void run(int nodeCount, QueueSupplier queueSupplier, Supplier<RandomSource> randomSupplier, TopologyFactory factory, Supplier<Packet> commands) throws IOException
    {
        List<Id> nodes = new ArrayList<>();
        for (int i = 1 ; i <= nodeCount ; ++i)
            nodes.add(new Id(i));

        Cluster.run(toArray(nodes, Id[]::new), queueSupplier, ignore -> {}, randomSupplier, () -> new AtomicLong()::incrementAndGet, factory, commands, System.err);
    }
}
