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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Node.Id;
import accord.maelstrom.Cluster.Queue;
import accord.maelstrom.Cluster.QueueSupplier;
import accord.utils.DefaultRandom;
import accord.utils.RandomSource;

import static accord.utils.Utils.toArray;

public class Runner
{
    private static final Logger logger = LoggerFactory.getLogger(Runner.class);

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
        public synchronized void add(T item)
        {
            add(item, random.nextInt(500), TimeUnit.MILLISECONDS);
        }

        @Override
        public synchronized void add(T item, long delay, TimeUnit units)
        {
            queue.add(new Item<>(now + units.toMillis(delay), seq++, item));
        }

        @Override
        public synchronized T poll()
        {
            Item<T> item = queue.poll();
            if (item == null)
                return null;
            now = item.time;
            return item.item;
        }

        @Override
        public synchronized int size()
        {
            return queue.size();
        }
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

    public static Builder test()
    {
        return new Builder(Thread.currentThread().getStackTrace()[2].getMethodName());
    }

    public static class Builder
    {
        public static final String ACCORD_TEST_SEED = "accord.test.%s.seed";

        private final String key;
        private long seed;
        private int nodeCount = 3;
        private TopologyFactory factory = new TopologyFactory(4, 3);

        private Builder(String name)
        {
            key = String.format(ACCORD_TEST_SEED, name);
            String userSeed = System.getProperty(key, null);
            seed = userSeed != null ? Long.parseLong(userSeed) : System.nanoTime();
        }

        Builder seed(long seed)
        {
            this.seed = seed;
            return this;
        }

        Builder nodeCount(int nodeCount)
        {
            this.nodeCount = nodeCount;
            return this;
        }

        Builder factory(TopologyFactory factory)
        {
            this.factory = factory;
            return this;
        }

        void run(String ... commands) throws IOException
        {
            logger.info("Seed {}; rerun with -D{}={}", seed, key, seed);
            RandomSource randomSource = new DefaultRandom(seed);
            Runner.run(nodeCount, new StandardQueue.Factory(randomSource), randomSource::fork, factory, commands);
        }
    }
}
