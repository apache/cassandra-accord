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

package accord.impl.basic;

import accord.burn.random.FrequentLargeRange;
import accord.utils.RandomSource;

import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static accord.utils.Invariants.illegalArgument;

public class RandomDelayQueue implements PendingQueue
{
    public static class Factory implements Supplier<RandomDelayQueue>
    {
        final RandomSource seeds;

        public Factory(RandomSource seeds)
        {
            this.seeds = seeds;
        }

        @Override
        public RandomDelayQueue get()
        {
            return new RandomDelayQueue(seeds.fork());
        }
    }

    static class Item implements Comparable<Item>
    {
        final long time;
        final int seq;
        final Pending item;

        Item(long time, int seq, Pending item)
        {
            this.time = time;
            this.seq = seq;
            this.item = item;
        }

        @Override
        public int compareTo(Item that)
        {
            int c = Long.compare(this.time, that.time);
            if (c == 0) c = Integer.compare(this.seq, that.seq);
            return c;
        }

        @Override
        public boolean equals(Object o)
        {
            if (o instanceof Pending) return item.equals(o);
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Item item1 = (Item) o;
            return time == item1.time && seq == item1.seq && item.equals(item1.item);
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return "@" + time + "/" + seq + ":" + item;
        }
    }

    final PriorityQueue<Item> queue = new PriorityQueue<>();
    private final LongSupplier jitterMillis;
    long now;
    int seq;

    public RandomDelayQueue(RandomSource random)
    {
        this.jitterMillis = FrequentLargeRange.builder(random)
                                              .small(0, 50, TimeUnit.MICROSECONDS)
                                              .large(50, TimeUnit.MICROSECONDS, 5, TimeUnit.MILLISECONDS)
                                              .build()
                                              .mapAsLong(TimeUnit.NANOSECONDS::toMillis)
                                              .asLongSupplier(random);
    }

    @Override
    public void add(Pending item)
    {
        add(item, 0, TimeUnit.NANOSECONDS);
    }

    @Override
    public void addNoDelay(Pending item)
    {
        queue.add(new Item(now, seq++, item));
    }

    @Override
    public void add(Pending item, long delay, TimeUnit units)
    {
        if (delay < 0)
            throw illegalArgument("Delay must be positive or 0, but given " + delay);
        queue.add(new Item(now + units.toMillis(delay) + jitterMillis.getAsLong(), seq++, item));
    }

    @Override
    public boolean remove(Pending item)
    {
        return queue.remove(item);
    }

    @Override
    public Pending poll()
    {
        Item item = queue.poll();
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

    @Override
    public long nowInMillis()
    {
        return now;
    }
}
