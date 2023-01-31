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

import accord.utils.RandomSource;

import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class UniformRandomQueue<T> implements PendingQueue
{
    public static class Factory implements Supplier<PendingQueue>
    {
        final RandomSource seeds;

        public Factory(RandomSource seeds)
        {
            this.seeds = seeds;
        }

        @Override
        public PendingQueue get()
        {
            return new UniformRandomQueue<>(seeds.fork());
        }
    }

    static class Item implements Comparable<Item>
    {
        final double priority;
        final Pending value;

        Item(double priority, Pending value)
        {
            this.priority = priority;
            this.value = value;
        }

        @Override
        public int compareTo(Item that)
        {
            return Double.compare(this.priority, that.priority);
        }
    }

    final PriorityQueue<Item> queue = new PriorityQueue<>();
    final RandomSource random;

    public UniformRandomQueue(RandomSource random)
    {
        this.random = random;
    }

    @Override
    public int size()
    {
        return queue.size();
    }

    @Override
    public void add(Pending item)
    {
        queue.add(new Item(random.nextDouble(), item));
    }

    @Override
    public void add(Pending item, long delay, TimeUnit units)
    {
        queue.add(new Item(random.nextDouble(), item));
    }

    @Override
    public Pending poll()
    {
        return unwrap(queue.poll());
    }

    private static Pending unwrap(Item e)
    {
        return e == null ? null : e.value;
    }
}
