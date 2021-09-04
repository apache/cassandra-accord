package accord.impl.basic;

import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class UniformRandomQueue<T> implements PendingQueue
{
    public static class Factory implements Supplier<PendingQueue>
    {
        final Random seeds;

        public Factory(Random seeds)
        {
            this.seeds = seeds;
        }

        @Override
        public PendingQueue get()
        {
            return new UniformRandomQueue<>(new Random(seeds.nextLong()));
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
    final Random random;

    public UniformRandomQueue(Random random)
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
