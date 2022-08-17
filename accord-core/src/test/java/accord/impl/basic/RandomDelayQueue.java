package accord.impl.basic;

import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class RandomDelayQueue<T> implements PendingQueue
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
            return new RandomDelayQueue<>(new Random(seeds.nextLong()));
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
        public String toString()
        {
            return "@" + time + "/" + seq + ":" + item;
        }
    }

    final PriorityQueue<Item> queue = new PriorityQueue<>();
    final Random random;
    long now;
    int seq;

    RandomDelayQueue(Random random)
    {
        this.random = random;
    }

    @Override
    public void add(Pending item)
    {
        add(item, random.nextInt(500), TimeUnit.MILLISECONDS);
    }

    @Override
    public void add(Pending item, long delay, TimeUnit units)
    {
        queue.add(new Item(now + units.toMillis(delay), seq++, item));
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
}
