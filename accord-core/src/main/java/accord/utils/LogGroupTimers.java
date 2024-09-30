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

package accord.utils;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import accord.api.Scheduler;

/**
 * Basic idea is we collect timers in buckets that are logarithmically/exponentially spaced,
 * with buckets nearer in time closer together (down to some minimum spacing).
 *
 * These buckets are contiguous but non-overlapping, and are split on insert when they both exceed
 * a certain size and are eligible to cover a smaller span due to the passing of time.
 *
 * A bucket becomes the current epoch once "now" truncated to the minBucketSpan is equal to the bucket's epoch.
 * At this point, the bucket is heapified so that the entries may be visited in order. Prior to this point,
 * insertions and deletions within a bucket are constant time.
 *
 * This design expects to have a maximum of log2(maxDelay)-K buckets, so bucket lookups are log(log(maxDelay)).
 *
 * This design permits log(log(maxDelay)) time insertion and removal for all items not in the nearest bucket, and log(K)
 * for the nearest bucket, where K is the size of the current epoch's bucket.
 *
 * Given that we may split buckets logarithmically many times, amortised insertion time is logarithmic for entries
 * that survive multiple bucket splits. However, due to the nature of these timer events (essentially timeouts), and
 * that further out timers are grouped in exponentially larger buckets, we expect most entries to be inserted and deleted
 * in constant time.
 *
 * TODO (desired): consider the case of repeatedly splitting the nearest bucket, as can maybe lead to complexity between
 *  n.lg(n) and n^2. In the worst case every item is in the nearest bucket that has lg(D) buckets that are split lg(D)
 *  times and either
 *  (1) all stay in the same bucket. This yields lg(D).n.lg(n) complexity, but we could perhaps avoid this with some summary
 *      data about where we could split a bucket, or by shrinking the bucket to smaller than its ideal span on split when
 *      we detect it.
 *  (2) splits half into the next bucket each time. So each lg(D) round incurs (n/D^2).lg(n/D^2) costs.
 *  However, in both cases, if D is small we probably don't care - and if it is large then this will happen over a very long
 *  period of time and so we still probably don't care.
 * @param <T>
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class LogGroupTimers<T extends LogGroupTimers.Timer>
{
    public static class Timer extends IntrusivePriorityHeap.Node
    {
        private long deadline;
        protected long deadline()
        {
            return deadline;
        }
    }

    public class Scheduling
    {
        final Scheduler scheduler;
        final Function<T, Runnable> taskFactory;
        final long schedulerImpreciseLateTolerance;
        final long schedulerPreciseLateTolerance;
        final long preciseDelayThreshold;

        Scheduler.Scheduled scheduled = Scheduler.CANCELLED;
        long lastNow; // now can go backwards in time since we calculate it before taking the lock
        long scheduledAt = Long.MAX_VALUE;

        /**
         * Note that the parameter to taskFactory may be null
         */
        public Scheduling(Scheduler scheduler, Function<T, Runnable> taskFactory, long schedulerImpreciseLateTolerance, long schedulerPreciseLateTolerance, long preciseDelayThreshold)
        {
            this.scheduler = scheduler;
            this.taskFactory = taskFactory;
            this.schedulerImpreciseLateTolerance = schedulerImpreciseLateTolerance;
            this.schedulerPreciseLateTolerance = schedulerPreciseLateTolerance;
            this.preciseDelayThreshold = preciseDelayThreshold;
        }

        public void ensureScheduled(long now)
        {
            now = Math.max(lastNow, now);
            T next = peekIfSoonerThan(now + preciseDelayThreshold);
            long runAt;
            if (next == null)
            {
                runAt = nextDeadlineEpoch();
                if (runAt < 0)
                    return;

                // when scheduling an imprecise run time, we don't mind if we're late by some amount
                // (often we will be early)
                runAt = Math.max(now, runAt + schedulerImpreciseLateTolerance);
            }
            else
            {
                runAt = Math.max(now, next.deadline());
            }

            if (!scheduled.isDone())
            {
                if (runAt > scheduledAt - schedulerPreciseLateTolerance)
                    return;

                scheduled.cancel();
            }

            scheduledAt = runAt;
            lastNow = now;
            long delay = Math.max(0, runAt - now);
            scheduled = scheduler.once(taskFactory.apply(next), delay, TimeUnit.MICROSECONDS);
        }

        public void maybeReschedule(long now, long newDeadline)
        {
            if (scheduled.isDone() || newDeadline < scheduledAt - schedulerPreciseLateTolerance)
                ensureScheduled(now);
        }

        public void clear()
        {
            if (!scheduled.isDone())
                scheduled.cancel();
            scheduled = Scheduler.CANCELLED;
            lastNow = 0;
            scheduledAt = Long.MAX_VALUE;
        }

        public long scheduledAt()
        {
            return scheduledAt;
        }
    }

    static class Bucket<T extends Timer> extends IntrusivePriorityHeap<T> implements Comparable<Bucket<T>>
    {
        final LogGroupTimers<T> owner;
        final long epoch;
        private long span;

        Bucket(LogGroupTimers<T> owner, long epoch, long span)
        {
            this.epoch = epoch;
            this.owner = owner;
            this.span = span;
        }

        @Override
        protected void heapify()
        {
            owner.maybeSplit(this);
            super.heapify();
        }

        void setSpan(long newSpan)
        {
            this.span = newSpan;
        }

        protected void redistribute()
        {
            heapifiedSize = 0;
            filterUnheapified(this, Bucket::maybeRedistribute);
        }

        private boolean maybeRedistribute(T timer)
        {
            long deadline = timer.deadline();
            if (contains(deadline))
                return false;

            owner.addInternal(deadline, timer);
            return true;
        }

        @Override
        protected void append(T timer)
        {
            Invariants.checkState(epoch + span > timer.deadline());
            super.append(timer);
        }

        @Override
        protected void update(T timer)
        {
            Invariants.checkState(epoch + span > timer.deadline());
            super.update(timer);
        }

        @Override
        public int compareTo(Bucket<T> that)
        {
            return Long.compare(epoch, that.epoch);
        }

        @Override
        public int compare(T a, T b)
        {
            return compareTimers(a, b);
        }

        boolean contains(long deadline)
        {
            deadline -= epoch;
            return deadline >= 0 && deadline < span;
        }

        private static int compareTimers(Timer a, Timer b)
        {
            return Long.compare(a.deadline, b.deadline);
        }

        public long end()
        {
            return epoch + span;
        }
    }

    final TimeUnit units;
    final long bucketShift;
    final long minBucketSpan;
    final int bucketSplitSize;

    Bucket[] buckets = new Bucket[8];
    Bucket addFinger;

    int bucketsStart, bucketsEnd;
    int timerCount;
    long curEpoch;

    public LogGroupTimers(TimeUnit units)
    {
        this(units, defaultBucketShift(units));
    }

    public LogGroupTimers(TimeUnit units, int bucketShift)
    {
        this(units, bucketShift, 256);
    }

    public LogGroupTimers(TimeUnit units, int bucketShift, int bucketSplitSize)
    {
        this.units = units;
        this.bucketShift = Invariants.checkArgument(bucketShift, bucketShift < 31);
        this.minBucketSpan = 1L << bucketShift;
        this.bucketSplitSize = bucketSplitSize;
    }

    // by default group together ~16ms
    private static int defaultBucketShift(TimeUnit units)
    {
        switch (units)
        {
            default: return 0;
            case MILLISECONDS: return 4;
            case MICROSECONDS: return 14;
            case NANOSECONDS: return 24;
        }
    }

    public T peekIfSoonerThan(long deadlineThreshold)
    {
        while (bucketsStart < bucketsEnd)
        {
            Bucket<T> head = buckets[bucketsStart];
            if (head.epoch >= deadlineThreshold)
                return null;

            head.ensureHeapified();
            if (!head.isEmpty())
                return head.peekNode();

            buckets[bucketsStart++] = null;
            if (head == addFinger) addFinger = null;
        }

        return null;
    }

    /**
     * Return the epoch of the next timer we have to schedule
     */
    public long nextDeadlineEpoch()
    {
        int i = bucketsStart;
        while (i < bucketsEnd)
        {
            Bucket<T> head = buckets[i++];
            if (!head.isEmpty())
                return head.epoch;
        }

        return -1L;
    }

    // unsafe for reentry during advance
    public T poll()
    {
        while (bucketsStart < bucketsEnd)
        {
            Bucket<T> head = buckets[bucketsStart];
            head.ensureHeapified();
            if (!head.isEmpty())
            {
                --timerCount;
                return head.pollNode();
            }

            buckets[bucketsStart++] = null;
            if (head == addFinger) addFinger = null;
        }

        return null;
    }

    /**
     * Visit IN ARBITRARY ORDER all timers expired at {@code now}
     *
     * Permits reentrancy on {@link #add}
     */
    public <P> void advance(long now, P param, BiConsumer<P, T> expiredTimers)
    {
        long nextEpoch = now & -minBucketSpan;
        if (nextEpoch < curEpoch)
            return;

        curEpoch = nextEpoch;
        while (bucketsStart < bucketsEnd)
        {
            // drain buckets that are wholly contained by our new time
            Bucket<T> head = buckets[bucketsStart];
            if (head.epoch + head.span <= now)
            {
                timerCount -= head.size;
                head.drain(param, expiredTimers);
            }
            else
            {
                T timer;
                while (null != (timer = head.peekNode()))
                {
                    if (timer.deadline() > now)
                        return;

                    --timerCount;
                    Invariants.checkState(timer == head.pollNode());
                    Invariants.checkState(!timer.isInHeap());
                    expiredTimers.accept(param, timer);
                }
            }

            Invariants.checkState(head.isEmpty());
            Invariants.checkState(head == buckets[bucketsStart]);
            buckets[bucketsStart++] = null;
            if (head == addFinger)
                addFinger = null;
        }

        Invariants.checkState(addFinger == null || addFinger == findBucket(addFinger.epoch));
    }

    public void add(long deadline, T timer)
    {
        Invariants.checkState(deadline >= curEpoch);
        addInternal(deadline, timer);
        ++timerCount;
    }

    public void update(long deadline, T timer)
    {
        Invariants.checkState(deadline >= curEpoch);
        Timer t = timer; // cast to access private field
        Bucket<T> bucket = findBucket(t.deadline);
        Invariants.checkState(bucket != null);
        if (bucket.contains(deadline))
        {
            t.deadline = deadline;
            bucket.update(timer);
        }
        else
        {
            bucket.remove(timer);
            addInternal(deadline, timer);
        }
    }

    private void addInternal(long deadline, T timer)
    {
        Bucket<T> bucket = addFinger;
        if (bucket == null || !bucket.contains(deadline))
        {
            int index = findBucketIndex(buckets, bucketsStart, bucketsEnd, deadline);
            bucket = ensureBucket(index, deadline);
        }

        set(timer, deadline);
        bucket.append(timer);
        addFinger = bucket;
    }

    public void remove(T timer)
    {
        Timer t = timer; // cast to access private field
        Bucket<T> bucket = findBucket(t.deadline);
        Invariants.checkState(bucket != null);
        bucket.remove(timer);
        --timerCount;
    }

    public int size()
    {
        return timerCount;
    }

    public boolean isEmpty()
    {
        return timerCount == 0;
    }

    private long firstEpoch(long deadline)
    {
        return deadline & -minBucketSpan;
    }

    private long idealSpan(long epoch)
    {
        if (epoch <= curEpoch)
            return this.minBucketSpan;

        long bucketSpan = Long.highestOneBit(epoch - curEpoch);
        bucketSpan = Math.max(minBucketSpan, bucketSpan);
        return bucketSpan;
    }

    private long minSpan(long epoch, long deadline)
    {
        long bucketSpan = 2 * Long.highestOneBit(deadline - epoch);
        if (bucketSpan < 0)
        {
            bucketSpan = Long.MAX_VALUE;
            Invariants.checkState(deadline - epoch >= 0);
        }
        bucketSpan = Math.max(minBucketSpan, bucketSpan);
        return bucketSpan;
    }

    private Bucket<T> findBucket(long bucketEpoch)
    {
        int i = findBucketIndex(buckets, bucketsStart, bucketsEnd, bucketEpoch);
        if (i < bucketsStart) return null;
        return buckets[i];
    }

    private Bucket<T> ensureBucket(int index, long deadline)
    {
        if (index >= bucketsStart && index < bucketsEnd)
        {
            Bucket<T> bucket = buckets[index];
            if (bucket.contains(deadline))
                return bucket;
            ++index;
            Invariants.checkState(index == bucketsEnd);
        }

        if (index < bucketsStart || bucketsStart == bucketsEnd)
        {
            long insertEpoch = firstEpoch(deadline);
            long insertBucketSpan;
            if (bucketsStart < bucketsEnd)
                insertBucketSpan = buckets[bucketsStart].epoch - insertEpoch;
            else
                insertBucketSpan = Math.max(idealSpan(insertEpoch), minSpan(insertEpoch, deadline));
            return prependBucket(insertEpoch, insertBucketSpan);
        }
        else
        {
            Bucket<T> tail = buckets[bucketsEnd - 1];
            long insertEpoch = tail.epoch + tail.span;
            long insertBucketSpan = Math.max(idealSpan(insertEpoch), minSpan(insertEpoch, deadline));
            return appendBucket(insertEpoch, insertBucketSpan);
        }
    }

    private Bucket<T> appendBucket(long bucketEpoch, long bucketSpan)
    {
        if (bucketsStart > 0)
        {
            int count = bucketsEnd - bucketsStart;
            System.arraycopy(buckets, bucketsStart, buckets, 0, count);
            Arrays.fill(buckets, count, bucketsEnd, null);
            bucketsStart = 0;
            bucketsEnd = count;
        }
        else if (bucketsEnd == buckets.length)
        {
            buckets = Arrays.copyOf(buckets, bucketsEnd * 2);
        }
        Bucket<T> bucket = new Bucket<>(this, bucketEpoch, bucketSpan);
        buckets[bucketsEnd++] = bucket;
        if (Invariants.isParanoid()) checkContiguous();
        return bucket;
    }

    private Bucket<T> prependBucket(long bucketEpoch, long bucketSpan)
    {
        if (bucketsStart == 0)
        {
            Bucket[] prevBuckets = buckets;
            if (bucketsEnd == buckets.length)
                buckets = new Bucket[buckets.length * 2];
            System.arraycopy(prevBuckets, 0, buckets, 1, bucketsEnd - bucketsStart);
            ++bucketsStart;
            ++bucketsEnd;
        }
        Bucket<T> bucket = new Bucket<>(this, bucketEpoch, bucketSpan);
        buckets[--bucketsStart] = bucket;
        if (Invariants.isParanoid()) checkContiguous();
        return bucket;
    }

    private void maybeSplit(Bucket<T> bucket)
    {
        if (bucket.size < bucketSplitSize)
            return;

        long idealSpan = idealSpan(bucket.epoch);
        if (idealSpan > bucket.span / 2)
            return;

        if (Invariants.isParanoid()) checkContiguous();
        split(bucket, idealSpan);
    }

    private void split(Bucket<T> bucket, long idealSpan)
    {
        int index = findBucketIndex(buckets, bucketsStart, bucketsEnd, bucket.epoch);
        Invariants.checkState(buckets[index] == bucket);
        int splitCount = 1;
        {
            long nextSpan = idealSpan * 2;
            long sumSpan = nextSpan;
            while (sumSpan + nextSpan <= bucket.span)
            {
                ++splitCount;
                sumSpan += nextSpan;
                nextSpan *= 2;
            }
        }

        Bucket[] oldBuckets = buckets;
        if (splitCount + (bucketsEnd - bucketsStart) > buckets.length)
            buckets = new Bucket[Math.max(buckets.length * 2, splitCount + bucketsEnd - bucketsStart)];

        ++index;
        int newIndex = index - bucketsStart;
        System.arraycopy(oldBuckets, bucketsStart, buckets, 0, newIndex);
        System.arraycopy(oldBuckets, index, buckets, newIndex + splitCount, bucketsEnd - index);
        int prevCount = bucketsEnd - bucketsStart;
        int newEnd = splitCount + prevCount;
        if (newEnd < bucketsEnd && buckets == oldBuckets)
            Arrays.fill(buckets, newEnd, bucketsEnd, null);
        bucketsEnd = splitCount + bucketsEnd - bucketsStart;
        bucketsStart = 0;
        long epoch = bucket.epoch + idealSpan;
        long nextSpan = idealSpan;
        long remainingSpan = bucket.span - idealSpan;
        bucket.setSpan(idealSpan);
        while (splitCount-- > 0)
        {
            if (splitCount == 0) nextSpan = remainingSpan;
            buckets[newIndex++] = new Bucket<>(this, epoch, nextSpan);
            remainingSpan -= nextSpan;
            epoch += nextSpan;
            nextSpan *= 2;
        }
        if (Invariants.isParanoid()) checkContiguous();
        bucket.redistribute();
    }

    private void set(Timer timer, long deadline)
    {
        timer.deadline = deadline;
    }

    // copied and simplified from SortedArrays
    private static int findBucketIndex(Bucket[] buckets, int from, int to, long find)
    {
        int lb = from;
        while (lb < to)
        {
            int i = (lb + to) >>> 1;
            int c = Long.compare(find, buckets[i].epoch);
            if (c < 0) to = i;
            else if (c > 0) lb = i + 1;
            else return i;
        }
        return lb - 1;
    }

    private void checkContiguous()
    {
        for (int i = bucketsStart + 1 ; i < bucketsEnd ; ++i)
            Invariants.checkState(buckets[i - 1].end() == buckets[i].epoch);
    }

    public void clear()
    {
        while (bucketsStart < bucketsEnd)
        {
            buckets[bucketsStart].clear();
            buckets[bucketsStart++] = null;
        }
        bucketsStart = bucketsEnd = 0;
        curEpoch = 0;
        addFinger = null;
    }
}
