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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.api.Scheduler;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class LogGroupTimersTest
{
    @Test
    public void test()
    {
        Random random = new Random();
        for (int i = 0 ; i < 100 ; ++i)
            testOne(random.nextLong(), 1000, 100);
    }

    @Test
    public void testOne()
    {
        testOne(8059992770813814538L, 1000, 100);
    }

    static class Timer extends LogGroupTimers.Timer
    {
        long deadline;
        int randomWeight;
        final int uniqueId;

        Timer(long deadline, int randomWeight, int uniqueId)
        {
            this.deadline = deadline;
            this.randomWeight = randomWeight;
            this.uniqueId = uniqueId;
        }

        void update(long deadline, int randomWeight)
        {
            this.deadline = deadline;
            this.randomWeight = randomWeight;
        }

        private static int compareDeadline(Timer a, Timer b)
        {
            int c = Long.compare(a.deadline, b.deadline);
            if (c == 0) c = Integer.compare(a.uniqueId, b.uniqueId);
            return c;
        }

        private static int compareUnique(Timer a, Timer b)
        {
            return Integer.compare(a.uniqueId, b.uniqueId);
        }

        private static int compareRandom(Timer a, Timer b)
        {
            int c = Integer.compare(a.randomWeight, b.randomWeight);
            if (c == 0) c = Integer.compare(a.uniqueId, b.uniqueId);
            return c;
        }

        @Override
        public String toString()
        {
            return String.format("[%d,%d]", deadline, uniqueId);
        }
    }

    private void testOne(long seed, int rounds, int maxBatchSize)
    {
        RandomTestRunner.test().withSeed(seed).check(rnd -> {
            TimeUnit scale = rnd.randomWeightedPicker(new TimeUnit[] { MINUTES, HOURS, DAYS }).get();
            long globalMaxDeadline = scale.toMicros(rnd.nextInt(1, 8));
            long maxDeadlineClustering = scale.toMicros(1) / rnd.nextInt(1, 8);
            int maxRemovalBatchSize = Math.max(1, (int) ((0.4f + (rnd.nextFloat() * 0.5f)) * maxBatchSize));
            float readChance = rnd.nextFloat();
            LogGroupTimers<Timer> test = new LogGroupTimers<>(MICROSECONDS);
            TreeSet<Timer> canonical = new TreeSet<>(Timer::compareDeadline);
            List<Timer> readTest = new ArrayList<>(), readCanonical = new ArrayList<>();
            TestScheduler testScheduler = new TestScheduler(rnd, test, globalMaxDeadline, canonical);
            TreeSet<Timer> randomOrder = new TreeSet<>(Timer::compareRandom);
            int timerId = 0;
            for (int round = 0 ; round < rounds ; ++round)
            {
                int adds = rnd.nextInt(maxBatchSize);
                int updates = rnd.nextInt(maxBatchSize);
                int removes = rnd.nextInt(maxRemovalBatchSize);
                long deadlineClustering = rnd.nextLong(maxDeadlineClustering / 8, maxDeadlineClustering);
                long minDeadline = test.curEpoch + rnd.nextLong(globalMaxDeadline - deadlineClustering);
                long maxDeadline = minDeadline + deadlineClustering;

                if (rnd.nextBoolean())
                    testScheduler.updateNow(test.curEpoch);

                while (adds > 0 || ((removes + updates) > 0 && !canonical.isEmpty()))
                {
                    boolean shouldAdd = adds > 0 && ((removes + updates == 0) || canonical.isEmpty() || rnd.nextBoolean());
                    if (shouldAdd)
                    {
                        --adds;
                        Timer add = new Timer(rnd.nextLong(minDeadline, maxDeadline), rnd.nextInt(), ++timerId);
                        test.add(add.deadline, add);
                        testScheduler.add(add.deadline);
                        canonical.add(add);
                        randomOrder.add(add);
                    }
                    else
                    {
                        Timer timer = randomOrder.pollFirst();
                        canonical.remove(timer);
                        if (removes == 0 || (updates > 0 && rnd.nextBoolean()))
                        {
                            --updates;
                            long newDeadline = rnd.nextLong(minDeadline, maxDeadline);
                            int newWeight = rnd.nextInt();
                            timer.update(newDeadline, newWeight);
                            test.update(newDeadline, timer);
                            testScheduler.add(newDeadline);
                            randomOrder.add(timer);
                            canonical.add(timer);
                        }
                        else
                        {
                            --removes;
                            test.remove(timer);
                        }
                    }
                }

                if (rnd.decide(readChance))
                    testAdvance(test, canonical, randomOrder, readTest, readCanonical, maxBatchSize, rnd);
            }

            while (!canonical.isEmpty())
                testAdvance(test, canonical, randomOrder, readTest, readCanonical, maxBatchSize, rnd);
            Assertions.assertTrue(test.isEmpty());
        });
    }

    @SuppressWarnings("unused") private static int i = 0; // to assist debugging
    private static void testAdvance(LogGroupTimers<Timer> test, TreeSet<Timer> canonical, TreeSet<Timer> randomOrder,
                                    List<Timer> nextTest, List<Timer> nextCanonical,
                                    int maxReadBatchSize, RandomSource rnd)
    {
        Assertions.assertEquals(canonical.size(), test.size());
        if (canonical.isEmpty())
            return;
        
        int batchSize = Math.min(canonical.size(), rnd.nextInt(1, maxReadBatchSize));
        if (rnd.nextBoolean())
        {
            for (int j = 0 ; j < batchSize ; ++j)
            {
                nextCanonical.add(canonical.pollFirst());
                nextTest.add(test.poll());
                while (!canonical.isEmpty() && canonical.first().deadline == nextCanonical.get(nextCanonical.size() - 1).deadline)
                {
                    nextCanonical.add(canonical.pollFirst());
                    nextTest.add(test.poll());
                    --batchSize;
                }
            }

            for (int j = 1 ; j < nextTest.size() ; ++j)
                Assertions.assertTrue(nextTest.get(j - 1).deadline <= nextTest.get(j).deadline);
        }
        else
        {
            for (int j = 0 ; j < batchSize ; ++j)
                nextCanonical.add(canonical.pollFirst());
            while (!canonical.isEmpty() && canonical.first().deadline == nextCanonical.get(nextCanonical.size() - 1).deadline)
                nextCanonical.add(canonical.pollFirst());

            long advanceTo = nextCanonical.get(nextCanonical.size() - 1).deadline;
            if (rnd.nextBoolean())
            {
                Timer first = nextCanonical.get(0);
                Assertions.assertTrue(test.nextDeadlineEpoch() <= first.deadline);
                Assertions.assertEquals(first, test.peekIfSoonerThan(first.deadline));
            }
            test.advance(advanceTo, nextTest, List::add);
        }

        nextTest.sort(Timer::compareDeadline); // sort to ensure uniqueId are ordered, as LogGroupTimers sorts arbitrarily between equal deadlines
        Assertions.assertEquals(nextCanonical, nextTest);
        nextCanonical.forEach(randomOrder::remove);
        nextCanonical.clear();
        nextTest.clear();
        ++i;
    }

    static class TestScheduler implements Scheduler
    {
        final RandomSource rnd;
        final long schedulerImpreciseLateTolerance;
        final long schedulerPreciseLateTolerance;
        final long schedulerPreciseDelayThreshold;
        final LogGroupTimers<Timer>.Scheduling scheduling;
        final TreeSet<Timer> canonical;

        long now;
        long maxNow;
        long at;

        TestScheduler(RandomSource rnd, LogGroupTimers<Timer> test, long globalMaxDeadline, TreeSet<Timer> canonical)
        {
            this.rnd = rnd;
            this.canonical = canonical;
            schedulerImpreciseLateTolerance = rnd.nextLong(globalMaxDeadline / 128);
            schedulerPreciseLateTolerance = rnd.nextLong(globalMaxDeadline / 128);
            schedulerPreciseDelayThreshold = rnd.nextLong(globalMaxDeadline / 128);
            scheduling = test.new Scheduling(this, timer -> null, schedulerImpreciseLateTolerance, schedulerPreciseLateTolerance, schedulerPreciseDelayThreshold);
        }

        void updateNow(long epoch)
        {
            long prevAt = at;
            now = epoch + rnd.nextLong(schedulerImpreciseLateTolerance) - schedulerPreciseLateTolerance/2;
            maxNow = Math.max(now, maxNow);
            scheduling.ensureScheduled(now);
            check(prevAt);
        }

        @Override
        public Scheduled recurring(Runnable run, long delay, TimeUnit units)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Scheduled once(Runnable run, long delay, TimeUnit units)
        {
            at = now + delay;
            return new Scheduled()
            {
                @Override
                public void cancel()
                {
                    at = 0;
                }

                @Override
                public boolean isDone()
                {
                    return false;
                }
            };
        }

        @Override
        public void now(Runnable run)
        {
            throw new UnsupportedOperationException();
        }

        public void add(long deadline)
        {
            long prev = at;
            scheduling.maybeReschedule(now, deadline);
            check(prev);
        }

        private void check(long prevAt)
        {
            if (canonical.isEmpty())
                return;

            Timer first = canonical.first();
            Assertions.assertTrue(at <= prevAt || prevAt < first.deadline);
        }
    }

}
