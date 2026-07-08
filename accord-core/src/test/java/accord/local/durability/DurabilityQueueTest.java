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

package accord.local.durability;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.slf4j.LoggerFactory;

import accord.api.RoutingKey;
import accord.api.Scheduler;
import accord.coordinate.ExecuteSyncPoint;
import accord.coordinate.ExecuteSyncPoint.SyncPointErased;
import accord.impl.PrefixedIntHashKey;
import accord.impl.TopologyFactory;
import accord.impl.basic.Pending;
import accord.impl.basic.PendingQueue;
import accord.impl.basic.RandomDelayQueue;
import accord.impl.basic.RecurringPendingRunnable;
import accord.impl.basic.SimulatedFault;
import accord.local.Node;
import accord.local.durability.DurabilityService.SyncLocal;
import accord.local.durability.DurabilityService.SyncRemote;
import accord.primitives.AbstractRanges;
import accord.primitives.Deps;
import accord.primitives.FullRangeRoute;
import accord.primitives.PartialSyncPoint;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.SyncPoint;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.QueueScheduler;
import accord.utils.RandomSource;
import accord.utils.RandomTestRunner;
import accord.utils.ReducingRangeMap;
import accord.utils.SimpleBitSet;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.async.AsyncResults;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import static accord.burn.BurnTestBase.HASH_RANGE_END;
import static accord.burn.BurnTestBase.HASH_RANGE_START;
import static accord.burn.BurnTestBase.generateIds;
import static accord.impl.PrefixedIntHashKey.ranges;
import static accord.local.durability.DurabilityService.SyncLocal.NoLocal;
import static accord.local.durability.DurabilityService.SyncLocal.Self;
import static accord.primitives.Routables.Slice.Minimal;

public class DurabilityQueueTest
{
    static class Submission
    {
        final SyncPoint syncPoint;
        final DurabilityRequest request;
        final Shard[] shards;
        final SimpleBitSet quorums;
        final SimpleBitSet all;
        Ranges requestNotDone;

        boolean erased;

        Submission(SyncPoint syncPoint, DurabilityRequest request, Shard[] shards)
        {
            this.syncPoint = syncPoint;
            this.request = request;
            this.shards = shards;
            this.quorums = SimpleBitSet.allocate(shards.length);
            this.all = SimpleBitSet.allocate(shards.length);
            this.requestNotDone = request == null ? null : syncPoint.route.toRanges();
        }
    }

    static class TestAdapter implements DurabilityQueue.Adapter
    {
        static final int FULL_HASH_RANGE = HASH_RANGE_END - HASH_RANGE_START;
        static final SyncLocal[] SYNC_LOCALS = SyncLocal.values();
        static final SyncRemote[] SYNC_REMOTES = SyncRemote.values();

        final RandomSource rnd;
        final PendingQueue pending;
        final Scheduler scheduler;
        final Node.Id self;
        final int medianCoverage;
        final float requestRatio;
        final int maxInProgress;

        final Map<TxnId, Submission> inProgress = new TreeMap<>();
        final Map<TxnId, Submission> submissions = new TreeMap<>();
        ReducingRangeMap<TxnId> maxQuorum = new ReducingRangeMap<>();
        ReducingRangeMap<TxnId> maxSubmitted = new ReducingRangeMap<>();
        Topology topology;

        TestAdapter(RandomSource rnd, Topology topology, Node.Id self)
        {
            this.rnd = rnd;
            this.pending = new RandomDelayQueue(rnd);
            this.topology = topology;
            this.self = self;
            this.scheduler = new QueueScheduler(0, pending);
            this.medianCoverage = rnd.nextBiasedInt(FULL_HASH_RANGE/128, FULL_HASH_RANGE/16, FULL_HASH_RANGE/4);
            this.requestRatio = 1f / rnd.nextBiasedInt(2, 10, 20);
            this.maxInProgress = rnd.nextBiasedInt(5, 20, 50);
        }

        @Override public Topology currentTopology() { return topology; }
        @Override public long retryDelay(int attempts, TimeUnit units) { return units.convert(rnd.nextInt(1, 180), TimeUnit.SECONDS); }
        @Override public long elapsed(TimeUnit units) { return units.convert(pending.nowInMillis(), TimeUnit.MILLISECONDS); }
        @Override public Scheduler scheduler() { return scheduler; }
        @Override public void unregister(DurabilityRequest request) { }

        @Override public void abandon(DurabilityRequest request, PartialSyncPoint syncPoint, boolean pruned)
        {
            if (pruned)
            {
                Submission submission = inProgress.remove(syncPoint.syncId);
                Assertions.assertNotNull(submission);
                Assertions.assertNull(submission.request);
                Assertions.assertTrue(maxSubmitted.foldl(syncPoint.route, TxnId::min, TxnId.MAX).compareTo(syncPoint.syncId) > 0);
                inProgress.remove(syncPoint.syncId);
                return;
            }

            Submission submission = submissions.get(syncPoint.syncId);
            Assertions.assertEquals(submission, inProgress.remove(syncPoint.syncId));
            if (submission.erased && request == null)
                return;

            if (submission.shards.length > submission.quorums.getSetBitCount())
            {
                for (int i = 0; i < submission.shards.length; ++i)
                {
                    if (submission.quorums.get(i)) continue;
                    if (maxQuorum.foldl(syncPoint.route.slice(Ranges.of(submission.shards[i].range), Minimal), TxnId::nonNullOrMax, TxnId.NONE).compareTo(syncPoint.syncId) >= 0)
                        submission.quorums.set(i);
                }
            }
            Assertions.assertEquals(submission.shards.length, submission.quorums.getSetBitCount());
            if (submission.request != null)
                Assertions.assertEquals(submission.requestNotDone, Ranges.EMPTY);

        }

        @Override public void done(DurabilityRequest request, PartialSyncPoint syncPoint)
        {
            Submission submission = submissions.get(syncPoint.syncId);
            Assertions.assertEquals(submission.shards.length, submission.all.getSetBitCount());
            if (submission.request != null)
                Assertions.assertEquals(submission.requestNotDone, Ranges.EMPTY);
            inProgress.remove(syncPoint.syncId);
        }

        @Override public void retry(DurabilityRequest request, PartialSyncPoint syncPoint)
        {
            Assertions.assertTrue(submissions.get(syncPoint.syncId).erased);
            inProgress.remove(syncPoint.syncId);
        }

        @Override
        public ExecuteSyncPoint.DurabilityResults execute(PartialSyncPoint syncPoint, int attempt)
        {
            Submission submission = submissions.get(syncPoint.syncId);
            ExecuteSyncPoint.DurabilityResults results = new ExecuteSyncPoint.DurabilityResults();
            if (rnd.decide(0.1f))
            {
                Throwable failure;
                if (rnd.decide(0.05f))
                {
                    failure = new SyncPointErased();
                    submission.erased = true;
                }
                else failure = new SimulatedFault("Failed execution");
                int latencyMillis = rnd.nextBiasedInt(100, 1000, 10000);
                scheduler.once(() -> {
                    results.accept(null, failure);
                }, latencyMillis, TimeUnit.MILLISECONDS);
            }
            else
            {
                Topology topology = this.topology.selectIfExists(syncPoint.route);
                List<Node.Id> candidates = new ArrayList<>(topology.nodes());
                SortedArrayList<Node.Id> including = select(rnd, candidates, rnd.nextInt(candidates.size()));

                DurabilityResult quorum;
                {
                    SortedArrayList<Node.Id> includingQuorum = null;
                    for (Shard shard : topology.shards())
                    {
                        SortedArrayList<Node.Id> intersecting = shard.nodes.intersecting(including);
                        if (intersecting.size() < shard.slowQuorumSize)
                        {
                            includingQuorum = null;
                            break;
                        }

                        SortedArrayList<Node.Id> missing = includingQuorum == null ? intersecting : intersecting.without(includingQuorum);
                        candidates.clear();
                        candidates.addAll(missing);
                        int addCount = shard.slowQuorumSize - (intersecting.size() - missing.size());
                        if (addCount > 0)
                        {
                            SortedArrayList<Node.Id> add = select(rnd, candidates, addCount);
                            if (includingQuorum == null) includingQuorum = add;
                            else includingQuorum = includingQuorum.with(add);
                        }
                    }

                    if (includingQuorum != null) quorum = new DurabilityResult(syncPoint, result(includingQuorum, topology), null);
                    else quorum = null;
                }

                int latencyMillis = rnd.nextBiasedInt(100, 1000, 10000);
                if (quorum != null)
                {
                    int quorumLatencyMillis = rnd.decide(0.1f)
                                              ? rnd.nextBiasedInt(99, latencyMillis, latencyMillis * 2)
                                              : rnd.nextBiasedInt(Math.min(latencyMillis/2, 100) - 1, latencyMillis / 2, latencyMillis);
                    scheduler.once(() -> {
                        submission.quorums.setRange(0, submission.shards.length);
                        maxQuorum = ReducingRangeMap.merge(maxQuorum, ReducingRangeMap.create(syncPoint.route, syncPoint.syncId), TxnId::max);
                        ((AsyncResults.SettableResult<DurabilityResult>)results.onQuorumOrDone()).trySuccess(quorum);
                    }, quorumLatencyMillis, TimeUnit.MILLISECONDS);
                }
                DurabilityResult result = new DurabilityResult(syncPoint, result(including, topology), null);
                scheduler.once(() -> {

                    Ranges newQuorum = Ranges.EMPTY;
                    for (int i = 0; i < submission.shards.length ; ++i)
                    {
                        Shard shard = submission.shards[i];
                        SortedArrayList<Node.Id> intersecting = including.intersecting(shard.nodes);
                        int count = intersecting.size();
                        if (count == shard.nodes.size())
                            submission.all.set(i);
                        if (count >= shard.slowQuorumSize && submission.quorums.set(i))
                            newQuorum = newQuorum.with(Ranges.of(shard.range).intersecting(syncPoint.route, Minimal));
                    }
                    if (!newQuorum.isEmpty())
                        maxQuorum = ReducingRangeMap.merge(maxQuorum, ReducingRangeMap.create(syncPoint.route, syncPoint.syncId), TxnId::max);

                    for (Submission s : inProgress.values())
                    {
                        if (s.requestNotDone == null || !s.requestNotDone.intersects((AbstractRanges) syncPoint.route))
                            continue;

                        if (syncPoint.syncId.compareTo(s.request.min) < 0)
                            continue;

                        if (s.request.kind != null && syncPoint.syncId.kind() != s.request.kind)
                            continue;

                        DurabilityLevel require = s.request.require;
                        for (int i = 0; i < submission.shards.length ; ++i)
                        {
                            Shard shard = submission.shards[i];
                            if (s.requestNotDone.contains(shard.range))
                            {
                                if (require.local == Self && shard.nodes.contains(self) && !including.contains(self))
                                    continue;

                                switch (require.remote)
                                {
                                    case NoRemote: break;
                                    case MinorityQuorum:
                                        if (shard.minorityQuorumSize() != shard.slowQuorumSize)
                                        {
                                            int count = including.intersecting(shard.nodes).size();
                                            if (count < shard.minorityQuorumSize()) continue;
                                            break;
                                        }
                                    case Quorum:
                                        if (!submission.quorums.get(i)) continue;
                                        break;
                                    case All:
                                        if (!submission.all.get(i)) continue;
                                        break;
                                }

                                if (require.including != null)
                                {
                                    SortedArrayList<Node.Id> excluding = shard.nodes.without(including);
                                    if (require.including.intersects(excluding))
                                        continue;
                                }
                                s.requestNotDone = s.requestNotDone.without(syncPoint.route.slice(Ranges.of(shard.range), Minimal));
                            }
                        }
                    }

                    results.accept(result, null);
                }, latencyMillis, TimeUnit.MILLISECONDS);
            }
            return results;
        }

        private ReducingRangeMap<DurabilityLevel> result(SortedArrayList<Node.Id> including, Topology topology)
        {
            SyncLocal syncLocal = including.contains(self) ? Self : NoLocal;
            ReducingRangeMap.Builder<DurabilityLevel> builder = new ReducingRangeMap.Builder<>(topology.size());
            for (Shard shard : topology.shards())
            {
                SortedArrayList<Node.Id> shardIncluding = shard.nodes.intersecting(including);
                SyncRemote syncRemote;
                if (shardIncluding.size() == shard.rf) syncRemote = SyncRemote.All;
                else if (shardIncluding.size() >= shard.slowQuorumSize) syncRemote = SyncRemote.Quorum;
                else if (shardIncluding.size() >= shard.minorityQuorumSize()) syncRemote = SyncRemote.MinorityQuorum;
                else syncRemote = SyncRemote.NoRemote;

                builder.appendNoOverlap(shard.range.start(), new DurabilityLevel(syncLocal, syncRemote, including.intersecting(shard.nodes), shard.nodes.without(including)));
                builder.appendNoOverlap(shard.range.end(), null);
            }
            return builder.build();
        }

        Submission newSubmission()
        {
            int size = rnd.nextBiasedInt(1, medianCoverage, FULL_HASH_RANGE);
            int maxCount = Math.min(Math.min(size, HASH_RANGE_END - size), 5);
            Range[] ranges = new Range[maxCount == 1 ? 1 : rnd.nextInt(1, maxCount)];
            int min = HASH_RANGE_START;
            // this is a biased distribution, but probably not a major issue
            for (int i = 0 ; i < ranges.length ; ++i)
            {
                int maxStart = HASH_RANGE_END - (size + (ranges.length - (1 + i)));
                int start = min == maxStart ? min : rnd.nextInt(min, maxStart);
                int maxEnd = Math.min(start + size, HASH_RANGE_END) - (ranges.length - (1 + i));
                int end = start + 1 == maxEnd ? maxEnd : rnd.nextInt(start + 1, maxEnd);
                ranges[i] = PrefixedIntHashKey.range(0, start, end);
                min = end;
                size -= (end - start);
            }
            RoutingKey homeKey = ranges[rnd.nextInt(ranges.length)].someIntersectingRoutingKey(null);
            FullRangeRoute route = new FullRangeRoute(homeKey, ranges);
            TxnId maxSubmitted = this.maxSubmitted.foldl(route, TxnId::max, TxnId.NONE);
            Txn.Kind kind = rnd.decide(0.1f) ? Txn.Kind.VisibilitySyncPoint : Txn.Kind.ExclusiveSyncPoint;
            TxnId txnId;
            do
            {
                long hlcDelta = rnd.nextBiasedInt(1, 10, 100);
                if (rnd.decide(0.1f)) hlcDelta = -hlcDelta;
                long hlc = Math.max(1, (maxSubmitted == null ? 1 : maxSubmitted.hlc()) + hlcDelta);
                Node.Id node = rnd.pick(topology.nodes());
                txnId = new TxnId(1, hlc, kind, Routable.Domain.Range, node);
            } while (submissions.containsKey(txnId));

            Topology topology = this.topology.selectIfExists(route);
            SyncPoint syncPoint = new SyncPoint(txnId, txnId, route, Deps.NONE);
            DurabilityRequest request = null;
            if (rnd.decide(0.1f))
            {
                long timeout = TimeUnit.MILLISECONDS.toMicros(pending.nowInMillis() + TimeUnit.SECONDS.toMillis(rnd.nextBiasedInt(1, 100, 1000)));
                SortedArrayList<Node.Id> including = null;
                if (rnd.decide(0.5f))
                {
                    List<Node.Id> candidates = new ArrayList<>(topology.nodes());
                    int count = rnd.nextInt(1, candidates.size() - 1);
                    including = select(rnd, candidates, count);
                }
                DurabilityLevel require = new DurabilityLevel(rnd.pick(SYNC_LOCALS), rnd.pick(SYNC_REMOTES), including, null);
                request = new DurabilityRequest("", kind == Txn.Kind.VisibilitySyncPoint ? kind : null, txnId, Ranges.of(ranges), require, 0, timeout);
            }
            Submission submission = new Submission(syncPoint, request, topology.shards().toArray(Shard[]::new));
            this.maxSubmitted = ReducingRangeMap.merge(this.maxSubmitted, ReducingRangeMap.create(route, txnId), TxnId::max);
            submissions.put(txnId, submission);
            inProgress.put(txnId, submission);
            return submission;
        }

        void run()
        {
            DurabilityQueue queue = new DurabilityQueue(this);

            int count = 100;
            while (count-- > 0)
            {
                Submission submission = newSubmission();
                queue.submit(submission.syncPoint, submission.request);
                while (inProgress.size() >= maxInProgress && runPending());
            }
            while (!inProgress.isEmpty() && runPending());
            Assertions.assertTrue(inProgress.isEmpty());
        }

        boolean runPending()
        {
            Pending p = pending.poll();
            if (p == null)
                return false;
            ((RecurringPendingRunnable)p).run();
            return true;
        }
    }

    private static SortedArrayList<Node.Id> select(RandomSource rnd, List<Node.Id> candidates, int outputCount)
    {
        Node.Id[] out = new Node.Id[outputCount];
        int inputCount = candidates.size();
        while (outputCount-- > 0)
        {
            int i = rnd.nextInt(inputCount);
            out[outputCount] = candidates.get(i);
            --inputCount;
            candidates.set(i, candidates.get(inputCount));
            candidates.remove(inputCount);
        }
        Arrays.sort(out);
        candidates.clear();
        return new SortedArrayList<>(out);
    }

    @Test
    public void test()
    {
        Logger logger = (Logger)LoggerFactory.getLogger(DurabilityQueue.class);
        logger.setLevel(Level.ERROR);
        RandomTestRunner.test().withSeed(0).check(DurabilityQueueTest::test);
        for (int i = 0 ; i < 100 ; ++i)
            RandomTestRunner.test().check(DurabilityQueueTest::test);
    }

    public static void main(String[] args)
    {
        Logger logger = (Logger)LoggerFactory.getLogger(DurabilityQueue.class);
        logger.setLevel(Level.ERROR);
        for (int counter = 1000; counter > 0 ; --counter)
        {
            System.out.print(counter + ", ");
            if (counter % 10 == 0) System.out.println();
            RandomTestRunner.test().check(DurabilityQueueTest::test);
        }
    }

    private static void test(RandomSource rnd)
    {
        int rf = rnd.nextInt(3, 9);
        List<Node.Id> nodes = generateIds(false, rnd.nextInt(rf, rf * 3));

        TopologyFactory factory = new TopologyFactory(rf, ranges(0, HASH_RANGE_START, HASH_RANGE_END, rnd.nextInt(Math.max(nodes.size() + 1, rf), nodes.size() * 3)));
        Topology topology = factory.toTopology(nodes);
        new TestAdapter(rnd, topology, rnd.pick(nodes)).run();
    }

}
