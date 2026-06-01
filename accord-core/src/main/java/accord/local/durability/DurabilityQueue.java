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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Scheduler;
import accord.coordinate.ExecuteSyncPoint.DurabilityResults;
import accord.coordinate.ExecuteSyncPoint.SyncPointErased;
import accord.coordinate.Exhausted;
import accord.coordinate.Timeout;
import accord.local.Node;
import accord.primitives.AbstractRanges;
import accord.primitives.PartialSyncPoint;
import accord.primitives.SyncPoint;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.topology.TopologyRetiredException;
import accord.utils.IntrusiveHeapNode;
import accord.utils.IntrusivePriorityHeap;
import accord.utils.Invariants;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.SymmetricComparator;
import accord.utils.async.Cancellable;
import accord.utils.btree.BTree;
import accord.utils.btree.IntervalBTree;
import accord.utils.btree.IntervalBTree.IntervalComparators;

import static accord.api.ProtocolModifiers.isRangeEndInclusive;
import static accord.coordinate.ExecuteSyncPoint.coordinateIncluding;
import static accord.local.durability.DurabilityQueue.Status.ABANDONED;
import static accord.local.durability.DurabilityQueue.Status.ACTIVE;
import static accord.local.durability.DurabilityQueue.Status.COMPLETING;
import static accord.local.durability.DurabilityQueue.Status.DONE;
import static accord.local.durability.DurabilityQueue.Status.QUEUED;
import static accord.local.durability.DurabilityQueue.Status.RESTARTING;
import static accord.local.durability.DurabilityService.SyncRemote.All;
import static accord.local.durability.DurabilityService.SyncRemote.Quorum;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Txn.Kind.VisibilitySyncPoint;
import static accord.utils.btree.IntervalBTree.InclusiveEndHelper.endWithStart;
import static accord.utils.btree.IntervalBTree.InclusiveEndHelper.startWithEnd;
import static accord.utils.btree.IntervalBTree.InclusiveEndHelper.startWithStart;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Tracks and schedules durability requests, executing a sync point after it has been agreed.
 *
 * SyncPoints go through a fairly simple lifecycle. On submission they are registered in {@link #pendingByRange}
 * and set to QUEUED. If there is no conflicting SyncPoint scheduled to run earlier, they are also added to {@link #queued},
 * otherwise they are added to the {@link Pending#deferred} collection of the conflict with the highest execution time.
 *
 * When the time arrives to submit a task and it has been added to {@link #queued}, if there are fewer than
 * {@link #maxConcurrency} tasks executing the task is moved to ACTIVE and executed.
 *
 * Once an ACTIVE task is completed, its {@link Pending#deferred} collection is re-queued.
 *
 * A transaction may be moved to ABANDONED if it is QUEUED, has no active {@link DurabilityRequest}, and either
 *  1) has achieved QUORUM and has a superseding transaction for each of its ranges; or
 *  2) has a superseding transaction with more than {@link #overlapPruneThreshold} preceding conflicts.
 *
 * A transaction may be RESTARTED only if it has an incomplete {@link DurabilityRequest} and can no longer succeed.
 *
 * Up to {@code maxConcurrency} tasks are allowed to be in flight. All subsequent tasks are put into the {@code pending}
 * queue, which new tasks are pulled from after the current in-progress ones complete. To prevent unbounded growth of the
 * pending queue, pending tasks are periodically pruned.
 */
public class DurabilityQueue
{
    private static final Logger logger = LoggerFactory.getLogger(DurabilityQueue.class);
    private static final ConcurrentSkipListMap<Long, Collection<Node.Id>> WARNINGS_LOGGED = new ConcurrentSkipListMap<>();
    private static final long EXHAUSTED_LOG_INTERVAL_MINUTES = 5;
    private static final long SCHEDULING_SLACK_MICROS = MILLISECONDS.toMicros(100);

    public interface Adapter
    {
        Topology currentTopology();

        long retryDelay(int attempts, TimeUnit units);
        long elapsed(TimeUnit units);
        Scheduler scheduler();

        void unregister(DurabilityRequest request);
        void abandon(DurabilityRequest request, PartialSyncPoint syncPoint, boolean pruned);
        void done(DurabilityRequest request, PartialSyncPoint syncPoint);
        void retry(DurabilityRequest request, PartialSyncPoint syncPoint);

        DurabilityResults execute(PartialSyncPoint syncPoint, int attempt);
    }

    public static class NodeAdapter implements Adapter
    {
        final Node node;
        public NodeAdapter(Node node) { this.node = node; }

        @Override public Topology currentTopology() { return node.topology().current(); }
        @Override public long retryDelay(int attempts, TimeUnit units) { return node.agent().retryDurabilityDelay(node, attempts, units); }
        @Override public long elapsed(TimeUnit units) { return node.elapsed(units); }
        @Override public Scheduler scheduler() { return node.scheduler(); }
        @Override public void unregister(DurabilityRequest request) { node.durability().unregister(request); }

        @Override public void abandon(DurabilityRequest request, PartialSyncPoint syncPoint, boolean pruned) {}
        @Override public void done(DurabilityRequest request, PartialSyncPoint syncPoint) {}

        @Override
        public void retry(DurabilityRequest request, PartialSyncPoint syncPoint)
        {
            node.durability().shards().request(request, request.stillWaiting(syncPoint.route));
        }

        @Override
        public DurabilityResults execute(PartialSyncPoint syncPoint, int attempt)
        {
            return coordinateIncluding(node, syncPoint, node.someExclusiveExecutor(), attempt);
        }
    }

    private static final IntervalComparators<PendingRange> BY_RANGE = new InclusiveEndPendingComparators();
    private static final Comparator<Pending> BY_PRIORITY = (a, b) -> {
        if ((a.status == ACTIVE) != (b.status == ACTIVE))
            return a.status == ACTIVE ? -1 : 1;
        if (a.startAt != b.startAt)
            return a.startAt < b.startAt ? -1 : 1;
        return a.syncPoint.syncId.compareTo(b.syncPoint.syncId);
    };

    private static final Comparator<Pending> BY_ID = (a, b) -> {
        return a.syncPoint.syncId.compareTo(b.syncPoint.syncId);
    };

    private static class InclusiveEndPendingComparators implements IntervalComparators<PendingRange>
    {
        @Override public Comparator<PendingRange> totalOrder()
        {
            return (a, b) -> {
                int c = a.range.compare(b.range);
                if (c == 0) c = a.pending.syncPoint.syncId.compareTo(b.pending.syncPoint.syncId);
                return c;
            };
        }
        @Override public Comparator<PendingRange> endWithEndSorter() { return (a, b) -> a.range.end().compareTo(b.range.end()); }
        @Override public SymmetricComparator<PendingRange> startWithStartSeeker() { return (a, b) -> startWithStart(a.range.start().compareTo(b.range.start())); }
        @Override public SymmetricComparator<PendingRange> startWithEndSeeker() { return (a, b) -> startWithEnd(a.range.start().compareTo(b.range.end())); }
        @Override public SymmetricComparator<PendingRange> endWithStartSeeker() { return (a, b) -> endWithStart(a.range.end().compareTo(b.range.start())); }
    }

    static class PendingRange
    {
        final Range range;
        final Pending pending;

        PendingRange(Range range, Pending pending)
        {
            this.range = range;
            this.pending = pending;
        }

        @Override
        public String toString()
        {
            return pending + "@" + range;
        }
    }

    enum Status { QUEUED, ACTIVE, COMPLETING, RESTARTING, ABANDONED, DONE }

    static class Pending extends IntrusiveHeapNode
    {
        final @Nullable DurabilityRequest request;
        PartialSyncPoint syncPoint;
        Object[] byRange;

        long startAt;
        int attempt = 1;
        Status status = QUEUED;

        DurabilityResult achieved;
        List<Pending> deferred;
        Pending parent;

        Pending(SyncPoint syncPoint, @Nullable DurabilityRequest request, long startAt)
        {
            this.syncPoint = syncPoint;
            this.request = request;
            this.startAt = startAt;
            updateByRange();
        }

        void updateByRange()
        {
            List<PendingRange> pendingRanges = new ArrayList<>(syncPoint.route.size());
            for (Range range : syncPoint.route)
                pendingRanges.add(new PendingRange(range, this));
            this.byRange = IntervalBTree.build(pendingRanges, BY_RANGE);
        }

        @Override
        protected boolean isInHeap()
        {
            return super.isInHeap();
        }

        boolean isAncestor(Pending pending)
        {
            Pending p = parent;
            while (p != null && p != pending)
                p = p.parent;
            return p != null;
        }

        @Override
        public String toString()
        {
            return syncPoint.syncId.toString() + ":" + status;
        }

        private void addDeferred(Pending add)
        {
            Invariants.require(add.parent == null);
            Invariants.require(status.compareTo(ACTIVE) <= 0);
            Invariants.require(BY_PRIORITY.compare(this, add) < 0);
            Invariants.require(deferred == null || !deferred.contains(add));
            Invariants.require(!isAncestor(add));
            if (deferred == null)
                deferred = new ArrayList<>();
            deferred.add(add);
            add.parent = this;
        }

        private void removeDeferred(Pending remove)
        {
            Invariants.require(remove.status == QUEUED);
            Invariants.require(remove.parent == this);
            remove.parent = null;
            boolean removed = deferred.remove(remove);
            Invariants.require(removed);
            if (deferred.isEmpty())
                deferred = null;
        }

        private void unsetParent()
        {
            Invariants.require(parent.deferred == null);
            parent = null;
        }
    }

    // TODO (desired): prioritise by least recently updated range
    static final class PendingQueue extends IntrusivePriorityHeap<Pending>
    {
        final Comparator<Pending> comparator;

        PendingQueue(Comparator<Pending> comparator)
        {
            this.comparator = comparator;
        }

        Pending poll() { heapify(); return super.pollNode(); }
        Pending peek() { heapify(); return super.peekNode(); }
        @Override public int compare(Pending o1, Pending o2) { return comparator.compare(o1, o2); }
        void append(Pending node) { super.appendNode(node); }
        void remove(Pending node) { super.removeNode(node); }
        @Override protected void clear() { super.clear(); }
    }

    private final Adapter adapter;
    private int maxConcurrency = 16;
    private int overlapPruneThreshold = 8;
    private int globalPruneThreshold = 256;

    int activeCount;
    int pendingCount;
    private final PendingQueue queued = new PendingQueue(Comparator.comparing(p -> (Long)p.startAt));
    private Object[] pendingByRange = IntervalBTree.empty();
    private final ArrayDeque<Pending> requeue = new ArrayDeque<>();

    private long processingQueueAt = Long.MAX_VALUE;
    private Cancellable cancelProcessingQueue;

    public DurabilityQueue(Node node)
    {
        this(new NodeAdapter(node));
    }

    public DurabilityQueue(Adapter adapter)
    {
        this.adapter = adapter;
        Invariants.require(isRangeEndInclusive(), "Need to implement range-exclusive IntervalComparators");
    }

    void submit(SyncPoint syncPoint, @Nullable DurabilityRequest request)
    {
        long nowMicros = adapter.elapsed(MICROSECONDS);
        if (request != null)
            request.register(syncPoint.syncId, nowMicros);

        Pending pending = new Pending(syncPoint, request, nowMicros);
        synchronized (this)
        {
            register(pending);
            addAndProcessQueue(pending);
        }
    }

    private void addAndProcessQueue(Pending enqueue)
    {
        while (enqueue != null)
        {
            Invariants.require(enqueue.status == QUEUED);
            Invariants.require(enqueue.parent == null);
            if (supersededAndDroppable(enqueue))
            {
                abandon(enqueue, true, false);
                logger.debug("{}: already achieved quorum durability and there are superseding durability requests pending; not retrying", enqueue.syncPoint.syncId);
                enqueue = requeue.poll();
                continue;
            }

            Conflicts conflicts = conflicts(enqueue);
            Pending next = conflicts.preceding;
            boolean add = true;
            if (next != null && next != enqueue)
            {
                if (BY_PRIORITY.compare(next, enqueue) <= 0)
                {
                    next.addDeferred(enqueue);
                    add = false;
                }
                else if (next.isInHeap())
                {
                    queued.remove(next);
                    enqueue.addDeferred(next);
                } // otherwise already deferred on another entry, no need to add it here (will happen later if necessary)
            }

            if (add)
                queued.append(enqueue);

            maybePrune(conflicts.supersedes);
            enqueue = requeue.poll();
        }

        processQueue();
    }

    private void maybePrune(List<Pending> superseded)
    {
        if (!shouldPrune(superseded))
            return;

        // first remove any that have already achieved quorum and have no associated request, or where the request has been finished
        for (Pending p : superseded)
        {
            if (p.status != QUEUED) continue;
            if (p.request != null && !p.request.isDone()) continue;
            if (p.request == null && (p.achieved == null || p.achieved.min.remote.compareTo(Quorum) < 0)) continue;

            if (p.parent != null)
                p.parent.removeDeferred(p);
            abandon(p, true, true);
            return;
        }

        if (shouldPrune(superseded))
        {
            superseded.sort(BY_ID);
            int middle = (superseded.size() - 1)/2;
            int last = superseded.size() - 1;
            for (int i = 0 ; i <= last ; ++i)
            {
                int position = (i & 1) == 0 ? middle - i/2 : middle + (i+1)/2;
                Pending remove = superseded.get(position);
                if (remove.status != QUEUED) continue;
                if (remove.request != null) continue;
                if (remove.parent != null)
                    remove.parent.removeDeferred(remove);
                abandon(remove, true, true);
                return;
            }
        }
    }

    private boolean shouldPrune(List<Pending> conflicts)
    {
        return conflicts.size() > overlapPruneThreshold || pendingCount > globalPruneThreshold;
    }

    static class Conflicts
    {
        final Pending preceding;
        final List<Pending> supersedes; // note: may contain duplicates

        Conflicts(Pending preceding, List<Pending> superseded)
        {
            this.preceding = preceding;
            this.supersedes = superseded;
        }
    }
    private Conflicts conflicts(Pending submit)
    {
        List<Pending> supersedes = new ArrayList<>();
        Pending preceding = null;
        for (PendingRange range : BTree.<PendingRange>iterable(submit.byRange))
        {
            preceding = IntervalBTree.accumulate(pendingByRange, BY_RANGE, range, (sup, sub, pr, nxt) -> {
                Pending test = pr.pending;

                // only consider queued or active commands
                if (test.status.compareTo(ACTIVE) > 0)
                    return nxt;

                // if queued and we supersede it, register as a conflict for possible pruning
                if (test.status == QUEUED && BY_ID.compare(test, sub) < 0 && sub.syncPoint.route.containsAll((AbstractRanges) test.syncPoint.route))
                    sup.add(test);

                // now pick the last to execute transaction that precedes us
                if (BY_PRIORITY.compare(sub, test) < 0 || (nxt != null && BY_PRIORITY.compare(nxt, test) >= 0))
                    return nxt;
                return test;
            }, supersedes, submit, preceding);
        }
        return new Conflicts(preceding, supersedes);
    }

    private boolean supersededAndDroppable(Pending p)
    {
        if (p.achieved == null) return false;
        if (p.request != null) return false;
        if (p.achieved.min.remote.compareTo(Quorum) < 0) return false;

        for (PendingRange range : BTree.<PendingRange>iterable(p.byRange))
        {
            Ranges missing = IntervalBTree.accumulate(pendingByRange, BY_RANGE, range, (s, id, pr, rs) -> {
                Pending v = pr.pending;
                if (v.status.compareTo(ACTIVE) > 0) return rs;
                if (v.syncPoint.syncId.compareTo(id) < 0) return rs;
                if (s.request != null && s.request.kind == VisibilitySyncPoint && (v.request == null || v.request.kind != VisibilitySyncPoint)) return rs;
                if (!rs.contains(pr.range)) return rs;
                return rs.without(Ranges.of(pr.range));
            }, p, p.syncPoint.syncId, Ranges.of(range.range));

            if (!missing.isEmpty())
                return false;
        }
        return true;
    }

    private void register(Pending pending)
    {
        ++pendingCount;
        pendingByRange = IntervalBTree.update(pendingByRange, pending.byRange, BY_RANGE);
    }

    private void unregister(Pending pending)
    {
        Invariants.require(pending.deferred == null);
        Invariants.require(!pending.isInHeap());
        --pendingCount;
        pendingByRange = IntervalBTree.subtract(pendingByRange, pending.byRange, BY_RANGE);
    }

    private void start(Pending pending)
    {
        logger.debug("{}: Awaiting durability for {}", pending.syncPoint.syncId, pending.syncPoint.route.toRanges());
        DurabilityResults coordinate = adapter.execute(pending.syncPoint, pending.attempt);

        long startedAt = pending.startAt;
        Invariants.require(pending.status == QUEUED);
        pending.status = ACTIVE;
        ++activeCount;
        if (pending.request != null)
            pending.request.reportAttempt(pending.syncPoint.syncId, adapter.elapsed(MICROSECONDS));

        coordinate.onQuorumOrDone().invoke((success, fail) -> {
            synchronized (DurabilityQueue.this)
            {
                completing(pending, startedAt);
            }
        });
        coordinate.onDone().invoke((success, fail) -> {
            TxnId txnId;
            Ranges ranges;
            DurabilityRequest request;

            synchronized (DurabilityQueue.this)
            {
                completing(pending, startedAt); // should already have been done by onQuorum, but to avoid future surprises if invoked out of order

                txnId = pending.syncPoint.syncId;
                ranges = pending.syncPoint.route.toRanges();
                request = pending.request;
                if (success != null)
                {
                    if (pending.achieved == null) pending.achieved = success;
                    else pending.achieved = pending.achieved.max(success);
                }

            }

            String requestor = null;
            Ranges achieved;
            boolean isDone;
            if (request == null)
            {
                if (success == null) achieved = Ranges.EMPTY;
                else if (success.min.remote == All) achieved = ranges;
                else achieved = success.achieved.foldlWithBounds((p, r, s, e) -> p.remote == All ? r.with(Ranges.of(s.rangeFactory().newRange(s, e))) : r, Ranges.EMPTY);
                isDone = achieved.containsAll(ranges);
            }
            else
            {
                requestor = " requested by " + request.requestedBy;
                isDone = request.isDone(ranges);
                achieved = request.achieved().slice(ranges, Minimal);
                if (!isDone && request.require.including != null)
                {
                    Topology topology = adapter.currentTopology();
                    SortedArrayList<Node.Id> removed = topology.removedIds().intersecting(request.require.including);
                    SortedArrayList<Node.Id> hardRemoved = topology.hardRemovedIds().intersecting(request.require.including);
                    if (!removed.isEmpty() || !hardRemoved.isEmpty())
                    {
                        abandon(pending, false, false);
                        String message = String.format("%s: Cannot achieve durability requested by %s as (%s/%s) are (removed/hard removed)", txnId, request, removed, hardRemoved);
                        logger.info(message);
                        // TODO (desired): more specific exception?
                        request.result.tryFailure(new RuntimeException(message));
                        adapter.unregister(request);
                        return;
                    }
                }
            }

            if (isDone)
            {
                synchronized (DurabilityQueue.this)
                {
                    done(pending);
                }
            }
            else
            {
                if (success != null)
                    fail = success.failure;

                if (fail != null)
                {
                    if (logger.isTraceEnabled()) logger.trace("{}: failed awaiting durability for {}{}.", txnId, ranges, requestor, fail);
                    if (fail instanceof SyncPointErased || fail instanceof TopologyRetiredException)
                    {
                        synchronized (DurabilityQueue.this)
                        {
                            if (request == null) abandon(pending, false, false);
                            else restart(pending);
                        }
                        return;
                    }
                }

                if (fail instanceof Exhausted || success != null)
                {
                    Collection<Node.Id> failedNodes = success != null ? success.min.excluding : ((Exhausted)fail).failedNodes();
                    Ranges failedRanges = success != null ? ranges : ((Exhausted)fail).failedRanges();
                    boolean log = failedNodes == null;
                    if (!log)
                    {
                        Set<Node.Id> unlogged = new HashSet<>(failedNodes);
                        for (Collection<Node.Id> logged : WARNINGS_LOGGED.tailMap(System.nanoTime() - MINUTES.toNanos(EXHAUSTED_LOG_INTERVAL_MINUTES)).values())
                            unlogged.removeAll(logged);
                        log = !unlogged.isEmpty();
                    }
                    if (log)
                    {
                        logger.info("{}: Incomplete durability for {}{}. {} were unsuccessful.", txnId, failedRanges, requestor, failedNodes == null ? "some nodes" : failedNodes);
                        WARNINGS_LOGGED.headMap(System.nanoTime() - MINUTES.toNanos(EXHAUSTED_LOG_INTERVAL_MINUTES)).clear();
                        WARNINGS_LOGGED.put(System.nanoTime(), failedNodes == null ? Collections.emptyList() : failedNodes);
                    }
                }
                else
                {
                    if (fail instanceof Timeout) logger.info("{}: Timeout awaiting durability for {}{}", txnId, ranges, requestor, fail);
                    else if (fail != null) logger.info("{}: Failed awaiting durability for {}{}; will retry", txnId, ranges, requestor, fail);
                }

                synchronized (DurabilityQueue.this)
                {
                    if (achieved.intersects(ranges))
                    {
                        // if we're partially done, re-register with updated ranges so as not to conflict with other operations on these portions
                        unregister(pending);
                        pending.syncPoint = pending.syncPoint.without(achieved);
                        pending.updateByRange();
                        register(pending);
                    }

                    retry(pending);
                }
            }
        });
    }

    private void done(Pending done)
    {
        Invariants.require(done.deferred == null);
        unregister(done);
        done.status = DONE;
        adapter.done(done.request, done.syncPoint);
    }

    private void abandon(Pending abandon, boolean requeueDeferred, boolean pruned)
    {
        Invariants.require(requeueDeferred || abandon.deferred == null);
        Invariants.require(abandon.parent == null);
        if (abandon.isInHeap())
            queued.remove(abandon);
        abandon.status = ABANDONED;
        if (requeueDeferred)
            requeueDeferred(abandon);
        unregister(abandon);
        adapter.abandon(abandon.request, abandon.syncPoint, pruned);
    }

    private void completing(Pending pending, long startedAt)
    {
        if (pending.status != ACTIVE || pending.startAt != startedAt)
            return;

        pending.status = COMPLETING;
        --activeCount;
        requeueDeferred(pending);
        addAndProcessQueue(requeue.poll());
    }

    private void requeueDeferred(Pending parent)
    {
        Invariants.require(parent.status.compareTo(COMPLETING) >= 0);
        List<Pending> deferreds = parent.deferred;
        if (deferreds == null)
            return;

        parent.deferred = null;
        deferreds.sort(BY_PRIORITY);
        int count = 0;
        outer: for (int i = 0 ; i < deferreds.size() ; ++i)
        {
            Pending deferred = deferreds.get(i);
            deferred.unsetParent();
            for (int j = 0 ; j < count ; ++j)
            {
                Pending newParent = deferreds.get(j);
                if (newParent.syncPoint.route.intersects((AbstractRanges) deferred.syncPoint.route))
                {
                    newParent.addDeferred(deferred);
                    continue outer;
                }
            }
            // collect any we are requeueing at the front of the list to permit us to scan them for intersections
            deferreds.set(count++, deferred);
            requeue.add(deferred);
        }
    }

    void retry(Pending retry)
    {
        Invariants.require(retry.deferred == null);
        Invariants.require(retry.status == COMPLETING);
        retry.status = QUEUED;
        prepareRetry(retry, "Retrying");
        addAndProcessQueue(retry);
    }

    void restart(Pending restart)
    {
        Invariants.require(restart.request != null);
        Invariants.require(restart.deferred == null);
        Invariants.require(restart.status == COMPLETING);
        restart.status = RESTARTING;
        unregister(restart);
        prepareRetry(restart, "Restarting");
        queued.append(restart);
        processQueue();
    }

    private void prepareRetry(Pending retry, String action)
    {
        long now = adapter.elapsed(MICROSECONDS);
        long retryDelay = adapter.retryDelay(++retry.attempt, MICROSECONDS);
        Invariants.require(retryDelay > 0);
        if (retry.request != null) logger.info("{}: {} durability for {} requested by {} in {}s.", retry.syncPoint.syncId, action, retry.syncPoint.route.toRanges(), retry.request.requestedBy, String.format("%.3f", retryDelay/1000_000.0));
        else if (logger.isDebugEnabled()) logger.debug("{}: {} durability for {} in {}s.", retry.syncPoint.syncId, action, retry.syncPoint.route.toRanges(), String.format("%.3f", retryDelay/1000_000.0));
        retry.startAt = Math.max(retry.startAt + 1, now + retryDelay);
    }

    private synchronized void processQueue()
    {
        long nowMicros = adapter.elapsed(MICROSECONDS);
        Pending next;
        while (null != (next = queued.peek()))
        {
            if (next.startAt > nowMicros)
            {
                ensureScheduled(nowMicros, next.startAt);
                break;
            }

            if (next.status == RESTARTING)
            {
                queued.poll();
                adapter.retry(next.request, next.syncPoint);
                continue;
            }

            if (activeCount >= maxConcurrency)
                break;

            queued.poll();
            start(next);
        }
    }

    private void ensureScheduled(long nowMicros, long startAtMicros)
    {
        if (processingQueueAt > startAtMicros + SCHEDULING_SLACK_MICROS)
        {
            if (cancelProcessingQueue != null)
                cancelProcessingQueue.cancel();
            processingQueueAt = startAtMicros;
            cancelProcessingQueue = adapter.scheduler().selfRecurring(() -> {
                synchronized (this)
                {
                    processingQueueAt = Long.MAX_VALUE;
                    cancelProcessingQueue = null;
                    processQueue();
                }
            }, startAtMicros - nowMicros, MICROSECONDS);
        }
    }

    public synchronized void setMaxConcurrency(int newMaxConcurrency)
    {
        this.maxConcurrency = newMaxConcurrency;
    }

    public synchronized int pendingCount()
    {
        return pendingCount;
    }

    public synchronized int activeCount()
    {
        return activeCount;
    }

    public synchronized void setOverlapPruneThreshold(int maxDeferred)
    {
        this.overlapPruneThreshold = maxDeferred;
    }

    public synchronized void setGlobalPruneThreshold(int globalPruneThreshold)
    {
        this.globalPruneThreshold = globalPruneThreshold;
    }
}