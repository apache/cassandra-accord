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

package accord.topology;

import java.util.ArrayDeque;
import java.util.Set;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import accord.api.AsyncExecutor;
import accord.api.Timeouts;
import accord.api.Timeouts.RegisteredTimeout;
import accord.api.Timeouts.SentinelRegisteredTimeout;
import accord.coordinate.EpochTimeout;
import accord.local.Node;
import accord.primitives.Ranges;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.agrona.collections.ObjectHashSet;

import static accord.primitives.AbstractRanges.UnionMode.MERGE_ADJACENT;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

class PendingEpoch implements Timeouts.Timeout
{
    private static final AtomicReferenceFieldUpdater<PendingEpoch, RegisteredTimeout> timeoutUpdater = AtomicReferenceFieldUpdater.newUpdater(PendingEpoch.class, RegisteredTimeout.class, "timeout");
    private static final RegisteredTimeout DONE = new SentinelRegisteredTimeout();

    final long epoch;

    private final TopologyManager manager;
    private boolean isActive;
    private final ArrayDeque<WaitingForEpoch> waiting = new ArrayDeque<>();
    private volatile RegisteredTimeout timeout;
    private volatile Topology topology;
    volatile int fetchAttempts;
    AsyncResult<Topology> fetching;

    @GuardedBy("manager")
    final Set<Node.Id> ready = new ObjectHashSet<>();
    @GuardedBy("manager")
    Ranges closed = Ranges.EMPTY, retired = Ranges.EMPTY;

    public PendingEpoch(long epoch, TopologyManager manager)
    {
        this.epoch = epoch;
        this.manager = manager;
    }

    @GuardedBy("manager")
    void remoteReadyToCoordinate(Node.Id id)
    {
        ready.add(id);
    }

    @GuardedBy("manager")
    Ranges closed(Ranges ranges)
    {
        ranges = ranges.without(closed);
        closed = closed.union(MERGE_ADJACENT, ranges);
        return ranges;
    }

    @GuardedBy("manager")
    Ranges retired(Ranges ranges)
    {
        ranges = ranges.without(retired);
        retired = retired.union(MERGE_ADJACENT, ranges);
        return ranges;
    }

    Topology topology()
    {
        return topology;
    }

    void setTopology(Topology topology)
    {
        this.topology = topology;
    }

    // TODO (expected): pass through request deadline
    WaitingForEpoch whenActive()
    {
        WaitingForEpoch result, last;
        synchronized (this)
        {
            if (isActive)
                return WaitingForEpoch.DONE;

            long timeoutMicros = manager.node.agent().expireEpochWait(MICROSECONDS);
            long deadlineMicros = manager.time.elapsed(MICROSECONDS) + timeoutMicros;
            result = last = waiting.peekLast();
            if (last == null || last.deadlineMicros < deadlineMicros)
                waiting.add(result = new WaitingForEpoch(deadlineMicros + (timeoutMicros / 10)));
        }
        if (last == null)
            ensureTimeout(result.deadlineMicros);
        return result;
    }

    private void ensureTimeout(long deadlineMicros)
    {
        // we first set a sentinel entry to handle the unlikely case of the timeout firing before we can update the timeout field
        SentinelRegisteredTimeout sentinel;
        if (timeoutUpdater.compareAndSet(this, null, sentinel = new SentinelRegisteredTimeout()))
        {
            RegisteredTimeout timeout = manager.timeouts.registerAt(this, deadlineMicros, MICROSECONDS);
            if (!timeoutUpdater.compareAndSet(this, sentinel, timeout))
                timeout.cancel();
        }
    }

    void setActive()
    {
        synchronized (this)
        {
            isActive = true;
            WaitingForEpoch next;
            while (null != (next = waiting.poll()))
                next.result.trySuccess(null);
        }
        RegisteredTimeout cancel = timeoutUpdater.getAndSet(this, DONE);
        if (cancel != null)
            cancel.cancel();
    }

    @Override
    public void timeout()
    {
        RegisteredTimeout curTimeout = this.timeout;
        long nextDeadlineMicros = 0;
        synchronized (this)
        {
            if (isActive || curTimeout == DONE)
                return;

            long nowMicros = manager.time.elapsed(MICROSECONDS);
            while (true)
            {
                WaitingForEpoch next = waiting.peek();
                if (next == null || next.deadlineMicros > nowMicros)
                {
                    // clear our now-expired registration so a new one can be registered later
                    timeoutUpdater.compareAndSet(this, curTimeout, null);
                    if (next != null)
                        nextDeadlineMicros = next.deadlineMicros;
                    break;
                }

                waiting.poll();
                if (next.result.tryFailure(EpochTimeout.timeout(epoch, manager.node.agent())))
                    manager.node.agent().systemEvents().onTimeoutForEpoch(epoch, next.waiting);
            }
        }
        if (nextDeadlineMicros > 0)
            ensureTimeout(nextDeadlineMicros);
    }

    @Override
    public int stripe()
    {
        return (int) epoch;
    }

    static class WaitingForEpoch
    {
        private static final WaitingForEpoch DONE = new WaitingForEpoch(0);
        static { DONE.result.setSuccess(null); }

        private final AsyncResults.SettableResult<Void> result = new AsyncResults.SettableResult<>();
        private final long deadlineMicros;

        private volatile int waiting;
        private static final AtomicIntegerFieldUpdater<WaitingForEpoch> waitingUpdater = AtomicIntegerFieldUpdater.newUpdater(WaitingForEpoch.class, "waiting");

        WaitingForEpoch(long deadlineMicros)
        {
            this.deadlineMicros = deadlineMicros;
        }

        AsyncChain<Void> chainImmediatelyElse(@Nullable AsyncExecutor executor)
        {
            AsyncChain<Void> chain = result.chain();
            if (result.isDone())
                return chain;

            waitingUpdater.incrementAndGet(this);
            return chain.withExecutor(executor);
        }

        AsyncResult<Void> get()
        {
            return result;
        }
    }
}
