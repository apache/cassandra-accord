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

package accord.impl;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.CoordinatorEventListener;
import accord.api.OwnershipEventListener;
import accord.api.ProgressLog;
import accord.api.Result;
import accord.impl.mock.MockCluster;
import accord.impl.mock.MockStore;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.local.TimeService;
import accord.messages.MessageType;
import accord.primitives.Ballot;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestAgent implements Agent, OwnershipEventListener
{
    private static final Logger logger = LoggerFactory.getLogger(TestAgent.class);

    public static class RethrowAgent extends TestAgent implements CoordinatorEventListener
    {
        public RethrowAgent()
        {
            super();
        }

        public RethrowAgent(TimeService clock)
        {
            super(clock);
        }

        @Override
        public CoordinatorEventListener coordinatorEvents()
        {
            return this;
        }

        @Override
        public void onRecoveryStopped(Node node, TxnId txnId, Ballot ballot, Result success, Throwable fail)
        {
            if (fail != null)
                throw new AssertionError("Unexpected exception", fail);
        }

        @Override
        public void onFailedBootstrap(int attempt, String phase, Ranges ranges, Runnable retry, Runnable fail, Throwable failure)
        {
            if (failure != null)
                throw new AssertionError("Unexpected exception", failure);
        }

        @Override
        public void onException(Throwable t)
        {
            throw new AssertionError("Unexpected exception", t);
        }

        @Override
        public void onException(Throwable t, String context)
        {
            throw new AssertionError("Unexpected exception", t);
        }
    }

    final TimeService clock;
    public TestAgent()
    {
        this(new MockCluster.Clock(0));
    }

    public TestAgent(TimeService clock)
    {
        this.clock = clock;
    }

    @Override
    public void onFailedBootstrap(int attempt, String phase, Ranges ranges, Runnable retry, Runnable fail, Throwable failure)
    {
        retry.run();
    }

    @Override
    public void onStale(Timestamp staleSince, Ranges ranges)
    {

    }

    @Override
    public OwnershipEventListener ownershipEvents()
    {
        return this;
    }

    @Override
    public void onException(Throwable t)
    {
        logger.error("Uncaught exception", t);
    }

    @Override
    public void onException(Throwable t, String context)
    {
    }

    @Override
    public boolean rejectPreAccept(TimeService time, TxnId txnId)
    {
        return false;
    }

    @Override
    public long cfkHlcPruneDelta()
    {
        return 1000;
    }

    @Override
    public int cfkPruneInterval()
    {
        return 1;
    }

    @Override
    public long maxConflictsHlcPruneDelta()
    {
        return 500;
    }

    @Override
    public Txn emptySystemTxn(Txn.Kind kind, Domain domain)
    {
        return new Txn.InMemory(kind, domain == Domain.Key ? Keys.EMPTY : Ranges.EMPTY, MockStore.read(Keys.EMPTY), MockStore.QUERY, null);
    }

    @Override
    public long slowCoordinatorDelay(Node node, SafeCommandStore safeStore, TxnId txnId, TimeUnit units, int attempt)
    {
        return units.convert(1L, SECONDS);
    }

    @Override
    public boolean isSlowCoordinator(long elapsed, TimeUnit units, TxnId txnId, int attempt)
    {
        return units.toSeconds(elapsed) > 1;
    }

    @Override
    public long slowReplicaDelay(Node node, SafeCommandStore safeStore, TxnId txnId, int attempt, ProgressLog.BlockedUntil blockedUntil, TimeUnit units)
    {
        return units.convert(1L, SECONDS);
    }

    @Override
    public long slowAwaitDelay(Node node, SafeCommandStore safeStore, TxnId txnId, int attempt, ProgressLog.BlockedUntil retrying, TimeUnit units)
    {
        return units.convert(1L, SECONDS);
    }

    @Override
    public long retrySyncPointDelay(Node node, int attempt, TimeUnit units)
    {
        return units.convert(1L, MINUTES);
    }

    @Override
    public long retryTopologyDelay(Node node, int attempt, TimeUnit units)
    {
        return units.convert(1L, MINUTES);
    }

    @Override
    public long retryDurabilityDelay(Node node, int attempt, TimeUnit units)
    {
        return units.convert(1L, MINUTES);
    }

    @Override
    public long expireEpochWait(TimeUnit units)
    {
        return units.convert(10, SECONDS);
    }

    @Override
    public long selfSlowAt(TxnId txnId, MessageType messageType, TimeUnit unit)
    {
        return clock.elapsed(unit) + unit.convert(100L, MICROSECONDS);
    }

    @Override
    public long selfExpiresAt(TxnId txnId, MessageType messageType, TimeUnit unit)
    {
        return clock.elapsed(unit) + unit.convert(1L, SECONDS);
    }

    @Override
    public AsyncChain<TxnId> awaitStaleId(Node node, TxnId staleId, boolean isRequested)
    {
        return AsyncChains.success(staleId);
    }

    @Override
    public long minStaleHlc(Node node, boolean requested)
    {
        return node.now() - SECONDS.toMillis(1);
    }
}
