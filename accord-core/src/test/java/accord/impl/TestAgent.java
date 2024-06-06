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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.Result;
import accord.impl.mock.MockStore;
import accord.local.Command;
import accord.local.Node;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import javax.annotation.Nonnull;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestAgent implements Agent
{
    private static final Logger logger = LoggerFactory.getLogger(TestAgent.class);

    public final ConcurrentMap<Timestamp, AtomicInteger> completedLocalBarriers = new ConcurrentHashMap<>();

    public static class RethrowAgent extends TestAgent
    {
        @Override
        public void onRecover(Node node, Result success, Throwable fail)
        {
            if (fail != null)
                throw new AssertionError("Unexpected exception", fail);
        }


        @Override
        public void onFailedBootstrap(String phase, Ranges ranges, Runnable retry, Throwable failure)
        {
            if (failure != null)
                throw new AssertionError("Unexpected exception", failure);
        }

        @Override
        public void onUncaughtException(Throwable t)
        {
            throw new AssertionError("Unexpected exception", t);
        }

        @Override
        public void onHandledException(Throwable t)
        {
            throw new AssertionError("Unexpected exception", t);
        }
    }

    @Override
    public void onRecover(Node node, Result success, Throwable fail)
    {
        // do nothing, intended for use by implementations to decide what to do about recovered transactions
        // specifically if and how they should inform clients of the result
        // e.g. in Maelstrom we send the full result directly, in other impls we may simply acknowledge success via the coordinator
    }

    @Override
    public void onInconsistentTimestamp(Command command, Timestamp prev, Timestamp next)
    {
        throw new AssertionError();
    }

    @Override
    public void onFailedBootstrap(String phase, Ranges ranges, Runnable retry, Throwable failure)
    {
        retry.run();
    }

    @Override
    public void onStale(Timestamp staleSince, Ranges ranges)
    {

    }

    @Override
    public void onUncaughtException(Throwable t)
    {
        logger.error("Uncaught exception", t);
    }

    @Override
    public void onHandledException(Throwable t)
    {
    }

    @Override
    public long preAcceptTimeout()
    {
        return MICROSECONDS.convert(10, SECONDS);
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
    public Txn emptyTxn(Txn.Kind kind, Seekables<?, ?> keysOrRanges)
    {
        return new Txn.InMemory(kind, keysOrRanges, MockStore.read(Keys.EMPTY), MockStore.QUERY, null);
    }

    @Override
    public void onLocalBarrier(@Nonnull Seekables<?, ?> keysOrRanges, @Nonnull TxnId txnId)
    {
        completedLocalBarriers.computeIfAbsent(txnId, ignored -> new AtomicInteger()).incrementAndGet();
    }
}
