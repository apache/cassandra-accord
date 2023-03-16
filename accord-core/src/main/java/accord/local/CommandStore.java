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

package accord.local;

import accord.api.ProgressLog;
import accord.api.DataStore;
import accord.local.CommandStores.RangesForEpochHolder;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.ReducingRangeMap;
import accord.utils.async.AsyncChain;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nullable;
import accord.api.Agent;

import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;

/**
 * Single threaded internal shard of accord transaction metadata
 */
public abstract class CommandStore implements AgentExecutor
{
    public interface Factory
    {
        CommandStore create(int id,
                            NodeTimeService time,
                            Agent agent,
                            DataStore store,
                            ProgressLog.Factory progressLogFactory,
                            RangesForEpochHolder rangesForEpoch);
    }

    private static final ThreadLocal<CommandStore> CURRENT_STORE = new ThreadLocal<>();

    protected final int id;
    protected final NodeTimeService time;
    protected final Agent agent;
    protected final DataStore store;
    protected final ProgressLog progressLog;
    protected final RangesForEpochHolder rangesForEpochHolder;
    @Nullable private ReducingRangeMap<Timestamp> rejectBefore;

    protected CommandStore(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpochHolder rangesForEpochHolder)
    {
        this.id = id;
        this.time = time;
        this.agent = agent;
        this.store = store;
        this.progressLog = progressLogFactory.create(this);
        this.rangesForEpochHolder = rangesForEpochHolder;
    }

    public int id()
    {
        return id;
    }

    @Override
    public Agent agent()
    {
        return agent;
    }

    public abstract boolean inStore();

    public abstract AsyncChain<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer);

    public abstract <T> AsyncChain<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> apply);

    public abstract void shutdown();

    // implementations are expected to override this for persistence
    protected void setRejectBefore(ReducingRangeMap<Timestamp> newRejectBefore)
    {
        this.rejectBefore = newRejectBefore;
    }

    public Timestamp preaccept(TxnId txnId, Seekables<?, ?> keys, SafeCommandStore safeStore)
    {
        NodeTimeService time = safeStore.time();
        boolean isExpired = agent().isExpired(txnId, safeStore.time().now());
        if (rejectBefore != null && !isExpired)
            isExpired = null == rejectBefore.foldl(keys, (rejectIfBefore, test) -> rejectIfBefore.compareTo(test) >= 0 ? null : test, txnId, Objects::isNull);

        if (isExpired)
            return time.uniqueNow(txnId).asRejected();

        if (txnId.rw() == ExclusiveSyncPoint)
        {
            Ranges ranges = (Ranges)keys;
            ReducingRangeMap<Timestamp> newRejectBefore = rejectBefore != null ? rejectBefore : new ReducingRangeMap<>(Timestamp.NONE);
            newRejectBefore = ReducingRangeMap.add(newRejectBefore, ranges, txnId, Timestamp::max);
            setRejectBefore(newRejectBefore);
        }

        Timestamp maxConflict = safeStore.maxConflict(keys, safeStore.ranges().at(txnId.epoch()));
        if (txnId.compareTo(maxConflict) > 0 && txnId.epoch() >= time.epoch())
            return txnId;

        return time.uniqueNow(maxConflict);
    }

    protected void unsafeRunIn(Runnable fn)
    {
        CommandStore prev = maybeCurrent();
        CURRENT_STORE.set(this);
        try
        {
            fn.run();
        }
        finally
        {
            if (prev == null) CURRENT_STORE.remove();
            else CURRENT_STORE.set(prev);
        }
    }

    protected <T> T unsafeRunIn(Callable<T> fn) throws Exception
    {
        CommandStore prev = maybeCurrent();
        CURRENT_STORE.set(this);
        try
        {
            return fn.call();
        }
        finally
        {
            if (prev == null) CURRENT_STORE.remove();
            else CURRENT_STORE.set(prev);
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "{" +
               "id=" + id +
               '}';
    }

    @Nullable
    public static CommandStore maybeCurrent()
    {
        return CURRENT_STORE.get();
    }

    public static CommandStore current()
    {
        CommandStore cs = maybeCurrent();
        if (cs == null)
            throw new IllegalStateException("Attempted to access current CommandStore, but not running in a CommandStore");
        return cs;
    }

    protected static void register(CommandStore store)
    {
        if (!store.inStore())
            throw new IllegalStateException("Unable to register a CommandStore when not running in it; store " + store);
        CURRENT_STORE.set(store);
    }

    public static void checkInStore()
    {
        CommandStore store = maybeCurrent();
        if (store == null) throw new IllegalStateException("Expected to be running in a CommandStore but is not");
    }

    public static void checkNotInStore()
    {
        CommandStore store = maybeCurrent();
        if (store != null)
            throw new IllegalStateException("Expected to not be running in a CommandStore, but running in " + store);
    }
}
