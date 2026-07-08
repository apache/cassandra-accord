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

import accord.local.cfk.CommandsForKey;
import accord.primitives.Ranges;
import accord.primitives.Routables.Slice;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import accord.primitives.Unseekables;
import accord.utils.Invariants;
import net.nicoulaj.compilecommand.annotations.Inline;

import java.util.AbstractList;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import static accord.local.LoadKeys.INCR;
import static accord.local.LoadKeys.NONE;
import static accord.local.LoadKeys.SYNC;
import static accord.local.LoadKeysFor.READ_WRITE;
import static accord.local.LoadKeysFor.WRITE;

/**
 * Lists txnids and keys of commands and commands for key that will be needed for an operation. Used
 * to ensure the necessary state is in memory for an operation before it executes.
 *
 * TODO (desired): rename to simply Context, or LoadContext
 */
public interface ExecutionContext
{
    enum ExecutionKind
    {
        PREACCEPT,
        ACCEPT,
        COMMIT,
        STABLE,
        APPLY,
        OTHER,
    }

    enum ExecutionSequence
    {
        /**
         * The task may run as soon as it is ready, without any regard to ordering on other tasks on the same keys.
         */
        UNSEQUENCED,

        /**
         * The task is ordered with respect to other tasks' priorities, but if the task is INCR each batch may
         * interleave with other work on those keys.
         */
        BY_PRIORITY,

        /**
         * Appears to be processed "atomically" both itself and with the task that submits it, with respect to other tasks.
         * Meaningful only when submitted by an already running task, or against an incremental task.
         * In the latter case, if the execution partially succeeds, any failing keys are blocked from further work
         * to avoid witnessing a partial update.
         */
        ATOMIC;
    }

    @Nullable TxnId primaryTxnId();
    String reason();

    /**
     * @return ids of the {@link Command} objects that need to be loaded into memory before this operation is run
     *
     * This should ONLY be non-null if primaryTxnId() is non-null
     *
     * TODO (expected): this is used for Apply, NotifyWaitingOn and listenerContexts; others only use a single txnId
     *  The information we need in memory is super minimal for secondary transactions (mostly just SaveStatus?).
     *
     *  NOTE: this currently can change during execution for NotifyWaitingOn.
     *  This should not be treated as readable after execution is started.
     */
    default @Nullable TxnId additionalTxnId() { return null; }

    // TODO (desired): minimise call-sites, or see if hotspot can optimise this effectively
    default List<TxnId> txnIds()
    {
        TxnId primaryTxnId = primaryTxnId();
        TxnId additionalTxnId = additionalTxnId();
        Invariants.require(primaryTxnId != null || additionalTxnId == null);
        return new AbstractList<>()
        {
            @Override
            public TxnId get(int index)
            {
                return index == 0 ? primaryTxnId : additionalTxnId;
            }

            @Override
            public int size()
            {
                return primaryTxnId == null ? 0 : additionalTxnId == null ? 1 : 2;
            }
        };
    }

    @Inline
    default void forEachId(Consumer<TxnId> consumer)
    {
        TxnId primaryTxnId = primaryTxnId();
        if (primaryTxnId != null)
            consumer.accept(primaryTxnId);
        TxnId additionalTxnId = additionalTxnId();
        if (additionalTxnId != null)
            consumer.accept(additionalTxnId);
    }

    default ExecutionContext slice(Ranges ranges, Slice slice)
    {
        Unseekables<?> keys = keys();
        int size = keys.size();
        if (size == 0 || loadKeys() == NONE)
            return this;

        Unseekables<?> newKeys = keys.slice(ranges, slice);
        if (newKeys == keys)
            return this;

        return new OverrideKeys(this, newKeys);
    }

    /**
     * @return keys of the {@link CommandsForKey} objects that need to be loaded into memory before this operation is run
     */
    default Unseekables<?> keys() { return RoutingKeys.EMPTY; }

    default LoadKeys loadKeys() { return NONE; }

    default LoadKeysFor loadKeysFor() { return WRITE; }

    /**
     * Whether this execution may be retried safely; useful only for INCR tasks that may partially succeed,
     * so that the failed portions may be safely retried. It is expected that all INCR tasks are idempotent.
     */
    default boolean isIdempotent() { return false; }

    default ExecutionKind executionKind() { return ExecutionKind.OTHER; }

    default ExecutionSequence executionSequence() { return ExecutionSequence.BY_PRIORITY; }

    default Timestamp executeAt() { return primaryTxnId(); }

    default boolean isEmpty()
    {
        boolean isEmpty = primaryTxnId() == null && keys().isEmpty();
        Invariants.require(additionalTxnId() == null);
        return isEmpty;
    }
    
    /**
     * Is the provided PreLoadContext guaranteed to have a superset of our requested information?
     * Note that for this calculation we are asking if all the information we want is known to be available,
     * not whether a subset has been requested - that is, a superset with INCR or ASYNC key information
     * cannot be relied upon for serving INCR or ASYNC subsets in this calculation.
     */
    default boolean isSubsetOf(ExecutionContext superset)
    {
        Unseekables<?> keys = keys();
        if (!keys.isEmpty())
        {
            LoadKeys requiredHistory = loadKeys();
            if (requiredHistory != NONE)
            {
                if (requiredHistory.compareTo(SYNC) < 0)
                    requiredHistory = SYNC;
                if (requiredHistory.compareTo(superset.loadKeys()) < 0)
                    return false;
                if (loadKeysFor().compareTo(superset.loadKeysFor()) > 0)
                    return false;
            }

            Unseekables<?> supersetKeys = superset.keys();
            if (supersetKeys.domain() != keys.domain() || !supersetKeys.containsAll(keys()))
                return false;
        }

        return isTxnIdSubsetOf(superset);
    }

    default boolean isTxnIdSubsetOf(ExecutionContext txnIdSuperset)
    {
        TxnId primaryTxnId = primaryTxnId();
        if (primaryTxnId == null)
            return true;

        if (!primaryTxnId.equals(txnIdSuperset.primaryTxnId()))
            return false;

        TxnId additionalTxnId = additionalTxnId();
        return additionalTxnId == null || additionalTxnId.equals(txnIdSuperset.additionalTxnId());
    }

    default String describe()
    {
        List<TxnId> txnIds = txnIds();
        Unseekables<?> keys = keys();
        return reason() + (txnIds.isEmpty() ? "" : " for " + txnIds) + (keys.isEmpty() ? "" : (txnIds.isEmpty() ? " for " : " and ") + keys());
    }

    interface Wrapped extends ExecutionContext
    {
        @Nullable @Override default TxnId primaryTxnId() { return wrapped().primaryTxnId(); }
        @Nullable @Override default TxnId additionalTxnId() { return wrapped().additionalTxnId(); }
        @Override default Unseekables<?> keys() { return wrapped().keys(); }
        @Override default ExecutionSequence executionSequence() { return wrapped().executionSequence(); }
        @Override default ExecutionKind executionKind() { return wrapped().executionKind(); }
        @Override default boolean isIdempotent() { return wrapped().isIdempotent(); }
        @Override default LoadKeys loadKeys() { return wrapped().loadKeys(); }
        @Override default LoadKeysFor loadKeysFor() { return wrapped().loadKeysFor(); }
        @Override default Timestamp executeAt() { return wrapped().executeAt(); }
        @Override default String reason() { return wrapped().reason(); }
        @Override default String describe() { return wrapped().describe(); }
        ExecutionContext wrapped();
    }

    class OverrideKeys implements Wrapped
    {
        final ExecutionContext wrapped;
        final Unseekables<?> keys;
        public OverrideKeys(ExecutionContext wrapped, Unseekables<?> keys)
        {
            this.wrapped = wrapped;
            this.keys = keys;
        }

        @Override public Unseekables<?> keys() { return keys; }
        @Override public ExecutionContext wrapped() { return wrapped; }
    }

    static ExecutionContext contextFor(@Nullable TxnId primary, @Nullable TxnId additional, Unseekables<?> keys, LoadKeys loadKeys, LoadKeysFor loadKeysFor, String reason)
    {
        Invariants.require(primary == null ? additional == null : !primary.equals(additional));
        return new ExecutionContext()
        {
            @Override public @Nullable TxnId primaryTxnId() { return primary; }
            @Override public @Nullable TxnId additionalTxnId() { return additional; }
            @Override public Unseekables<?> keys() { return keys; }
            @Override public LoadKeys loadKeys() { return loadKeys; }
            @Override public LoadKeysFor loadKeysFor() { return loadKeysFor; }
            @Override public String reason() { return reason; }
            @Override public String toString() { return describe(); }
        };
    }

    default boolean contains(TxnId txnId)
    {
        TxnId primaryTxnId = primaryTxnId();
        return primaryTxnId != null && (txnId.equals(primaryTxnId) || txnId.equals(additionalTxnId()));
    }

    static ExecutionContext unsequenced(TxnId primary, String reason)
    {
        return new ExecutionContext()
        {
            @Override public @Nullable TxnId primaryTxnId() { return primary; }
            @Override public String reason() { return reason; }
            @Override public ExecutionSequence executionSequence() { return ExecutionSequence.UNSEQUENCED; }
            @Override public String toString() { return describe(); }
        };
    }

    class UnsequencedIdempotentIncrementalWrite implements ExecutionContext
    {
        final Unseekables<?> keys;
        final String reason;

        public UnsequencedIdempotentIncrementalWrite(Unseekables<?> keys, String reason)
        {
            this.keys = keys;
            this.reason = reason;
        }

        @Override public @Nullable TxnId primaryTxnId() { return null; }
        @Override public Unseekables<?> keys() { return keys; }
        @Override public LoadKeys loadKeys() { return INCR; }
        @Override public boolean isIdempotent() { return true; }
        @Override public ExecutionSequence executionSequence() { return ExecutionSequence.UNSEQUENCED; }
        @Override public String reason() { return reason; }
        @Override public String toString() { return describe(); }
    }

    static ExecutionContext unsequencedIdempotentIncrementalWrite(Unseekables<?> keys, String reason)
    {
        return new UnsequencedIdempotentIncrementalWrite(keys, reason);
    }

    static ExecutionContext unsequencedWrite(TxnId txnId, Unseekables<?> keys, String reason)
    {
        return new ExecutionContext()
        {
            @Override public @Nullable TxnId primaryTxnId() { return txnId; }
            @Override public Unseekables<?> keys() { return keys; }
            @Override public LoadKeys loadKeys() { return SYNC; }
            @Override public ExecutionSequence executionSequence() { return ExecutionSequence.UNSEQUENCED; }
            @Override public String reason() { return reason; }
            @Override public String toString() { return describe(); }
        };
    }

    static ExecutionContext unsequencedReadWrite(TxnId txnId, Unseekables<?> keys, String reason)
    {
        return new ExecutionContext()
        {
            @Override public @Nullable TxnId primaryTxnId() { return txnId; }
            @Override public Unseekables<?> keys() { return keys; }
            @Override public LoadKeys loadKeys() { return SYNC; }
            @Override public LoadKeysFor loadKeysFor() { return READ_WRITE; }
            @Override public ExecutionSequence executionSequence() { return ExecutionSequence.UNSEQUENCED; }
            @Override public String reason() { return reason; }
            @Override public String toString() { return describe(); }
        };
    }

    static ExecutionContext unsequencedReadWrite(Unseekables<?> keys, String reason)
    {
        return new ExecutionContext()
        {
            @Override public @Nullable TxnId primaryTxnId() { return null; }
            @Override public Unseekables<?> keys() { return keys; }
            @Override public LoadKeys loadKeys() { return SYNC; }
            @Override public LoadKeysFor loadKeysFor() { return READ_WRITE; }
            @Override public ExecutionSequence executionSequence() { return ExecutionSequence.UNSEQUENCED; }
            @Override public String reason() { return reason; }
            @Override public String toString() { return describe(); }
        };
    }

    interface Empty extends ExecutionContext
    {
        @Override default @Nullable TxnId primaryTxnId() { return null; }
    }
}
