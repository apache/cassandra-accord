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

import accord.api.RoutingKey;
import accord.local.cfk.CommandsForKey;
import accord.primitives.RoutingKeys;
import accord.primitives.TxnId;

import accord.primitives.Unseekables;
import accord.utils.Invariants;
import net.nicoulaj.compilecommand.annotations.Inline;

import java.util.AbstractList;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import static accord.local.KeyHistory.ASYNC;
import static accord.local.KeyHistory.INCR;
import static accord.local.KeyHistory.NONE;
import static accord.local.KeyHistory.SYNC;

/**
 * Lists txnids and keys of commands and commands for key that will be needed for an operation. Used
 * to ensure the necessary state is in memory for an operation before it executes.
 */
public interface PreLoadContext
{

    @Nullable TxnId primaryTxnId();

    /**
     * @return ids of the {@link Command} objects that need to be loaded into memory before this operation is run
     *
     * This should ONLY be non-null if primaryTxnId() is non-null
     *
     * TODO (expected): this is used for Apply, NotifyWaitingOn and listenerContexts; others only use a single txnId
     *  firstly, it would be nice to simply have that txnId as a single value.
     *  In the case of Apply, we can likely avoid loading all dependent transactions, if we can track which ranges
     *  out of memory have un-applied transactions (and try not to evict those that are not applied).
     *  Either way, the information we need in memory is super minimal for secondary transactions.
     */
    default @Nullable TxnId additionalTxnId() { return null; }

    default List<TxnId> txnIds()
    {
        TxnId primaryTxnId = primaryTxnId();
        TxnId additionalTxnId = additionalTxnId();
        Invariants.checkState(primaryTxnId != null || additionalTxnId == null);
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

    /**
     * @return keys of the {@link CommandsForKey} objects that need to be loaded into memory before this operation is run
     *
     * TODO (expected, efficiency): this used for only two things: calculateDeps and CommandStore.register.
     *  Both can be done without. For range transactions calculateDeps needs to be asynchronous anyway to support
     *  potentially large scans, and for register we do not need to load into memory, we can perform a blind write.
     */
    // TODO (required): specify epochs for which we should load, so we can narrow to owned keys
    default Unseekables<?> keys() { return RoutingKeys.EMPTY; }

    default KeyHistory keyHistory() { return NONE; }

    default boolean isEmpty()
    {
        boolean isEmpty = primaryTxnId() == null && keys().isEmpty();
        Invariants.checkState(additionalTxnId() == null);
        return isEmpty;
    }

    /**
     * Is the provided PreLoadContext guaranteed to have a superset of our requested information?
     * Note that for this calculation we are asking if all the information we want is known to be available,
     * not whether a subset has been requested - that is, a superset with INCR or ASYNC key information
     * cannot be relied upon for serving INCR or ASYNC subsets in this calculation.
     */
    default boolean isSubsetOf(PreLoadContext superset)
    {
        Unseekables<?> keys = keys();
        if (!keys.isEmpty())
        {
            KeyHistory requiredHistory = keyHistory();
            if (requiredHistory != NONE)
            {
                if (requiredHistory == INCR || requiredHistory == ASYNC)
                    requiredHistory = SYNC;
                if (requiredHistory != superset.keyHistory())
                    return false;
            }

            Unseekables<?> supersetKeys = superset.keys();
            if (supersetKeys.domain() != keys.domain() || !supersetKeys.containsAll(keys()))
                return false;
        }

        TxnId primaryId = primaryTxnId();
        TxnId additionalId = additionalTxnId();
        if (additionalId == null)
        {
            return primaryId == null || primaryId.equals(superset.primaryTxnId()) || primaryId.equals(superset.additionalTxnId());
        }
        else
        {
            Invariants.checkState(primaryId != null);
            TxnId supersetPrimaryId = superset.primaryTxnId();
            TxnId supersetAdditionalId = superset.additionalTxnId();
            return (primaryId.equals(supersetPrimaryId) || primaryId.equals(supersetAdditionalId)) && (additionalId.equals(supersetAdditionalId) || additionalId.equals(supersetPrimaryId));
        }
    }

    static PreLoadContext contextFor(@Nullable TxnId primary, @Nullable TxnId additional, Unseekables<?> keys, KeyHistory keyHistory)
    {
        Invariants.checkState(primary == null ? additional == null : !primary.equals(additional));
        return new Standard(primary, additional, keys, keyHistory);
    }

    class Standard implements PreLoadContext
    {
        private final @Nullable TxnId primary;
        private final @Nullable TxnId additional;
        private final Unseekables<?> keys;
        private final KeyHistory keyHistory;

        public Standard(@Nullable TxnId primary, @Nullable TxnId additional, Unseekables<?> keys, KeyHistory keyHistory)
        {
            Invariants.checkState(primary != null || additional == null);
            this.primary = primary;
            this.additional = additional;
            this.keys = keys;
            this.keyHistory = keyHistory;
        }

        public String toString()
        {
            return "PreLoadContext{" +
                   "primary=" + primary +
                   ", additional=" + additional +
                   ", keys=" + keys +
                   ", keyHistory=" + keyHistory +
                   '}';
        }

        @Override
        @Nullable
        public TxnId primaryTxnId()
        {
            return primary;
        }

        @Override
        public TxnId additionalTxnId()
        {
            return additional;
        }

        @Override
        public Unseekables<?> keys() { return keys; }

        @Override
        public KeyHistory keyHistory()
        {
            return keyHistory;
        }
    }

    static PreLoadContext contextFor(TxnId primary, TxnId additional, Unseekables<?> keys)
    {
        return contextFor(primary, additional, keys, NONE);
    }

    static PreLoadContext contextFor(TxnId primary, TxnId additional)
    {
        return contextFor(primary, additional, RoutingKeys.EMPTY);
    }

    static PreLoadContext contextFor(TxnId txnId, Unseekables<?> keysOrRanges, KeyHistory keyHistory)
    {
        return contextFor(txnId, null, keysOrRanges, keyHistory);
    }

    static PreLoadContext contextFor(TxnId txnId, Unseekables<?> keysOrRanges)
    {
        return contextFor(txnId, keysOrRanges, NONE);
    }

    static PreLoadContext contextFor(TxnId txnId)
    {
        return contextFor(txnId, RoutingKeys.EMPTY);
    }

    static PreLoadContext contextFor(RoutingKey key, KeyHistory keyHistory)
    {
        return contextFor(null, null, RoutingKeys.of(key), keyHistory);
    }

    static PreLoadContext contextFor(RoutingKey key)
    {
        return contextFor(key, NONE);
    }

    static PreLoadContext contextFor(Unseekables<?> keys)
    {
        return contextFor(null, null, keys);
    }

    static PreLoadContext contextFor(Unseekables<?> keys, KeyHistory keyHistory)
    {
        return contextFor(null, null, keys, keyHistory);
    }

    static PreLoadContext empty()
    {
        return EMPTY_PRELOADCONTEXT;
    }

    PreLoadContext EMPTY_PRELOADCONTEXT = contextFor(null, null, RoutingKeys.EMPTY);
}
