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

import accord.api.Key;
import accord.primitives.Keys;
import accord.primitives.Seekables;
import accord.primitives.TxnId;
import com.google.common.collect.Iterators;

import accord.utils.Invariants;
import net.nicoulaj.compilecommand.annotations.Inline;

import com.google.common.collect.Sets;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;
import javax.annotation.Nullable;

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
     * TODO (expected): this is used for Apply, NotifyWaitingOn and listenerContexts; others only use a single txnId
     *  firstly, it would be nice to simply have that txnId as a single value.
     *  In the case of Apply, we can likely avoid loading all dependent transactions, if we can track which ranges
     *  out of memory have un-applied transactions (and try not to evict those that are not applied).
     *  Either way, the information we need in memory is super minimal for secondary transactions.
     */
    default Collection<TxnId> additionalTxnIds() { return Collections.emptyList(); }

    default Collection<TxnId> txnIds()
    {
        TxnId primaryTxnId = primaryTxnId();
        Collection<TxnId> additional = additionalTxnIds();
        if (primaryTxnId == null) return additional;
        if (additional.isEmpty()) return Collections.singleton(primaryTxnId);
        return new AbstractCollection<TxnId>()
        {
            @Override
            public Iterator<TxnId> iterator()
            {
                return Iterators.concat(Iterators.singletonIterator(primaryTxnId), additional.iterator());
            }

            @Override
            public int size()
            {
                return 1 + additional.size();
            }
        };
    }

    @Inline
    default void forEachId(Consumer<TxnId> consumer)
    {
        TxnId primaryTxnId = primaryTxnId();
        if (primaryTxnId != null)
            consumer.accept(primaryTxnId);
        additionalTxnIds().forEach(consumer);
    }

    /**
     * @return keys of the {@link CommandsForKey} objects that need to be loaded into memory before this operation is run
     *
     * TODO (expected, efficiency): this used for only two things: calculateDeps and CommandStore.register.
     *  Both can be done without. For range transactions calculateDeps needs to be asynchronous anyway to support
     *  potentially large scans, and for register we do not need to load into memory, we can perform a blind write.
     */
    // TODO (required): specify epochs for which we should load, so we can narrow to owned keys
    default Seekables<?, ?> keys() { return Keys.EMPTY; }

    default KeyHistory keyHistory() { return KeyHistory.NONE; }

    default boolean isSubsetOf(PreLoadContext superset)
    {
        KeyHistory requiredHistory = keyHistory();
        if (requiredHistory != KeyHistory.NONE && requiredHistory != superset.keyHistory())
            return false;

        if (superset.keys().domain() != keys().domain() || !superset.keys().containsAll(keys()))
            return false;

        TxnId primaryId = primaryTxnId();
        Collection<TxnId> additionalIds = additionalTxnIds();
        if (additionalIds.isEmpty())
        {
            if (primaryId == null || primaryId.equals(superset.primaryTxnId()))
                return true;

            return superset.additionalTxnIds().contains(primaryTxnId());
        }
        else
        {
            TxnId supersetPrimaryId = superset.primaryTxnId();
            Set<TxnId> supersetAdditionalIds = superset.additionalTxnIds() instanceof Set ? (Set<TxnId>) superset.additionalTxnIds() : Sets.newHashSet(superset.additionalTxnIds());
            if (primaryId != null && !primaryId.equals(supersetPrimaryId) && !supersetAdditionalIds.contains(primaryId))
                return false;

            for (TxnId txnId : additionalIds)
            {
                if (!txnId.equals(supersetPrimaryId) && !supersetAdditionalIds.contains(txnId))
                    return false;
            }
            return true;
        }
    }

    static PreLoadContext contextFor(TxnId primary, Collection<TxnId> additional, Seekables<?, ?> keys, KeyHistory keyHistory)
    {
        Invariants.checkState(!additional.contains(primary));
        return new PreLoadContext()
        {
            @Override
            public TxnId primaryTxnId()
            {
                return primary;
            }

            @Override
            public Collection<TxnId> additionalTxnIds() { return additional; }

            @Override
            public Seekables<?, ?> keys() { return keys; }

            @Override
            public KeyHistory keyHistory() { return keyHistory; }
        };
    }

    static PreLoadContext contextFor(TxnId primary, Collection<TxnId> additional, Seekables<?, ?> keys)
    {
        return contextFor(primary, additional, keys, KeyHistory.NONE);
    }

    static PreLoadContext contextFor(TxnId primary, TxnId additional)
    {
        return contextFor(primary, Collections.singletonList(additional), Keys.EMPTY);
    }

    static PreLoadContext contextFor(TxnId primary, TxnId additional, Seekables<?, ?> keys)
    {
        return contextFor(primary, Collections.singletonList(additional), keys);
    }

    static PreLoadContext contextFor(TxnId txnId, Seekables<?, ?> keysOrRanges, KeyHistory keyHistory)
    {
        return contextFor(txnId, Collections.emptyList(), keysOrRanges, keyHistory);
    }

    static PreLoadContext contextFor(TxnId txnId, Seekables<?, ?> keysOrRanges)
    {
        return contextFor(txnId, keysOrRanges, KeyHistory.NONE);
    }

    static PreLoadContext contextFor(TxnId txnId)
    {
        return contextFor(txnId, Keys.EMPTY);
    }

    static PreLoadContext contextFor(TxnId primary, Collection<TxnId> additional)
    {
        return contextFor(primary, additional, Keys.EMPTY);
    }

    static PreLoadContext contextFor(Key key, KeyHistory keyHistory)
    {
        return contextFor(null, Collections.emptyList(), Keys.of(key), keyHistory);
    }

    static PreLoadContext contextFor(Key key)
    {
        return contextFor(key, KeyHistory.NONE);
    }

    static PreLoadContext contextFor(Collection<TxnId> ids, Seekables<?, ?> keys)
    {
        return contextFor(null, ids, keys);
    }

    static PreLoadContext contextFor(Seekables<?, ?> keys)
    {
        return contextFor(null, Collections.emptyList(), keys);
    }

    static PreLoadContext empty()
    {
        return EMPTY_PRELOADCONTEXT;
    }

    PreLoadContext EMPTY_PRELOADCONTEXT = contextFor(null, Collections.emptyList(), Keys.EMPTY);
}
