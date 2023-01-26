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
import accord.impl.CommandsForKey;
import accord.primitives.Keys;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.TxnId;
import accord.utils.Utils;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.Set;

/**
 * Lists txnids and keys of commands and commands for key that will be needed for an operation. Used
 * to ensure the necessary state is in memory for an operation before it executes.
 */
public interface PreLoadContext
{
    /**
     * @return ids of the {@link Command} objects that need to be loaded into memory before this operation is run
     *
     * TODO (expected): this is used for Apply, NotifyWaitingOn and listenerContexts; others only use a single txnId
     *  firstly, it would be nice to simply have that txnId as a single value.
     *  In the case of Apply, we can likely avoid loading all dependent transactions, if we can track which ranges
     *  out of memory have un-applied transactions (and try not to evict those that are not applied).
     *  Either way, the information we need in memory is super minimal for secondary transactions.
     */
    Iterable<TxnId> txnIds();

    /**
     * @return keys of the {@link CommandsForKey} objects that need to be loaded into memory before this operation is run
     *
     * TODO (expected, efficiency): this used for only two things: calculateDeps and CommandStore.register.
     *  Both can be done without. For range transactions calculateDeps needs to be asynchronous anyway to support
     *  potentially large scans, and for register we do not need to load into memory, we can perform a blind write.
     */
    Seekables<?, ?> keys();

    default boolean isSubsetOf(PreLoadContext superset)
    {
        Set<TxnId> superIds = superset.txnIds() instanceof Set ? (Set<TxnId>) superset.txnIds() : Sets.newHashSet(superset.txnIds());
        Set<Seekable> superKeys = Sets.newHashSet(superset.keys());
        return Iterables.all(txnIds(), superIds::contains) && Iterables.all(keys(), superKeys::contains);
    }

    static PreLoadContext contextFor(Iterable<TxnId> txnIds, Seekables<?, ?> keys)
    {
        return new PreLoadContext()
        {
            @Override
            public Iterable<TxnId> txnIds() { return txnIds; }

            @Override
            public Seekables<?, ?> keys() { return keys; }
        };
    }

    static PreLoadContext contextFor(TxnId txnId, Seekables<?, ?> keysOrRanges)
    {
        switch (keysOrRanges.domain())
        {
            default: throw new AssertionError();
            case Range: return contextFor(txnId); // TODO (required, correctness): this won't work for actual range queries
            case Key: return contextFor(Collections.singleton(txnId), keysOrRanges);
        }
    }

    static PreLoadContext contextFor(TxnId... txnIds)
    {
        return contextFor(Utils.listOf(txnIds), Keys.EMPTY);
    }

    static PreLoadContext contextFor(TxnId txnId)
    {
        return contextFor(Collections.singleton(txnId), Keys.EMPTY);
    }

    static PreLoadContext contextFor(Iterable<TxnId> txnIds)
    {
        return contextFor(txnIds, Keys.EMPTY);
    }

    static PreLoadContext contextFor(Key key)
    {
        return contextFor(Collections.emptyList(), Keys.of(key));
    }

    static PreLoadContext empty()
    {
        return contextFor(Collections.emptyList(), Keys.EMPTY);
    }

    PreLoadContext EMPTY_PRELOADCONTEXT = empty();
}
