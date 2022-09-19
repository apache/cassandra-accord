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

import java.util.Collections;

/**
 * Lists txnids and keys of commands and commands for key that will be needed for an operation. Used
 * to ensure the necessary state is in memory for an operation before it executes.
 */
public interface PreLoadContext
{
    /**
     * @return ids of the {@link Command} objects that need to be loaded into memory before this operation is run
     */
    Iterable<TxnId> txnIds();

    /**
     * @return keys of the {@link CommandsForKey} objects that need to be loaded into memory before this operation is run
     */
    Seekables<?, ?> keys();

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
        switch (keysOrRanges.kindOfContents())
        {
            default: throw new AssertionError();
            case Range: return contextFor(txnId); // TODO (soon): this won't work for actual range queries
            case Key: return contextFor(Collections.singleton(txnId), keysOrRanges);
        }
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
}
