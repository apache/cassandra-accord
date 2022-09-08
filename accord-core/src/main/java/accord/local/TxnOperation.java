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
import accord.primitives.TxnId;

import java.util.Collections;

/**
 * An operation that is executed in the context of a command store.
 *
 * In implementations that do not keep all data in memory, the implementation needs to know which
 * commands and commands for keys need to be in memory before it passes the function or consumer
 * off to the command store for processing. The methods on TxnOperation do this.
 */
public interface TxnOperation
{
    /**
     * @return ids of the {@link Command} objects that need to be loaded into memory before this operation is run
     */
    Iterable<TxnId> txnIds();

    /**
     * @return keys of the {@link CommandsForKey} objects that need to be loaded into memory before this operation is run
     */
    Iterable<Key> keys();

    static TxnOperation scopeFor(Iterable<TxnId> txnIds, Iterable<Key> keys)
    {
        return new TxnOperation()
        {
            @Override
            public Iterable<TxnId> txnIds() { return txnIds; }

            @Override
            public Iterable<Key> keys() { return keys; }
        };
    }

    static TxnOperation scopeFor(TxnId txnId, Iterable<Key> keys)
    {
        return scopeFor(Collections.singleton(txnId), keys);
    }

    static TxnOperation scopeFor(TxnId txnId)
    {
        return scopeFor(Collections.singleton(txnId), Collections.emptyList());
    }

    static TxnOperation scopeFor(Key key)
    {
        return scopeFor(Collections.emptyList(), Collections.singleton(key));
    }
}
