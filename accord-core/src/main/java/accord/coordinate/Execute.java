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

package accord.coordinate;

import java.util.function.BiConsumer;

import accord.api.Result;
import accord.local.Node;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Participants;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.primitives.Txn.Kind;

import static accord.utils.Invariants.checkArgument;

public interface Execute
{
    interface Factory
    {
        Execute create(Node node, TxnId txnId, Txn txn, FullRoute<?> route, Participants<?> readScope, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback);
    }

    void start();

    static void execute(Node node, TxnId txnId, Txn txn, FullRoute<?> route, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
    {
        Seekables<?, ?> readKeys = txn.read().keys();
        Participants<?> readScope = readKeys.toParticipants();
        // Recovery calls execute and we would like execute to run BlockOnDeps because that will notify the agent
        // of the local barrier
        if (txn.kind() == Kind.SyncPoint)
        {
            checkArgument(txnId.equals(executeAt));
            BlockOnDeps.blockOnDeps(node, txnId, txn, route, deps, callback);
        }
        else
        {
            if (readKeys.isEmpty())
            {
                Result result = txn.result(txnId, executeAt, null);
                Writes writes = txn.execute(txnId, executeAt, null);
                Persist.persist(node, txnId, route, txn, executeAt, deps, writes, result, callback);
            }
            else
            {
                Execute execute = node.executionFactory().create(node, txnId, txn, route, readScope, executeAt, deps, callback);
                execute.start();
            }
        }
    }
}
