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

package accord.impl.list;

import java.util.function.Function;
import javax.annotation.Nullable;

import accord.api.Data;
import accord.api.DataStore;
import accord.coordinate.tracking.AbstractTracker;
import accord.impl.AbstractFetchCoordinator;
import accord.local.CommandStore;
import accord.local.ExecutionContext.Empty;
import accord.local.Node;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.TopologyException;
import accord.utils.SortedArrays;

public class ListFetchCoordinator extends AbstractFetchCoordinator
{
    private final ListStore listStore;

    public ListFetchCoordinator(Node node, TxnId atLeast, Ranges ranges, SortedArrays.SortedArrayList<Node.Id> readable, DataStore.FetchRanges fetchRanges, CommandStore commandStore, ListStore listStore) throws TopologyException
    {
        super(node, node.someExclusiveExecutor(), ranges, atLeast, readable, fetchRanges, commandStore);
        this.listStore = listStore;
    }

    @Override
    protected void onReadOk(Node.Id from, CommandStore commandStore, Data data, Ranges received)
    {
        if (data == null)
            return;

        ListData listData = (ListData) data;
        persisting.add(commandStore.chain((Empty) () -> "List Fetch", safeStore -> {
            listData.forEach(listStore::writeUnsafe);
        }).flatMapResult(ignore -> listStore.snapshot(true)).invoke((success, fail) -> {
            if (fail == null) success(from, received);
            else fail(from, received, fail);
        }).beginAsResult());
    }

    @Override
    protected AbstractFetchCoordinator.FetchRequest newFetchRequest(long sourceEpoch, TxnId syncId, Ranges ranges)
    {
        return new ListFetchRequest(sourceEpoch, syncId, ranges, rangeReadTxn(ranges));
    }

    private PartialTxn rangeReadTxn(Ranges ranges)
    {
        return new PartialTxn.InMemory(Txn.Kind.Read, ranges, new ListRead(Function.identity(), false, ranges, ranges), new ListQuery(Node.Id.NONE, Long.MIN_VALUE, false), null);
    }

    @Nullable
    @Override
    public AbstractTracker<?> tracker()
    {
        return null;
    }

    static class ListFetchRequest extends FetchRequest
    {
        public ListFetchRequest(long sourceEpoch, TxnId syncId, Ranges ranges, PartialTxn partialTxn)
        {
            super(sourceEpoch, syncId, ranges, partialTxn);
        }
    }
}
