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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import accord.api.Data;
import accord.api.DataStore;
import accord.impl.AbstractFetchCoordinator;
import accord.impl.InMemoryCommandStore;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Timestamped;
import accord.utils.async.AsyncResult;

public class ListFetchCoordinator extends AbstractFetchCoordinator
{
    private final ListStore listStore;
    final List<AsyncResult<Void>> persisting = new ArrayList<>();

    public ListFetchCoordinator(Node node, Ranges ranges, SyncPoint syncPoint, DataStore.FetchRanges fetchRanges, CommandStore commandStore, ListStore listStore)
    {
        super(node, ranges, syncPoint, fetchRanges, commandStore);
        this.listStore = listStore;
    }

    @Override
    protected PartialTxn rangeReadTxn(Ranges ranges)
    {
        return new PartialTxn.InMemory(Txn.Kind.Read, ranges, new ListRead(Function.identity(), false, ranges, ranges), new ListQuery(Node.Id.NONE, Long.MIN_VALUE, false), null);
    }

    @Override
    protected void onReadOk(Node.Id from, CommandStore commandStore, Data data, Ranges received)
    {
        if (data == null)
            return;

        ListData listData = (ListData) data;
        persisting.add(commandStore.execute(PreLoadContext.empty(), safeStore -> {
            listData.forEach((key, value) -> listStore.data.merge(key, value, Timestamped::merge));
        }).flatMap(ignore -> listStore.snapshot(received, syncPoint.syncId)).addCallback((success, fail) -> {
            if (fail == null) success(from, received);
            else fail(from, received, fail);
        }).beginAsResult());
    }

    @Override
    protected FetchRequest newFetchRequest(long sourceEpoch, TxnId syncId, Ranges ranges, PartialDeps partialDeps, PartialTxn partialTxn)
    {
        if (((ListAgent)node.agent()).collectMaxApplied())
            return new CollectMaxAppliedFetchRequest(sourceEpoch, syncId, ranges, partialDeps, partialTxn);

        return super.newFetchRequest(sourceEpoch, syncId, ranges, partialDeps, partialTxn);
    }

    class CollectMaxAppliedFetchRequest extends FetchRequest
    {
        private transient Timestamp maxApplied;

        public CollectMaxAppliedFetchRequest(long sourceEpoch, TxnId syncId, Ranges ranges, PartialDeps partialDeps, PartialTxn partialTxn)
        {
            super(sourceEpoch, syncId, ranges, partialDeps, partialTxn);
        }

        @Override
        protected void readComplete(CommandStore commandStore, Data result, Ranges unavailable)
        {
            Ranges slice = commandStore.unsafeRangesForEpoch().allAt(txnId).without(unavailable);
            ((InMemoryCommandStore)commandStore).maxAppliedFor((Ranges)readScope, slice).begin((newMaxApplied, failure) -> {
                if (failure != null)
                {
                    commandStore.agent().onUncaughtException(failure);
                }
                else
                {
                    synchronized (this)
                    {
                        if (maxApplied == null) maxApplied = newMaxApplied;
                        else maxApplied = Timestamp.max(maxApplied, newMaxApplied);
                        super.readComplete(commandStore, result, unavailable);
                    }
                }
            });
        }

        @Override
        protected Timestamp maxApplied()
        {
            return maxApplied;
        }
    }

}
