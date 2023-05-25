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

import accord.api.Data;
import accord.api.DataStore;
import accord.impl.AbstractFetchCoordinator;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.SyncPoint;
import accord.primitives.Txn;
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
        return new PartialTxn.InMemory(ranges, Txn.Kind.Read, ranges, new ListRead(unsafeStore -> unsafeStore, ranges, ranges), new ListQuery(Node.Id.NONE, Long.MIN_VALUE), null);
    }

    @Override
    protected void onReadOk(Node.Id from, CommandStore commandStore, Data data, Ranges received)
    {
        if (data == null)
            return;

        ListData listData = (ListData) data;
        persisting.add(commandStore.execute(PreLoadContext.empty(), safeStore -> {
            listData.forEach((key, value) -> listStore.data.merge(key, value, Timestamped::merge));
        }).addCallback((ignore, fail) -> {
            if (fail == null) success(from, received);
            else fail(from, received, fail);
        }).beginAsResult());
    }
}
