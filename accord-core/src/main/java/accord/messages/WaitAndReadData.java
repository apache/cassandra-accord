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

package accord.messages;

import accord.local.LocalBarrier;
import accord.local.SafeCommandStore;
import accord.local.Status;
import accord.primitives.Deps;
import accord.primitives.PartialTxn;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.agrona.collections.Int2ObjectHashMap;

public abstract class WaitAndReadData extends ReadData
{
    final Status waitForStatus;
    final Deps waitOn;
    protected final Timestamp waitUntil; // this may be set to Timestamp.MAX if we want to wait for all deps, regardless of when they execute
    protected final Timestamp executeReadAt;
    final PartialTxn read;
    final Int2ObjectHashMap<LocalBarrier> barriers = new Int2ObjectHashMap<>();

    protected WaitAndReadData(Seekables<?, ?> readScope, long waitForEpoch, Status waitForStatus, Deps waitOn, Timestamp waitUntil, Timestamp executeReadAt, PartialTxn read)
    {
        super(TxnId.NONE, readScope, waitForEpoch);
        this.waitForStatus = waitForStatus;
        this.waitOn = waitOn;
        this.waitUntil = waitUntil;
        this.executeReadAt = executeReadAt;
        this.read = read;
    }

    @Override
    public synchronized ReadNack apply(SafeCommandStore safeStore)
    {
        waitingOn.set(safeStore.commandStore().id());
        ++waitingOnCount;
        barriers.put(safeStore.commandStore().id(), LocalBarrier.register(safeStore, waitForStatus, waitOn, waitUntil, executeReadAt, safeStore0 -> {
            read(safeStore0, executeReadAt, read);
        }));
        return null;
    }

    protected void cancel()
    {
    }

    @Override
    protected long executeAtEpoch()
    {
        return executeReadAt.epoch();
    }
}
