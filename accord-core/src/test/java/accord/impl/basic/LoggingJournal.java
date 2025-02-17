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

package accord.impl.basic;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.NavigableMap;

import accord.api.Journal;
import accord.local.Command;
import accord.local.CommandStores;
import accord.local.DurableBefore;
import accord.local.RedundantBefore;
import accord.primitives.EpochSupplier;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.PersistentField;

/**
 * Logging journal, a wrapper over journal for debugging / inspecting history purposes.
 */
public class LoggingJournal implements Journal
{
    private final BufferedWriter log;
    private final Journal delegate;

    public LoggingJournal(Journal delegate, String path)
    {
        this.delegate = delegate;
        File f = new File(path);
        try
        {
            log = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f)));
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }

    private synchronized void log(String format, Object... objects)
    {
        try
        {
            log.write(String.format(format, objects));
            log.flush();
        }
        catch (IOException e)
        {
            // ignore
        }
    }

    @Override
    public Command loadCommand(int commandStoreId, TxnId txnId, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        return delegate.loadCommand(commandStoreId, txnId, redundantBefore, durableBefore);
    }

    @Override
    public Command.Minimal loadMinimal(int commandStoreId, TxnId txnId, Load load, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        return delegate.loadMinimal(commandStoreId, txnId, load, redundantBefore, durableBefore);
    }

    @Override
    public void saveCommand(int store, CommandUpdate update, Runnable onFlush)
    {
        log("%d: %s\n", store, update.after);
        delegate.saveCommand(store, update, onFlush);
    }

    @Override
    public Iterator<? extends TopologyUpdate> replayTopologies()
    {
        log("REPLAY TOPOLOGIES\n");
        return delegate.replayTopologies();
    }

    @Override
    public void saveTopology(TopologyUpdate topologyUpdate, Runnable onFlush)
    {
        log("%d: %s\n", topologyUpdate);
        if (onFlush != null)
            onFlush.run();
        throw new IllegalArgumentException();
    }

    @Override
    public void purge(CommandStores commandStores, EpochSupplier minEpoch)
    {
        log("PURGE\n");
        delegate.purge(commandStores, minEpoch);
    }

    @Override
    public void replay(CommandStores commandStores)
    {
        delegate.replay(commandStores);
    }

    @Override
    public RedundantBefore loadRedundantBefore(int commandStoreId)
    {
        return delegate.loadRedundantBefore(commandStoreId);
    }

    @Override
    public NavigableMap<TxnId, Ranges> loadBootstrapBeganAt(int commandStoreId)
    {
        return delegate.loadBootstrapBeganAt(commandStoreId);
    }

    @Override
    public NavigableMap<Timestamp, Ranges> loadSafeToRead(int commandStoreId)
    {
        return delegate.loadSafeToRead(commandStoreId);
    }

    @Override
    public CommandStores.RangesForEpoch loadRangesForEpoch(int commandStoreId)
    {
        return delegate.loadRangesForEpoch(commandStoreId);
    }

    @Override
    public PersistentField.Persister<DurableBefore, DurableBefore> durableBeforePersister()
    {
        return delegate.durableBeforePersister();
    }

    public void saveStoreState(int store, FieldUpdates fieldUpdates, Runnable onFlush)
    {
        log("%d: %s", store, fieldUpdates);
        delegate.saveStoreState(store, fieldUpdates, onFlush);
    }
}