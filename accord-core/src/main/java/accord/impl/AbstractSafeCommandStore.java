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

package accord.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.local.ExecutionContext.OverrideKeys;
import accord.local.LoadKeys;
import accord.local.ExecutionContext;
import accord.local.RedundantBefore;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.cfk.SafeCommandsForKey;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;

import static accord.api.Journal.FieldUpdates;
import static accord.local.CommandStores.RangesForEpoch;

public abstract class AbstractSafeCommandStore<C extends SafeCommand,
                                              CFK extends SafeCommandsForKey,
                                              Caches extends AbstractSafeCommandStore.CommandStoreCaches<C, CFK>>
extends SafeCommandStore
{
    protected final ExecutionContext context;
    private FieldUpdates fieldUpdates;

    protected AbstractSafeCommandStore(ExecutionContext context)
    {
        this.context = context;
    }

    public interface CommandStoreCaches<C, CFK> extends AutoCloseable
    {
        void close();

        C acquireIfLoaded(TxnId txnId);
        CFK acquireIfLoaded(RoutingKey key);
    }

    protected abstract Caches tryGetCaches();
    protected abstract C add(C safeCommand, Caches caches);
    protected abstract CFK add(CFK safeCfk, Caches caches);

    @Override
    public ExecutionContext canExecute(ExecutionContext with)
    {
        Unseekables<?> withKeys = with.keys();
        if (withKeys.domain() == Domain.Range)
            return with.isSubsetOf(context) ? with : null;

        LoadKeys loadKeys = with.loadKeys();
        if (loadKeys != LoadKeys.NONE && with.findKeys().compareTo(context.findKeys()) > 0)
            return null;

        Caches caches = null;
        try
        {
            TxnId primaryTxnId = with.primaryTxnId();
            if (primaryTxnId != null)
            {
                if (!isPresent(primaryTxnId))
                {
                    caches = tryGetCaches();
                    if (ifLoadedInternal(primaryTxnId) == null)
                        return null;
                }

                TxnId additionalTxnId = with.additionalTxnId();
                if (additionalTxnId != null && !isPresent(additionalTxnId))
                {
                    if (caches == null)
                        caches = tryGetCaches();

                    if (ifLoadedInternal(additionalTxnId) == null)
                        return null;
                }
            }

            if (loadKeys == LoadKeys.NONE || withKeys.isEmpty())
                return with;

            List<RoutingKey> unavailable = null;
            for (int i = 0 ; i < withKeys.size() ; ++i)
            {
                RoutingKey key = (RoutingKey) withKeys.get(i);
                if (isPresent(key))
                    continue;

                if (unavailable == null && caches == null)
                    caches = tryGetCaches();

                if (ifLoadedInternal(caches, key) != null)
                    continue;

                if (unavailable == null)
                    unavailable = new ArrayList<>();

                unavailable.add(key);
            }

            if (unavailable == null)
                return with;

            if (unavailable.size() == withKeys.size())
                return null;

            return new OverrideKeys(with, withKeys.without(RoutingKeys.ofSortedUnique(unavailable)));
        }
        finally
        {
            if (caches != null)
                caches.close();
        }
    }

    private boolean isPresent(TxnId txnId)
    {
        return getInternal(txnId) != null;
    }

    private boolean isPresentOrLoadedInternal(@Nonnull Caches caches, TxnId txnId)
    {
        return getInternal(txnId) != null || ifLoadedInternal(caches, txnId) != null;
    }

    private C ifLoadedInternal(@Nullable Caches caches, TxnId txnId)
    {
        if (caches == null)
            return null;

        C command = caches.acquireIfLoaded(txnId);
        if (command == null)
            return null;

        return add(command, caches);
    }

    @Override
    protected C ifLoadedInternal(TxnId txnId)
    {
        try (Caches caches = tryGetCaches())
        {
            return ifLoadedInternal(caches, txnId);
        }
    }

    private boolean isPresentOrLoadedInternal(@Nonnull Caches caches, RoutingKey key)
    {
        return getInternal(key) != null || ifLoadedInternal(caches, key) != null;
    }

    private boolean isPresent(RoutingKey key)
    {
        return getInternal(key) != null;
    }

    protected CFK ifLoadedInternal(@Nullable Caches caches, RoutingKey key)
    {
        if (caches == null)
            return null;

        CFK cfk = caches.acquireIfLoaded(key);
        if (cfk == null)
            return null;

        return add(cfk, caches);
    }

    @Override
    protected CFK ifLoadedInternal(RoutingKey key)
    {
        try (Caches caches = tryGetCaches())
        {
            return ifLoadedInternal(caches, key);
        }
    }

    @Override
    public ExecutionContext context()
    {
        return context;
    }

    // TODO (expected): cleanup the integration hooks here; they're a bit byzantine. Also clearly document behaviour.
    public void postExecute()
    {
        commandStore().unsafeProgressLog().maybeNotify();
        flushFieldUpdates();
    }

    protected void persistFieldUpdates()
    {
        flushFieldUpdates();
    }

    protected void flushFieldUpdates()
    {
        if (fieldUpdates == null)
            return;

        if (fieldUpdates.newRedundantBefore != null)
            super.unsafeSetRedundantBefore(fieldUpdates.newRedundantBefore);

        if (fieldUpdates.newBootstrapBeganAt != null)
            super.setBootstrapBeganAt(fieldUpdates.newBootstrapBeganAt);

        if (fieldUpdates.newSafeToRead != null)
            super.setSafeToRead(fieldUpdates.newSafeToRead);

        if (fieldUpdates.newRangesForEpoch != null)
            super.setRangesForEpoch(fieldUpdates.newRangesForEpoch);

        if (fieldUpdates.newPermanentlyUnsafeToRead != null)
            super.setPermanentlyUnsafeToRead(fieldUpdates.newPermanentlyUnsafeToRead);

        fieldUpdates = null;
    }

    /**
     * Persistent field update logic
     */

    @Override
    public final void upsertRedundantBefore(RedundantBefore addRedundantBefore)
    {
        // TODO (required): this is potentially unsafe: if the update is not persisted for some reason (due to some later exception)
        //   we can continue with a stale redundantBefore
        // TODO (expected): fix RedundantBefore sorting issue and switch to upsert mode
        ensureFieldUpdates().newRedundantBefore = RedundantBefore.merge(redundantBefore(), addRedundantBefore);
        unsafeUpsertRedundantBefore(addRedundantBefore);
    }

    @Override
    public final void setBootstrapBeganAt(NavigableMap<TxnId, Ranges> newBootstrapBeganAt)
    {
        ensureFieldUpdates().newBootstrapBeganAt = newBootstrapBeganAt;
    }

    @Override
    public final void setSafeToRead(NavigableMap<Timestamp, Ranges> newSafeToRead)
    {
        ensureFieldUpdates().newSafeToRead = newSafeToRead;
    }

    @Override
    public final void setPermanentlyUnsafeToRead(Ranges newPermanentlyUnsafeToRead)
    {
        ensureFieldUpdates().newPermanentlyUnsafeToRead = newPermanentlyUnsafeToRead;
    }

    @Override
    public void setRangesForEpoch(RangesForEpoch rangesForEpoch)
    {
        if (rangesForEpoch != null)
        {
            super.setRangesForEpoch(rangesForEpoch);
            ensureFieldUpdates().newRangesForEpoch = rangesForEpoch;
        }
    }

    @Override
    public RangesForEpoch ranges()
    {
        // TODO (expected): do we even need this? We should probably reflect this immediately in CommandStore, and revert if we fail
        // if we remove it
        if (fieldUpdates != null && fieldUpdates.newRangesForEpoch != null)
            return fieldUpdates.newRangesForEpoch;

        return commandStore().unsafeGetRangesForEpoch();
    }

    @Override
    public NavigableMap<TxnId, Ranges> bootstrapBeganAt()
    {
        if (fieldUpdates != null && fieldUpdates.newBootstrapBeganAt != null)
            return fieldUpdates.newBootstrapBeganAt;

        return super.bootstrapBeganAt();
    }

    @Override
    public NavigableMap<Timestamp, Ranges> safeToReadAt()
    {
        if (fieldUpdates != null && fieldUpdates.newSafeToRead != null)
            return fieldUpdates.newSafeToRead;

        return super.safeToReadAt();
    }

    @Override
    public RedundantBefore redundantBefore()
    {
        if (fieldUpdates != null && fieldUpdates.newRedundantBefore != null)
            return fieldUpdates.newRedundantBefore;

        return super.redundantBefore();
    }

    private FieldUpdates ensureFieldUpdates()
    {
        if (fieldUpdates == null) fieldUpdates = new FieldUpdates();
        return fieldUpdates;
    }

    public FieldUpdates fieldUpdates()
    {
        return fieldUpdates;
    }
}
