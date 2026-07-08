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

package accord.impl.progresslog;

import java.util.function.BiConsumer;

import javax.annotation.Nullable;

import accord.local.ExecutionContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.TxnId;

import static accord.impl.progresslog.TxnStateKind.Home;
import static accord.impl.progresslog.TxnStateKind.Waiting;

final class CallbackInvoker<P, V> extends DefaultProgressLog.PendingTask implements BiConsumer<V, Throwable>, ExecutionContext
{
    static <P, V> CallbackInvoker<P, V> invokeWaitingCallback(DefaultProgressLog instance, TxnId txnId, P param, Callback<P, V> callback)
    {
        return invokeCallback(Waiting, instance, txnId, param, callback);
    }

    static <P, V> CallbackInvoker<P, V> invokeHomeCallback(DefaultProgressLog owner, TxnId txnId, P param, Callback<P, V> callback)
    {
        return invokeCallback(Home, owner, txnId, param, callback);
    }

    static <P, V> CallbackInvoker<P, V> invokeCallback(TxnStateKind kind, DefaultProgressLog owner, TxnId txnId, P param, Callback<P, V> callback)
    {
        CallbackInvoker<P, V> invoker = new CallbackInvoker<>(owner, kind, owner.nextCallbackId(), txnId, param, callback);
        owner.registerPending(kind, txnId, invoker);
        return invoker;
    }

    final boolean isHome;
    final long id;
    final TxnId txnId;
    final P param;
    final Callback<P, V> callback;

    CallbackInvoker(DefaultProgressLog owner, TxnStateKind kind, long id, TxnId txnId, P param, Callback<P, V> callback)
    {
        super(owner);
        this.isHome = kind == Home;
        this.id = id;
        this.txnId = txnId;
        this.param = param;
        this.callback = callback;
    }

    private TxnStateKind kind()
    {
        return isHome ? Home : Waiting;
    }

    private boolean complete()
    {
        return owner.complete(kind(), id, txnId, this);
    }

    @Override
    public void accept(V success, Throwable fail)
    {
        owner.commandStore.execute(this, safeStore -> {
            try
            {
                // we load safeCommand first so that if it clears the progress log we abandon the callback
                SafeCommand safeCommand = safeStore.ifInitialised(txnId);
                if (complete() && safeCommand != null)
                    acceptInternal(safeStore, safeCommand, success, fail);
            }
            finally
            {
                postRun(safeStore);
            }
        }, owner.commandStore.agent());
    }

    private void acceptInternal(SafeCommandStore safeStore, SafeCommand safeCommand, V success, Throwable fail)
    {
        callback.callback(safeStore, safeCommand, owner, txnId, param, success, fail);
    }

    @Override
    public String toString()
    {
        return txnId + (isHome ? ":Home:" : ":Waiting:") + owner.commandStore;
    }

    @Nullable
    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public String reason()
    {
        return "Callback " + this;
    }
}

