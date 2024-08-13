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

import accord.local.SafeCommand;
import accord.primitives.TxnId;
import accord.utils.Invariants;

import static accord.impl.progresslog.TxnStateKind.Home;
import static accord.impl.progresslog.TxnStateKind.Waiting;
import static accord.local.PreLoadContext.contextFor;

class CallbackInvoker<P, V> implements BiConsumer<V, Throwable>
{
    static <P, V> BiConsumer<V, Throwable> invokeWaitingCallback(DefaultProgressLog instance, TxnId txnId, P param, Callback<P, V> callback)
    {
        return invokeCallback(Waiting, instance, txnId, param, callback);
    }

    static <P, V> BiConsumer<V, Throwable> invokeHomeCallback(DefaultProgressLog owner, TxnId txnId, P param, Callback<P, V> callback)
    {
        return invokeCallback(Home, owner, txnId, param, callback);
    }

    static <P, V> BiConsumer<V, Throwable> invokeCallback(TxnStateKind kind, DefaultProgressLog owner, TxnId txnId, P param, Callback<P, V> callback)
    {
        CallbackInvoker<P, V> invoker = new CallbackInvoker<>(owner, kind, owner.nextInvokerId(), txnId, param, callback);
        owner.registerActive(kind, txnId, invoker);
        return invoker;
    }

    final DefaultProgressLog owner;
    final boolean isHome;
    final long id;
    final TxnId txnId;
    final P param;
    final Callback<P, V> callback;

    CallbackInvoker(DefaultProgressLog owner, TxnStateKind kind, long id, TxnId txnId, P param, Callback<P, V> callback)
    {
        this.owner = owner;
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

    @Override
    public void accept(V success, Throwable fail)
    {
        owner.commandStore.execute(contextFor(txnId), safeStore -> {

            // we load safeCommand first so that if it clears the progress log we abandon the callback
            SafeCommand safeCommand = safeStore.ifInitialised(txnId);
            if (!owner.deregisterActive(kind(), this))
                return;

            Invariants.checkState(safeCommand != null);
            callback.callback(safeStore, safeCommand, owner, txnId, param, success, fail);

        }).begin(owner.commandStore.agent());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null) return false;
        if (obj.getClass() == TxnId.class) return txnId.equals(obj);
        if (obj.getClass() != getClass()) return false;
        CallbackInvoker<?, ?> that = (CallbackInvoker<?, ?>) obj;
        return id == that.id && callback == that.callback;
    }

    @Override
    public int hashCode()
    {
        return txnId.hashCode();
    }
}

