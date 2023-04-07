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

import java.util.function.Function;

import accord.local.*;
import accord.local.Status.Known;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.agrona.collections.IntHashSet;

import static accord.messages.Defer.Ready.Expired;
import static accord.messages.Defer.Ready.No;
import static accord.messages.Defer.Ready.Yes;

class Defer implements CommandListener
{
    public enum Ready { No, Yes, Expired }

    final Function<Command, Ready> waitUntil;
    final TxnRequest<?> request;
    final IntHashSet waitingOn = new IntHashSet();
    int waitingOnCount;
    boolean isDone;

    Defer(Known waitUntil, Known expireAt, TxnRequest<?> request)
    {
        this(command -> {
            if (!waitUntil.isSatisfiedBy(command.known()))
                return No;
            if (expireAt.isSatisfiedBy(command.known()))
                return Expired;
            return Yes;
        }, request);
    }

    Defer(Function<Command, Ready> waitUntil, TxnRequest<?> request)
    {
        this.waitUntil = waitUntil;
        this.request = request;
    }

    synchronized void add(SafeCommandStore safeStore, SafeCommand safeCommand, CommandStore commandStore)
    {
        if (isDone)
            throw new IllegalStateException("Recurrent retry of " + request);

        waitingOn.add(commandStore.id());
        ++waitingOnCount;
        safeCommand.addListener(this);
    }

    @Override
    public synchronized void onChange(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        Command command = safeCommand.current();
        Ready ready = waitUntil.apply(command);
        if (ready == No) return;

        safeCommand.removeListener(this);

        if (ready == Expired) return;

        int id = safeStore.commandStore().id();
        if (!waitingOn.contains(id))
            throw new IllegalStateException("Not waiting on CommandStore " + id);
        waitingOn.remove(id);

        ack();
    }

    synchronized void ack() {
        if (-1 == --waitingOnCount)
        {
            isDone = true;
            request.process();
        }
    }

    @Override
    public PreLoadContext listenerPreLoadContext(TxnId caller)
    {
        Invariants.checkState(caller.equals(request.txnId));
        return request;
    }

    @Override
    public boolean isTransient()
    {
        return true;
    }
}

