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

package accord.local;

import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

import accord.primitives.Deps;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import static accord.local.PreLoadContext.contextFor;
import static accord.local.Status.Known.Done;
import static accord.local.Status.PreCommitted;

/**
 * A mechanism for waiting for the local execution of a set of dependencies
 */
public class LocalBarrier implements Command.TransientListener, Commands.WaitingOnVisitor<Void>
{
    final Status waitForStatus;
    final Timestamp waitUntil;
    final Deps deps;
    Set<TxnId> waitingOn;
    final Consumer<? super SafeCommandStore> onCompletion;

    public static LocalBarrier register(SafeCommandStore safeStore, Status waitFor, Deps waitOn, Timestamp waitUntil, Timestamp executeAt, Consumer<? super SafeCommandStore> onCompletion)
    {
        LocalBarrier barrier = new LocalBarrier(waitFor, waitUntil, waitOn, onCompletion);
        boolean invoke;
        synchronized (barrier)
        {
            Commands.visitWaitingOn(safeStore, TxnId.NONE, waitUntil, waitOn, executeAt, barrier, null);
            invoke = barrier.waitingOn == null;
        }
        if (invoke)
            onCompletion.accept(safeStore);
        return barrier;
    }

    private LocalBarrier(Status waitForStatus, Timestamp waitUntil, Deps deps, Consumer<? super SafeCommandStore> onCompletion)
    {
        this.waitForStatus = waitForStatus;
        this.waitUntil = waitUntil;
        this.deps = deps;
        this.onCompletion = onCompletion;
    }

    @Override
    public synchronized void onChange(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        if (isStillWaiting(safeStore, safeCommand.current()))
            safeCommand.removeListener(this);
    }

    private boolean isStillWaiting(SafeCommandStore safeStore, Command command)
    {
        if (!(command.status().hasBeen(waitForStatus) || (command.status().hasBeen(PreCommitted) && command.executeAt().compareTo(waitUntil) > 0)))
            return false;

        if (waitingOn.remove(command.txnId()) && waitingOn.isEmpty())
            onCompletion.accept(safeStore);
        return true;
    }

    private synchronized void register(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        if (!isStillWaiting(safeStore, safeCommand.current()))
        {
            safeStore.progressLog().waiting(safeCommand.txnId(), Done, deps.someUnseekables(safeCommand.txnId()));
            safeCommand.addListener(this);
        }
    }

    @Override
    public PreLoadContext listenerPreLoadContext(TxnId caller)
    {
        return PreLoadContext.contextFor(caller);
    }

    @Override
    public void visit(SafeCommandStore safeStore, TxnId waitingId, Timestamp executeAt, TxnId dependencyId, Void param)
    {
        SafeCommand command = safeStore.ifLoaded(dependencyId);
        if (command == null)
        {
            if (waitingOn == null)
                waitingOn = new TreeSet<>();

            waitingOn.add(dependencyId);
            safeStore.commandStore()
                     .execute(contextFor(dependencyId), safeStore0 -> register(safeStore0, safeStore0.command(dependencyId)))
                     .begin(safeStore.agent());
        }
        else if (!command.current().hasBeen(waitForStatus))
        {
            if (waitingOn == null)
                waitingOn = new TreeSet<>();

            waitingOn.add(dependencyId);
        }
    }
}
