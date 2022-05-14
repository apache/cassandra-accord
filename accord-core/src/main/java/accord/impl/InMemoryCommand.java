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

import accord.api.Result;
import accord.api.RoutingKey;
import accord.local.*;
import accord.local.Status.Durability;
import accord.local.Status.Known;
import accord.primitives.*;

import java.util.*;

import static accord.local.Status.Durability.Local;
import static accord.local.Status.Durability.NotDurable;

public class InMemoryCommand extends Command
{
    public final CommandStore commandStore;
    private final TxnId txnId;

    private AbstractRoute route;
    private RoutingKey homeKey, progressKey;
    private PartialTxn partialTxn;
    private Ballot promised = Ballot.ZERO, accepted = Ballot.ZERO;
    private Timestamp executeAt;
    private PartialDeps partialDeps = PartialDeps.NONE;
    private Writes writes;
    private Result result;

    private SaveStatus status = SaveStatus.NotWitnessed;

    private Durability durability = NotDurable; // only set on home shard

    private NavigableSet<TxnId> waitingOnCommit;
    private NavigableMap<Timestamp, TxnId> waitingOnApply;

    private final Listeners listeners = new Listeners();

    public InMemoryCommand(CommandStore commandStore, TxnId txnId)
    {
        this.commandStore = commandStore;
        this.txnId = txnId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InMemoryCommand command = (InMemoryCommand) o;
        return commandStore == command.commandStore
                && txnId.equals(command.txnId)
                && Objects.equals(homeKey, command.homeKey)
                && Objects.equals(progressKey, command.progressKey)
                && Objects.equals(partialTxn, command.partialTxn)
                && promised.equals(command.promised)
                && accepted.equals(command.accepted)
                && Objects.equals(executeAt, command.executeAt)
                && partialDeps.equals(command.partialDeps)
                && Objects.equals(writes, command.writes)
                && Objects.equals(result, command.result)
                && status == command.status
                && durability == command.durability
                && Objects.equals(waitingOnCommit, command.waitingOnCommit)
                && Objects.equals(waitingOnApply, command.waitingOnApply)
                && Objects.equals(listeners, command.listeners);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(commandStore, txnId, partialTxn, promised, accepted, executeAt, partialDeps, writes, result, status, waitingOnCommit, waitingOnApply, listeners);
    }

    @Override
    public TxnId txnId()
    {
        return txnId;
    }

    @Override
    public RoutingKey homeKey()
    {
        return homeKey;
    }

    @Override
    public Txn.Kind kind()
    {
        // TODO (now): pack this into TxnId
        return partialTxn.kind();
    }

    @Override
    protected void setHomeKey(RoutingKey key)
    {
        this.homeKey = key;
    }

    @Override
    public RoutingKey progressKey()
    {
        return progressKey;
    }

    @Override
    protected void setProgressKey(RoutingKey key)
    {
        this.progressKey = key;
    }

    @Override
    public AbstractRoute route()
    {
        return route;
    }

    @Override
    protected void setRoute(AbstractRoute route)
    {
        this.route = route;
    }

    @Override
    public PartialTxn partialTxn()
    {
        return partialTxn;
    }

    @Override
    protected void setPartialTxn(PartialTxn txn)
    {
        this.partialTxn = txn;
    }

    @Override
    public Ballot promised()
    {
        return promised;
    }

    @Override
    public void setPromised(Ballot ballot)
    {
        this.promised = ballot;
    }

    @Override
    public Ballot accepted()
    {
        return accepted;
    }

    @Override
    public void setAccepted(Ballot ballot)
    {
        this.accepted = ballot;
    }

    @Override
    public Timestamp executeAt()
    {
        return executeAt;
    }

    @Override
    public void setExecuteAt(Timestamp timestamp)
    {
        this.executeAt = timestamp;
    }

    @Override
    public PartialDeps partialDeps()
    {
        return partialDeps;
    }

    @Override
    public void setPartialDeps(PartialDeps deps)
    {
        this.partialDeps = deps;
    }

    @Override
    public Writes writes()
    {
        return writes;
    }

    @Override
    public void setWrites(Writes writes)
    {
        this.writes = writes;
    }

    @Override
    public Result result()
    {
        return result;
    }

    @Override
    public void setResult(Result result)
    {
        this.result = result;
    }

    @Override
    public SaveStatus saveStatus()
    {
        return status;
    }

    @Override
    public void setSaveStatus(SaveStatus status)
    {
        this.status = status;
    }

    @Override
    public Known known()
    {
        return status.known;
    }

    @Override
    public Durability durability()
    {
        if (status.compareTo(SaveStatus.PreApplied) >= 0 && durability == NotDurable)
            return Local; // not necessary anywhere, but helps for logical consistency
        return durability;
    }

    @Override
    public void setDurability(Durability v)
    {
        durability = v;
    }

    @Override
    public Command addListener(CommandListener listener)
    {
        listeners.add(listener);
        return this;
    }

    @Override
    public void removeListener(CommandListener listener)
    {
        listeners.remove(listener);
    }

    @Override
    public void notifyListeners(SafeCommandStore safeStore)
    {
        listeners.forEach(this, safeStore);
    }

    @Override
    public void addWaitingOnCommit(TxnId txnId)
    {
        if (waitingOnCommit == null)
            waitingOnCommit = new TreeSet<>();

        waitingOnCommit.add(txnId);
    }

    @Override
    public boolean isWaitingOnCommit()
    {
        return waitingOnCommit != null && !waitingOnCommit.isEmpty();
    }

    @Override
    public void removeWaitingOnCommit(TxnId txnId)
    {
        if (waitingOnCommit == null)
            return;
        waitingOnCommit.remove(txnId);
    }

    @Override
    public TxnId firstWaitingOnCommit()
    {
        return isWaitingOnCommit() ? waitingOnCommit.first() : null;
    }

    @Override
    public void addWaitingOnApplyIfAbsent(TxnId txnId, Timestamp executeAt)
    {
        if (waitingOnApply == null)
            waitingOnApply = new TreeMap<>();

        waitingOnApply.put(executeAt, txnId);
    }

    @Override
    public boolean isWaitingOnApply()
    {
        return waitingOnApply != null && !waitingOnApply.isEmpty();
    }

    @Override
    public void removeWaitingOn(TxnId txnId, Timestamp executeAt)
    {
        if (waitingOnCommit != null)
            waitingOnCommit.remove(txnId);

        if (waitingOnApply != null)
            waitingOnApply.remove(executeAt);
    }

    @Override
    public TxnId firstWaitingOnApply()
    {
        return isWaitingOnApply() ? waitingOnApply.firstEntry().getValue() : null;
    }
}
