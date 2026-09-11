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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.local.Command;
import accord.local.DepsCalculator;
import accord.local.LoadKeys;
import accord.local.LoadKeysFor;
import accord.local.Node.Id;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.LatestDeps;
import accord.primitives.PartialDeps;
import accord.primitives.Route;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.async.Cancellable;

import static accord.messages.MessageType.StandardMessage.GET_LATEST_DEPS_REQ;
import static accord.messages.MessageType.StandardMessage.GET_LATEST_DEPS_RSP;

public class GetLatestDeps extends RouteRequest.WithUnsynced<GetLatestDeps.GetLatestDepsReply>
{
    public static final class SerializationSupport
    {
        public static GetLatestDeps create(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, @Nullable Ballot ballot, Timestamp executeAt)
        {
            return new GetLatestDeps(txnId, scope, waitForEpoch, minEpoch, ballot, executeAt);
        }
    }

    public final Ballot ballot;
    public final Timestamp executeAt;

    public GetLatestDeps(Id to, Topologies topologies, Route<?> route, TxnId txnId, Ballot ballot, Timestamp executeAt)
    {
        super(to, topologies, txnId, route);
        this.ballot = ballot;
        this.executeAt = executeAt;
    }

    protected GetLatestDeps(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, Ballot ballot, Timestamp executeAt)
    {
        super(txnId, scope, waitForEpoch, minEpoch);
        this.ballot = ballot;
        this.executeAt = executeAt;
    }

    @Override
    public Cancellable submit()
    {
        return node.commandStores().mapReduceConsume(minEpoch, executeAt.epoch(), this);
    }

    @Override
    public GetLatestDepsReply applyInternal(SafeCommandStore safeStore)
    {
        StoreParticipants participants = StoreParticipants.read(safeStore, scope, txnId, minEpoch, executeAt.epoch());
        SafeCommand safeCommand = safeStore.get(txnId, participants);
        Command command = safeCommand.current();
        if (ballot != null)
        {
            if (command.promised().compareTo(ballot) > 0)
                return GetLatestDepsNack.INSTANCE;
            command = safeCommand.updatePromised(ballot);
        }
        PartialDeps coordinatedDeps = command.partialDeps();
        Deps localDeps = null;
        if (!command.known().deps().hasCommittedOrDecidedDeps() && !command.hasBeen(Status.Truncated))
        {
            localDeps = DepsCalculator.calculateDeps(safeStore, txnId, participants, minEpoch, txnId, false);
        }

        LatestDeps deps = LatestDeps.create(participants.owns(), command.known().deps(), command.acceptedOrCommitted(), coordinatedDeps, localDeps);
        return new GetLatestDepsOk(deps);
    }

    @Override
    public GetLatestDepsReply reduce(GetLatestDepsReply r1, GetLatestDepsReply r2)
    {
        if (!r1.isOk()) return r1;
        if (!r2.isOk()) return r2;
        return new GetLatestDepsOk(LatestDeps.merge(((GetLatestDepsOk)r1).deps, ((GetLatestDepsOk)r2).deps));
    }

    @Override
    public MessageType type()
    {
        return GET_LATEST_DEPS_REQ;
    }

    @Override
    public String toString()
    {
        return "CalculateDeps{" +
               "txnId:" + txnId +
               ", scope:" + scope +
               ", executeAt:" + executeAt +
               '}';
    }

    @Override
    public LoadKeys loadKeys()
    {
        return LoadKeys.SYNC;
    }

    @Override
    public LoadKeysFor loadKeysFor()
    {
        return LoadKeysFor.READ_WRITE;
    }

    @Override
    protected boolean abort(Refuse.MinMax refuses)
    {
        return refuses.max != Refuse.NONE;
    }

    public interface GetLatestDepsReply extends Reply
    {
        boolean isOk();
    }

    public static final class GetLatestDepsNack implements GetLatestDepsReply
    {
        public static final GetLatestDepsNack INSTANCE = new GetLatestDepsNack();
        private GetLatestDepsNack(){}

        @Override
        public boolean isOk()
        {
            return false;
        }

        @Override
        public MessageType type()
        {
            return GET_LATEST_DEPS_RSP;
        }
    }

    public static class GetLatestDepsOk implements GetLatestDepsReply
    {
        public final LatestDeps deps;

        public GetLatestDepsOk(@Nonnull LatestDeps deps)
        {
            this.deps = Invariants.nonNull(deps);
        }

        @Override
        public String toString()
        {
            return "GetLatestDepsOk{" + deps + '}' ;
        }

        @Override
        public MessageType type()
        {
            return GET_LATEST_DEPS_RSP;
        }

        @Override
        public boolean isOk()
        {
            return true;
        }
    }

}
