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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.local.Command;
import accord.local.DepsCalculator;
import accord.local.DepsCalculator.AbstractDepsReply;
import accord.local.LoadKeys;
import accord.local.FindKeys;
import accord.local.Node.Id;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.Known.KnownDeps;
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

public class GetLatestDeps extends RouteRequest.WithUnsynced<ReplyList<GetLatestDeps.GetLatestDepsReply>>
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
    protected boolean abort(Refuse.MinMax refuses)
    {
        return refuses.max != Refuse.NONE;
    }

    @Override
    public Cancellable submit()
    {
        return node.commandStores().mapReduceConsume(minEpoch, executeAt.epoch(), this);
    }

    @Override
    public ReplyList<GetLatestDepsReply> applyInternal(SafeCommandStore safeStore)
    {
        StoreParticipants participants = StoreParticipants.read(safeStore, scope, txnId, minEpoch, executeAt.epoch());
        SafeCommand safeCommand = safeStore.get(txnId, participants);
        Command command = safeCommand.current();
        if (ballot != null)
        {
            if (command.promised().compareTo(ballot) > 0)
                return GetLatestDepsReply.NACK;
            command = safeCommand.updatePromised(ballot);
        }

        PartialDeps coordinatedDeps = command.partialDeps();
        KnownDeps knownDeps = command.known().deps();
        Ballot acceptedOrCommitted = command.acceptedOrCommitted();
        if (knownDeps.hasCommittedOrDecidedDeps() || command.hasBeen(Status.Truncated))
        {
            LatestDeps deps = LatestDeps.create(participants.owns(), knownDeps, acceptedOrCommitted, coordinatedDeps, null);
            return new GetLatestDepsReply(deps);
        }

        //noinspection resource
        GetLatestDepsCalculator calculator = new GetLatestDepsCalculator(txnId, participants, knownDeps, acceptedOrCommitted, coordinatedDeps);
        return calculator.calculate(safeStore, minEpoch, false);
    }

    @Override
    public ReplyList<GetLatestDepsReply> reduce(ReplyList<GetLatestDepsReply> r1, ReplyList<GetLatestDepsReply> r2)
    {
        return ReplyList.merge(r1, r2);
    }

    @Override
    protected void acceptInternal(ReplyList<GetLatestDepsReply> replies, Throwable failure)
    {
        if (failure != null) acceptReply(null, failure);
        else ReplyList.invoke(replies, GetLatestDepsReply::reduce, this::acceptReply);
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
    public FindKeys findKeys()
    {
        return FindKeys.CONFLICTS;
    }

    static class GetLatestDepsCalculator extends DepsCalculator.DepsReplyCalculator<GetLatestDepsReply> implements Function<Void, GetLatestDepsReply>
    {
        final StoreParticipants participants;
        final KnownDeps known;
        final Ballot acceptedOrCommitted;
        final Deps coordinatedDeps;

        public GetLatestDepsCalculator(TxnId txnId, StoreParticipants participants, KnownDeps known, Ballot acceptedOrCommitted, Deps coordinatedDeps)
        {
            super(txnId, txnId, participants);
            this.participants = participants;
            this.known = known;
            this.acceptedOrCommitted = acceptedOrCommitted;
            this.coordinatedDeps = coordinatedDeps;
        }

        public GetLatestDepsReply apply(Void ignore)
        {
            try
            {
                Deps localDeps = deps();
                LatestDeps deps = LatestDeps.create(participants.owns(), known, acceptedOrCommitted, coordinatedDeps, localDeps);
                return new GetLatestDepsReply(deps);
            }
            finally
            {
                close();
            }
        }
    }

    public static class GetLatestDepsReply extends AbstractDepsReply<GetLatestDepsReply>
    {
        public static final GetLatestDepsReply NACK = new GetLatestDepsReply();
        public final LatestDeps deps;

        public GetLatestDepsReply(@Nonnull LatestDeps deps)
        {
            this.deps = Invariants.nonNull(deps);
        }

        private GetLatestDepsReply()
        {
            this.deps = null;
        }

        @Override
        public String toString()
        {
            return "GetLatestDepsReply{" + deps + '}' ;
        }

        @Override
        public MessageType type()
        {
            return GET_LATEST_DEPS_RSP;
        }

        public boolean isOk()
        {
            return deps != null;
        }

        private static GetLatestDepsReply reduce(GetLatestDepsReply r1, GetLatestDepsReply r2)
        {
            if (!r1.isOk()) return r1;
            if (!r2.isOk()) return r2;
            return new GetLatestDepsReply(LatestDeps.merge(r1.deps, r2.deps));
        }
    }
}
