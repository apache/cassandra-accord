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

import accord.api.Key;
import accord.local.*;
import accord.api.Result;
import accord.local.CommandsForKey.TxnIdWithExecuteAt;
import accord.local.Status.Phase;
import accord.primitives.*;
import accord.topology.Topologies;

import java.util.List;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import accord.local.Node.Id;

import java.util.Collections;

import static accord.local.CommandsForKey.CommandTimeseries.TestDep.WITH;
import static accord.local.CommandsForKey.CommandTimeseries.TestDep.WITHOUT;
import static accord.local.CommandsForKey.CommandTimeseries.TestKind.RorWs;
import static accord.local.CommandsForKey.CommandTimeseries.TestStatus.*;
import static accord.local.Status.*;
import static accord.messages.PreAccept.calculatePartialDeps;

public class BeginRecovery extends TxnRequest<BeginRecovery.RecoverReply>
{
    public static class SerializationSupport
    {
        public static BeginRecovery create(TxnId txnId, PartialRoute scope, long waitForEpoch, PartialTxn partialTxn, Ballot ballot, @Nullable Route route)
        {
            return new BeginRecovery(txnId, scope, waitForEpoch, partialTxn, ballot, route);
        }
    }

    public final PartialTxn partialTxn;
    public final Ballot ballot;
    public final @Nullable Route route;

    public BeginRecovery(Id to, Topologies topologies, TxnId txnId, Txn txn, Route route, Ballot ballot)
    {
        super(to, topologies, route, txnId);
        this.partialTxn = txn.slice(scope.covering, scope.contains(scope.homeKey));
        this.ballot = ballot;
        this.route = scope.contains(scope.homeKey) ? route : null;
    }

    private BeginRecovery(TxnId txnId, PartialRoute scope, long waitForEpoch, PartialTxn partialTxn, Ballot ballot, @Nullable Route route)
    {
        super(txnId, scope, waitForEpoch);
        this.partialTxn = partialTxn;
        this.ballot = ballot;
        this.route = route;
    }

    @Override
    protected void process()
    {
        node.mapReduceConsumeLocal(this, txnId.epoch, txnId.epoch, this);
    }

    @Override

    public RecoverReply apply(SafeCommandStore safeStore)
    {
        Command command = safeStore.command(txnId);

        switch (command.recover(safeStore, partialTxn, route != null ? route : scope, progressKey, ballot))
        {
            default:
                throw new IllegalStateException("Unhandled Outcome");

            case Redundant:
                throw new IllegalStateException("Invalid Outcome");

            case RejectedBallot:
                return new RecoverNack(command.promised());

            case Success:
        }

        PartialDeps deps = command.partialDeps();
        if (!command.known().deps.isProposalKnown())
        {
            deps = calculatePartialDeps(safeStore, txnId, partialTxn.keys(), partialTxn.kind(), txnId, safeStore.ranges().at(txnId.epoch));
        }

        boolean rejectsFastPath;
        Deps earlierCommittedWitness, earlierAcceptedNoWitness;

        if (command.hasBeen(PreCommitted))
        {
            rejectsFastPath = false;
            earlierCommittedWitness = earlierAcceptedNoWitness = Deps.NONE;
        }
        else
        {
            rejectsFastPath = acceptedStartedAfterWithoutWitnessing(safeStore, txnId, partialTxn.keys()).anyMatch(ignore -> true);
            if (!rejectsFastPath)
                rejectsFastPath = committedExecutesAfterAndDidNotWitness(safeStore, txnId, partialTxn.keys()).anyMatch(ignore -> true);

            // TODO: introduce some good unit tests for verifying these two functions in a real repair scenario
            // committed txns with an earlier txnid and have our txnid as a dependency
            earlierCommittedWitness = committedStartedBeforeAndDidWitness(safeStore, txnId, partialTxn.keys());

            // accepted txns with an earlier txnid that don't have our txnid as a dependency
            earlierAcceptedNoWitness = acceptedStartedBeforeAndDidNotWitness(safeStore, txnId, partialTxn.keys());
        }
        return new RecoverOk(txnId, command.status(), command.accepted(), command.executeAt(), deps, earlierCommittedWitness, earlierAcceptedNoWitness, rejectsFastPath, command.writes(), command.result());
    }

    @Override
    public RecoverReply reduce(RecoverReply r1, RecoverReply r2)
    {
        // TODO: should not operate on dependencies directly here, as we only merge them;
        //       should have a cheaply mergeable variant (or should collect them before merging)

        if (!r1.isOk()) return r1;
        if (!r2.isOk()) return r2;
        RecoverOk ok1 = (RecoverOk) r1;
        RecoverOk ok2 = (RecoverOk) r2;

        // set ok1 to the most recent of the two
        if (ok1.status.compareTo(ok2.status) < 0 || (ok1.status == ok2.status && ok1.accepted.compareTo(ok2.accepted) < 0))
        {
            RecoverOk tmp = ok1;
            ok1 = ok2;
            ok2 = tmp;
        }
        if (!ok1.status.hasBeen(PreAccepted)) throw new IllegalStateException();

        PartialDeps deps = ok1.deps.with(ok2.deps);
        Deps earlierCommittedWitness = ok1.earlierCommittedWitness.with(ok2.earlierCommittedWitness);
        Deps earlierAcceptedNoWitness = ok1.earlierAcceptedNoWitness.with(ok2.earlierAcceptedNoWitness)
                .without(earlierCommittedWitness::contains);
        Timestamp timestamp = ok1.status == PreAccepted ? Timestamp.max(ok1.executeAt, ok2.executeAt) : ok1.executeAt;

        return new RecoverOk(
                txnId, ok1.status, Ballot.max(ok1.accepted, ok2.accepted), timestamp, deps,
                earlierCommittedWitness, earlierAcceptedNoWitness,
                ok1.rejectsFastPath | ok2.rejectsFastPath,
                ok1.writes, ok1.result);
    }

    @Override
    public void accept(RecoverReply reply, Throwable failure)
    {
        node.reply(replyTo, replyContext, reply);
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Collections.singleton(txnId);
    }

    @Override
    public Iterable<Key> keys()
    {
        return partialTxn.keys();
    }

    @Override
    public MessageType type()
    {
        return MessageType.BEGIN_RECOVER_REQ;
    }

    @Override
    public String toString()
    {
        return "BeginRecovery{" +
               "txnId:" + txnId +
               ", txn:" + partialTxn +
               ", ballot:" + ballot +
               '}';
    }

    public interface RecoverReply extends Reply
    {
        @Override
        default MessageType type()
        {
            return MessageType.BEGIN_RECOVER_RSP;
        }

        boolean isOk();
    }

    public static class RecoverOk implements RecoverReply
    {
        public final TxnId txnId; // TODO for debugging?
        public final Status status;
        public final Ballot accepted;
        public final Timestamp executeAt;
        public final PartialDeps deps;
        public final Deps earlierCommittedWitness;  // counter-point to earlierAcceptedNoWitness
        public final Deps earlierAcceptedNoWitness; // wait for these to commit
        public final boolean rejectsFastPath;
        public final Writes writes;
        public final Result result;

        public RecoverOk(TxnId txnId, Status status, Ballot accepted, Timestamp executeAt, PartialDeps deps, Deps earlierCommittedWitness, Deps earlierAcceptedNoWitness, boolean rejectsFastPath, Writes writes, Result result)
        {
            Preconditions.checkNotNull(deps);
            this.txnId = txnId;
            this.accepted = accepted;
            this.executeAt = executeAt;
            this.status = status;
            this.deps = deps;
            this.earlierCommittedWitness = earlierCommittedWitness;
            this.earlierAcceptedNoWitness = earlierAcceptedNoWitness;
            this.rejectsFastPath = rejectsFastPath;
            this.writes = writes;
            this.result = result;
        }

        @Override
        public boolean isOk()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return toString("RecoverOk");
        }

        String toString(String kind)
        {
            return kind + "{" +
                   "txnId:" + txnId +
                   ", status:" + status +
                   ", accepted:" + accepted +
                   ", executeAt:" + executeAt +
                   ", deps:" + deps +
                   ", earlierCommittedWitness:" + earlierCommittedWitness +
                   ", earlierAcceptedNoWitness:" + earlierAcceptedNoWitness +
                   ", rejectsFastPath:" + rejectsFastPath +
                   ", writes:" + writes +
                   ", result:" + result +
                   '}';
        }

        public static RecoverOk maxAcceptedOrLater(List<RecoverOk> recoverOks)
        {
            return Status.max(recoverOks, r -> r.status, r -> r.accepted, r -> r.status.phase.compareTo(Phase.Accept) >= 0);
        }
    }

    public static class RecoverNack implements RecoverReply
    {
        public final Ballot supersededBy;
        public RecoverNack(Ballot supersededBy)
        {
            this.supersededBy = supersededBy;
        }

        @Override
        public boolean isOk()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "RecoverNack{" +
                   "supersededBy:" + supersededBy +
                   '}';
        }
    }

    private static Deps acceptedStartedBeforeAndDidNotWitness(SafeCommandStore commandStore, TxnId txnId, Keys keys)
    {
        try (Deps.OrderedBuilder builder = Deps.orderedBuilder(true);)
        {
            keys.forEach(key -> {
                CommandsForKey forKey = commandStore.maybeCommandsForKey(key);
                if (forKey == null)
                    return;

                // accepted txns with an earlier txnid that do not have our txnid as a dependency
                builder.nextKey(key);
                /**
                 * The idea here is to discover those transactions that have been Accepted without witnessing us
                 * and whom may not have adopted us as dependencies as responses to the Accept. Once we have
                 * reached a quorum for recovery any re-proposals will discover us. So we do not need to look
                 * at AcceptedInvalidate, as these will either be invalidated OR will witness us when they propose
                 * to Accept.
                 *
                 * Note that we treat PreCommitted as whatever status it held previously.
                 * Which is to say, we expect the previously proposed dependencies (if any) to be used to evaluate this
                 * condition.
                 */
                forKey.uncommitted().before(txnId, RorWs, WITHOUT, txnId, HAS_BEEN, Accepted).forEach((command) -> {
                    if (command.executeAt.compareTo(txnId) > 0)
                        builder.add(command.txnId);
                });
            });
            return builder.build();
        }
    }

    private static Deps committedStartedBeforeAndDidWitness(SafeCommandStore commandStore, TxnId txnId, Keys keys)
    {
        try (Deps.OrderedBuilder builder = Deps.orderedBuilder(true);)
        {
            keys.forEach(key -> {
                CommandsForKey forKey = commandStore.maybeCommandsForKey(key);
                if (forKey == null)
                    return;
                /**
                 * The idea here is to discover those transactions that have been Committed and DID witness us
                 * so that we can remove these from the set of acceptedStartedBeforeAndDidNotWitness
                 * on other nodes, to minimise the number of transactions we try to wait for on recovery
                 */
                builder.nextKey(key);
                forKey.committedById().before(txnId, RorWs, WITH, txnId, ANY_STATUS, null)
                        .forEach(builder::add);
            });
            return builder.build();
        }
    }

    private static Stream<TxnIdWithExecuteAt> acceptedStartedAfterWithoutWitnessing(SafeCommandStore commandStore, TxnId startedAfter, Keys keys)
    {
        return keys.stream().flatMap(key -> {
            CommandsForKey forKey = commandStore.maybeCommandsForKey(key);
            if (forKey == null)
                return Stream.of();

            /**
             * The idea here is to discover those transactions that were started after us and have been Accepted
             * and did not witness us as part of their pre-accept round, as this means that we CANNOT have taken
             * the fast path. This is central to safe recovery, as if every transaction that executes later has
             * witnessed us we are safe to propose the pre-accept timestamp regardless, whereas if any transaction
             * has not witnessed us we can safely invalidate it.
             */

            // AcceptedInvalidate, if successful, cannot take a dependency on us as it is a no-op; if another
            // Accepted is successful then its status here is irrelevant, as it will either be informed by other
            // replicas, or else will have to perform a new round that will adopt us as a dependency
            return forKey.uncommitted().after(startedAfter, RorWs, WITHOUT, startedAfter, HAS_BEEN, Accepted);
        });
    }

    private static Stream<TxnId> committedExecutesAfterAndDidNotWitness(SafeCommandStore commandStore, TxnId startedAfter, Keys keys)
    {
        return keys.stream().flatMap(key -> {
            CommandsForKey forKey = commandStore.maybeCommandsForKey(key);
            if (forKey == null)
                return Stream.of();

            /**
             * The idea here is to discover those transactions that have been decided to execute after us
             * and did not witness us as part of their pre-accept or accept round, as this means that we CANNOT have
             * taken the fast path. This is central to safe recovery, as if every transaction that executes later has
             * witnessed us we are safe to propose the pre-accept timestamp regardless, whereas if any transaction
             * has not witnessed us we can safely invalidate it.
             */

            // We declare HAS_BEEN Committed here only for clarity, as it is currently redundant, but if in future
            // we revisit the statuses we store it is worth making clear that we require the full decided dependencies
            // here in order for this evaluation to be valid.
            return forKey.committedByExecuteAt().after(startedAfter, RorWs, WITHOUT, startedAfter, HAS_BEEN, Committed);
        });
    }
}
