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

import java.util.List;
import javax.annotation.Nullable;

import accord.api.Result;
import accord.local.*;
import accord.local.Node.Id;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.LatestDeps;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Topologies;

import static accord.local.SafeCommandStore.TestDep.WITH;
import static accord.local.SafeCommandStore.TestDep.WITHOUT;
import static accord.local.SafeCommandStore.TestStartedAt.ANY;
import static accord.local.SafeCommandStore.TestStatus.IS_STABLE;
import static accord.local.SafeCommandStore.TestStatus.IS_PROPOSED;
import static accord.local.SafeCommandStore.TestStartedAt.STARTED_AFTER;
import static accord.local.SafeCommandStore.TestStartedAt.STARTED_BEFORE;
import static accord.local.Status.Phase;
import static accord.local.Status.PreAccepted;
import static accord.local.Status.PreCommitted;
import static accord.messages.PreAccept.calculatePartialDeps;
import static accord.utils.Invariants.illegalState;

public class BeginRecovery extends TxnRequest<BeginRecovery.RecoverReply>
{
    public static class SerializationSupport
    {
        public static BeginRecovery create(TxnId txnId, Route<?> scope, long waitForEpoch, PartialTxn partialTxn, Ballot ballot, @Nullable FullRoute<?> route)
        {
            return new BeginRecovery(txnId, scope, waitForEpoch, partialTxn, ballot, route);
        }
    }

    public final PartialTxn partialTxn;
    public final Ballot ballot;
    public final FullRoute<?> route;

    public BeginRecovery(Id to, Topologies topologies, TxnId txnId, Txn txn, FullRoute<?> route, Ballot ballot)
    {
        super(to, topologies, route, txnId);
        // TODO (expected): only scope.contains(route.homeKey); this affects state eviction and is low priority given size in C*
        this.partialTxn = txn.intersecting(scope, true);
        this.ballot = ballot;
        this.route = route;
    }

    private BeginRecovery(TxnId txnId, Route<?> scope, long waitForEpoch, PartialTxn partialTxn, Ballot ballot, @Nullable FullRoute<?> route)
    {
        super(txnId, scope, waitForEpoch);
        this.partialTxn = partialTxn;
        this.ballot = ballot;
        this.route = route;
    }

    @Override
    protected void process()
    {
        node.mapReduceConsumeLocal(this, txnId.epoch(), txnId.epoch(), this);
    }

    @Override

    public RecoverReply apply(SafeCommandStore safeStore)
    {
        SafeCommand safeCommand = safeStore.get(txnId, txnId, route);
        switch (Commands.recover(safeStore, safeCommand, txnId, partialTxn, route, ballot))
        {
            default:
                throw illegalState("Unhandled Outcome");

            case Redundant:
            case Truncated:
                return new RecoverNack(null);

            case RejectedBallot:
                return new RecoverNack(safeCommand.current().promised());

            case Success:
        }

        Command command = safeCommand.current();
        PartialDeps coordinatedDeps = command.partialDeps();
        PartialDeps localDeps = null;
        if (!command.known().deps.hasCommittedOrDecidedDeps())
        {
            // TODO (required): consider whether we are safe ignoring the concept of minUnsyncedEpoch here
            localDeps = calculatePartialDeps(safeStore, txnId, partialTxn.keys(), scope, txnId, txnId, safeStore.ranges().coordinates(txnId));
        }

        LatestDeps deps = LatestDeps.create(safeStore.ranges().allAt(txnId.epoch()), command.known().deps, command.acceptedOrCommitted(), coordinatedDeps, localDeps);

        boolean rejectsFastPath;
        Deps earlierCommittedWitness, earlierAcceptedNoWitness;

        if (command.hasBeen(PreCommitted))
        {
            rejectsFastPath = false;
            earlierCommittedWitness = earlierAcceptedNoWitness = Deps.NONE;
        }
        else
        {
            // TODO (expected): modify the mapReduce API to perform this check in a single pass
            Ranges ranges = safeStore.ranges().allAt(txnId);
            rejectsFastPath = hasAcceptedOrCommittedStartedAfterWithoutWitnessing(safeStore, txnId, ranges, partialTxn.keys());
            if (!rejectsFastPath)
                rejectsFastPath = hasStableExecutesAfterWithoutWitnessing(safeStore, txnId, ranges, partialTxn.keys());

            // TODO (expected, testing): introduce some good unit tests for verifying these two functions in a real repair scenario
            // committed txns with an earlier txnid and have our txnid as a dependency
            earlierCommittedWitness = stableStartedBeforeAndWitnessed(safeStore, txnId, ranges, partialTxn.keys());

            // accepted txns with an earlier txnid that don't have our txnid as a dependency
            earlierAcceptedNoWitness = acceptedOrCommittedStartedBeforeWithoutWitnessing(safeStore, txnId, ranges, partialTxn.keys());
        }

        Status status = command.status();
        Ballot accepted = command.acceptedOrCommitted();
        Timestamp executeAt = command.executeAt();
        Writes writes = command.writes();
        Result result = command.result();
        return new RecoverOk(txnId, status, accepted, executeAt, deps, earlierCommittedWitness, earlierAcceptedNoWitness, rejectsFastPath, writes, result);
    }

    @Override
    public RecoverReply reduce(RecoverReply r1, RecoverReply r2)
    {
        // TODO (low priority, efficiency): should not operate on dependencies directly here, as we only merge them;
        //                                  want a cheaply mergeable variant (or should collect them before merging)

        if (!r1.isOk()) return r1;
        if (!r2.isOk()) return r2;
        RecoverOk ok1 = (RecoverOk) r1;
        RecoverOk ok2 = (RecoverOk) r2;

        // set ok1 to the most recent of the two
        if (ok1 != Status.max(ok1, ok1.status, ok1.accepted, ok2, ok2.status, ok2.accepted))
        {
            RecoverOk tmp = ok1;
            ok1 = ok2;
            ok2 = tmp;
        }
        if (!ok1.status.hasBeen(PreAccepted)) throw new IllegalStateException();

        LatestDeps deps = LatestDeps.merge(ok1.deps, ok2.deps);
        Deps earlierCommittedWitness = ok1.earlierCommittedWitness.with(ok2.earlierCommittedWitness);
        Deps earlierAcceptedNoWitness = ok1.earlierAcceptedNoWitness.with(ok2.earlierAcceptedNoWitness)
                .without(earlierCommittedWitness::contains);
        Timestamp timestamp = ok1.status == PreAccepted ? Timestamp.max(ok1.executeAt, ok2.executeAt) : ok1.executeAt;

        return new RecoverOk(
            txnId, ok1.status, ok1.accepted, timestamp,
            deps, earlierCommittedWitness, earlierAcceptedNoWitness,
            ok1.rejectsFastPath | ok2.rejectsFastPath,
            ok1.writes, ok1.result
        );
    }

    @Override
    public void accept(RecoverReply reply, Throwable failure)
    {
        node.reply(replyTo, replyContext, reply, failure);
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public Seekables<?, ?> keys()
    {
        return partialTxn.keys();
    }

    @Override
    public KeyHistory keyHistory()
    {
        return KeyHistory.COMMANDS;
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

    public static abstract class RecoverReply implements Reply
    {
        @Override
        public MessageType type()
        {
            return MessageType.BEGIN_RECOVER_RSP;
        }

        public abstract boolean isOk();
    }

    public static class RecoverOk extends RecoverReply
    {
        public final TxnId txnId; // for debugging
        public final Status status;
        public final Ballot accepted;
        public final Timestamp executeAt;
        public final LatestDeps deps;
        public final Deps earlierCommittedWitness;  // counter-point to earlierAcceptedNoWitness
        public final Deps earlierAcceptedNoWitness; // wait for these to commit
        public final boolean rejectsFastPath;
        public final Writes writes;
        public final Result result;

        public RecoverOk(TxnId txnId, Status status, Ballot accepted, Timestamp executeAt, LatestDeps deps, Deps earlierCommittedWitness, Deps earlierAcceptedNoWitness, boolean rejectsFastPath, Writes writes, Result result)
        {
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

        public static RecoverOk maxAccepted(List<RecoverOk> recoverOks)
        {
            return Status.max(recoverOks, r -> r.status, r -> r.accepted, r -> r != null && r.status.phase.compareTo(Phase.Accept) >= 0);
        }

        public static RecoverOk maxAcceptedNotTruncated(List<RecoverOk> recoverOks)
        {
            return Status.max(recoverOks, r -> r.status, r -> r.accepted, r -> r != null && r.status.phase.compareTo(Phase.Accept) >= 0 && r.status.phase.compareTo(Phase.Cleanup) < 0);
        }
    }

    public static class RecoverNack extends RecoverReply
    {
        public final @Nullable Ballot supersededBy;
        public RecoverNack(@Nullable Ballot supersededBy)
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

    private static Deps acceptedOrCommittedStartedBeforeWithoutWitnessing(SafeCommandStore safeStore, TxnId startedBefore, Ranges ranges, Seekables<?, ?> keys)
    {
        try (Deps.Builder builder = Deps.builder())
        {
            // any transaction that started
            safeStore.mapReduceFull(keys, ranges, startedBefore, startedBefore.kind().witnessedBy(), STARTED_BEFORE, WITHOUT, IS_PROPOSED,
                                    (startedBefore0, keyOrRange, txnId, executeAt, prev) -> {
                        if (executeAt.compareTo(startedBefore0) > 0)
                            builder.add(keyOrRange, txnId);
                        return builder;
                    }, startedBefore, builder);
            return builder.build();
        }
    }

    private static Deps stableStartedBeforeAndWitnessed(SafeCommandStore safeStore, TxnId startedBefore, Ranges ranges, Seekables<?, ?> keys)
    {
        try (Deps.Builder builder = Deps.builder())
        {
            safeStore.mapReduceFull(keys, ranges, startedBefore, startedBefore.kind().witnessedBy(), STARTED_BEFORE, WITH, IS_STABLE,
                                    (p1, keyOrRange, txnId, executeAt, prev) -> builder.add(keyOrRange, txnId), null, (Deps.AbstractBuilder<Deps>)builder);
            return builder.build();
        }
    }

    private static boolean hasAcceptedOrCommittedStartedAfterWithoutWitnessing(SafeCommandStore safeStore, TxnId startedAfter, Ranges ranges, Seekables<?, ?> keys)
    {
        /*
         * The idea here is to discover those transactions that were started after us and have been Accepted
         * and did not witness us as part of their pre-accept round, as this means that we CANNOT have taken
         * the fast path. This is central to safe recovery, as if every transaction that executes later has
         * witnessed us we are safe to propose the pre-accept timestamp regardless, whereas if any transaction
         * has not witnessed us we can safely invalidate (us).
         *
         * TODO (required): consider carefully how _adding_ ranges to a CommandStore affects this
         */
        return safeStore.mapReduceFull(keys, ranges, startedAfter, startedAfter.kind().witnessedBy(), STARTED_AFTER, WITHOUT, IS_PROPOSED,
                                       (p1, keyOrRange, txnId, executeAt, prev) -> true, null, false);
    }

    private static boolean hasStableExecutesAfterWithoutWitnessing(SafeCommandStore safeStore, TxnId executesAfter, Ranges ranges, Seekables<?, ?> keys)
    {
        /*
         * The idea here is to discover those transactions that have been decided to execute after us
         * and did not witness us as part of their pre-accept or accept round, as this means that we CANNOT have
         * taken the fast path. This is central to safe recovery, as if every transaction that executes later has
         * witnessed us we are safe to propose the pre-accept timestamp regardless, whereas if any transaction
         * has not witnessed us we can safely invalidate it.
         */
        return safeStore.mapReduceFull(keys, ranges, executesAfter, executesAfter.kind().witnessedBy(), ANY, WITHOUT, IS_STABLE,
                                       (p1, keyOrRange, txnId, executeAt, prev) -> true, null, false);
    }
}
