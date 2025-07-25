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

import java.util.Collection;
import javax.annotation.Nullable;

import accord.api.Result;
import accord.local.*;
import accord.local.Node.Id;
import accord.local.CommandSummaries.IsDep;
import accord.local.CommandSummaries.SummaryStatus;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.LatestDeps;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Status.Durability;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;
import accord.utils.async.Cancellable;

import static accord.local.CommandSummaries.ComputeIsDep.EITHER;
import static accord.local.CommandSummaries.SummaryStatus.NOT_DIRECTLY_WITNESSED;
import static accord.local.CommandSummaries.SummaryStatus.ACCEPTED;
import static accord.local.CommandSummaries.TestStartedAt.ANY;
import static accord.messages.BeginRecovery.RecoverReply.Kind.Ok;
import static accord.messages.BeginRecovery.RecoverReply.Kind.Reject;
import static accord.messages.BeginRecovery.RecoverReply.Kind.Retired;
import static accord.messages.BeginRecovery.RecoverReply.Kind.Truncated;
import static accord.messages.MessageType.StandardMessage.BEGIN_RECOVER_REQ;
import static accord.messages.MessageType.StandardMessage.BEGIN_RECOVER_RSP;
import static accord.primitives.Known.KnownDeps.DepsUnknown;
import static accord.primitives.Status.AcceptedMedium;
import static accord.primitives.Status.Phase;
import static accord.primitives.Status.PreAccepted;
import static accord.primitives.Timestamp.Flag.HLC_BOUND;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithDeps;

public class BeginRecovery extends TxnRequest.WithUnsynced<BeginRecovery.RecoverReply>
{
    public static class SerializationSupport
    {
        public static BeginRecovery create(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, PartialTxn partialTxn, Ballot ballot, @Nullable FullRoute<?> route, long executeAtOrTxnIdEpoch)
        {
            return new BeginRecovery(txnId, scope, waitForEpoch, minEpoch, partialTxn, ballot, route, executeAtOrTxnIdEpoch);
        }
    }

    public final PartialTxn partialTxn;
    public final Ballot ballot;
    public final FullRoute<?> route;
    public final long executeAtOrTxnIdEpoch;

    public BeginRecovery(Id to, Topologies topologies, TxnId txnId, @Nullable Timestamp executeAt, Txn txn, FullRoute<?> route, Ballot ballot)
    {
        super(to, topologies, txnId, route);
        this.partialTxn = txn.intersecting(scope, true);
        this.ballot = ballot;
        this.route = route;
        this.executeAtOrTxnIdEpoch = topologies.currentEpoch();
        Invariants.require(executeAt == null || executeAt.epoch() == topologies.currentEpoch());
    }

    private BeginRecovery(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, PartialTxn partialTxn, Ballot ballot, @Nullable FullRoute<?> route, long executeAtOrTxnIdEpoch)
    {
        super(txnId, scope, waitForEpoch, minEpoch);
        this.partialTxn = partialTxn;
        this.ballot = ballot;
        this.route = route;
        this.executeAtOrTxnIdEpoch = executeAtOrTxnIdEpoch;
    }

    @Override
    protected Cancellable submit()
    {
        return node.mapReduceConsumeLocal(this, minEpoch, executeAtOrTxnIdEpoch, this);
    }

    @Override
    public RecoverReply apply(SafeCommandStore safeStore)
    {
        StoreParticipants participants = StoreParticipants.update(safeStore, route, minEpoch, txnId, executeAtOrTxnIdEpoch);
        if (!safeStore.safeToCoordinate(txnId, participants.touches()))
            throw new CommandStore.TransactionLostException();

        SafeCommand safeCommand = safeStore.get(txnId, participants);
        Commands.AcceptOutcome outcome = Commands.recover(safeStore, safeCommand, participants, txnId, partialTxn, ballot);
        switch (outcome)
        {
            default:             throw UnhandledEnum.unknown(outcome);
            case Redundant:      throw UnhandledEnum.invalid(outcome);
            case Truncated:      return new RecoverNack(Truncated, null);
            case Retired:        return new RecoverNack(Retired, null);
            case RejectedBallot: return new RecoverNack(Reject, safeCommand.current().promised());
            case Success:
        }

        Command command = safeCommand.current();

        LatestDeps deps; {
            PartialDeps coordinatedDeps = command.partialDeps();
            Deps localDeps = null;
            if (!command.known().deps().hasCommittedOrDecidedDeps())
            {
                localDeps = DepsCalculator.calculateDeps(safeStore, txnId, participants, minEpoch, txnId, false);
            }
            if (localDeps != null && coordinatedDeps != null && !participants.touches().equals(coordinatedDeps.covering))
            {
                deps = LatestDeps.create(coordinatedDeps.covering, command.known().deps(), command.acceptedOrCommitted(), coordinatedDeps, null);
                deps = LatestDeps.merge(deps, LatestDeps.create(participants.touches(), DepsUnknown, Ballot.ZERO, null, localDeps));
            }
            else
            {
                Participants<?> knownFor = coordinatedDeps == null ? participants.touches() : coordinatedDeps.covering;
                deps = LatestDeps.create(knownFor, command.known().deps(), command.acceptedOrCommitted(), coordinatedDeps, localDeps);
            }
        }

        boolean supersedingRejects;
        Deps earlierNoWait, earlierWait;
        Deps laterCoordRejects;
        if (command.hasBeen(AcceptedMedium))
        {
            supersedingRejects = false;
            earlierNoWait = earlierWait = Deps.NONE;
            laterCoordRejects = Deps.NONE;
        }
        else
        {
            try (Visitor visitor = new Visitor())
            {
                safeStore.visit(participants.owns(), txnId, txnId.witnessedBy(), ANY, txnId, EITHER, visitor);
                supersedingRejects = visitor.supersedingRejects;
                earlierNoWait = visitor.earlierNoWait == null ? Deps.NONE : visitor.earlierNoWait.build();
                earlierWait = visitor.earlierWait == null ? Deps.NONE : visitor.earlierWait.build();
                laterCoordRejects = visitor.laterCoordRejects == null ? Deps.NONE : visitor.laterCoordRejects.build();
            }
        }

        SaveStatus saveStatus = command.saveStatus();
        Ballot accepted = command.acceptedOrCommitted();
        Timestamp executeAt = command.executeAt();
        Writes writes = command.writes();
        Result result = command.result();
        Participants<?> coordinatorAcceptsFastPath = saveStatus.known.hasPrivilegedVote() ? participants.owns() : null;
        boolean acceptsFastPath = acceptsFastPath(txnId, participants, saveStatus, executeAt);
        return new RecoverOk(txnId, saveStatus.status, accepted, executeAt, deps, earlierWait, earlierNoWait, laterCoordRejects, acceptsFastPath, coordinatorAcceptsFastPath, supersedingRejects, writes, result);
    }

    static boolean acceptsFastPath(TxnId txnId, StoreParticipants participants, SaveStatus saveStatus, @Nullable Timestamp executeAt)
    {
        return participants.owns().isEmpty() || (txnId.hasPrivilegedCoordinator() ? saveStatus.known.hasPrivilegedVote() : txnId.equals(executeAt));
    }

    @Override
    public RecoverReply reduce(RecoverReply r1, RecoverReply r2)
    {
        // TODO (low priority, efficiency): should not operate on dependencies directly here, as we only merge them;
        //                                  want a cheaply mergeable variant (or should collect them before merging)

        RecoverReply.Kind r1kind = r1.kind(), r2kind = r2.kind();
        if (r1kind != Ok || r2kind != Ok)
        {
            if (r1kind == Retired && r2kind == Ok) return r2;
            if (r2kind == Retired && r1kind == Ok) return r1;
            return r1kind.compareTo(r2kind) >= 0 ? r1 : r2;
        }
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
        Deps earlierNoWait = ok1.earlierNoWait.with(ok2.earlierNoWait);
        Deps earlierWait = ok1.earlierWait.with(ok2.earlierWait)
                                          .without(earlierNoWait);
        Deps laterNoVote = ok1.laterCoordRejects.with(ok2.laterCoordRejects);
        Timestamp timestamp = ok1.status == PreAccepted ? Timestamp.max(ok1.executeAt, ok2.executeAt) : ok1.executeAt;

        return new RecoverOk(
            txnId, ok1.status, ok1.accepted, timestamp,
            deps, earlierWait, earlierNoWait, laterNoVote,
            ok1.selfAcceptsFastPath & ok2.selfAcceptsFastPath,
                Participants.merge(ok1.coordinatorAcceptsFastPath, (Participants)ok2.coordinatorAcceptsFastPath),
                ok1.supersedingRejects | ok2.supersedingRejects,
            ok1.writes, ok1.result
        );
    }

    @Override
    public KeyHistory keyHistory()
    {
        return KeyHistory.RECOVER;
    }

    @Override
    public MessageType type()
    {
        return BEGIN_RECOVER_REQ;
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

    class Visitor implements CommandSummaries.AllCommandVisitor, AutoCloseable
    {
        Deps.Builder earlierWait, earlierNoWait;
        Deps.Builder laterCoordRejects;
        boolean supersedingRejects;

        @Override
        public boolean visit(Unseekable keyOrRange, TxnId testTxnId, Timestamp testExecuteAt, SummaryStatus status, IsDep dep, Durability minDurability)
        {
            if (status == NOT_DIRECTLY_WITNESSED || !txnId.witnessedBy(testTxnId))
                return true;

            int c = testTxnId.compareTo(txnId);
            if (c == 0)
                return true;

            if (c < 0)
            {
                if (testTxnId.is(ExclusiveSyncPoint) && testTxnId.hlc() > txnId.hlc() && txnId.is(Write))
                {
                    // TODO (required): define our invariants and make sure they're enforce elsewhere.
                    //   Specifically, consider whether truncation/GC can lead to erroneous answers here.
                    //   We're relying on a TxnId that sorts earlier but has a higher HLC to reject a higher
                    //   TxnId with lower HLC.
                    //  Note that right now we are requiring that any sync point is >= any prior syncpoint on
                    //  both HLC and epoch, which likely makes this safe. Let's confirm this works and
                    //  make sure this is properly enforced.
                    switch (status)
                    {
                        default: throw new UnhandledEnum(status);
                        case APPLIED:
                        case STABLE:
                        case COMMITTED:
                        case ACCEPTED:
                            if (testExecuteAt.is(HLC_BOUND))
                                return markSupersedingRejects();
                            if (status != ACCEPTED)
                                break;
                        case PREACCEPTED:
                            ensureEarlierWait().add(keyOrRange, testTxnId);
                        case NOTACCEPTED:
                        case INVALIDATED:
                    }
                    return true;
                }
                switch (dep)
                {
                    default: throw new UnhandledEnum(dep);
                    case IS_STABLE_DEP:
                        ensureEarlierNoWait().add(keyOrRange, testTxnId);
                        break;

                    case IS_NOT_STABLE_DEP:
                        /*
                         * The idea here is to discover those transactions that have been decided to execute after us
                         * and did not witness us as part of their pre-accept or accept round, as this means that we CANNOT have
                         * taken the fast path. This is central to safe recovery, as if every transaction that executes later has
                         * witnessed us we are safe to propose the pre-accept timestamp regardless, whereas if any transaction
                         * has not witnessed us we can safely invalidate.
                         */
                        return markSupersedingRejects();

                    case NOT_ELIGIBLE:
                        switch (status)
                        {
                            case INVALIDATED:
                                // TODO (desired): optionally exclude these and other normally-unnecessary entries on e.g. first recovery attempt
                                ensureEarlierNoWait().add(keyOrRange, testTxnId);
                                break;

                            case ACCEPTED:
                                if (testExecuteAt.compareTo(txnId) > 0)
                                    ensureEarlierWait().add(keyOrRange, testTxnId);
                                break;

                            case PREACCEPTED:
                            case NOTACCEPTED:
                                // no need to wait for potential medium path transactions started before us, only after
                                // however, both privileged coordinator optimisations require waiting for the earlier potential fast path to decide itself
                                // (that is, if either transaction use the optimisation, we must wait for the earlier transaction)
                                // TODO (desired): compute against shard whether this is a necessary wait condition - for many quorum configurations it isn't
                                if (testTxnId.hasPrivilegedCoordinator() || txnId.hasPrivilegedCoordinator())
                                    ensureEarlierWait().add(keyOrRange, testTxnId);
                        }
                }
            }
            else
            {
                switch (dep)
                {
                    case IS_NOT_STABLE_DEP:
                        /*
                         * The idea here is to discover those transactions that were started after us and have been Accepted
                         * and did not witness us as part of their pre-accept round, as this means that we CANNOT have taken
                         * the fast path. This is central to safe recovery, as if every transaction that executes later has
                         * witnessed us we are safe to propose the pre-accept timestamp regardless, whereas if any transaction
                         * has not witnessed us we can safely invalidate (us).
                         */
                        return markSupersedingRejects();

                    case NOT_ELIGIBLE:
                        // the command doesn't have any coordinator deps; or we are its coordinator and cannot commit on the privileged fast path
                    case IS_STABLE_DEP:
                        // the command has been committed with stable deps that witness us, so we're a durable dependency
                    case IS_COORD_DEP:
                        // the original coordinator witnessed us, so if it takes the fast or medium path we will be a durable dependency
                        // if it doesn't, it will take the slow path (and witness us), or be invalidated (in which case it doesn't matter)
                        break;

                    case IS_NOT_COORD_DEP:
                        Invariants.requireArgument(testTxnId.is(PrivilegedCoordinatorWithDeps));
                        ensureLaterCoordRejects().add(keyOrRange, testTxnId);
                }
            }

            return true;
        }

        private boolean markSupersedingRejects()
        {
            this.supersedingRejects = true;
            return false;
        }

        private Deps.Builder ensureEarlierNoWait()
        {
            if (earlierNoWait == null)
                earlierNoWait = new Deps.Builder(true);
            return earlierNoWait;
        }

        private Deps.Builder ensureEarlierWait()
        {
            if (earlierWait == null)
                earlierWait = new Deps.Builder(true);
            return earlierWait;
        }

        private Deps.Builder ensureLaterCoordRejects()
        {
            if (laterCoordRejects == null)
                laterCoordRejects = new Deps.Builder(true);
            return laterCoordRejects;
        }

        @Override
        public void close()
        {
            if (earlierNoWait != null)
            {
                earlierNoWait.close();
                earlierNoWait = null;
            }
            if (earlierWait != null)
            {
                earlierWait.close();
                earlierWait = null;
            }
            if (laterCoordRejects != null)
            {
                laterCoordRejects.close();
                laterCoordRejects = null;
            }
        }
    }

    public static abstract class RecoverReply implements Reply
    {
        // TODO (required): recover should gracefully handle partial truncation (currently expected to be handled by MaybeRecover)
        public enum Kind { Ok, Retired, Truncated, Reject }

        @Override
        public MessageType type()
        {
            return BEGIN_RECOVER_RSP;
        }

        public abstract Kind kind();
    }

    public static class RecoverOk extends RecoverReply
    {
        public final TxnId txnId; // for debugging
        public final Status status;
        public final Ballot accepted;
        public final Timestamp executeAt;
        public final LatestDeps deps;
        public final Deps earlierWait, earlierNoWait;
        public final Deps laterCoordRejects;
        public final boolean selfAcceptsFastPath;
        public final @Nullable Participants<?> coordinatorAcceptsFastPath;
        public final boolean supersedingRejects;
        public final Writes writes;
        public final Result result;

        public RecoverOk(TxnId txnId, Status status, Ballot accepted, Timestamp executeAt, LatestDeps deps,
                         Deps earlierWait, Deps earlierNoWait, Deps laterCoordRejects,
                         boolean selfAcceptsFastPath, Participants<?> coordinatorAcceptsFastPath, boolean supersedingRejects, Writes writes, Result result)
        {
            this.txnId = txnId;
            this.accepted = accepted;
            this.executeAt = executeAt;
            this.status = status;
            this.deps = deps;
            this.earlierWait = earlierWait;
            this.earlierNoWait = earlierNoWait;
            this.laterCoordRejects = laterCoordRejects;
            this.selfAcceptsFastPath = selfAcceptsFastPath;
            this.coordinatorAcceptsFastPath = coordinatorAcceptsFastPath;
            this.supersedingRejects = supersedingRejects;
            this.writes = writes;
            this.result = result;
        }

        @Override
        public Kind kind()
        {
            return Ok;
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
                   ", earlierWait:" + earlierWait +
                   ", earlierNoWait:" + earlierNoWait +
                   ", laterCoordRejects:" + laterCoordRejects +
                   ", selfAcceptsFastPath:" + selfAcceptsFastPath +
                   (txnId.hasPrivilegedCoordinator() ? ", coordinatorFastPath:" + selfAcceptsFastPath : "") +
                   ", supersedingRejects:" + supersedingRejects +
                   ", writes:" + writes +
                   ", result:" + result +
                   '}';
        }

        public static RecoverOk maxAccepted(Collection<RecoverOk> recoverOks)
        {
            return Status.max(recoverOks, r -> r.status, r -> r.accepted, r -> r != null && r.status.phase.compareTo(Phase.Accept) >= 0);
        }

        public static RecoverOk maxAcceptedNotTruncated(Collection<RecoverOk> recoverOks)
        {
            return Status.max(recoverOks, r -> r.status, r -> r.accepted, r -> r != null && r.status.phase.compareTo(Phase.Accept) >= 0 && r.status.phase.compareTo(Phase.Cleanup) < 0);
        }
    }

    public static class RecoverNack extends RecoverReply
    {
        public final Kind kind;
        public final @Nullable Ballot supersededBy;

        public RecoverNack(Kind kind, @Nullable Ballot supersededBy)
        {
            this.kind = kind;
            this.supersededBy = supersededBy;
        }

        @Override
        public Kind kind()
        {
            return kind;
        }

        @Override
        public String toString()
        {
            return "RecoverNack{" +
                   "supersededBy:" + supersededBy +
                   '}';
        }
    }
}
