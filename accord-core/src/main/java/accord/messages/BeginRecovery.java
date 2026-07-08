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
import accord.utils.TinyEnumSet;
import accord.utils.UnhandledEnum;
import accord.utils.async.Cancellable;

import static accord.local.CommandSummaries.SummaryStatus.APPLIED;
import static accord.local.CommandSummaries.SummaryStatus.NOT_DIRECTLY_WITNESSED;
import static accord.local.CommandSummaries.SummaryStatus.ACCEPTED;
import static accord.local.CommandSummaries.SummaryStatus.STABLE;
import static accord.messages.BeginRecovery.RecoverReply.Kind.Ok;
import static accord.messages.BeginRecovery.RecoverReply.Kind.Reject;
import static accord.messages.BeginRecovery.RecoverReply.Kind.Retired;
import static accord.messages.BeginRecovery.RecoverReply.Kind.Truncated;
import static accord.messages.BeginRecovery.RecoveryFlags.forceRecoverFastPath;
import static accord.messages.BeginRecovery.RecoveryFlags.isFastPathDecided;
import static accord.messages.MessageType.StandardMessage.BEGIN_RECOVER_REQ;
import static accord.messages.MessageType.StandardMessage.BEGIN_RECOVER_RSP;
import static accord.primitives.Known.KnownDeps.DepsUnknown;
import static accord.primitives.Status.AcceptedMedium;
import static accord.primitives.Status.Phase;
import static accord.primitives.Status.PreAccepted;
import static accord.primitives.Timestamp.Flag.HLC_BOUND;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithDeps;

public class BeginRecovery extends RouteRequest.WithUnsynced<BeginRecovery.RecoverReply>
{
    public static class SerializationSupport
    {
        public static BeginRecovery create(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, PartialTxn partialTxn, Ballot ballot, @Nullable FullRoute<?> route, long executeAtOrTxnIdEpoch, int flags)
        {
            return new BeginRecovery(txnId, scope, waitForEpoch, minEpoch, partialTxn, ballot, route, executeAtOrTxnIdEpoch, flags);
        }
    }

    public enum RecoveryFlags
    {
        FAST_PATH_DECIDED,
        FORCE_RECOVER_FAST_PATH,
        NO_CALCULATE_DEPS;

        public static boolean isFastPathDecided(int encoded)
        {
            return TinyEnumSet.contains(encoded, FAST_PATH_DECIDED);
        }

        public static boolean forceRecoverFastPath(int encoded)
        {
            return TinyEnumSet.contains(encoded, FORCE_RECOVER_FAST_PATH);
        }

        public static boolean calculateDeps(int encoded)
        {
            return !TinyEnumSet.contains(encoded, NO_CALCULATE_DEPS);
        }
    }

    public final PartialTxn partialTxn;
    public final Ballot ballot;
    public final FullRoute<?> route;
    public final long executeAtOrTxnIdEpoch;
    public final int flags;

    public BeginRecovery(Id to, Topologies topologies, TxnId txnId, @Nullable Timestamp committedExecuteAt, int flags, Txn txn, FullRoute<?> route, Ballot ballot)
    {
        super(to, topologies, txnId, route);
        this.partialTxn = txn.intersecting(scope, true);
        this.ballot = ballot;
        this.route = route;
        this.executeAtOrTxnIdEpoch = topologies.currentEpoch();
        this.flags = flags;
        Invariants.require(committedExecuteAt == null || committedExecuteAt.epoch() == topologies.currentEpoch());
    }

    private BeginRecovery(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, PartialTxn partialTxn, Ballot ballot, @Nullable FullRoute<?> route, long executeAtOrTxnIdEpoch, int flags)
    {
        super(txnId, scope, waitForEpoch, minEpoch);
        this.partialTxn = partialTxn;
        this.ballot = ballot;
        this.route = route;
        this.executeAtOrTxnIdEpoch = executeAtOrTxnIdEpoch;
        this.flags = flags;
    }

    @Override
    protected Cancellable submit()
    {
        return node.commandStores().mapReduceConsume(minEpoch, executeAtOrTxnIdEpoch, this);
    }

    @Override
    public RecoverReply applyInternal(SafeCommandStore safeStore)
    {
        StoreParticipants participants = StoreParticipants.update(safeStore, route, minEpoch, txnId, executeAtOrTxnIdEpoch);
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
            if (!command.known().deps().hasCommittedOrDecidedDeps() && calculateDeps())
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
        Deps simpleNoWait, simpleWait;
        Deps laterCoordRejects;
        if (command.hasBeen(AcceptedMedium) || !recoverFastPath())
        {
            supersedingRejects = true;
            simpleNoWait = simpleWait = Deps.NONE;
            laterCoordRejects = Deps.NONE;
        }
        else
        {
            try (Visitor visitor = new Visitor())
            {
                safeStore.visit(participants.owns(), txnId, txnId.witnessedBy(), visitor);
                supersedingRejects = visitor.supersedingRejects;
                simpleNoWait = visitor.simpleNoWait == null ? Deps.NONE : visitor.simpleNoWait.build();
                simpleWait = visitor.simpleWait == null ? Deps.NONE : visitor.simpleWait.build();
                laterCoordRejects = visitor.supersedingCoordRejects == null ? Deps.NONE : visitor.supersedingCoordRejects.build();
            }
        }

        SaveStatus saveStatus = command.saveStatus();
        Ballot accepted = command.acceptedOrCommitted();
        Timestamp executeAt = command.executeAt();
        Writes writes = command.writes();
        Result result = command.result();
        Participants<?> coordinatorAcceptsFastPath = saveStatus.known.hasPrivilegedVote() ? participants.owns() : null;
        boolean acceptsFastPath = acceptsFastPath(txnId, participants, saveStatus, executeAt);
        return new RecoverOk(txnId, saveStatus.status, accepted, executeAt, deps, simpleWait, simpleNoWait, laterCoordRejects, acceptsFastPath, coordinatorAcceptsFastPath, supersedingRejects, writes, result);
    }

    private boolean recoverFastPath()
    {
        if (forceRecoverFastPath(flags))
            return true;
        return txnId.hasFastPath() && !isFastPathDecided(flags);
    }

    private boolean calculateDeps()
    {
        return RecoveryFlags.calculateDeps(flags);
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
        Deps earlierNoWait = ok1.simpleNoWait.with(ok2.simpleNoWait);
        Deps earlierWait = ok1.simpleWait.with(ok2.simpleWait)
                                         .without(earlierNoWait);
        Deps laterNoVote = ok1.supersedingCoordRejects.with(ok2.supersedingCoordRejects);
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
    public LoadKeys loadKeys()
    {
        return LoadKeys.SYNC;
    }

    @Override
    public ExecutionKind executionKind()
    {
        return ExecutionKind.PREACCEPT;
    }

    @Override
    public LoadKeysFor loadKeysFor()
    {
        if (recoverFastPath())
            return LoadKeysFor.RECOVERY;
        if (calculateDeps())
            return LoadKeysFor.READ_WRITE;
        return LoadKeysFor.WRITE;
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

    class Visitor implements CommandSummaries.SupersedingCommandVisitor, AutoCloseable
    {
        Deps.Builder simpleWait, simpleNoWait;
        Deps.Builder supersedingCoordRejects;
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
                if (testTxnId.isSyncPoint() && testTxnId.hlc() > txnId.hlc() && txnId.is(Write))
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
                            ensureSimpleWait().add(keyOrRange, testTxnId);
                        case NOTACCEPTED:
                        case INVALIDATED:
                    }
                    return true;
                }
                switch (dep)
                {
                    default: throw new UnhandledEnum(dep);
                    case IS_PROPOSED_OR_STABLE_DEP:
                        if (status == STABLE || status == APPLIED)
                            ensureSimpleNoWait().add(keyOrRange, testTxnId);
                        break;

                    case IS_NOT_PROPOSED_OR_STABLE_DEP:
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
                                ensureSimpleNoWait().add(keyOrRange, testTxnId);
                                break;

                            case ACCEPTED:
                                if (testExecuteAt.compareTo(txnId) > 0)
                                    ensureSimpleWait().add(keyOrRange, testTxnId);
                                break;

                            case PREACCEPTED:
                            case NOTACCEPTED:
                                // no need to wait for potential medium path transactions started before us, only after
                                // however, both privileged coordinator optimisations require waiting for the earlier potential fast path to decide itself
                                // (that is, if either transaction use the optimisation, we must wait for the earlier transaction)
                                // TODO (desired): compute against shard whether this is a necessary wait condition - for many quorum configurations it isn't
                                if (testTxnId.hasPrivilegedCoordinator() || txnId.hasPrivilegedCoordinator())
                                    ensureSimpleWait().add(keyOrRange, testTxnId);
                        }
                }
            }
            else
            {
                switch (dep)
                {
                    case IS_NOT_PROPOSED_OR_STABLE_DEP:
                        /*
                         * The idea here is to discover those transactions that were started after us and have been Accepted
                         * and did not witness us as part of their pre-accept round, as this means that we CANNOT have taken
                         * the fast path. This is central to safe recovery, as if every transaction that executes later has
                         * witnessed us we are safe to propose the pre-accept timestamp regardless, whereas if any transaction
                         */
                        return markSupersedingRejects();

                    case NOT_ELIGIBLE:
                        // the command doesn't have any coordinator deps; or we are its coordinator and cannot commit on the privileged fast path
                    case IS_PROPOSED_OR_STABLE_DEP:
                        // the command has been committed with stable deps that witness us, so we're a durable dependency
                    case IS_COORD_DEP:
                        // the original coordinator witnessed us, so if it takes the fast or medium path we will be a durable dependency
                        // if it doesn't, it will take the slow path (and witness us), or be invalidated (in which case it doesn't matter)
                        break;

                    case IS_NOT_COORD_DEP:
                        Invariants.requireArgument(testTxnId.is(PrivilegedCoordinatorWithDeps));
                        // TODO (expected): if we are the original coordinator and we know we cannot fast path commit then we should not include this in the reply
                        ensureSupersedingCoordRejects().add(keyOrRange, testTxnId);
                }
                if (testTxnId.isSyncPoint())
                {
                    if (status.compareTo(ACCEPTED) < 0) ensureSimpleWait().add(keyOrRange, testTxnId);
                    else ensureSimpleNoWait().add(keyOrRange, testTxnId);
                }
            }

            return true;
        }

        private boolean markSupersedingRejects()
        {
            this.supersedingRejects = true;
            return false;
        }

        private Deps.Builder ensureSimpleNoWait()
        {
            if (simpleNoWait == null)
                simpleNoWait = new Deps.Builder(true);
            return simpleNoWait;
        }

        private Deps.Builder ensureSimpleWait()
        {
            if (simpleWait == null)
                simpleWait = new Deps.Builder(true);
            return simpleWait;
        }

        private Deps.Builder ensureSupersedingCoordRejects()
        {
            if (supersedingCoordRejects == null)
                supersedingCoordRejects = new Deps.Builder(true);
            return supersedingCoordRejects;
        }

        @Override
        public void close()
        {
            if (simpleNoWait != null)
            {
                simpleNoWait.close();
                simpleNoWait = null;
            }
            if (simpleWait != null)
            {
                simpleWait.close();
                simpleWait = null;
            }
            if (supersedingCoordRejects != null)
            {
                supersedingCoordRejects.close();
                supersedingCoordRejects = null;
            }
        }
    }

    public static abstract class RecoverReply implements Reply
    {
        // TODO (expected): recover should gracefully handle partial truncation (currently handled by MaybeRecover)
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
        // either preceding transactions or a potentially-superseding sync point (with no intervening decided sync point);
        // these transactions cannot be blocked by our decision, so we can simply wait for their decision,
        // and if they execute after us determine if they reject our execution
        public final Deps simpleWait, simpleNoWait;
        // superseding transactions where the coordinator had not witnessed us, and so they may reject our execution;
        // these transactions may await our decision, so we must treat them differently to ensure there is no deadlock
        public final Deps supersedingCoordRejects;
        public final boolean selfAcceptsFastPath;
        public final @Nullable Participants<?> coordinatorAcceptsFastPath;
        public final boolean supersedingRejects;
        public final Writes writes;
        public final Result result;

        public RecoverOk(TxnId txnId, Status status, Ballot accepted, Timestamp executeAt, LatestDeps deps,
                         Deps simpleWait, Deps simpleNoWait, Deps supersedingCoordRejects,
                         boolean selfAcceptsFastPath, Participants<?> coordinatorAcceptsFastPath, boolean supersedingRejects, Writes writes, Result result)
        {
            this.txnId = txnId;
            this.accepted = accepted;
            this.executeAt = executeAt;
            this.status = status;
            this.deps = deps;
            this.simpleWait = simpleWait;
            this.simpleNoWait = simpleNoWait;
            this.supersedingCoordRejects = supersedingCoordRejects;
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
                   ", simpleWait:" + simpleWait +
                   ", simpleNoWait:" + simpleNoWait +
                   ", laterCoordRejects:" + supersedingCoordRejects +
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
