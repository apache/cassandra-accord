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

import accord.api.Result;
import accord.local.SafeCommandStore;
import accord.local.SafeCommandStore.SlowSearcher;
import accord.local.Status.Phase;
import accord.primitives.*;
import accord.topology.Topologies;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.utils.AsyncMapReduceConsume;
import accord.utils.Invariants;

import accord.local.Node.Id;
import accord.local.Command;
import accord.local.Status;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import java.util.Collections;

import static accord.local.SafeCommandStore.TestDep.WITH;
import static accord.local.SafeCommandStore.TestDep.WITHOUT;
import static accord.local.SafeCommandStore.TestKind.RorWs;
import static accord.local.SafeCommandStore.TestTimestamp.*;
import static accord.local.Status.*;
import static accord.messages.PreAccept.addDeps;

public class BeginRecovery extends TxnRequest implements AsyncMapReduceConsume<SafeCommandStore, BeginRecovery.RecoverReply>
{
    public static class SerializationSupport
    {
        public static BeginRecovery create(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, PartialTxn partialTxn, Ballot ballot, @Nullable FullRoute<?> route)
        {
            return new BeginRecovery(txnId, scope, waitForEpoch, partialTxn, ballot, route);
        }
    }

    public final PartialTxn partialTxn;
    public final Ballot ballot;
    public final @Nullable FullRoute<?> route;

    public BeginRecovery(Id to, Topologies topologies, TxnId txnId, Txn txn, FullRoute<?> route, Ballot ballot)
    {
        super(to, topologies, route, txnId);
        this.partialTxn = txn.slice(scope.covering(), scope.contains(scope.homeKey()));
        this.ballot = ballot;
        this.route = scope.contains(scope.homeKey()) ? route : null;
    }

    private BeginRecovery(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, PartialTxn partialTxn, Ballot ballot, @Nullable FullRoute<?> route)
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
    public Future<RecoverReply> apply(SafeCommandStore safeStore)
    {
        Command command = safeStore.command(txnId);

        switch (command.recover(safeStore, partialTxn, route != null ? route : scope, progressKey, ballot))
        {
            default:
                throw new IllegalStateException("Unhandled Outcome");

            case Redundant:
                throw new IllegalStateException("Invalid Outcome");

            case RejectedBallot:
                return ImmediateFuture.success(new RecoverNack(command.promised()));

            case Success:
        }

        Ranges ranges = safeStore.ranges().at(txnId.epoch());
        RecoverOk basic = new RecoverOk(txnId, command.status(), command.accepted(), command.executeAt(),
                command.partialDeps() == null ? PartialDeps.NONE : command.partialDeps(), Deps.NONE, Deps.NONE,
                false, command.writes(), command.result());

        if (command.hasBeen(Committed))
            return ImmediateFuture.success(basic);

        class Collector implements AutoCloseable
        {
            PartialDeps.Builder deps;
            Deps.Builder earlierAcceptedNoWitness;
            Deps.Builder earlierCommittedWitness;
            boolean rejectsFastPath;

            @Override
            public void close()
            {
                if (deps != null) deps.close();
                if (earlierAcceptedNoWitness != null) earlierAcceptedNoWitness.close();
                if (earlierCommittedWitness != null) earlierCommittedWitness.close();
            }
        }

        Collector collector = new Collector();
        if (!command.known().deps.hasProposedOrDecidedDeps())
        {
            collector.deps = PartialDeps.builder(ranges);
        }

        if (!basic.status.hasBeen(PreCommitted))
        {
            collector.earlierCommittedWitness = Deps.builder();
            collector.earlierAcceptedNoWitness = Deps.builder();
        }

        Future<Collector> collected = safeStore.slowFold(partialTxn.keys(), ranges, (searcher, keyOrRange, c) -> {
            if (c.deps != null)
                addDeps(searcher, txnId, txnId, c.deps);

            if (!basic.status.hasBeen(PreCommitted))
            {
                addAcceptedStartedBeforeWithoutWitnessing(searcher, txnId, c.earlierAcceptedNoWitness);
                addCommittedStartedBeforeAndWitnessed(searcher, txnId, c.earlierCommittedWitness);
                if (!c.rejectsFastPath)
                {
                    c.rejectsFastPath = hasAcceptedStartedAfterWithoutWitnessing(searcher, txnId);
                    if (!c.rejectsFastPath)
                        c.rejectsFastPath = hasCommittedExecutesAfterWithoutWitnessing(searcher, txnId);
                }
            }
            return c;
        }, collector, null);

        return collected.map(c -> {
            RecoverOk ok = basic.merge(c.deps, c.earlierCommittedWitness, c.earlierAcceptedNoWitness, c.rejectsFastPath);
            c.close();
            return ok;
        });
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

        PartialDeps deps = ok1.deps.with(ok2.deps);
        Deps earlierCommittedWitness = ok1.earlierCommittedWitness.with(ok2.earlierCommittedWitness);
        Deps earlierAcceptedNoWitness = ok1.earlierAcceptedNoWitness.with(ok2.earlierAcceptedNoWitness)
                .without(earlierCommittedWitness::contains);
        Timestamp timestamp = ok1.status == PreAccepted ? Timestamp.max(ok1.executeAt, ok2.executeAt) : ok1.executeAt;

        return new RecoverOk(
                txnId, ok1.status, ok1.accepted, timestamp, deps,
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
    public Seekables<?, ?> keys()
    {
        // TODO (expected): we shouldn't need this; it's currently used only for maxConflict IF we haven't preaccepted,
        //  which we can always answer pessimistically, so should always be answered from cache
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
        public final PartialDeps deps;
        public final Deps earlierCommittedWitness;  // counter-point to earlierAcceptedNoWitness
        public final Deps earlierAcceptedNoWitness; // wait for these to commit
        public final boolean rejectsFastPath;
        public final Writes writes;
        public final Result result;

        public RecoverOk(TxnId txnId, Status status, Ballot accepted, Timestamp executeAt, @Nonnull PartialDeps deps, Deps earlierCommittedWitness, Deps earlierAcceptedNoWitness, boolean rejectsFastPath, Writes writes, Result result)
        {
            this.txnId = txnId;
            this.accepted = accepted;
            this.executeAt = executeAt;
            this.status = status;
            this.deps = Invariants.nonNull(deps);
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

        RecoverOk merge(@Nullable PartialDeps.Builder deps, @Nullable Deps.Builder earlierCommittedWitness, @Nullable Deps.Builder earlierAcceptedNoWitness, boolean rejectsFastPath)
        {
            return new RecoverOk(txnId, status, accepted, executeAt,
                    deps != null ? deps.build() : this.deps,
                    earlierCommittedWitness != null ? earlierCommittedWitness.build() : this.earlierCommittedWitness,
                    earlierAcceptedNoWitness != null ? earlierAcceptedNoWitness.build() : this.earlierAcceptedNoWitness,
                    rejectsFastPath | this.rejectsFastPath,
                    writes, result);
        }
    }

    public static class RecoverNack extends RecoverReply
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

    private static void addAcceptedStartedBeforeWithoutWitnessing(SlowSearcher searcher, TxnId startedBefore, Deps.Builder builder)
    {
        searcher.fold(RorWs, STARTED_BEFORE, startedBefore, WITHOUT, startedBefore, Accepted, PreCommitted,
                (keyOrRange, txnId, executeAt, prev) -> {
                    if (executeAt.compareTo(startedBefore) > 0)
                        builder.add(keyOrRange, txnId);
                    return builder;
                }, builder, null);
    }

    private static void addCommittedStartedBeforeAndWitnessed(SlowSearcher searcher, TxnId startedBefore, Deps.Builder builder)
    {
        searcher.fold(RorWs, STARTED_BEFORE, startedBefore, WITH, startedBefore, Committed, null,
                (keyOrRange, txnId, executeAt, prev) -> builder.add(keyOrRange, txnId), (Deps.AbstractBuilder<Deps>)builder, null);
    }

    private static boolean hasAcceptedStartedAfterWithoutWitnessing(SlowSearcher searcher, TxnId startedAfter)
    {
        /*
         * The idea here is to discover those transactions that were started after us and have been Accepted
         * and did not witness us as part of their pre-accept round, as this means that we CANNOT have taken
         * the fast path. This is central to safe recovery, as if every transaction that executes later has
         * witnessed us we are safe to propose the pre-accept timestamp regardless, whereas if any transaction
         * has not witnessed us we can safely invalidate (us).
         */
        return searcher.fold(RorWs, STARTED_AFTER, startedAfter, WITHOUT, startedAfter, Accepted, PreCommitted,
                (keyOrRange, txnId, executeAt, prev) -> true, false, true);
    }

    private static boolean hasCommittedExecutesAfterWithoutWitnessing(SlowSearcher searcher, TxnId startedAfter)
    {
        /*
         * The idea here is to discover those transactions that have been decided to execute after us
         * and did not witness us as part of their pre-accept or accept round, as this means that we CANNOT have
         * taken the fast path. This is central to safe recovery, as if every transaction that executes later has
         * witnessed us we are safe to propose the pre-accept timestamp regardless, whereas if any transaction
         * has not witnessed us we can safely invalidate it.
         */
        return searcher.fold(RorWs, EXECUTES_AFTER, startedAfter, WITHOUT, startedAfter, Committed, null,
                (keyOrRange, txnId, executeAt, prev) -> true,false, true);
    }
}
