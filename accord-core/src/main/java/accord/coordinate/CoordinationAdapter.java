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

package accord.coordinate;

import java.util.function.BiConsumer;

import accord.api.Result;
import accord.coordinate.ExecuteSyncPoint.ExecuteBlocking;
import accord.local.Node;
import accord.messages.Apply;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.utils.Faults;
import javax.annotation.Nullable;

import static accord.coordinate.ExecutePath.FAST;
import static accord.coordinate.ExecutePath.SLOW;
import static accord.messages.Apply.Kind.Maximal;
import static accord.messages.Apply.Kind.Minimal;
import static accord.primitives.Routable.Domain.Range;

public interface CoordinationAdapter<R>
{
    interface Factory
    {
        enum Step { Continue, InitiateRecovery }
        <R> CoordinationAdapter<R> get(TxnId txnId, Step step);
    }

    void propose(Node node, @Nullable Topologies preaccept, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback);
    void stabilise(Node node, @Nullable Topologies any, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback);
    void execute(Node node, @Nullable Topologies any, FullRoute<?> route, ExecutePath path, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback);
    void persist(Node node, @Nullable Topologies any, FullRoute<?> route, Participants<?> sendTo, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super R, Throwable> callback);
    default void persist(Node node, @Nullable Topologies any, FullRoute<?> route, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super R, Throwable> callback)
    {
        persist(node, any, route, route, txnId, txn, executeAt, deps, writes, result, callback);
    }

    class DefaultFactory implements Factory
    {
        @Override
        public <R> CoordinationAdapter<R> get(TxnId txnId, Step step)
        {
            switch (step)
            {
                default: throw new AssertionError("Unhandled step: " + step);
                case Continue: return (CoordinationAdapter<R>) Adapters.standard();
                case InitiateRecovery: return (CoordinationAdapter<R>) Adapters.recovery();
            }
        }
    }

    class Adapters
    {
        public static CoordinationAdapter<Result> standard()
        {
            return StandardTxnAdapter.INSTANCE;
        }

        // note that by default the recovery adapter is only used for the initial recovery decision - if e.g. propose is initiated
        // then we revert back to standard adapter behaviour for later steps
        public static CoordinationAdapter<Result> recovery()
        {
            return RecoveryTxnAdapter.INSTANCE;
        }

        public static <S extends Seekables<?, ?>> SyncPointAdapter<S> inclusiveSyncPoint()
        {
            return AsyncInclusiveSyncPointAdapter.INSTANCE;
        }

        public static <S extends Seekables<?, ?>> SyncPointAdapter<S> inclusiveSyncPointBlocking()
        {
            return InclusiveSyncPointBlockingAdapter.INSTANCE;
        }

        public static <S extends Seekables<?, ?>> SyncPointAdapter<S> exclusiveSyncPoint()
        {
            return ExclusiveSyncPointAdapter.INSTANCE;
        }

        public static abstract class AbstractTxnAdapter implements CoordinationAdapter<Result>
        {
            @Override
            public void propose(Node node, @Nullable Topologies preaccept, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
            {
                Topologies all = preaccept == null ? node.topology().withUnsyncedEpochs(route, txnId, executeAt) : preaccept;
                new ProposeTxn(node, all, route, ballot, txnId, txn, executeAt, deps, callback).start();
            }

            @Override
            public void stabilise(Node node, Topologies any, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
            {
                if (!node.topology().hasEpoch(executeAt.epoch()))
                {
                    node.withEpoch(executeAt.epoch(), () -> stabilise(node, any, route, ballot, txnId, txn, executeAt, deps, callback));
                    return;
                }

                Topologies coordinates = any.forEpochs(txnId.epoch(), txnId.epoch());
                Topologies all;
                if (txnId.epoch() == executeAt.epoch()) all = coordinates;
                else if (any.currentEpoch() >= executeAt.epoch() && any.oldestEpoch() <= txnId.epoch()) all = any.forEpochs(txnId.epoch(), executeAt.epoch());
                else all = node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());

                if (Faults.TRANSACTION_INSTABILITY) execute(node, all, route, SLOW, txnId, txn, executeAt, deps, callback);
                else new StabiliseTxn(node, coordinates, all, route, ballot, txnId, txn, executeAt, deps, callback).start();
            }

            @Override
            public void execute(Node node, Topologies any, FullRoute<?> route, ExecutePath path, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
            {
                Topologies all = execution(node, any, route, txnId, executeAt);

                if (txn.read().keys().isEmpty()) persist(node, all, route, txnId, txn, executeAt, deps, txn.execute(txnId, executeAt, null), txn.result(txnId, executeAt, null), callback);
                else new ExecuteTxn(node, all, route, path, txnId, txn, txn.read().keys().toParticipants(), executeAt, deps, callback).start();
            }

            Topologies execution(Node node, Topologies any, Participants<?> participants, TxnId txnId, Timestamp executeAt)
            {
                if (any != null && any.oldestEpoch() <= txnId.epoch() && any.currentEpoch() >= executeAt.epoch())
                    return any.forEpochs(txnId.epoch(), executeAt.epoch());
                else
                    return node.topology().preciseEpochs(participants, txnId.epoch(), executeAt.epoch());
            }
        }

        public static class StandardTxnAdapter extends AbstractTxnAdapter
        {
            public static final StandardTxnAdapter INSTANCE = new StandardTxnAdapter();

            @Override
            public void persist(Node node, Topologies any, FullRoute<?> route, Participants<?> participants, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super Result, Throwable> callback)
            {
                Topologies all = execution(node, any, participants, txnId, executeAt);

                if (callback != null) callback.accept(result, null);
                new PersistTxn(node, all, txnId, route, txn, executeAt, deps, writes, result)
                    .start(Apply.FACTORY, Minimal, all, writes, result);
            }
        }

        public static class RecoveryTxnAdapter extends AbstractTxnAdapter
        {
            public static final RecoveryTxnAdapter INSTANCE = new RecoveryTxnAdapter();
            @Override
            public void persist(Node node, Topologies any, FullRoute<?> route, Participants<?> participants, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super Result, Throwable> callback)
            {
                Topologies all = execution(node, any, participants, txnId, executeAt);

                if (callback != null) callback.accept(result, null);
                new PersistTxn(node, all, txnId, route, txn, executeAt, deps, writes, result)
                    .start(Apply.FACTORY, Maximal, all, writes, result);
            }
        }

        public static abstract class SyncPointAdapter<S extends Seekables<?, ?>> implements CoordinationAdapter<SyncPoint<S>>
        {
            abstract Topologies forDecision(Node node, FullRoute<?> route, TxnId txnId);
            abstract Topologies forExecution(Node node, FullRoute<?> route, TxnId txnId, Timestamp executeAt, Deps deps);

            void invokeSuccess(Node node, FullRoute<?> route, TxnId txnId, Txn txn, Deps deps, BiConsumer<? super SyncPoint<S>, Throwable> callback)
            {
                if (txn.keys().domain() == Range)
                    node.configService().reportEpochClosed((Ranges)txn.keys(), txnId.epoch());
                callback.accept(new SyncPoint<>(txnId, deps, (S)txn.keys(), route), null);
            }

            @Override
            public void propose(Node node, Topologies any, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super SyncPoint<S>, Throwable> callback)
            {
                Topologies all = forDecision(node, route, txnId);
                new ProposeSyncPoint<>(this, node, all, route, ballot, txnId, txn, executeAt, deps, callback).start();
            }

            @Override
            public void stabilise(Node node, Topologies any, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super SyncPoint<S>, Throwable> callback)
            {
                Topologies all = forExecution(node, route, txnId, executeAt, deps);
                Topologies coordinates = all.forEpochs(txnId.epoch(), txnId.epoch());
                new StabiliseSyncPoint<>(this, node, coordinates, all, route, ballot, txnId, txn, executeAt, deps, callback).start();
            }

            @Override
            public void execute(Node node, Topologies any, FullRoute<?> route, ExecutePath path, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super SyncPoint<S>, Throwable> callback)
            {
                Topologies all = forExecution(node, route, txnId, executeAt, deps);
                persist(node, all, route, txnId, txn, executeAt, deps, txn.execute(txnId, executeAt, null), txn.result(txnId, executeAt, null), callback);
            }

            @Override
            public void persist(Node node, Topologies any, FullRoute<?> route, Participants<?> participants, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super SyncPoint<S>, Throwable> callback)
            {
                Topologies all = forExecution(node, route, txnId, executeAt, deps);

                invokeSuccess(node, route, txnId, txn, deps, callback);
                new PersistTxn(node, all, txnId, route, txn, executeAt, deps, writes, result)
                    .start(Apply.FACTORY, Maximal, any, writes, result);
            }
        }

        public static class ExclusiveSyncPointAdapter<S extends Seekables<?, ?>> extends SyncPointAdapter<S>
        {
            private static final ExclusiveSyncPointAdapter INSTANCE = new ExclusiveSyncPointAdapter();

            @Override
            Topologies forDecision(Node node, FullRoute<?> route, TxnId txnId)
            {
                return node.topology().withOpenEpochs(route, null, txnId);
            }

            @Override
            Topologies forExecution(Node node, FullRoute<?> route, TxnId txnId, Timestamp executeAt, Deps deps)
            {
                TxnId minId = TxnId.nonNullOrMin(txnId, deps.minTxnId());
                return node.topology().withUncompletedEpochs(route, minId, txnId);
            }

            @Override
            public void execute(Node node, Topologies any, FullRoute<?> route, ExecutePath path, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super SyncPoint<S>, Throwable> callback)
            {
                // TODO (required, consider): remember and document why we don't use fast path for exclusive sync points
                if (path == FAST) stabilise(node, any, route, Ballot.ZERO, txnId, txn, executeAt, deps, callback);
                else super.execute(node, any, route, path, txnId, txn, executeAt, deps, callback);
            }
        }

        private static abstract class AbstractInclusiveSyncPointAdapter<S extends Seekables<?, ?>> extends SyncPointAdapter<S>
        {
            protected AbstractInclusiveSyncPointAdapter()
            {
                super();
            }

            @Override
            Topologies forDecision(Node node, FullRoute<?> route, TxnId txnId)
            {
                return node.topology().withUnsyncedEpochs(route, txnId, txnId);
            }

            @Override
            Topologies forExecution(Node node, FullRoute<?> route, TxnId txnId, Timestamp executeAt, Deps deps)
            {
                return node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());
            }

            @Override
            public void execute(Node node, Topologies any, FullRoute<?> route, ExecutePath path, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super SyncPoint<S>, Throwable> callback)
            {
                Topologies all = forExecution(node, route, txnId, executeAt, deps);

                ExecuteBlocking<S> execute = ExecuteBlocking.atQuorum(node, all, new SyncPoint<>(txnId, deps, (S)txn.keys(), route), executeAt);
                execute.start();
                addOrExecuteCallback(execute, callback);
            }

            protected abstract void addOrExecuteCallback(ExecuteBlocking<S> execute, BiConsumer<? super SyncPoint<S>, Throwable> callback);
        }

        /*
         * Async meaning that the result of the distributed sync point is not known when this returns
         * At most the caller can wait for the sync point to complete locally. This does mean that the sync
         * point is being executed and that eventually information will be known locally everywhere about the last
         * sync point for the keys/ranges this sync point covered.
         */
        private static class AsyncInclusiveSyncPointAdapter<S extends Seekables<?, ?>> extends AbstractInclusiveSyncPointAdapter<S>
        {
            private static final AsyncInclusiveSyncPointAdapter INSTANCE = new AsyncInclusiveSyncPointAdapter();

            protected AsyncInclusiveSyncPointAdapter() {
                super();
            }

            @Override
            protected void addOrExecuteCallback(ExecuteBlocking<S> execute, BiConsumer<? super SyncPoint<S>, Throwable> callback)
            {
                // If this is the async adapter then we want to invoke the callback immediately
                // and the caller can wait on the txn locally if they want
                callback.accept(execute.syncPoint, null);
            }
        }

        private static class InclusiveSyncPointBlockingAdapter<S extends Seekables<?, ?>> extends AbstractInclusiveSyncPointAdapter<S>
        {
            private static final InclusiveSyncPointBlockingAdapter INSTANCE = new InclusiveSyncPointBlockingAdapter();

            protected InclusiveSyncPointBlockingAdapter() {
                super();
            }

            @Override
            protected void addOrExecuteCallback(ExecuteBlocking<S> execute, BiConsumer<? super SyncPoint<S>, Throwable> callback)
            {
                execute.addCallback(callback);
            }

            @Override
            public void persist(Node node, Topologies any, FullRoute<?> route, Participants<?> participants, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super SyncPoint<S>, Throwable> callback)
            {
                throw new UnsupportedOperationException();
            }
        }
    }

}
