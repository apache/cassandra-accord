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

    void propose(Node node, Topologies withUnsynced, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback);
    void stabilise(Node node, Topologies coordinates, Topologies all, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback);
    void execute(Node node, Topologies all, FullRoute<?> route, ExecutePath path, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback);
    void persist(Node node, Topologies all, FullRoute<?> route, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super R, Throwable> callback);

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

    /**
     * Utility methods for correctly invoking the next phase of the state machine via a CoordinationAdapter.
     * Simply ensures the topologies are correct before being passed to the instance method.
     */
    class Invoke
    {
        public static <R> void propose(CoordinationAdapter<R> adapter, Node node, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback)
        {
            propose(adapter, node, node.topology().withUnsyncedEpochs(route, txnId, executeAt), route, ballot, txnId, txn, executeAt, deps, callback);
        }

        public static <R> void propose(CoordinationAdapter<R> adapter, Node node, Topologies withUnsynced, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback)
        {
            adapter.propose(node, withUnsynced, route, ballot, txnId, txn, executeAt, deps, callback);
        }

        public static <R> void stabilise(CoordinationAdapter<R> adapter, Node node, Topologies any, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback)
        {
            if (!node.topology().hasEpoch(executeAt.epoch()))
            {
                node.withEpoch(executeAt.epoch(), () -> stabilise(adapter, node, any, route, ballot, txnId, txn, executeAt, deps, callback));
                return;
            }
            Topologies coordinates = any.forEpochs(txnId.epoch(), txnId.epoch());
            Topologies all;
            if (txnId.epoch() == executeAt.epoch()) all = coordinates;
            else if (any.currentEpoch() >= executeAt.epoch() && any.oldestEpoch() <= txnId.epoch()) all = any.forEpochs(txnId.epoch(), executeAt.epoch());
            else all = node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());

            adapter.stabilise(node, coordinates, all, route, ballot, txnId, txn, executeAt, deps, callback);
        }

        public static <R> void execute(CoordinationAdapter<R> adapter, Node node, Topologies any, FullRoute<?> route, ExecutePath path, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback)
        {
            if (any.oldestEpoch() <= txnId.epoch() && any.currentEpoch() >= executeAt.epoch()) any = any.forEpochs(txnId.epoch(), executeAt.epoch());
            else any = node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());
            adapter.execute(node, any, route, path, txnId, txn, executeAt, deps, callback);
        }

        public static <R> void persist(CoordinationAdapter<R> adapter, Node node, Topologies any, FullRoute<?> route, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, @Nullable BiConsumer<? super R, Throwable> callback)
        {
            if (any.oldestEpoch() <= txnId.epoch() && any.currentEpoch() >= executeAt.epoch()) any = any.forEpochs(txnId.epoch(), executeAt.epoch());
            else any = node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());
            Topologies executes = any.forEpochs(executeAt.epoch(), executeAt.epoch());

            adapter.persist(node, any, route, txnId, txn, executeAt, deps, writes, result, callback);
        }

        public static <R> void persist(CoordinationAdapter<R> adapter, Node node, FullRoute<?> route, Participants<?> sendTo, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, @Nullable BiConsumer<? super R, Throwable> callback)
        {
            Topologies all = node.topology().preciseEpochs(sendTo, txnId.epoch(), executeAt.epoch());
            Topologies executes = all.forEpochs(executeAt.epoch(), executeAt.epoch());

            adapter.persist(node, all, route, txnId, txn, executeAt, deps, writes, result, callback);
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

        public static <S extends Seekables<?, ?>> CoordinationAdapter<SyncPoint<S>> inclusiveSyncPoint()
        {
            return AsyncInclusiveSyncPointAdapter.INSTANCE;
        }

        public static <S extends Seekables<?, ?>> CoordinationAdapter<SyncPoint<S>> inclusiveSyncPointBlocking()
        {
            return InclusiveSyncPointBlockingAdapter.INSTANCE;
        }

        public static <S extends Seekables<?, ?>> CoordinationAdapter<SyncPoint<S>> exclusiveSyncPoint()
        {
            return ExclusiveSyncPointAdapter.INSTANCE;
        }

        public static abstract class AbstractTxnAdapter implements CoordinationAdapter<Result>
        {
            @Override
            public void propose(Node node, Topologies withUnsynced, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
            {
                new ProposeTxn(node, withUnsynced, route, ballot, txnId, txn, executeAt, deps, callback).start();
            }

            @Override
            public void stabilise(Node node, Topologies coordinates, Topologies all, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
            {
                if (Faults.TRANSACTION_INSTABILITY) execute(node, all, route, SLOW, txnId, txn, executeAt, deps, callback);
                else new StabiliseTxn(node, coordinates, all, route, ballot, txnId, txn, executeAt, deps, callback).start();
            }

            @Override
            public void execute(Node node, Topologies all, FullRoute<?> route, ExecutePath path, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
            {
                if (txn.read().keys().isEmpty()) Invoke.persist(this, node, all, route, txnId, txn, executeAt, deps, txn.execute(txnId, executeAt, null), txn.result(txnId, executeAt, null), callback);
                else new ExecuteTxn(node, all, route, path, txnId, txn, txn.read().keys().toParticipants(), executeAt, deps, callback).start();
            }
        }

        public static class StandardTxnAdapter extends AbstractTxnAdapter
        {
            public static final StandardTxnAdapter INSTANCE = new StandardTxnAdapter();
            @Override
            public void persist(Node node, Topologies all, FullRoute<?> route, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super Result, Throwable> callback)
            {
                if (callback != null) callback.accept(result, null);
                new PersistTxn(node, all, txnId, route, txn, executeAt, deps, writes, result)
                    .start(Apply.FACTORY, Minimal, all, writes, result);
            }
        }

        public static class RecoveryTxnAdapter extends AbstractTxnAdapter
        {
            public static final RecoveryTxnAdapter INSTANCE = new RecoveryTxnAdapter();
            @Override
            public void persist(Node node, Topologies all, FullRoute<?> route, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super Result, Throwable> callback)
            {
                if (callback != null) callback.accept(result, null);
                new PersistTxn(node, all, txnId, route, txn, executeAt, deps, writes, result)
                    .start(Apply.FACTORY, Maximal, all, writes, result);
            }
        }

        public static abstract class AbstractSyncPointAdapter<S extends Seekables<?, ?>> implements CoordinationAdapter<SyncPoint<S>>
        {
            void invokeSuccess(Node node, FullRoute<?> route, TxnId txnId, Txn txn, Deps deps, BiConsumer<? super SyncPoint<S>, Throwable> callback)
            {
                if (txn.keys().domain() == Range)
                    node.configService().reportEpochClosed((Ranges)txn.keys(), txnId.epoch());
                callback.accept(new SyncPoint<>(txnId, deps, (S)txn.keys(), route), null);
            }

            @Override
            public void propose(Node node, Topologies withUnsynced, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super SyncPoint<S>, Throwable> callback)
            {
                new ProposeSyncPoint<>(this, node, withUnsynced, route, ballot, txnId, txn, executeAt, deps, callback).start();
            }

            @Override
            public void stabilise(Node node, Topologies coordinates, Topologies all, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super SyncPoint<S>, Throwable> callback)
            {
                new StabiliseSyncPoint<>(this, node, coordinates, all, route, ballot, txnId, txn, executeAt, deps, callback).start();
            }

            @Override
            public void execute(Node node, Topologies all, FullRoute<?> route, ExecutePath path, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super SyncPoint<S>, Throwable> callback)
            {
                Invoke.persist(this, node, all, route, txnId, txn, executeAt, deps, txn.execute(txnId, executeAt, null), txn.result(txnId, executeAt, null), callback);
            }

            @Override
            public void persist(Node node, Topologies all, FullRoute<?> route, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super SyncPoint<S>, Throwable> callback)
            {
                invokeSuccess(node, route, txnId, txn, deps, callback);
                new PersistTxn(node, all, txnId, route, txn, executeAt, deps, writes, result)
                    .start(Apply.FACTORY, Maximal, all, writes, result);
            }
        }

        public static class ExclusiveSyncPointAdapter<S extends Seekables<?, ?>> extends AbstractSyncPointAdapter<S>
        {
            private static final ExclusiveSyncPointAdapter INSTANCE = new ExclusiveSyncPointAdapter();

            @Override
            public void execute(Node node, Topologies all, FullRoute<?> route, ExecutePath path, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super SyncPoint<S>, Throwable> callback)
            {
                // TODO (required): remember and document why we don't use fast path for exclusive sync points
                if (path == FAST)
                {
                    Invoke.stabilise(this, node, all, route, Ballot.ZERO, txnId, txn, executeAt, deps, callback);
                }
                else
                {
                    super.execute(node, all, route, path, txnId, txn, executeAt, deps, callback);
                }
            }
        }

        private static abstract class AbstractInclusiveSyncPointAdapter<S extends Seekables<?, ?>> extends AbstractSyncPointAdapter<S>
        {

            protected AbstractInclusiveSyncPointAdapter()
            {
                super();
            }

            @Override
            public void execute(Node node, Topologies all, FullRoute<?> route, ExecutePath path, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super SyncPoint<S>, Throwable> callback)
            {
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
            public void persist(Node node, Topologies all, FullRoute<?> route, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super SyncPoint<S>, Throwable> callback)
            {
                throw new UnsupportedOperationException();
            }
        }
    }

}
