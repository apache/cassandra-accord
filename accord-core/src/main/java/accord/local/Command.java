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

package accord.local;

import accord.api.*;
import accord.local.Node.Id;
import accord.primitives.*;
import accord.txn.Txn;
import accord.txn.Writes;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.cassandra.utils.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.function.Consumer;
import java.util.function.Function;

import static accord.local.Status.*;
import static accord.utils.Utils.listOf;

public abstract class Command implements Listener, Consumer<Listener>, PreLoadContext, PartialCommand.WithDeps
{
    private static final Logger logger = LoggerFactory.getLogger(Command.class);

    public abstract CommandStore commandStore();

    public abstract Key homeKey();

    protected abstract void setHomeKey(Key key);

    public abstract Key progressKey();

    protected abstract void setProgressKey(Key key);

    protected abstract void setTxn(Txn txn);

    public abstract Ballot promised();

    public abstract void promised(Ballot ballot);

    public abstract Ballot accepted();

    public abstract void accepted(Ballot ballot);

    public abstract void executeAt(Timestamp timestamp);

    public abstract Deps savedDeps();

    public abstract void savedDeps(Deps deps);

    public abstract Writes writes();

    public abstract void writes(Writes writes);

    public abstract Result result();

    public abstract void result(Result result);

    public abstract void status(Status status);

    public abstract boolean isGloballyPersistent();

    public abstract void isGloballyPersistent(boolean v);

    public abstract Command addListener(Listener listener);

    public abstract void notifyListeners();

    public abstract void addWaitingOnCommit(Command command);

    public abstract boolean isWaitingOnCommit();

    public abstract void removeWaitingOnCommit(PartialCommand command);

    public abstract PartialCommand firstWaitingOnCommit();

    public abstract void addWaitingOnApplyIfAbsent(PartialCommand command);

    public abstract boolean isWaitingOnApply();

    public abstract void removeWaitingOnApply(PartialCommand command);

    public abstract PartialCommand firstWaitingOnApply();

    public boolean isUnableToApply()
    {
        return isWaitingOnCommit() || isWaitingOnApply();
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Iterables.concat(Collections.singleton(txnId()), savedDeps().txnIds());
    }

    @Override
    public Iterable<Key> keys()
    {
        return txn().keys();
    }

    public void setGloballyPersistent(Key homeKey, Timestamp executeAt)
    {
        homeKey(homeKey);
        if (!hasBeen(Committed))
            this.executeAt(executeAt);
        else if (!this.executeAt().equals(executeAt))
            commandStore().agent().onInconsistentTimestamp(this, this.executeAt(), executeAt);
        isGloballyPersistent(true);
    }

    // requires that command != null
    // relies on mutual exclusion for each key
    // note: we do not set status = newStatus, we only use it to decide how we register with the retryLog
    private void witness(Txn txn, Key homeKey, Key progressKey)
    {
        txn(txn);
        homeKey(homeKey);
        progressKey(progressKey);

        if (status() == NotWitnessed)
            status(PreAccepted);

        if (executeAt() == null)
        {
            Timestamp max = commandStore().maxConflict(txn.keys());
            // unlike in the Accord paper, we partition shards within a node, so that to ensure a total order we must either:
            //  - use a global logical clock to issue new timestamps; or
            //  - assign each shard _and_ process a unique id, and use both as components of the timestamp
            Timestamp witnessed = txnId().compareTo(max) > 0 && txnId().epoch >= commandStore().latestEpoch()
                    ? txnId() : commandStore().uniqueNow(max);
            executeAt(witnessed);

            txn.keys().foldl(commandStore().ranges().since(txnId().epoch), (i, key, param) -> {
                if (commandStore().hashIntersects(key))
                    commandStore().commandsForKey(key).register(this);
                return null;
            }, null);
        }
    }

    public boolean preaccept(Txn txn, Key homeKey, Key progressKey)
    {
        if (promised().compareTo(Ballot.ZERO) > 0)
        {
            logger.trace("{}: skipping preaccept - witnessed higher ballot ({})", txnId(), promised());
            return false;
        }

        if (hasBeen(PreAccepted))
        {
            logger.trace("{}: skipping preaccept - already preaccepted ({})", txnId(), status());
            return true;
        }

        witness(txn, homeKey, progressKey);
        boolean isProgressShard = progressKey != null && handles(txnId().epoch, progressKey);
        commandStore().progressLog().preaccept(this, isProgressShard, isProgressShard && progressKey.equals(homeKey));

        logger.trace("{}: preaccepted with executeAt: {}, deps: {}", txnId(), executeAt(), savedDeps());
        notifyListeners();
        return true;
    }

    public boolean accept(Ballot ballot, Txn txn, Key homeKey, Key progressKey, Timestamp executeAt, Deps deps)
    {
        if (promised().compareTo(ballot) > 0)
        {
            logger.trace("{}: skipping accept - witnessed higher ballot ({} > {})", txnId(), promised(), ballot);
            return false;
        }

        if (hasBeen(Committed))
        {
            logger.trace("{}: skipping accept - already committed ({})", txnId(), status());
            return false;
        }

        witness(txn, homeKey, progressKey);
        savedDeps(deps);
        executeAt(executeAt);
        promised(ballot);
        accepted(ballot);
        status(Accepted);

        boolean isProgressShard = progressKey != null && handles(txnId().epoch, progressKey);
        commandStore().progressLog().accept(this, isProgressShard, isProgressShard && progressKey.equals(homeKey));
        logger.trace("{}: preaccepted with executeAt: {}, deps: {}", txnId(), executeAt, deps);

        notifyListeners();
        return true;
    }

    public boolean acceptInvalidate(Ballot ballot)
    {
        if (this.promised().compareTo(ballot) > 0)
        {
            logger.trace("{}: skipping accept invalidated - witnessed higher ballot ({} > {})", txnId(), promised(), ballot);
            return false;
        }

        if (hasBeen(Committed))
        {
            logger.trace("{}: skipping accept invalidated - already committed ({})", txnId(), status());
            return false;
        }

        promised(ballot);
        accepted(ballot);
        status(AcceptedInvalidate);
        logger.trace("{}: accepted invalidated", txnId());

        notifyListeners();
        return true;
    }

    // relies on mutual exclusion for each key
    public boolean commit(Txn txn, Key homeKey, Key progressKey, Timestamp executeAt, Deps deps)
    {
        if (hasBeen(Committed))
        {
            logger.trace("{}: skipping commit - already committed ({})", txnId(), status());
            if (executeAt.equals(executeAt()) && status() != Invalidated)
                return false;

            commandStore().agent().onInconsistentTimestamp(this, (status() == Invalidated ? Timestamp.NONE : this.executeAt()), executeAt);
        }

        witness(txn, homeKey, progressKey);
        savedDeps(deps);
        executeAt(executeAt);
        status(Committed);
        Preconditions.checkState(!isWaitingOnCommit());
        Preconditions.checkState(!isWaitingOnApply());
        logger.trace("{}: committed with executeAt: {}, deps: {}", txnId(), executeAt, deps);

        KeyRanges ranges = commandStore().ranges().since(executeAt.epoch);
        if (ranges != null)
        {
            savedDeps().forEachOn(ranges, commandStore()::hashIntersects, txnId -> {
                Command command = commandStore().command(txnId);
                logger.trace("{}: dep {} is {}", txnId(), command.txnId(), command.status());
                switch (command.status())
                {
                    default:
                        throw new IllegalStateException();
                    case NotWitnessed:
                    case PreAccepted:
                    case Accepted:
                    case AcceptedInvalidate:
                        // we don't know when these dependencies will execute, and cannot execute until we do
                        command.addListener(this);
                        addWaitingOnCommit(command);
                        break;
                    case Committed:
                        // TODO: split into ReadyToRead and ReadyToWrite;
                        //       the distributed read can be performed as soon as those keys are ready, and in parallel with any other reads
                        //       the client can even ACK immediately after; only the write needs to be postponed until other in-progress reads complete
                    case ReadyToExecute:
                    case Executed:
                    case Applied:
                        command.addListener(this);
                        updatePredecessor(command);
                    case Invalidated:
                        break;
                }
            });
        }


        boolean isProgressShard = progressKey != null && handles(txnId().epoch, progressKey);
        commandStore().progressLog().commit(this, isProgressShard, isProgressShard && progressKey.equals(homeKey));

        return true;
    }

    public Future<Void> commitAndBeginExecution(Txn txn, Key homeKey, Key progressKey, Timestamp executeAt, Deps deps)
    {
        if (!commit(txn, homeKey, progressKey, executeAt, deps))
            return Writes.SUCCESS;

        return maybeExecute(true);
    }

    // TODO (now): commitInvalidate may need to update cfks _if_ possible
    public boolean commitInvalidate()
    {
        if (hasBeen(Committed))
        {
            logger.trace("{}: skipping commit invalidated - already committed ({})", txnId(), status());
            if (!hasBeen(Invalidated))
                commandStore().agent().onInconsistentTimestamp(this, Timestamp.NONE, executeAt());

            return false;
        }

        status(Invalidated);

        boolean isProgressShard = progressKey() != null && handles(txnId().epoch, progressKey());
        commandStore().progressLog().invalidate(this, isProgressShard, isProgressShard && progressKey().equals(homeKey()));
        logger.trace("{}: committed invalidated", txnId());

        notifyListeners();
        return true;
    }

    public Future<Void> apply(Txn txn, Key homeKey, Key progressKey, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        if (hasBeen(Executed) && executeAt.equals(executeAt()))
        {
            logger.trace("{}: skipping apply - already executed ({})", txnId(), status());
            return Writes.SUCCESS;
        }
        else if (!hasBeen(Committed))
            commit(txn, homeKey, progressKey, executeAt, deps);
        else if (!executeAt.equals(executeAt()))
            commandStore().agent().onInconsistentTimestamp(this, executeAt(), executeAt);

        executeAt(executeAt);
        writes(writes);
        result(result);
        status(Executed);
        logger.trace("{}: apply, status set to Executed with executeAt: {}, deps: {}", txnId(), executeAt, deps);

        boolean isProgressShard = progressKey != null && handles(txnId().epoch, progressKey);
        commandStore().progressLog().execute(this, isProgressShard, isProgressShard && progressKey.equals(homeKey));

        return maybeExecute(true);
    }

    public boolean recover(Txn txn, Key homeKey, Key progressKey, Ballot ballot)
    {
        if (this.promised().compareTo(ballot) > 0)
        {
            logger.trace("{}: skipping recover - higher ballot witnessed ({})", txnId(), promised());
            return false;
        }

        Status status = status();
        witness(txn, homeKey, progressKey);
        boolean isProgressShard = progressKey != null && handles(txnId().epoch, progressKey);
        if (status == NotWitnessed)
            commandStore().progressLog().preaccept(this, isProgressShard, isProgressShard && progressKey.equals(homeKey));
        promised(ballot);
        logger.trace("{}: recovery started with executeAt: {}, deps: {}", txnId(), executeAt(), savedDeps());
        return true;
    }

    public boolean preAcceptInvalidate(Ballot ballot)
    {
        if (promised().compareTo(ballot) > 0)
        {
            logger.trace("{}: skipping preaccept invalidate - higher ballot witnessed ({})", txnId(), promised());
            return false;
        }

        promised(ballot);
        logger.trace("{}: preaccepted invalidated", txnId());
        return true;
    }

    @Override
    public PreLoadContext listenerPreLoadContext(TxnId caller)
    {
        return PreLoadContext.contextFor(listOf(txnId(), caller), Collections.emptyList());
    }

    private void onChangeInternal(PartialCommand command)
    {
        logger.trace("{}: updating as listener in response to change on {} with status {} ({})",
                     txnId(), command.txnId(), command.status(), command);
        switch (command.status())
        {
            default:
                throw new IllegalStateException();
            case NotWitnessed:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
                break;

            case Committed:
            case ReadyToExecute:
            case Executed:
            case Applied:
            case Invalidated:
                if (isUnableToApply())
                {
                    updatePredecessor(command);
                    if (isWaitingOnCommit())
                    {
                        removeWaitingOnCommit(command);
                    }
                }
                else
                {
                    command.removeListener(this);
                }
                maybeExecute(false);
                break;
        }
    }

    @Override
    public void onChange(Command command)
    {
        onChangeInternal(command);
    }

    protected void postApply()
    {
        logger.trace("{} applied, setting status to Applied and notifying listeners", txnId());
        status(Applied);
        notifyListeners();
    }

    private static Function<CommandStore, Void> callPostApply(TxnId txnId)
    {
        return commandStore -> {
            commandStore.command(txnId).postApply();
            return null;
        };
    }

    protected Future<Void> apply()
    {
        // important: we can't include a reference to *this* in the lambda, since the C* implementation may evict
        // the command instance from memory between now and the write completing (and post apply being called)
        return writes().apply(commandStore()).flatMap(unused ->
            commandStore().process(this, callPostApply(txnId()))
        );
    }

    public Txn.ReadFuture read(Keys scope)
    {
        return txn().read(this, scope);
    }

    private Future<Void> maybeExecute(boolean notifyListenersOnNoop)
    {
        if (logger.isTraceEnabled())
            logger.trace("{}: Maybe executing with status {}. Will notify listeners on noop: {}", txnId(), status(), notifyListenersOnNoop);

        if (status() != Committed && status() != Executed)
        {
            if (notifyListenersOnNoop) notifyListeners();
            return Writes.SUCCESS;
        }

        if (isUnableToApply())
        {
            BlockedBy blockedBy = blockedBy();
            if (blockedBy != null)
            {
                logger.trace("{}: not executing, blocked on {}", txnId(), blockedBy.command.txnId());
                commandStore().progressLog().waiting(blockedBy.command, blockedBy.someKeys);
                if (notifyListenersOnNoop) notifyListeners();
                return Writes.SUCCESS;
            }
            assert !isWaitingOnApply();
        }

        switch (status())
        {
            case Committed:
                // TODO: maintain distinct ReadyToRead and ReadyToWrite states
                status(ReadyToExecute);
                logger.trace("{}: set to ReadyToExecute", txnId());
                boolean isProgressShard = progressKey() != null && handles(txnId().epoch, progressKey());
                commandStore().progressLog().readyToExecute(this, isProgressShard, isProgressShard && progressKey().equals(homeKey()));
                notifyListeners();
                break;
            case Executed:
                logger.trace("{}: applying", txnId());
                if (notifyListenersOnNoop) notifyListeners();
                return apply();
        }
        return Writes.SUCCESS;
    }

    /**
     * @param dependency is either committed or invalidated
     */
    private void updatePredecessor(PartialCommand dependency)
    {
        Preconditions.checkState(dependency.hasBeen(Committed));
        if (dependency.hasBeen(Invalidated))
        {
            logger.trace("{}: {} is invalidated. Stop listening and removing from waiting on commit set.", txnId(), dependency.txnId());
            dependency.removeListener(this);
            removeWaitingOnCommit(dependency);
        }
        else if (dependency.executeAt().compareTo(executeAt()) > 0)
        {
            // cannot be a predecessor if we execute later
            logger.trace("{}: {} executes after us. Stop listening.", txnId(), dependency.txnId());
            dependency.removeListener(this);
        }
        else if (dependency.hasBeen(Applied))
        {
            logger.trace("{}: {} has been applied. Stop listening and removing from waiting on apply set.", txnId(), dependency.txnId());
            removeWaitingOnApply(dependency);
            dependency.removeListener(this);
        }
        else
        {
            logger.trace("{}: adding {} to waiting on apply set.", txnId(), dependency.txnId());
            addWaitingOnApplyIfAbsent(dependency);
        }
    }

    // TEMPORARY: once we can invalidate commands that have not been witnessed on any shard, we do not need to know the home shard
    static class BlockedBy
    {
        final PartialCommand command;
        final Keys someKeys;

        BlockedBy(PartialCommand command, Keys someKeys)
        {
            this.command = command;
            this.someKeys = someKeys;
        }
    }

    public BlockedBy blockedBy()
    {
        Command prev = this;
        PartialCommand cur = directlyBlockedBy();
        if (cur == null)
            return null;

//        Command next;
//        while (null != (next = cur.directlyBlockedBy()))
//        {
//            prev = cur;
//            cur = next;
//        }

        Keys someKeys = cur.someKeys();
        if (someKeys == null)
            someKeys = prev.savedDeps().someKeys(cur.txnId());
        return new BlockedBy(cur, someKeys);
    }

    /**
     * A key nominated to represent the "home" shard - only members of the home shard may be nominated to recover
     * a transaction, to reduce the cluster-wide overhead of ensuring progress. A transaction that has only been
     * witnessed at PreAccept may however trigger a process of ensuring the home shard is durably informed of
     * the transaction.
     *
     * Note that for ProgressLog purposes the "home shard" is the shard as of txnId.epoch.
     * For recovery purposes the "home shard" is as of txnId.epoch until Committed, and executeAt.epoch once Executed
     *
     * TODO: Markdown documentation explaining the home shard and local shard concepts
     */

    public final void homeKey(Key homeKey)
    {
        Key current = homeKey();
        if (current == null) setHomeKey(homeKey);
        else if (!current.equals(homeKey)) throw new AssertionError();
    }


    public Key someKey()
    {
        if (homeKey() != null)
            return homeKey();

        if (txn().keys() != null)
            return txn().keys().get(0);

        return null;
    }

    /**
     * A key nominated to be the primary shard within this node for managing progress of the command.
     * It is nominated only as of txnId.epoch, and may be null (indicating that this node does not monitor
     * the progress of this command).
     *
     * Preferentially, this is homeKey on nodes that replicate it, and otherwise any key that is replicated, as of txnId.epoch
     */

    public final void progressKey(Key progressKey)
    {
        Key current = progressKey();
        if (current == null) setProgressKey(progressKey);
        else if (!current.equals(progressKey)) throw new AssertionError();
    }

    // does this specific Command instance execute (i.e. does it lose ownership post Commit)
    public boolean executes()
    {
        KeyRanges ranges = commandStore().ranges().at(executeAt().epoch);
        return ranges != null && txn().keys().any(ranges, commandStore()::hashIntersects);
    }

    public final void txn(Txn txn)
    {
        Txn current = txn();
        if (current == null) setTxn(txn);
        else if (!current.equals(txn)) throw new AssertionError();
    }

    public boolean handles(long epoch, Key someKey)
    {
        if (!commandStore().hashIntersects(someKey))
            return false;

        KeyRanges ranges = commandStore().ranges().at(epoch);
        if (ranges == null)
            return false;
        return ranges.contains(someKey);
    }

    private Id coordinator()
    {
        if (promised().equals(Ballot.ZERO))
            return txnId().node;
        return promised().node;
    }

    private PartialCommand directlyBlockedBy()
    {
        // firstly we're waiting on every dep to commit
        while (isWaitingOnCommit())
        {
            // TODO: when we change our liveness mechanism this may not be a problem
            // cannot guarantee that listener updating this set is invoked before this method by another listener
            // so we must check the entry is still valid, and potentially remove it if not
            PartialCommand waitingOn = firstWaitingOnCommit();
            if (!waitingOn.hasBeen(Committed)) return waitingOn;
            onChangeInternal(waitingOn);
        }

        while (isWaitingOnApply())
        {
            // TODO: when we change our liveness mechanism this may not be a problem
            // cannot guarantee that listener updating this set is invoked before this method by another listener
            // so we must check the entry is still valid, and potentially remove it if not
            PartialCommand waitingOn = firstWaitingOnApply();
            if (!waitingOn.hasBeen(Applied)) return waitingOn;
            onChangeInternal(waitingOn);
        }

        return null;
    }

    @Override
    public void accept(Listener listener)
    {
        listener.onChange(this);
    }

    @Override
    public String toString()
    {
        return "Command{" +
               "txnId=" + txnId() +
               ", status=" + status() +
               ", txn=" + txn() +
               ", executeAt=" + executeAt() +
               ", deps=" + savedDeps() +
               '}';
    }
}
