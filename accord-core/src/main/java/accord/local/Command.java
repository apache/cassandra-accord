package accord.local;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import accord.api.ProgressLog.ProgressShard;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.primitives.AbstractRoute;
import accord.primitives.KeyRanges;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;

import static accord.api.ProgressLog.ProgressShard.Home;
import static accord.api.ProgressLog.ProgressShard.Local;
import static accord.api.ProgressLog.ProgressShard.No;
import static accord.api.ProgressLog.ProgressShard.Unsure;
import static accord.local.Command.EnsureAction.Add;
import static accord.local.Command.EnsureAction.Check;
import static accord.local.Command.EnsureAction.Ignore;
import static accord.local.Command.EnsureAction.Set;
import static accord.local.Command.EnsureAction.TrySet;
import static accord.local.Status.Accepted;
import static accord.local.Status.AcceptedInvalidate;
import static accord.local.Status.Applied;
import static accord.local.Status.Committed;
import static accord.local.Status.Executed;
import static accord.local.Status.Invalidated;
import static accord.local.Status.NotWitnessed;
import static accord.local.Status.PreAccepted;
import static accord.local.Status.ReadyToExecute;

// TODO: this needs to be backed by persistent storage
public class Command implements Listener, Consumer<Listener>
{
    public final CommandStore commandStore;

    private final TxnId txnId;

    /**
     * homeKey is a global value that defines the home shard - the one tasked with ensuring the transaction is finished.
     * progressKey is a local value that defines the local shard responsible for ensuring progress on the transaction.
     * This will be homeKey if it is owned by the node, and some other key otherwise. If not the home shard, the progress
     * shard has much weaker responsibilities, only ensuring that the home shard has durably witnessed the txnId.
     */
    private RoutingKey homeKey, progressKey;

    /**
     * If this is the home shard, we require that this is a Route for all states > NotWitnessed;
     * otherwise for the local progress shard this is ordinarily a PartialRoute, and for other shards this is not set,
     * so that there is only one copy per node that can be consulted to construct the full set of involved keys.
     *
     * If hasBeen(Committed) this must contain the keys for both txnId.epoch and executeAt.epoch
     *
     * TODO: maybe set this for all local shards, but slice to only those participating keys
     * (would probably need to remove hashIntersects)
     */
    private @Nullable AbstractRoute route;

    /**
     * While !hasBeen(Committed), contains the portion of a transaction that applies on this node as of txnId.epoch
     * If hasBeen(Committed) and !hasBeen(Executed), represents the portion that applies as of txnId.epoch AND executeAt.epoch
     * If hasBeen(Executed) MAY BE NULL, or may cover only txnId.epoch; no point requiring this be populated at execution
     */
    private PartialTxn partialTxn;

    /**
     * While !hasBeen(Committed), used only as a register for Accept state, used by Recovery
     * If hasBeen(Committed), represents the full deps owned by this range for execution at both txnId.epoch
     * AND executeAt.epoch so that it may be used for Recovery (which contacts only txnId.epoch topology),
     * but also for execution.
     *
     * At present, Deps contain every declare key in the range, however this should not be relied upon
     */
    private PartialDeps partialDeps = null;
    private Ballot promised = Ballot.ZERO, accepted = Ballot.ZERO;
    private Timestamp executeAt;
    private Writes writes;
    private Result result;

    private Status status = NotWitnessed;
    private boolean isGloballyPersistent;

    private NavigableMap<TxnId, Command> waitingOnCommit;
    private NavigableMap<Timestamp, Command> waitingOnApply;

    private final Listeners listeners = new Listeners();

    public Command(CommandStore commandStore, TxnId id)
    {
        this.commandStore = commandStore;
        this.txnId = id;
    }

    public TxnId txnId()
    {
        return txnId;
    }

    public PartialTxn partialTxn()
    {
        return partialTxn;
    }

    public AbstractRoute route()
    {
        return route;
    }

    public void saveRoute(Route route)
    {
        this.route = route;
        updateHomeKey(route.homeKey);
    }

    public Ballot promised()
    {
        return promised;
    }

    public Ballot accepted()
    {
        return accepted;
    }

    public Timestamp executeAt()
    {
        return executeAt;
    }

    public PartialDeps savedPartialDeps()
    {
        return partialDeps;
    }

    public Writes writes()
    {
        return writes;
    }

    public Result result()
    {
        return result;
    }

    public Status status()
    {
        return status;
    }

    public boolean hasBeenWitnessed()
    {
        return partialTxn != null;
    }

    public boolean hasBeen(Status status)
    {
        return this.status.compareTo(status) >= 0;
    }

    public boolean is(Status status)
    {
        return this.status == status;
    }

    public boolean isGloballyPersistent()
    {
        return isGloballyPersistent;
    }

    public void setGloballyPersistent(RoutingKey homeKey, @Nullable Timestamp executeAt)
    {
        updateHomeKey(homeKey);
        if (executeAt != null && hasBeen(Committed) && !this.executeAt.equals(executeAt))
            commandStore.agent().onInconsistentTimestamp(this, this.executeAt, executeAt);
        isGloballyPersistent = true;
    }

    public enum AcceptOutcome
    {
        Success, Insufficient, Redundant, RejectedBallot
    }

    public AcceptOutcome preaccept(PartialTxn partialTxn, AbstractRoute route, @Nullable RoutingKey progressKey)
    {
        if (promised.compareTo(Ballot.ZERO) > 0)
            return AcceptOutcome.RejectedBallot;

        return preacceptInternal(partialTxn, route, progressKey);
    }

    private AcceptOutcome preacceptInternal(PartialTxn partialTxn, AbstractRoute route, @Nullable RoutingKey progressKey)
    {
        if (executeAt != null)
            return AcceptOutcome.Redundant;

        switch (status)
        {
            default: throw new IllegalStateException();
            case NotWitnessed:
            case AcceptedInvalidate:
            case Invalidated:
        }

        KeyRanges coordinateRanges = coordinateRanges();
        ProgressShard shard = progressShard(route, progressKey, coordinateRanges);
        if (!validate(KeyRanges.EMPTY, coordinateRanges, shard, route, partialTxn, Set, null, Ignore))
            throw new IllegalStateException();

        Timestamp max = commandStore.maxConflict(partialTxn.keys);
        // unlike in the Accord paper, we partition shards within a node, so that to ensure a total order we must either:
        //  - use a global logical clock to issue new timestamps; or
        //  - assign each shard _and_ process a unique id, and use both as components of the timestamp
        this.executeAt = txnId.compareTo(max) > 0 && txnId.epoch >= commandStore.latestEpoch()
                         ? txnId : commandStore.uniqueNow(max);

        set(KeyRanges.EMPTY, coordinateRanges, shard, route, partialTxn, Set, null, Ignore);
        if (status == NotWitnessed)
            this.status = PreAccepted;

        this.commandStore.progressLog().preaccept(txnId, shard);

        this.listeners.forEach(this);
        return AcceptOutcome.Success;
    }

    public boolean preacceptInvalidate(Ballot ballot)
    {
        if (this.promised.compareTo(ballot) > 0)
            return false;

        this.promised = ballot;
        return true;
    }

    public AcceptOutcome accept(Ballot ballot, PartialRoute route, @Nullable RoutingKey progressKey, @Nullable PartialTxn partialTxn, Timestamp executeAt, PartialDeps partialDeps)
    {
        if (this.promised.compareTo(ballot) > 0)
            return AcceptOutcome.RejectedBallot;

        if (hasBeen(Committed))
            return AcceptOutcome.Redundant;

        KeyRanges coordinateRanges = coordinateRanges();
        KeyRanges executeRanges = txnId.epoch == executeAt.epoch ? coordinateRanges : commandStore.ranges().at(executeAt.epoch);
        ProgressShard shard = progressShard(route, progressKey, coordinateRanges);

        if (!validate(coordinateRanges, executeRanges, shard, route, partialTxn, Add, partialDeps, Set))
            return AcceptOutcome.Insufficient;

        this.executeAt = executeAt;
        this.promised = this.accepted = ballot;
        set(coordinateRanges, executeRanges, shard, route, partialTxn, Add, partialDeps, Set);
        this.status = Accepted;

        this.commandStore.progressLog().accept(txnId, shard);
        this.listeners.forEach(this);

        return AcceptOutcome.Success;
    }

    public AcceptOutcome acceptInvalidate(Ballot ballot)
    {
        if (this.promised.compareTo(ballot) > 0)
            return AcceptOutcome.RejectedBallot;

        if (hasBeen(Committed))
            return AcceptOutcome.Redundant;

        promised = accepted = ballot;
        status = AcceptedInvalidate;

        listeners.forEach(this);
        return AcceptOutcome.Success;
    }

    public enum CommitOutcome { Success, Redundant, Insufficient }

    // relies on mutual exclusion for each key
    public CommitOutcome commit(AbstractRoute route, @Nullable RoutingKey progressKey, @Nullable PartialTxn partialTxn, Timestamp executeAt, PartialDeps partialDeps)
    {
        if (hasBeen(Committed))
        {
            if (executeAt.equals(this.executeAt) && status != Invalidated)
                return CommitOutcome.Redundant;

            commandStore.agent().onInconsistentTimestamp(this, (status == Invalidated ? Timestamp.NONE : this.executeAt), executeAt);
        }

        KeyRanges coordinateRanges = coordinateRanges();
        // TODO (now): consider ranges between coordinateRanges and executeRanges? Perhaps don't need them
        KeyRanges executeRanges = executeRanges(executeAt);
        ProgressShard shard = progressShard(route, progressKey, coordinateRanges);

        if (!validate(coordinateRanges, executeRanges, shard, route, partialTxn, Add, partialDeps, Set))
            return CommitOutcome.Insufficient;

        this.executeAt = executeAt;
        set(coordinateRanges, executeRanges, shard, route, partialTxn, Add, partialDeps, Set);

        this.status = Committed;
        populateWaitingOn();

        commandStore.progressLog().commit(txnId, shard);

        maybeExecute(shard, false);
        listeners.forEach(this);
        return CommitOutcome.Success;
    }

    private void populateWaitingOn()
    {
        if (waitingOnApply != null)
            return;

        this.waitingOnCommit = new TreeMap<>();
        this.waitingOnApply = new TreeMap<>();
        partialDeps.forEachOn(commandStore, executeAt, txnId -> {
            Command command = commandStore.command(txnId);
            switch (command.status)
            {
                default:
                    throw new IllegalStateException();
                case NotWitnessed:
                case PreAccepted:
                case Accepted:
                case AcceptedInvalidate:
                    // we don't know when these dependencies will execute, and cannot execute until we do
                    command.addListener(this);
                    waitingOnCommit.put(txnId, command);
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

        if (waitingOnCommit.isEmpty())
        {
            waitingOnCommit = null;
            if (waitingOnApply.isEmpty())
                waitingOnApply = null;
        }
    }

    public void commitInvalidate()
    {
        if (hasBeen(Committed))
        {
            if (!hasBeen(Invalidated))
                commandStore.agent().onInconsistentTimestamp(this, Timestamp.NONE, executeAt);

            return;
        }

        ProgressShard shard = progressShard();
        commandStore.progressLog().invalidate(txnId, shard);
        executeAt = txnId;
        if (partialDeps == null)
            partialDeps = PartialDeps.NONE;
        status = Invalidated;

        listeners.forEach(this);
    }

    public enum ApplyOutcome { Success, Redundant, OutOfRange, Insufficient }

    public ApplyOutcome apply(long untilEpoch, AbstractRoute route, Timestamp executeAt, @Nullable PartialDeps partialDeps, Writes writes, Result result)
    {
        if (hasBeen(Executed) && executeAt.equals(this.executeAt))
        {
            return ApplyOutcome.Redundant;
        }
        else if (hasBeen(Committed) && !executeAt.equals(this.executeAt))
        {
            commandStore.agent().onInconsistentTimestamp(this, this.executeAt, executeAt);
        }

        KeyRanges coordinateRanges = coordinateRanges();
        KeyRanges executeRanges = executeRanges(executeAt);
        if (untilEpoch < commandStore.latestEpoch())
        {
            // at present this should only happen if we receive an Apply message that was scoped to a node's earlier ownership
            // in which case this might be routed to a new shard on the replica; it shouldn't imply any
            KeyRanges expectedRanges = commandStore.ranges().between(executeAt.epoch, untilEpoch);
            if (!expectedRanges.contains(executeRanges))
                return ApplyOutcome.OutOfRange;
        }
        ProgressShard shard = progressShard(route, coordinateRanges);

        if (!validate(coordinateRanges, executeRanges, shard, route, null, Check, partialDeps, hasBeen(Committed) ? Add : TrySet))
            return ApplyOutcome.Insufficient; // TODO: this should probably be an assertion failure if !TrySet

        this.writes = writes;
        this.result = result;
        this.executeAt = executeAt;
        set(coordinateRanges, executeRanges, shard, route, null, Check, partialDeps, hasBeen(Committed) ? Add : TrySet);

        if (!hasBeen(Committed))
            populateWaitingOn();
        this.status = Executed;

        commandStore.progressLog().execute(txnId, shard);

        maybeExecute(shard, false);
        this.listeners.forEach(this);
        return ApplyOutcome.Success;
    }

    public AcceptOutcome recover(PartialTxn txn, AbstractRoute route, @Nullable RoutingKey progressKey, Ballot ballot)
    {
        if (this.promised.compareTo(ballot) > 0)
            return AcceptOutcome.RejectedBallot;

        if (executeAt == null)
        {
            Preconditions.checkState(status == NotWitnessed || status == AcceptedInvalidate);
            switch (preacceptInternal(txn, route, progressKey))
            {
                default:
                case RejectedBallot:
                case Insufficient:
                case Redundant:
                    throw new IllegalStateException();

                case Success:
            }
        }

        this.promised = ballot;
        return AcceptOutcome.Success;
    }

    public boolean addListener(Listener listener)
    {
        return listeners.add(listener);
    }

    public void removeListener(Listener listener)
    {
        listeners.remove(listener);
    }

    @Override
    public void onChange(Command command)
    {
        switch (command.status)
        {
            default: throw new IllegalStateException();
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
                updatePredecessorAndRefresh(command);
                maybeExecute(progressShard(), true);
                break;
        }
    }

    /**
     * @param dependency is either committed, applied or invalidated
     */
    private void updatePredecessor(Command dependency)
    {
        Preconditions.checkState(dependency.hasBeen(Committed));
        if (dependency.hasBeen(Invalidated))
        {
            dependency.removeListener(this);
        }
        else if (dependency.executeAt.compareTo(executeAt) > 0)
        {
            // cannot be a predecessor if we execute later
            dependency.removeListener(this);
        }
        else if (dependency.hasBeen(Applied))
        {
            waitingOnApply.remove(dependency.executeAt);
            dependency.removeListener(this);
        }
        else
        {
            waitingOnApply.putIfAbsent(dependency.executeAt, dependency);
        }
    }

    private void updatePredecessorAndRefresh(Command command)
    {
        if (waitingOnApply != null)
        {
            updatePredecessor(command);
            if (waitingOnCommit != null)
            {
                if (waitingOnCommit.remove(command.txnId) != null && waitingOnCommit.isEmpty())
                    waitingOnCommit = null;
            }
            if (waitingOnCommit == null && waitingOnApply.isEmpty())
                waitingOnApply = null;
        }
        else
        {
            command.removeListener(this);
        }
    }

    private void maybeExecute(ProgressShard shard, boolean notifyListeners)
    {
        if (status != Committed && status != Executed)
            return;

        if (waitingOnApply != null)
        {
            BlockedBy blockedBy = blockedBy();
            if (blockedBy != null)
            {
                commandStore.progressLog().waiting(blockedBy.txnId, blockedBy.until, blockedBy.someKeys);
                return;
            }
            assert waitingOnApply == null;
        }

        switch (status)
        {
            case Committed:
                // TODO: maintain distinct ReadyToRead and ReadyToWrite states
                status = ReadyToExecute;
                commandStore.progressLog().readyToExecute(txnId, shard);
                if (notifyListeners)
                    listeners.forEach(this);
                break;
            case Executed:
                writes.apply(commandStore);
                status = Applied;
                if (notifyListeners)
                    listeners.forEach(this);
        }
    }

    static class BlockedBy
    {
        final TxnId txnId;
        final Status until;
        final RoutingKeys someKeys; // some keys we know to be associated with this txnId, unlikely to be exhaustive

        BlockedBy(TxnId txnId, Status until, RoutingKeys someKeys)
        {
            this.txnId = txnId;
            this.until = until;
            this.someKeys = someKeys;
        }
    }

    public BlockedBy blockedBy()
    {
        Command prev = this;
        Command cur = directlyBlockedBy();
        if (cur == null)
            return null;

        Command next;
        while (null != (next = cur.directlyBlockedBy()))
        {
            prev = cur;
            cur = next;
        }

        RoutingKeys someKeys = cur.someRoutingKeys();
        if (someKeys == null)
            someKeys = prev.partialDeps.someRoutingKeys(cur.txnId);
        return new BlockedBy(cur.txnId, cur.hasBeen(Committed) ? Executed : Committed, someKeys);
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
    public RoutingKey homeKey()
    {
        return homeKey;
    }

    public void updateHomeKey(RoutingKey homeKey)
    {
        if (this.homeKey == null)
        {
            this.homeKey = homeKey;
            if (progressKey == null && owns(txnId.epoch, homeKey))
                progressKey = homeKey;
        }
        else if (!this.homeKey.equals(homeKey))
        {
            throw new IllegalStateException();
        }
    }

    private ProgressShard progressShard(AbstractRoute route, @Nullable RoutingKey progressKey, KeyRanges coordinateRanges)
    {
        updateHomeKey(route.homeKey);

        if (progressKey == null || progressKey == NO_PROGRESS_KEY)
        {
            if (this.progressKey == null)
                this.progressKey = NO_PROGRESS_KEY;

            return No;
        }

        if (this.progressKey == null) this.progressKey = progressKey;
        else if (!this.progressKey.equals(progressKey)) throw new AssertionError();

        if (!coordinateRanges.contains(progressKey))
            return No;

        if (!commandStore.hashIntersects(progressKey))
            return No;

        return progressKey.equals(homeKey) ? Home : Local;
    }

    private ProgressShard progressShard(AbstractRoute route, KeyRanges coordinateRanges)
    {
        if (progressKey == null)
            return Unsure;

        return progressShard(route, progressKey, coordinateRanges);
    }

    private ProgressShard progressShard()
    {
        if (progressKey == null)
            return Unsure;

        if (progressKey == NO_PROGRESS_KEY)
            return No;

        KeyRanges coordinateRanges = commandStore.ranges().at(txnId.epoch);
        if (!coordinateRanges.contains(progressKey))
            return No;

        if (!commandStore.hashIntersects(progressKey))
            return No;

        return progressKey.equals(homeKey) ? Home : Local;
    }

    private KeyRanges coordinateRanges()
    {
        return commandStore.ranges().at(txnId.epoch);
    }

    private KeyRanges executeRanges(Timestamp executeAt)
    {
        return commandStore.ranges().since(executeAt.epoch);
    }

    enum EnsureAction { Ignore, Check, Add, TrySet, Set }

    /**
     * Validate we have sufficient information for the route, partialTxn and partialDeps fields, and if so update them;
     * otherwise return false (or throw an exception if an illegal state is encountered)
     */
    private boolean validate(KeyRanges existingRanges, KeyRanges additionalRanges, ProgressShard shard, AbstractRoute route,
                             @Nullable PartialTxn partialTxn, EnsureAction ensurePartialTxn,
                             @Nullable PartialDeps partialDeps, EnsureAction ensurePartialDeps)
    {
        if (shard == Unsure)
            return false;

        // first validate route
        if (shard.isProgress())
        {
            // validate route
            if (shard.isHome())
            {
                if (!(this.route instanceof Route) && !(route instanceof Route))
                    return false;
            }
            else if (this.route == null)
            {
                // failing any of these tests is always an illegal state
                if (!route.covers(existingRanges))
                    return false;

                if (existingRanges != additionalRanges && !route.covers(additionalRanges))
                    throw new IllegalArgumentException("Incomplete route (" + route + ") provided; does not cover " + additionalRanges);
            }
            else if (existingRanges != additionalRanges && !this.route.covers(additionalRanges))
            {
                if (!route.covers(additionalRanges))
                    throw new IllegalArgumentException("Incomplete route (" + route + ") provided; does not cover " + additionalRanges);
            }
            else
            {
                if (!this.route.covers(existingRanges))
                    throw new IllegalStateException();
            }
        }

        // invalid to Add deps to Accepted or AcceptedInvalidate statuses, as Committed deps are not equivalent
        // and we may erroneously believe we have covered a wider range than we have infact covered
        if (ensurePartialDeps == Add)
            Preconditions.checkState(status != Accepted && status != AcceptedInvalidate);

        // validate new partial txn
        if (!validate(ensurePartialTxn, existingRanges, additionalRanges, covers(this.partialTxn), covers(partialTxn), "txn", partialTxn))
            return false;

        if (shard.isHome() && ensurePartialTxn != Ignore)
        {
            if (!hasQuery(this.partialTxn) && !hasQuery(partialTxn))
                throw new IllegalStateException();
        }

        return validate(ensurePartialDeps, existingRanges, additionalRanges, covers(this.partialDeps), covers(partialDeps), "deps", partialDeps);
    }

    private void set(KeyRanges existingRanges, KeyRanges additionalRanges, ProgressShard shard, AbstractRoute route,
                     @Nullable PartialTxn partialTxn, EnsureAction ensurePartialTxn,
                     @Nullable PartialDeps partialDeps, EnsureAction ensurePartialDeps)
    {
        Preconditions.checkState(progressKey != null);
        KeyRanges allRanges = existingRanges.union(additionalRanges);

        if (shard.isProgress()) this.route = AbstractRoute.merge(this.route, route);
        else this.route = AbstractRoute.merge(this.route, route.slice(allRanges));

        // TODO (now): apply hashIntersects, or stop round-robin hashing and partition only on ranges
        switch (ensurePartialTxn)
        {
            case Add:
                if (partialTxn == null)
                    break;

                if (this.partialTxn != null)
                {
                    partialTxn.keys.foldlDifference(this.partialTxn.keys, (i, key, p, v) -> {
                        if (commandStore.hashIntersects(key))
                            commandStore.commandsForKey(key).register(this);
                        return v;
                    }, 0, 0, 1);
                    this.partialTxn = this.partialTxn.with(partialTxn.slice(allRanges, shard.isHome()));
                    break;
                }

            case Set:
            case TrySet:
                this.partialTxn = partialTxn = partialTxn.slice(allRanges, shard.isHome());
                partialTxn.keys().forEach(key -> {
                    if (commandStore.hashIntersects(key))
                        commandStore.commandsForKey(key).register(this);
                });
                break;
        }

        // TODO (now): apply hashIntersects, or stop round-robin hashing and partition only on ranges
        switch (ensurePartialDeps)
        {
            case Add:
                if (partialDeps == null)
                    break;

                if (this.partialDeps != null)
                {
                    this.partialDeps = this.partialDeps.with(partialDeps.slice(allRanges));
                    break;
                }

            case Set:
            case TrySet:
                this.partialDeps = partialDeps.slice(allRanges);
                break;
        }
    }

    private static boolean validate(EnsureAction action, KeyRanges existingRanges, KeyRanges additionalRanges,
                                    KeyRanges existing, KeyRanges adding, String kind, Object obj)
    {
        switch (action)
        {
            default: throw new IllegalStateException();
            case Ignore:
                break;

            case TrySet:
                if (adding != null)
                {
                    if (!adding.contains(existingRanges))
                        return false;

                    if (additionalRanges != existingRanges && !adding.contains(additionalRanges))
                        return false;

                    break;
                }
            case Set:
                // failing any of these tests is always an illegal state
                Preconditions.checkState(adding != null);
                if (!adding.contains(existingRanges))
                    throw new IllegalArgumentException("Incomplete " + kind + " (" + obj + ") provided; does not cover " + existingRanges);

                if (additionalRanges != existingRanges && !adding.contains(additionalRanges))
                    throw new IllegalArgumentException("Incomplete " + kind + " (" + obj + ") provided; does not cover " + additionalRanges);
                break;

            case Check:
                if (existing == null)
                    return false;

            case Add:
                if (adding == null)
                {
                    if (existing == null)
                        return false;

                    Preconditions.checkState(existing.contains(existingRanges));
                    if (existingRanges != additionalRanges && !existing.contains(additionalRanges))
                        throw new IllegalArgumentException("Missing additional " + kind + "; existing does not cover " + additionalRanges.difference(existingRanges));
                }
                else if (existing != null)
                {
                    KeyRanges covering = adding.union(existing);
                    Preconditions.checkState(covering.contains(existingRanges));
                    if (existingRanges != additionalRanges && !covering.contains(additionalRanges))
                        throw new IllegalArgumentException("Incomplete additional " + kind + " (" + obj + ") provided; does not cover " + additionalRanges.difference(existingRanges));
                }
                else
                {
                    if (!adding.contains(existingRanges))
                        return false;

                    if (existingRanges != additionalRanges && !adding.contains(additionalRanges))
                        throw new IllegalArgumentException("Incomplete additional " + kind + " (" + obj + ") provided; does not cover " + additionalRanges.difference(existingRanges));
                }
                break;
        }

        return true;
    }

    // TODO: callers should try to consult the local progress shard (if any) to obtain the full set of keys owned locally
    public AbstractRoute someRoute()
    {
        Preconditions.checkState(homeKey != null);
        if (route != null)
            return route;

        if (partialTxn != null)
            return partialTxn.keys.toRoute(homeKey);

        if (partialDeps != null)
            return partialDeps.keys().toRoute(homeKey);

        return null;
    }

    public RoutingKeys someRoutingKeys()
    {
        if (route != null)
            return route instanceof Route ? route : route.with(route.homeKey);

        if (partialTxn != null && !partialTxn.keys.isEmpty())
            return partialTxn.keys.toRoutingKeys();

        if (partialDeps != null && !partialDeps.keys().isEmpty())
            return partialDeps.keys().toRoutingKeys();

        return null;
    }

    // does this specific Command instance execute (i.e. does it lose ownership post Commit)
    // TODO (now): remove this and all callers
    @Deprecated
    public boolean executes()
    {
        KeyRanges ranges = commandStore.ranges().at(executeAt.epoch);
        // TODO: take care when merging commandStores, as partialTxn may be incomplete
        return route.any(ranges, commandStore::hashIntersects);
    }

    /**
     * true iff this commandStore owns the given key on the given epoch
     */
    public boolean owns(long epoch, RoutingKey someKey)
    {
        if (!commandStore.hashIntersects(someKey))
            return false;

        return commandStore.ranges().at(epoch).contains(someKey);
    }

    private Command directlyBlockedBy()
    {
        // firstly we're waiting on every dep to commit
        while (waitingOnCommit != null)
        {
            // TODO: when we change our liveness mechanism this may not be a problem
            // cannot guarantee that listener updating this set is invoked before this method by another listener
            // so we must check the entry is still valid, and potentially remove it if not
            Command waitingOn = waitingOnCommit.firstEntry().getValue();
            if (!waitingOn.hasBeen(Committed))
                return waitingOn;
            // TODO (now): we can get into infinite loops here of invoking maybeExecute
            onChange(waitingOn);
        }

        while (waitingOnApply != null)
        {
            // TODO: when we change our liveness mechanism this may not be a problem
            // cannot guarantee that listener updating this set is invoked before this method by another listener
            // so we must check the entry is still valid, and potentially remove it if not
            Command waitingOn = waitingOnApply.firstEntry().getValue();
            if (!waitingOn.hasBeen(Applied))
                return waitingOn;
            onChange(waitingOn);
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
               "txnId=" + txnId +
               ", status=" + status +
               ", partialTxn=" + partialTxn +
               ", executeAt=" + executeAt +
               ", partialDeps=" + partialDeps +
               '}';
    }

    private static KeyRanges covers(@Nullable PartialTxn txn)
    {
        return txn == null ? null : txn.covering;
    }

    private static KeyRanges covers(@Nullable PartialDeps deps)
    {
        return deps == null ? null : deps.covering;
    }

    private static boolean hasQuery(PartialTxn txn)
    {
        return txn != null && txn.query != null;
    }

    // TODO: this is an ugly hack, need to encode progress/homeKey/Route state combinations much more clearly
    //  (perhaps introduce encapsulating class representing each possible arrangement)
    private static final RoutingKey NO_PROGRESS_KEY = new RoutingKey()
    {
        @Override
        public int routingHash()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int compareTo(RoutingKey ignore)
        {
            throw new UnsupportedOperationException();
        }
    };
}
