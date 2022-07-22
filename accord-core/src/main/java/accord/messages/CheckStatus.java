package accord.messages;

import javax.annotation.Nullable;

import accord.api.Result;
import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.primitives.AbstractKeys;
import accord.primitives.AbstractRoute;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Topologies;

import static accord.local.Status.Committed;
import static accord.local.Status.NotWitnessed;
import static accord.local.Status.PreAccepted;
import static accord.messages.TxnRequest.computeScope;

public class CheckStatus implements EpochRequest
{
    // order is important
    public enum IncludeInfo
    {
        No, Route, All
    }

    final TxnId txnId;
    final RoutingKeys someKeys;
    final long startEpoch;
    final long endEpoch;
    final IncludeInfo includeInfo;

    public CheckStatus(TxnId txnId, RoutingKeys someKeys, long startEpoch, long endEpoch, IncludeInfo includeInfo)
    {
        this.txnId = txnId;
        this.someKeys = someKeys;
        this.startEpoch = startEpoch;
        this.endEpoch = endEpoch;
        this.includeInfo = includeInfo;
    }

    public CheckStatus(Id to, Topologies topologies, TxnId txnId, RoutingKeys someKeys, IncludeInfo includeInfo)
    {
        this.txnId = txnId;
        if (someKeys instanceof AbstractRoute)
            this.someKeys = computeScope(to, topologies, (AbstractRoute) someKeys, 0, AbstractRoute::sliceStrict, PartialRoute::union);
        else
            this.someKeys = computeScope(to, topologies, someKeys, 0, RoutingKeys::slice, RoutingKeys::union);
        this.startEpoch = topologies.oldestEpoch();
        this.endEpoch = topologies.currentEpoch();
        this.includeInfo = includeInfo;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        CheckStatusOk ok = node.mapReduceLocal(someKeys, startEpoch, endEpoch, instance -> {
            Command command = instance.command(txnId);
            switch (includeInfo)
            {
                default: throw new IllegalStateException();
                case No:
                case Route:
                    return new CheckStatusOk(command.status(), command.promised(), command.accepted(), command.executeAt(),
                                             node.isCoordinating(txnId, command.promised()),
                                             command.isGloballyPersistent(), includeInfo == IncludeInfo.No ? null : command.route(), command.homeKey());
                case All:
                    return new CheckStatusOkFull(node, command);
            }
        }, CheckStatusOk::merge);

        if (ok == null)
        {
            node.reply(replyToNode, replyContext, CheckStatusNack.nack());
        }
        else
        {
            if (ok instanceof CheckStatusOkFull)
                ok = ((CheckStatusOkFull) ok).covering(someKeys);
            node.reply(replyToNode, replyContext, ok);
        }

    }

    public interface CheckStatusReply extends Reply
    {
        boolean isOk();
    }

    public static class CheckStatusOk implements CheckStatusReply
    {
        public final Status status;
        public final Ballot promised;
        public final Ballot accepted;
        public final @Nullable Timestamp executeAt; // not set if invalidating or invalidated
        public final boolean isCoordinating;
        public final boolean hasExecutedOnAllShards;
        public final @Nullable AbstractRoute route;
        public final @Nullable RoutingKey homeKey;

        public CheckStatusOk(Node node, Command command)
        {
            this(command.status(), command.promised(), command.accepted(), command.executeAt(),
                 node.isCoordinating(command.txnId(), command.promised()), command.isGloballyPersistent(), command.route(), command.homeKey());
        }

        CheckStatusOk(Status status, Ballot promised, Ballot accepted, @Nullable Timestamp executeAt,
                      boolean isCoordinating, boolean hasExecutedOnAllShards,
                      @Nullable AbstractRoute route, @Nullable RoutingKey homeKey)
        {
            this.status = status;
            this.promised = promised;
            this.accepted = accepted;
            this.executeAt = executeAt;
            this.isCoordinating = isCoordinating;
            this.hasExecutedOnAllShards = hasExecutedOnAllShards;
            this.route = route;
            this.homeKey = homeKey;
        }

        @Override
        public boolean isOk()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return "CheckStatusOk{" +
                   "status:" + status +
                   ", promised:" + promised +
                   ", accepted:" + accepted +
                   ", executeAt:" + executeAt +
                   ", hasExecutedOnAllShards:" + hasExecutedOnAllShards +
                   ", isCoordinating:" + isCoordinating +
                   ", route:" + route +
                   ", homeKey:" + homeKey +
                   '}';
        }

        public CheckStatusOk merge(CheckStatusOk that)
        {
            if (that.status.compareTo(this.status) > 0)
                return that.merge(this);

            // preferentially select the one that is coordinating, if any
            CheckStatusOk prefer = this.isCoordinating ? this : that;
            CheckStatusOk defer = prefer == this ? that : this;

            // then select the max along each criteria, preferring the coordinator
            CheckStatusOk maxAccepted = prefer.accepted.compareTo(defer.accepted) >= 0 ? prefer : defer;
            CheckStatusOk maxStatus; {
                int c = prefer.status.compareTo(defer.status);
                if (c > 0) maxStatus = prefer;
                else if (c < 0) maxStatus = defer;
                else maxStatus = maxAccepted;
            }
            CheckStatusOk maxPromised = prefer.promised.compareTo(defer.promised) >= 0 ? prefer : defer;
            CheckStatusOk maxHasExecuted = !defer.hasExecutedOnAllShards || prefer.hasExecutedOnAllShards ? prefer : defer;
            CheckStatusOk maxHomeKey = prefer.homeKey != null || defer.homeKey == null ? prefer : defer;
            AbstractRoute mergedRoute = AbstractRoute.merge(prefer.route, defer.route);

            // if the maximum (or preferred equal) is the same on all dimensions, return it
            if (maxStatus == maxPromised && maxStatus == maxAccepted && maxStatus == maxHasExecuted
                && maxStatus.route == mergedRoute && maxStatus == maxHomeKey)
            {
                return maxStatus;
            }

            // otherwise assemble the maximum of each, and propagate isCoordinating from the origin we selected the promise from
            boolean isCoordinating = maxPromised == prefer ? prefer.isCoordinating : defer.isCoordinating;
            return new CheckStatusOk(maxStatus.status, maxPromised.promised, maxAccepted.accepted, maxStatus.executeAt,
                                     isCoordinating, maxHasExecuted.hasExecutedOnAllShards, mergedRoute, maxHomeKey.homeKey);
        }

        @Override
        public MessageType type()
        {
            return MessageType.CHECK_STATUS_RSP;
        }
    }

    public static class CheckStatusOkFull extends CheckStatusOk
    {
        /**
         * The maximum Status that the full contents represents for the keys provided to the merge/covering methods
         * (or {@code status} otherwise). i.e., the maximum witnessed status for which there is sufficient data to
         * replicate the state to a node that replicates a subset of the keys.
         */
        public final Status fullStatus;
        public final PartialTxn partialTxn;
        public final PartialDeps committedDeps; // only set if status >= Committed, so safe to merge
        public final Writes writes;
        public final Result result;

        public CheckStatusOkFull(Node node, Command command)
        {
            super(node, command);
            this.partialTxn = command.partialTxn();
            this.committedDeps = command.status().compareTo(Committed) >= 0 ? command.savedPartialDeps() : null;
            this.writes = command.writes();
            this.result = command.result();
            this.fullStatus = status;
        }

        protected CheckStatusOkFull(Status status, Ballot promised, Ballot accepted, boolean isCoordinating, boolean hasExecutedOnAllShards, AbstractRoute route,
                          RoutingKey homeKey, PartialTxn partialTxn, Timestamp executeAt, PartialDeps committedDeps, Writes writes, Result result)
        {
            this(status, status, promised, accepted, executeAt, isCoordinating, hasExecutedOnAllShards, route, homeKey, partialTxn, committedDeps, writes, result);
        }

        private CheckStatusOkFull(Status status, Status fullStatus, Ballot promised, Ballot accepted, Timestamp executeAt,
                                  boolean isCoordinating, boolean hasExecutedOnAllShards, AbstractRoute route,
                                  RoutingKey homeKey, PartialTxn partialTxn, PartialDeps committedDeps, Writes writes, Result result)
        {
            super(status, promised, accepted, executeAt, isCoordinating, hasExecutedOnAllShards, route, homeKey);
            this.fullStatus = fullStatus;
            this.partialTxn = partialTxn;
            this.committedDeps = committedDeps;
            this.writes = writes;
            this.result = result;
        }

        /**
         * This method assumes parameter is of the same type and has the same additional info (modulo partial replication).
         * If parameters have different info, it is undefined which properties will be returned.
         *
         * This method is NOT guaranteed to return CheckStatusOkFull unless the parameter is also CheckStatusOkFull.
         * This method is NOT guaranteed to return either parameter: it may merge the two to represent the maximum
         * combined info, (and in this case if the parameter were not CheckStatusOkFull, and were the higher status
         * reply, the info would potentially be unsafe to act upon when given a higher status
         * (e.g. Accepted executeAt is very different to Committed executeAt))
         */
        public CheckStatusOk merge(CheckStatusOk that)
        {
            return merge(that, null);
        }

        public CheckStatusOk merge(CheckStatusOk that, @Nullable AbstractKeys<?, ?> forKeys)
        {
            CheckStatusOk max = super.merge(that);
            CheckStatusOk maxSrc = this.status.compareTo(that.status) >= 0 ? this : that;
            if (!(maxSrc instanceof CheckStatusOkFull))
                return max;

            CheckStatusOkFull fullMax = (CheckStatusOkFull) maxSrc;
            CheckStatusOk minSrc = maxSrc == this ? that : this;
            if (!(minSrc instanceof CheckStatusOkFull))
            {
                return new CheckStatusOkFull(max.status, max.promised, max.accepted, max.isCoordinating, max.hasExecutedOnAllShards, max.route,
                                             max.homeKey, fullMax.partialTxn, fullMax.executeAt, fullMax.committedDeps, fullMax.writes, fullMax.result);
            }

            CheckStatusOkFull fullMin = (CheckStatusOkFull) minSrc;

            PartialTxn partialTxn = PartialTxn.merge(fullMax.partialTxn, fullMin.partialTxn);
            PartialDeps committedDeps;
            if (fullMax.committedDeps == null) committedDeps = fullMin.committedDeps;
            else if (fullMin.committedDeps == null) committedDeps = fullMax.committedDeps;
            else committedDeps = fullMax.committedDeps.with(fullMin.committedDeps);

            Status fullStatus = forKeys == null ? max.status : fullStatus(forKeys, max.status, partialTxn, committedDeps, writes, result);

            return new CheckStatusOkFull(max.status, fullStatus, max.promised, max.accepted, fullMax.executeAt, max.isCoordinating, max.hasExecutedOnAllShards, max.route,
                                         max.homeKey, partialTxn, committedDeps, fullMax.writes, fullMax.result);
        }

        /**
         * If multiple shards have been merged, the maximum status will have been adopted, however not all shards may
         * have had this status. This method picks the highest status we have complete data covering a provided set of keys.
         *
         * TODO: maybe refactor this to introduce a second lower-bound status that is updated on each merge?
         */
        public CheckStatusOkFull covering(RoutingKeys keys)
        {
            return withFullStatus(fullStatus(keys, status, partialTxn, committedDeps, writes, result));
        }

        private static Status fullStatus(AbstractKeys<?, ?> forKeys, Status maxStatus, PartialTxn partialTxn, PartialDeps committedDeps, Writes writes, Result result)
        {
            Status status = maxStatus;
            switch (maxStatus)
            {
                default: throw new IllegalStateException();
                case Invalidated:
                    break;
                case Applied:
                case Executed:
                    if (writes != null && result != null
                        && committedDeps != null && committedDeps.covers(forKeys)
                        && partialTxn != null && partialTxn.covers(forKeys))
                        break;
                    status = Committed;
                case ReadyToExecute:
                case Committed:
                    if (committedDeps != null && committedDeps.covers(forKeys)
                        && partialTxn != null && partialTxn.covers(forKeys))
                        break;
                    status = PreAccepted;
                case Accepted:
                case AcceptedInvalidate:
                case PreAccepted:
                    if (partialTxn != null && partialTxn.covers(forKeys))
                        break;
                    // TODO (now): we should test Route presence here
                    status = NotWitnessed;
                case NotWitnessed:
            }
            return status;
        }

        private CheckStatusOkFull withFullStatus(Status newFullStatus)
        {
            if (newFullStatus == fullStatus)
                return this;
            return new CheckStatusOkFull(status, newFullStatus, promised, accepted, executeAt, isCoordinating, hasExecutedOnAllShards, route,
                                         homeKey, partialTxn, committedDeps, writes, result);
        }

        @Override
        public String toString()
        {
            return "CheckStatusOk{" +
                   "status:" + status +
                   ", promised:" + promised +
                   ", accepted:" + accepted +
                   ", executeAt:" + executeAt +
                   ", hasExecutedOnAllShards:" + hasExecutedOnAllShards +
                   ", isCoordinating:" + isCoordinating +
                   ", deps:" + committedDeps +
                   ", writes:" + writes +
                   ", result:" + result +
                   '}';
        }
    }

    public static class CheckStatusNack implements CheckStatusReply
    {
        private static final CheckStatusNack instance = new CheckStatusNack();

        private CheckStatusNack() { }

        @Override
        public MessageType type()
        {
            return MessageType.CHECK_STATUS_RSP;
        }

        static CheckStatusNack nack()
        {
            return instance;
        }

        @Override
        public boolean isOk()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "CheckStatusNack";
        }
    }

    @Override
    public String toString()
    {
        return "CheckStatus{" +
               "txnId:" + txnId +
               '}';
    }

    @Override
    public MessageType type()
    {
        return MessageType.CHECK_STATUS_REQ;
    }

    @Override
    public long waitForEpoch()
    {
        return endEpoch;
    }
}
