package accord.messages;

import accord.api.Key;
import accord.api.Result;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.txn.Ballot;
import accord.txn.Dependencies;
import accord.txn.Timestamp;
import accord.txn.TxnId;
import accord.txn.Writes;

public class CheckStatus implements Request
{
    // order is important
    public enum IncludeInfo
    {
        HomeKey, ExecuteAt, Dependencies, Writes, Result;

        public byte asBits()
        {
            return (byte) (1 << ordinal());
        }

        public byte and(IncludeInfo info)
        {
            return (byte) (info.asBits() | asBits());
        }

        public byte and(int bits)
        {
            return (byte) (bits | asBits());
        }

        public boolean isSet(byte bits)
        {
            return (bits & asBits()) != 0;
        }

        public static byte all()
        {
            return 31;
        }
    }

    final TxnId txnId;
    final Key key; // the key's commandStore to consult - not necessarily the homeKey
    final long epoch;
    final byte includeInfo;

    public CheckStatus(TxnId txnId, Key key, long epoch, byte includeInfo)
    {
        this.txnId = txnId;
        this.key = key;
        this.epoch = epoch;
        this.includeInfo = includeInfo;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {

        Reply reply = node.ifLocal(key, epoch, instance -> {
            Command command = instance.command(txnId);
            if (includeInfo != 0)
            {
                return (CheckStatusReply) new CheckStatusOkFull(command.status(), command.promised(), command.accepted(),
                                                                node.isCoordinating(txnId, command.promised()),
                                                                command.isGloballyPersistent(),
                                                                IncludeInfo.HomeKey.isSet(includeInfo) ? command.homeKey() : null,
                                                                IncludeInfo.ExecuteAt.isSet(includeInfo) ? command.executeAt() : null,
                                                                IncludeInfo.Dependencies.isSet(includeInfo) ? command.savedDeps() : null,
                                                                IncludeInfo.Writes.isSet(includeInfo) ? command.writes() : null,
                                                                IncludeInfo.Result.isSet(includeInfo) ? command.result() : null);
            }

            return new CheckStatusOk(command.status(), command.promised(), command.accepted(),
                                     node.isCoordinating(txnId, command.promised()),
                                     command.isGloballyPersistent());
        });

        if (reply == null)
            reply = CheckStatusNack.nack();

        node.reply(replyToNode, replyContext, reply);
    }

    public interface CheckStatusReply extends Reply
    {
        boolean isOk();
    }

    public static class CheckStatusOk implements CheckStatusReply, Comparable<CheckStatusOk>
    {
        public final Status status;
        public final Ballot promised;
        public final Ballot accepted;
        public final boolean isCoordinating;
        public final boolean hasExecutedOnAllShards;

        CheckStatusOk(Status status, Ballot promised, Ballot accepted, boolean isCoordinating, boolean hasExecutedOnAllShards)
        {
            this.status = status;
            this.promised = promised;
            this.accepted = accepted;
            this.isCoordinating = isCoordinating;
            this.hasExecutedOnAllShards = hasExecutedOnAllShards;
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
                   ", hasExecutedOnAllShards:" + hasExecutedOnAllShards +
                   ", isCoordinating:" + isCoordinating +
                   '}';
        }

        @Override
        public int compareTo(CheckStatusOk that)
        {
            int c = this.promised.compareTo(that.promised);
            if (c == 0) c = this.status.compareTo(that.status);
            if (c == 0) c = this.accepted.compareTo(that.accepted);
            if (c == 0 && this.hasExecutedOnAllShards != that.hasExecutedOnAllShards)
                return this.hasExecutedOnAllShards ? 1 : -1;
            return c;
        }

        public CheckStatusOk merge(CheckStatusOk that)
        {
            // preferentially select the one that is coordinating, if any
            CheckStatusOk prefer = this.isCoordinating ? this : that;
            CheckStatusOk defer = prefer == this ? that : this;

            // then select the max along each criteria, preferring the coordinator
            CheckStatusOk maxStatus = prefer.status.compareTo(defer.status) >= 0 ? prefer : defer;
            CheckStatusOk maxPromised = prefer.promised.compareTo(defer.promised) >= 0 ? prefer : defer;
            CheckStatusOk maxAccepted = prefer.accepted.compareTo(defer.accepted) >= 0 ? prefer : defer;
            CheckStatusOk maxHasExecuted = !defer.hasExecutedOnAllShards || prefer.hasExecutedOnAllShards ? prefer : defer;

            // if the maximum (or preferred equal) is the same on all dimensions, return it
            if (maxStatus == maxPromised && maxStatus == maxAccepted && maxStatus == maxHasExecuted)
                return maxStatus;

            // otherwise assemble the maximum of each, and propagate isCoordinating from the origin we selected the promise from

            boolean isCoordinating = maxPromised == prefer ? prefer.isCoordinating : defer.isCoordinating;
            return new CheckStatusOk(maxStatus.status, maxPromised.promised, maxAccepted.accepted,
                                     isCoordinating, maxHasExecuted.hasExecutedOnAllShards);
        }

        @Override
        public MessageType type()
        {
            return MessageType.CHECK_STATUS_RSP;
        }
    }

    public static class CheckStatusOkFull extends CheckStatusOk
    {
        public final Key homeKey;
        public final Timestamp executeAt;
        public final Dependencies deps;
        public final Writes writes;
        public final Result result;

        CheckStatusOkFull(Status status, Ballot promised, Ballot accepted, boolean isCoordinating, boolean hasExecutedOnAllShards,
                          Key homeKey, Timestamp executeAt, Dependencies deps, Writes writes, Result result)
        {
            super(status, promised, accepted, isCoordinating, hasExecutedOnAllShards);
            this.homeKey = homeKey;
            this.executeAt = executeAt;
            this.deps = deps;
            this.writes = writes;
            this.result = result;
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
                   ", deps:" + deps +
                   ", writes:" + writes +
                   ", result:" + result +
                   '}';
        }

        /**
         * This method assumes parameter is of the same type and has the same additional info.
         * If parameters have different info, it is undefined which properties will be returned - no effort
         * is made to merge different info.
         *
         * This method is NOT guaranteed to return CheckStatusOkFull unless the parameter is also CheckStatusOkFull.
         * This method is NOT guaranteed to return either parameter: it may merge the two to represent the maximum
         * combined info, (and in this case if the parameter were not CheckStatusOkFull, and were the higher status
         * reply, the info would potentially be unsafe to act upon when given a higher status
         * (e.g. Accepted executeAt is very different to Committed executeAt))
         */
        public CheckStatusOk merge(CheckStatusOk that)
        {
            CheckStatusOk max = super.merge(that);
            if (this == max || that == max) return max;

            CheckStatusOk maxSrc = this.status.compareTo(that.status) >= 0 ? this : that;
            if (!(maxSrc instanceof CheckStatusOkFull))
                return max;

            CheckStatusOkFull src = (CheckStatusOkFull) maxSrc;
            return new CheckStatusOkFull(max.status, max.promised, max.accepted, max.isCoordinating,
                                         max.hasExecutedOnAllShards, src.homeKey, src.executeAt, src.deps, src.writes, src.result);
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
}
