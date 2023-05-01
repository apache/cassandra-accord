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

import java.util.BitSet;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.messages.ReadData.ReadNack;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;

import static accord.messages.MessageType.READ_RSP;
import static accord.messages.TxnRequest.computeWaitForEpoch;
import static accord.messages.TxnRequest.latestRelevantEpochIndex;

// TODO (required, efficiency): dedup - can currently have infinite pending reads that will be executed independently
// TODO (review) this is really more at its core Execute rather than read because we use it to execute all kinds of things now and we should maybe rename it?
public abstract class ReadData extends AbstractEpochRequest<ReadNack>
{
    private static final Logger logger = LoggerFactory.getLogger(ReadData.class);

    public enum ReadType
    {
        readTxnData(0),
        waitUntilApplied(1),
        applyThenWaitUntilApplied(2);

        public final byte val;

        ReadType(int val)
        {
            this.val = (byte) val;
        }

        public static ReadType valueOf(int val)
        {
            switch (val)
            {
                case 0:
                    return readTxnData;
                case 1:
                    return waitUntilApplied;
                case 2:
                    return applyThenWaitUntilApplied;
                default:
                    throw new IllegalArgumentException("Unrecognized ReadType value " + val);
            }
        }
    }

    // TODO (expected, cleanup): should this be a Route?
    public final Participants<?> readScope;
    private final long waitForEpoch;

    private Data data;
    transient BitSet waitingOn;
    transient int waitingOnCount;
    transient Ranges unavailable;

    public ReadData(Node.Id to, Topologies topologies, TxnId txnId, Participants<?> readScope)
    {
        super(txnId);
        int startIndex = latestRelevantEpochIndex(to, topologies, readScope);
        this.readScope = TxnRequest.computeScope(to, topologies, readScope, startIndex, Participants::slice, Participants::with);
        this.waitForEpoch = computeWaitForEpoch(to, topologies, startIndex);
    }

    protected ReadData(TxnId txnId, Participants<?> readScope, long waitForEpoch)
    {
        super(txnId);
        this.readScope = readScope;
        this.waitForEpoch = waitForEpoch;
    }

    abstract public ReadType kind();

    protected abstract void cancel();
    protected abstract long executeAtEpoch();
    protected abstract void reply(@Nullable Ranges unavailable, @Nullable Data data, @Nullable Throwable fail);

    protected void onAllReadsComplete() {}

    @Override
    public long waitForEpoch()
    {
        return waitForEpoch;
    }

    @Override
    protected void process()
    {
        waitingOn = new BitSet();
        node.mapReduceConsumeLocal(this, readScope, executeAtEpoch(), executeAtEpoch(), this);
    }

    @Override
    public ReadNack reduce(ReadNack r1, ReadNack r2)
    {
        return r1 == null || r2 == null
                ? r1 == null ? r2 : r1
                : r1.compareTo(r2) >= 0 ? r1 : r2;
    }

    @Override
    public synchronized void accept(ReadNack reply, Throwable failure)
    {
        if (reply != null)
        {
            node.reply(replyTo, replyContext, reply, failure);
        }
        else if (failure != null)
        {
            // TODO (expected, testing): test
            node.reply(replyTo, replyContext, null, failure);
            data = null;
            // TODO (expected, exceptions): probably a better way to handle this, as might not be uncaught
            node.agent().onUncaughtException(failure);
            cancel();
        }

        // Unless failed always ack to indicate setup has completed otherwise the counter never gets to -1
        if (failure == null)
            ack(null);
    }

    private void ack(@Nullable Ranges newUnavailable)
    {
        if (newUnavailable != null && !newUnavailable.isEmpty())
        {
            newUnavailable = newUnavailable.intersecting(readScope);
            if (this.unavailable == null) this.unavailable = newUnavailable;
            else this.unavailable = newUnavailable.with(this.unavailable);
        }

        // wait for -1 to ensure the setup phase has also completed. Setup calls ack in its callback
        // and prevents races where we respond before dispatching all the required reads (if the reads are
        // completing faster than the reads can be setup on all required shards)
        if (-1 == --waitingOnCount)
        {
            onAllReadsComplete();
            reply(this.unavailable, data, null);
        }
    }

    protected synchronized void readComplete(CommandStore commandStore, @Nullable Data result, @Nullable Ranges unavailable)
    {
        Invariants.checkState(waitingOn.get(commandStore.id()), "Txn %s's waiting on does not contain store %d; waitingOn=%s", txnId, commandStore.id(), waitingOn);
        logger.trace("{}: read completed on {}", txnId, commandStore);
        if (result != null)
            data = data == null ? result : data.merge(result);

        waitingOn.clear(commandStore.id());
        ack(unavailable);
    }

    protected AsyncChain<Data> execute(SafeCommandStore safeStore, Timestamp executeAt, PartialTxn txn)
    {
        return txn.read(safeStore, executeAt);
    }

    void read(SafeCommandStore safeStore, Timestamp executeAt, PartialTxn txn)
    {
        CommandStore unsafeStore = safeStore.commandStore();
        Ranges unavailable = safeStore.ranges().unsafeToReadAt(executeAt);

        execute(safeStore, executeAt, txn).begin((next, throwable) -> {
            if (throwable != null)
            {
                // TODO (expected, exceptions): should send exception to client, and consistency handle/propagate locally
                logger.trace("{}: read failed for {}: {}", txnId, unsafeStore, throwable);
                node.reply(replyTo, replyContext, null, throwable);
                cancel();
            }
            else
                readComplete(unsafeStore, next, unavailable);
        });
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    public interface ReadReply extends Reply
    {
        boolean isOk();
    }

    public enum ReadNack implements ReadReply
    {
        Invalid, NotCommitted, Redundant;

        @Override
        public String toString()
        {
            return "Read" + name();
        }

        @Override
        public MessageType type()
        {
            return READ_RSP;
        }

        @Override
        public boolean isOk()
        {
            return false;
        }

        @Override
        public boolean isFinal()
        {
            return this != NotCommitted;
        }
    }

    public static class ReadOk implements ReadReply
    {
        /**
         * if the replica we contacted was unable to fully answer the query, due to bootstrapping some portion,
         * this is set to the ranges that were unavailable
         *
         * TODO (required): narrow to only the *intersecting* ranges that are unavailable, or do so on the recipient
         */
        public final @Nullable Ranges unavailable;

        public final @Nullable Data data;

        public ReadOk(@Nullable Ranges unavailable, @Nullable Data data)
        {
            this.unavailable = unavailable;
            this.data = data;
        }

        @Override
        public String toString()
        {
            return "ReadOk{" + data + (unavailable == null ? "" : ", unavailable:" + unavailable) + '}';
        }

        @Override
        public MessageType type()
        {
            return READ_RSP;
        }

        @Override
        public boolean isOk()
        {
            return true;
        }
    }
}