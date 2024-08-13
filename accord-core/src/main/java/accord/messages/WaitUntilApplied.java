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

import accord.api.Data;
import accord.local.Command;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.TxnId;
import accord.topology.Topologies;

import static accord.local.SaveStatus.Applied;
import static accord.local.SaveStatus.TruncatedApply;

/**
 * Wait until the transaction has been applied locally
 */
public class WaitUntilApplied extends ReadData
{
    public static class SerializerSupport
    {
        public static WaitUntilApplied create(TxnId txnId, Participants<?> scope, long executeAtEpoch)
        {
            return new WaitUntilApplied(txnId, scope, executeAtEpoch);
        }
    }

    private static final ExecuteOn EXECUTE_ON = new ExecuteOn(Applied, TruncatedApply);
    private long retryInLaterEpoch;

    public WaitUntilApplied(Node.Id to, Topologies topologies, TxnId txnId, Participants<?> readScope, long executeAtEpoch)
    {
        super(to, topologies, txnId, readScope, executeAtEpoch);
    }

    protected WaitUntilApplied(TxnId txnId, Participants<?> readScope, long executeAtEpoch)
    {
        super(txnId, readScope, executeAtEpoch);
    }

    @Override
    protected ExecuteOn executeOn()
    {
        return EXECUTE_ON;
    }

    @Override
    public ReadType kind()
    {
        return ReadType.waitUntilApplied;
    }

    @Override
    void read(SafeCommandStore safeStore, Command command)
    {
        long retryInLaterEpoch = ReadEphemeralTxnData.retryInLaterEpoch(executeAtEpoch, safeStore, command);
        if (retryInLaterEpoch > this.retryInLaterEpoch)
            this.retryInLaterEpoch = retryInLaterEpoch;
        onOneSuccess(safeStore.commandStore().id(), unavailable(safeStore, command));
    }

    @Override
    protected ReadOk constructReadOk(Ranges unavailable, Data data)
    {
        if (retryInLaterEpoch > 0)
            return new ReadOkWithFutureEpoch(unavailable, data, retryInLaterEpoch);
        return new ReadOk(unavailable, data);
    }

    @Override
    public MessageType type()
    {
        return MessageType.WAIT_UNTIL_APPLIED_REQ;
    }

    @Override
    public String toString()
    {
        return "WaitUntilApplied{" +
               "txnId:" + txnId +
               '}';
    }
}
