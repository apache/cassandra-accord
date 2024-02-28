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

import accord.local.Node;
import accord.primitives.Participants;
import accord.primitives.TxnId;
import accord.topology.Topologies;

import static accord.local.SaveStatus.ReadyToExecute;

// TODO (required, efficiency): dedup - can currently have infinite pending reads that will be executed independently
public class ReadTxnData extends ReadData
{
    public static class SerializerSupport
    {
        public static ReadTxnData create(TxnId txnId, Participants<?> scope, long executeAtEpoch)
        {
            return new ReadTxnData(txnId, scope, executeAtEpoch);
        }
    }

    private static final ExecuteOn EXECUTE_ON = new ExecuteOn(ReadyToExecute, ReadyToExecute);

    public ReadTxnData(Node.Id to, Topologies topologies, TxnId txnId, Participants<?> readScope, long executeAtEpoch)
    {
        super(to, topologies, txnId, readScope, executeAtEpoch);
    }

    public ReadTxnData(TxnId txnId, Participants<?> readScope, long executeAtEpoch)
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
        return ReadType.readTxnData;
    }

    @Override
    public MessageType type()
    {
        return MessageType.READ_REQ;
    }
}
