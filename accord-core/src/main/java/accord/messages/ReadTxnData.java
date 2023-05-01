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
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;

// TODO (required, efficiency): dedup - can currently have infinite pending reads that will be executed independently
public class ReadTxnData extends AbstractExecute
{
    public static class SerializerSupport
    {
        public static ReadTxnData create(TxnId txnId, Participants<?> scope, long executeAtEpoch, long waitForEpoch)
        {
            return new ReadTxnData(txnId, scope, executeAtEpoch, waitForEpoch);
        }
    }

    public ReadTxnData(Node.Id to, Topologies topologies, TxnId txnId, Participants<?> readScope, Timestamp executeAt)
    {
        super(to, topologies, txnId, readScope, executeAt);
    }

    public ReadTxnData(TxnId txnId, Participants<?> readScope, long waitForEpoch, long executeAtEpoch)
    {
        super(txnId, readScope, waitForEpoch, executeAtEpoch);
    }

    protected boolean canExecutePreApplied()
    {
        return false;
    }

    @Override
    public MessageType type()
    {
        return MessageType.READ_REQ;
    }
}
