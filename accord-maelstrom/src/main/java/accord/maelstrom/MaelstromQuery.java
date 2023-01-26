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

package accord.maelstrom;

import java.util.Map;

import accord.api.Data;
import accord.api.Key;
import accord.api.Query;
import accord.api.Read;
import accord.api.Result;
import accord.api.Update;
import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import javax.annotation.Nonnull;

public class MaelstromQuery implements Query
{
    final Node.Id client;
    final long requestId;

    public MaelstromQuery(Id client, long requestId)
    {
        this.client = client;
        this.requestId = requestId;
    }

    @Override
    public Result compute(@Nonnull TxnId txnId, @Nonnull Timestamp executeAt, @Nonnull Seekables<?, ?> keys, Data data, Read untypedRead, Update update)
    {
        MaelstromRead read = (MaelstromRead) untypedRead;
        Value[] values = new Value[read.readKeys.size()];
        for (Map.Entry<Key, Value> e : ((MaelstromData)data).entrySet())
            values[read.readKeys.indexOf(e.getKey())] = e.getValue();
        return new MaelstromResult(client, requestId, read.readKeys, values, (MaelstromUpdate) update);
    }
}
