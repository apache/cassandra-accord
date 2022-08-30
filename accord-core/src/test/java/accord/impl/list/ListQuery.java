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

package accord.impl.list;

import java.util.Map;

import accord.api.Read;
import accord.api.Update;
import accord.local.Node.Id;
import accord.api.Data;
import accord.api.Key;
import accord.api.Query;
import accord.api.Result;

public class ListQuery implements Query
{
    final Id client;
    final long requestId;

    public ListQuery(Id client, long requestId)
    {
        this.client = client;
        this.requestId = requestId;
    }

    @Override
    public Result compute(Data data, Read untypedRead, Update update)
    {
        ListRead read = (ListRead) untypedRead;
        int[][] values = new int[read.readKeys.size()][];
        for (Map.Entry<Key, int[]> e : ((ListData)data).entrySet())
        {
            int i = read.readKeys.indexOf(e.getKey());
            if (i >= 0)
                values[i] = e.getValue();
        }
        return new ListResult(client, requestId, read.readKeys, values, (ListUpdate) update);
    }
}
