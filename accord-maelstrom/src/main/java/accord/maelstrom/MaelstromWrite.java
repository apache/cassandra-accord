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

import accord.api.Key;
import accord.api.DataStore;
import accord.api.Write;
import accord.local.SafeCommandStore;
import accord.primitives.Seekable;
import accord.primitives.Timestamp;
import accord.primitives.Writes;
import accord.utils.Timestamped;
import accord.utils.async.AsyncChain;

import java.util.TreeMap;

public class MaelstromWrite extends TreeMap<Key, Value> implements Write
{
    @Override
    public AsyncChain<Void> apply(Seekable key, SafeCommandStore commandStore, Timestamp executeAt, DataStore store)
    {
        MaelstromStore s = (MaelstromStore) store;
        if (containsKey(key))
            s.data.merge((Key)key, new Timestamped<>(executeAt, get(key)), Timestamped::merge);
        return Writes.SUCCESS;
    }
}
