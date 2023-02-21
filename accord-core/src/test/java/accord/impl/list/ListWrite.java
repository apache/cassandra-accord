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

import java.util.Arrays;
import java.util.TreeMap;
import java.util.stream.Collectors;

import accord.api.Key;
import accord.api.DataStore;
import accord.api.Write;
import accord.local.SafeCommandStore;
import accord.primitives.Seekable;
import accord.primitives.Timestamp;
import accord.primitives.Writes;
import accord.utils.Timestamped;
import accord.utils.async.AsyncChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListWrite extends TreeMap<Key, int[]> implements Write
{
    private static final Logger logger = LoggerFactory.getLogger(ListWrite.class);

    @Override
    public AsyncChain<Void> apply(Seekable key, SafeCommandStore commandStore, Timestamp executeAt, DataStore store)
    {
        ListStore s = (ListStore) store;
        if (!containsKey(key))
            return Writes.SUCCESS;
        int[] data = get(key);
        s.data.merge((Key)key, new Timestamped<>(executeAt, data), Timestamped::merge);
        logger.trace("WRITE on {} at {} key:{} -> {}", s.node, executeAt, key, data);
        return Writes.SUCCESS;
    }

    @Override
    public String toString()
    {
        return entrySet().stream()
                         .map(e -> e.getKey() + ":" + Arrays.toString(e.getValue()))
                         .collect(Collectors.joining(", ", "{", "}"));
    }
}
