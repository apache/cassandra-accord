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

import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import accord.api.Key;
import accord.local.Node;
import accord.api.DataStore;
import accord.primitives.Range;
import accord.primitives.RoutableKey;
import accord.primitives.Seekable;
import accord.primitives.Timestamp;
import accord.utils.Timestamped;

public class ListStore implements DataStore
{
    static final int[] EMPTY = new int[0];
    final NavigableMap<RoutableKey, Timestamped<int[]>> data = new TreeMap<>();

    // adding here to help trace burn test queries
    public final Node.Id node;

    public ListStore(Node.Id node)
    {
        this.node = node;
    }

    public int[] get(Key key)
    {
        Timestamped<int[]> v = data.get(key);
        return v == null ? EMPTY : v.data;
    }

    public List<Map.Entry<Key, int[]>> get(Range range)
    {
        return data.subMap(range.start(), range.startInclusive(), range.end(), range.endInclusive())
                .entrySet()
                .stream().map(e -> new SimpleEntry<>((Key)e.getKey(), e.getValue().data))
                .collect(Collectors.toList());
    }

    public synchronized void write(Key key, Timestamp executeAt, int[] value)
    {
        data.merge(key, new Timestamped<>(executeAt, value), Timestamped::merge);
    }
}
