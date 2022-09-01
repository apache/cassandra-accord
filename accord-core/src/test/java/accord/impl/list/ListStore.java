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
import java.util.concurrent.ConcurrentHashMap;

import accord.api.Key;
import accord.local.Node;
import accord.api.DataStore;
import accord.utils.Timestamped;

public class ListStore implements DataStore
{
    static final int[] EMPTY = new int[0];
    final Map<Key, Timestamped<int[]>> data = new ConcurrentHashMap<>();

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
}
