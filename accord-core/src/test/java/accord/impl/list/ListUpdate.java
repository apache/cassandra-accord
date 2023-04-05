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
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import accord.api.Data;
import accord.api.Key;
import accord.api.Update;
import accord.local.CommandStore;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.utils.async.AsyncExecutor;

public class ListUpdate extends TreeMap<Key, Integer> implements Update
{
    private final Function<CommandStore, AsyncExecutor> executor;

    public ListUpdate(Function<CommandStore, AsyncExecutor> executor)
    {
        this.executor = executor;
    }

    @Override
    public Seekables<?, ?> keys()
    {
        return new Keys(navigableKeySet());
    }

    @Override
    public ListWrite apply(Data read)
    {
        ListWrite write = new ListWrite(executor);
        Map<Key, int[]> data = (ListData)read;
        for (Map.Entry<Key, Integer> e : entrySet())
            write.put(e.getKey(), append(data.get(e.getKey()), e.getValue()));
        return write;
    }

    @Override
    public Update slice(Ranges ranges)
    {
        ListUpdate result = new ListUpdate(executor);
        for (Map.Entry<Key, Integer> e : entrySet())
        {
            if (ranges.contains(e.getKey()))
                result.put(e.getKey(), e.getValue());
        }
        return result;
    }

    @Override
    public Update merge(Update other)
    {
        ListUpdate result = new ListUpdate(executor);
        result.putAll(this);
        result.putAll((ListUpdate) other);
        return result;
    }

    private static int[] append(int[] to, int append)
    {
        to = Arrays.copyOf(to, to.length + 1);
        to[to.length - 1] = append;
        return to;
    }

    @Override
    public String toString()
    {
        return entrySet().stream()
                         .map(e -> e.getKey() + ":" + e.getValue())
                         .collect(Collectors.joining(", ", "{", "}"));
    }
}
