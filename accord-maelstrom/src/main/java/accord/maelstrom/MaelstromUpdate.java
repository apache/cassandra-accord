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
import java.util.TreeMap;

import accord.api.Data;
import accord.api.Key;
import accord.api.RepairWrites;
import accord.api.Update;
import accord.primitives.Keys;
import accord.primitives.Ranges;

public class MaelstromUpdate extends TreeMap<Key, Value> implements Update
{
    @Override
    public Keys keys()
    {
        return new Keys(navigableKeySet());
    }

    @Override
    public MaelstromWrite apply(Data read, RepairWrites repairWrites)
    {
        MaelstromWrite write = new MaelstromWrite();

        if (repairWrites != null)
            ((MaelstromWrite)repairWrites).entrySet().forEach(e -> write.putIfAbsent(e.getKey(), e.getValue()));

        Map<Key, Value> data = (MaelstromData)read;
        for (Map.Entry<Key, Value> e : entrySet())
            write.put(e.getKey(), data.get(e.getKey()).append(e.getValue()));

        return write;
    }

    @Override
    public Update slice(Ranges ranges)
    {
        MaelstromUpdate result = new MaelstromUpdate();
        for (Map.Entry<Key, Value> e : entrySet())
        {
            if (ranges.contains(e.getKey()))
                result.put(e.getKey(), e.getValue());
        }
        return result;
    }

    @Override
    public Update merge(Update other)
    {
        MaelstromUpdate result = new MaelstromUpdate();
        result.putAll(this);
        result.putAll((MaelstromUpdate) other);
        return result;
    }
}
