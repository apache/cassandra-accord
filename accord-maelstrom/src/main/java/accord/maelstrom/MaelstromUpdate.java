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

import accord.api.Key;
import accord.api.Data;
import accord.api.Update;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Keys;
import accord.primitives.Routables;
import accord.primitives.Timestamp;

public class MaelstromUpdate extends TreeMap<Key, Value> implements Update
{
    @Override
    public Keys keys()
    {
        return new Keys(navigableKeySet());
    }

    @Override
    public MaelstromWrite apply(Timestamp executeAt, Data read)
    {
        MaelstromWrite write = new MaelstromWrite();
        Map<Key, Value> data = (MaelstromData)read;
        for (Map.Entry<Key, Value> e : entrySet())
            write.put(e.getKey(), data.get(e.getKey()).append(e.getValue()));
        return write;
    }

    @Override
    public Update slice(Ranges ranges)
    {
        return intersecting((Routables<?>) ranges);
    }

    @Override
    public Update intersecting(Participants<?> participants)
    {
        return intersecting((Routables<?>) participants);
    }

    public Update intersecting(Routables<?> routables)
    {
        MaelstromUpdate result = new MaelstromUpdate();
        for (Map.Entry<Key, Value> e : entrySet())
        {
            if (routables.contains(e.getKey()))
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
