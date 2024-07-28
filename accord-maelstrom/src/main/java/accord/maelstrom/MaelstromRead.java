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

import accord.api.Data;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.Read;
import accord.local.SafeCommandStore;
import accord.primitives.Keys;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Seekable;
import accord.primitives.Timestamp;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;

public class MaelstromRead implements Read
{
    final Keys readKeys;
    final Keys keys;

    public MaelstromRead(Keys readKeys, Keys keys)
    {
        this.readKeys = readKeys;
        this.keys = keys;
    }

    @Override
    public Keys keys()
    {
        return keys;
    }

    @Override
    public AsyncChain<Data> read(Seekable key, SafeCommandStore commandStore, Timestamp executeAt, DataStore store)
    {
        MaelstromStore s = (MaelstromStore)store;
        MaelstromData result = new MaelstromData();
        result.put((Key)key, s.get((Key)key));
        return AsyncChains.success(result);
    }

    @Override
    public Read slice(Ranges ranges)
    {
        return new MaelstromRead(readKeys.slice(ranges), keys.slice(ranges));
    }

    @Override
    public Read intersecting(Participants<?> participants)
    {
        return new MaelstromRead(readKeys.intersecting(participants), keys.intersecting(participants));
    }

    @Override
    public Read merge(Read other)
    {
        MaelstromRead that = (MaelstromRead) other;
        Keys readKeys = this.readKeys.with(that.readKeys);
        Keys keys = this.keys.with(that.keys);
        if (readKeys == this.readKeys && keys == this.keys)
            return this;
        if (readKeys == that.readKeys && keys == that.keys)
            return that;
        return new MaelstromRead(readKeys.with(that.readKeys), keys.with(that.keys));
    }
}
