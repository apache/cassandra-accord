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

import accord.api.*;
import accord.local.SafeCommandStore;
import accord.primitives.*;
import accord.primitives.Ranges;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
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
    public AsyncChain<Data> read(Seekable key, Txn.Kind kind, SafeCommandStore commandStore, Timestamp executeAt, DataStore store)
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
