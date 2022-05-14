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
import accord.primitives.KeyRanges;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

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
    public Future<Data> read(Key key, Txn.Kind kind, SafeCommandStore commandStore, Timestamp executeAt, DataStore store)
    {
        MaelstromStore s = (MaelstromStore)store;
        MaelstromData result = new MaelstromData();
        result.put(key, s.get(key));
        return ImmediateFuture.success(result);
    }

    @Override
    public Read slice(KeyRanges ranges)
    {
        return new MaelstromRead(readKeys.slice(ranges), keys.slice(ranges));
    }

    @Override
    public Read merge(Read other)
    {
        MaelstromRead that = (MaelstromRead) other;
        Keys readKeys = this.readKeys.union(that.readKeys);
        Keys keys = this.keys.union(that.keys);
        if (readKeys == this.readKeys && keys == this.keys)
            return this;
        if (readKeys == that.readKeys && keys == that.keys)
            return that;
        return new MaelstromRead(readKeys.union(that.readKeys), keys.union(that.keys));
    }
}
