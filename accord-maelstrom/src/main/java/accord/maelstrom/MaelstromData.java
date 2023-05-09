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

import java.util.TreeMap;

import accord.api.Data;
import accord.api.DataResolver;
import accord.api.Key;
import accord.api.Read;
import accord.api.ResolveResult;
import accord.api.UnresolvedData;
import accord.primitives.Timestamp;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;

public class MaelstromData extends TreeMap<Key, Value> implements Data, UnresolvedData, DataResolver
{
    public static final MaelstromData EMPTY = new MaelstromData();

    @Override
    public MaelstromData merge(Data data)
    {
        if (data != null)
            this.putAll(((MaelstromData)data));
        return this;
    }

    @Override
    public UnresolvedData merge(UnresolvedData unresolvedData)
    {
        return merge((Data)unresolvedData);
    }

    @Override
    public AsyncChain<ResolveResult> resolve(Timestamp executeAt, Read read, UnresolvedData unresolvedData, FollowupReader followUpReader)
    {
        return AsyncChains.success(new ResolveResult((MaelstromData)unresolvedData, MaelstromWrite.EMPTY));
    }
}
