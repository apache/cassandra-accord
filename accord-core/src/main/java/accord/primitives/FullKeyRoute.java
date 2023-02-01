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

package accord.primitives;

import accord.api.RoutingKey;
import accord.utils.Invariants;

public class FullKeyRoute extends KeyRoute implements FullRoute<RoutingKey>
{
    public static class SerializationSupport
    {
        public static FullKeyRoute create(RoutingKey homeKey, RoutingKey[] keys)
        {
            return new FullKeyRoute(homeKey, keys);
        }
    }

    public FullKeyRoute(RoutingKey homeKey, RoutingKey[] keys)
    {
        super(homeKey, keys);
    }

    @Override
    public UnseekablesKind kind()
    {
        return UnseekablesKind.FullKeyRoute;
    }

    @Override
    public boolean covers(Ranges ranges)
    {
        return true;
    }

    @Override
    public FullKeyRoute with(RoutingKey withKey)
    {
        Invariants.checkArgument(contains(withKey));
        return this;
    }

    @Override
    public PartialKeyRoute slice(Ranges newRanges)
    {
        return new PartialKeyRoute(newRanges, homeKey, slice(newRanges, RoutingKey[]::new));
    }

    @Override
    public PartialKeyRoute sliceStrict(Ranges ranges)
    {
        return slice(ranges);
    }

    @Override
    public FullKeyRoute toMaximalUnseekables()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return "{homeKey:" + homeKey + ',' + super.toString() + '}';
    }

}
