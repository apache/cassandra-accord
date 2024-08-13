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

import java.util.Arrays;

import accord.api.RoutingKey;

public class FullRangeRoute extends RangeRoute implements FullRoute<Range>
{
    public static class SerializationSupport
    {
        public static FullRangeRoute create(RoutingKey homeKey, Range[] ranges)
        {
            return new FullRangeRoute(homeKey, ranges);
        }
    }

    public FullRangeRoute(RoutingKey homeKey, Range[] ranges)
    {
        super(homeKey, ranges);
    }

    @Override
    public UnseekablesKind kind()
    {
        return UnseekablesKind.FullRangeRoute;
    }

    @Override
    public Route<Range> slice(int from, int to)
    {
        if (from == 0 && to == size())
            return this;
        return new PartialRangeRoute(homeKey, Arrays.copyOfRange(ranges, from, to));
    }

    @Override
    public FullRangeRoute withHomeKey()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return "{homeKey:" + homeKey + ',' + super.toString() + '}';
    }

}
