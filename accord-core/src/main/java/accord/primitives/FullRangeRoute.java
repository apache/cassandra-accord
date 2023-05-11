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

public class FullRangeRoute extends RangeRoute implements FullRoute<Range>
{
    public static class SerializationSupport
    {
        public static FullRangeRoute create(RoutingKey homeKey, boolean isParticipatingHomeKey, Range[] ranges)
        {
            return new FullRangeRoute(homeKey, isParticipatingHomeKey, ranges);
        }
    }

    public FullRangeRoute(RoutingKey homeKey, boolean isParticipatingHomeKey, Range[] ranges)
    {
        super(homeKey, isParticipatingHomeKey, ranges);
    }

    @Override
    public UnseekablesKind kind()
    {
        return UnseekablesKind.FullRangeRoute;
    }

    @Override
    public boolean covers(Ranges ranges)
    {
        return true;
    }

    @Override
    public PartialRangeRoute sliceStrict(Ranges ranges)
    {
        return slice(ranges);
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
