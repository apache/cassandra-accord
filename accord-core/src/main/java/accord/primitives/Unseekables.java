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

/**
 * Either a Route or a simple collection of keys or ranges
 */
public interface Unseekables<K extends Unseekable> extends Iterable<K>, Routables<K>
{
    enum UnseekablesKind
    {
        RoutingKeys, PartialKeyRoute, FullKeyRoute, RoutingRanges, PartialRangeRoute, FullRangeRoute;

        public boolean isRoute()
        {
            return this != RoutingKeys & this != RoutingRanges;
        }

        public boolean isFullRoute()
        {
            return this == FullKeyRoute | this == FullRangeRoute;
        }
    }

    @Override
    Unseekables<K> intersecting(Unseekables<?> intersecting);
    @Override
    Unseekables<K> intersecting(Unseekables<?> intersecting, Slice slice);

    @Override
    Unseekables<K> slice(int from, int to);
    @Override
    Unseekables<K> slice(Ranges ranges);
    @Override
    Unseekables<K> slice(Ranges ranges, Slice slice);

    Unseekables<K> without(Ranges ranges);
    Unseekables<K> without(Unseekables<?> subtract);

    /**
     * Return an object containing any {@code K} present in either of the original collections.
     *
     * Differs from {@link Route#with} in that the parameter does not need to be a {@link Route}
     * and the result may not be a {@link Route}, as the new object would not know the range
     * covered by the additional keys or ranges.
     */
    Unseekables<K> with(Unseekables<K> with);
    Unseekables<K> with(RoutingKey withKey);
    UnseekablesKind kind();

    /**
     * If both left and right are a Route, invoke {@link Route#with} on them. Otherwise invoke {@link #with}.
     */
    static <K extends Unseekable> Unseekables<K> merge(Unseekables<K> left, Unseekables<K> right)
    {
        if (left == null) return right;
        if (right == null) return left;

        UnseekablesKind leftKind = left.kind();
        UnseekablesKind rightKind = right.kind();
        if (leftKind.isRoute() || rightKind.isRoute())
        {
            if (leftKind.isRoute() != rightKind.isRoute())
            {
                // one is a route, one is not
                if (leftKind.isRoute() && left.containsAll(right))
                    return left;
                if (rightKind.isRoute() && right.containsAll(left))
                    return right;

                // non-route types can always accept route types as input, so just call its union method on the other
                return left.with(right);
            }

            if (leftKind.isFullRoute())
                return left;

            if (rightKind.isFullRoute())
                return right;

            return ((Route)left).with((Route)right);
        }

        return left.with(right);
    }
}
