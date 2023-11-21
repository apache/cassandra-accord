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

/**
 * A marker interface for cases where we do not include any non-participating home key in the collection.
 * Note that this interface is implemented by classes that may or may not contain only participants; it is expected
 * that these classes will only be used via interfaces such as Route that do not extend this interface,
 * so that the implementation may return itself when suitable, and a converted copy otherwise.
 */
public interface Participants<K extends Unseekable> extends Unseekables<K>
{
    Participants<K> with(Participants<K> with);

    @Override
    Participants<K> intersect(Unseekables<?> with);
    @Override
    Participants<K> slice(Ranges ranges);
    @Override
    Participants<K> slice(Ranges ranges, Slice slice);
    @Override
    Participants<K> subtract(Ranges ranges);
    Participants<K> subtract(Unseekables<?> without);

    Ranges toRanges();

    static boolean isParticipants(Unseekables<?> unseekables)
    {
        switch (unseekables.kind())
        {
            default: throw new AssertionError("Unhandled Unseekables.Kind: " + unseekables.kind());
            case FullKeyRoute:
            case FullRangeRoute:
            case PartialKeyRoute:
            case PartialRangeRoute:
                return Route.castToRoute(unseekables).isParticipatingHomeKey();
            case RoutingRanges:
            case RoutingKeys:
                return true;
        }
    }

    static Participants<?> participants(Unseekables<?> unseekables)
    {
        switch (unseekables.kind())
        {
            default: throw new AssertionError("Unhandled Unseekables.Kind: " + unseekables.kind());
            case FullKeyRoute:
            case FullRangeRoute:
            case PartialKeyRoute:
            case PartialRangeRoute:
                return Route.castToRoute(unseekables).participants();
            case RoutingRanges:
                return (Ranges) unseekables;
            case RoutingKeys:
                return (RoutingKeys) unseekables;
        }
    }

    /**
     * If both left and right are a Route, invoke {@link Route#union} on them. Otherwise invoke {@link #with}.
     */
    static <K extends Unseekable> Participants<K> merge(Participants<K> left, Participants<K> right)
    {
        if (left == null) return right;
        if (right == null) return left;
        return left.with(right);
    }
}
