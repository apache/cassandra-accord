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

import javax.annotation.Nullable;

public interface Route<K extends Unseekable> extends Participants<K>
{
    RoutingKey homeKey();
    Route<?> homeKeyOnlyRoute();

    boolean isHomeKeyOnlyRoute();
    default boolean isRoute() { return true; }

    /**
     * Return an object containing any {@code K} present in either of the original collections
     *
     * Differs from {@link Unseekables#with} in that the parameter must be a {@link Route}
     * and the result will be a {@link Route}.
     */
    @Override
    Route<K> with(Participants<K> participants);

    @Override
    Route<K> slice(int from, int to);
    @Override
    Route<K> slice(Ranges ranges);
    @Override
    Route<K> slice(Ranges ranges, Slice slice);

    @Override
    Route<K> intersecting(Unseekables<?> intersecting);
    @Override
    Route<K> intersecting(Unseekables<?> intersecting, Slice slice);

    Route<K> withHomeKey();
    Route<K> with(Unseekables<K> with);

    /**
     * Do any of the parts of the route that are not exclusively a homeKey intersect with the provided ranges
     */
    boolean participatesIn(Ranges ranges);

    /**
     * Return the unseekables excluding any coordination-only home key
     */
    Participants<K> participants();

    /**
     * Return the unseekables excluding any coordination-only home key, that intersect the provided ranges
     */
    Participants<K> participants(Ranges ranges);

    /**
     * Return the unseekables excluding any coordination-only home key, that intersect the provided ranges
     */
    Participants<K> participants(Ranges ranges, Slice slice);

    // this method exists solely to circumvent JDK bug with testing and casting interfaces
    static boolean isFullRoute(@Nullable Unseekables<?> unseekables) { return unseekables != null && unseekables.kind().isFullRoute(); }

    // this method exists solely to circumvent JDK bug with testing and casting interfaces
    static boolean isRoute(@Nullable Unseekables<?> unseekables) { return unseekables != null && unseekables.kind().isRoute(); }

    // this method exists solely to circumvent JDK bug with testing and casting interfaces
    static FullRoute<?> castToFullRoute(@Nullable Unseekables<?> unseekables)
    {
        if (unseekables == null)
            return null;

        switch (unseekables.domain())
        {
            default: throw new ClassCastException(unseekables + " is not a FullRoute");
            case Key: return (FullKeyRoute) unseekables;
            case Range: return (FullRangeRoute) unseekables;
        }
    }

    // this method exists solely to circumvent JDK bug with testing and casting interfaces
    static FullRoute<?> castToNonNullFullRoute(@Nullable Unseekables<?> unseekables)
    {
        FullRoute<?> route = tryCastToFullRoute(unseekables);
        if (route == null)
            throw new ClassCastException(unseekables + " is not a FullRoute");
        return route;
    }

    // this method exists solely to circumvent JDK bug with testing and casting interfaces
    static FullRoute<?> tryCastToFullRoute(@Nullable Unseekables<?> unseekables)
    {
        if (unseekables == null)
            return null;

        switch (unseekables.domain())
        {
            default: return null;
            case Key: return unseekables instanceof FullKeyRoute ? (FullKeyRoute) unseekables : null;
            case Range: return unseekables instanceof FullRangeRoute ? (FullRangeRoute) unseekables : null;
        }
    }

    static Route<?> castToRoute(@Nullable Unseekables<?> unseekables)
    {
        if (unseekables == null)
            return null;

        switch (unseekables.domain())
        {
            default: throw new AssertionError();
            case Key: return (KeyRoute) unseekables;
            case Range: return (RangeRoute) unseekables;
        }
    }

    // this method exists solely to circumvent JDK bug with testing and casting interfaces
    static Route<?> tryCastToRoute(@Nullable Unseekables<?> unseekables)
    {
        if (unseekables == null)
            return null;

        switch (unseekables.kind())
        {
            default: throw new AssertionError();
            case RoutingKeys:
            case RoutingRanges:
                return null;
            case PartialKeyRoute:
                return (PartialKeyRoute) unseekables;
            case PartialRangeRoute:
                return (PartialRangeRoute) unseekables;
            case FullKeyRoute:
                return (FullKeyRoute) unseekables;
            case FullRangeRoute:
                return (FullRangeRoute) unseekables;
        }
    }

    // this method exists solely to circumvent JDK bug with testing and casting interfaces
    static PartialRoute<?> castToPartialRoute(@Nullable Unseekables<?> unseekables)
    {
        if (unseekables == null)
            return null;

        switch (unseekables.domain())
        {
            default: throw new AssertionError();
            case Key: return (PartialKeyRoute) unseekables;
            case Range: return (PartialRangeRoute) unseekables;
        }
    }

    static <T extends Unseekable> Route<T> merge(@Nullable Route<T> prefer, @Nullable Route<T> defer)
    {
        if (defer == null) return prefer;
        if (prefer == null) return defer;
        return prefer.with(defer);
    }
}
