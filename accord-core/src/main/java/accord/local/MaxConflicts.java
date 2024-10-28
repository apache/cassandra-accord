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
package accord.local;

import accord.api.RoutingKey;
import accord.primitives.Routables;
import accord.primitives.Timestamp;
import accord.primitives.Unseekables;
import accord.utils.BTreeReducingRangeMap;

// TODO (expected): track read/write conflicts separately
public class MaxConflicts extends BTreeReducingRangeMap<Timestamp>
{
    public static final MaxConflicts EMPTY = new MaxConflicts();

    private MaxConflicts()
    {
        super();
    }

    private MaxConflicts(boolean inclusiveEnds, Object[] tree)
    {
        super(inclusiveEnds, tree);
    }

    Timestamp get(Routables<?> keysOrRanges)
    {
        return foldl(keysOrRanges, Timestamp::max, Timestamp.NONE);
    }

    public MaxConflicts update(Unseekables<?> keysOrRanges, Timestamp maxConflict)
    {
        return update(this, keysOrRanges, maxConflict, Timestamp::max, MaxConflicts::new, Builder::new);
    }

    private static class Builder extends AbstractBoundariesBuilder<RoutingKey, Timestamp, MaxConflicts>
    {
        protected Builder(boolean inclusiveEnds, int capacity)
        {
            super(inclusiveEnds, capacity);
        }

        @Override
        protected MaxConflicts buildInternal(Object[] tree)
        {
            return new MaxConflicts(inclusiveEnds, tree);
        }
    }
}
