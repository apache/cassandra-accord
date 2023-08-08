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

/**
 * Something that can be found in the cluster, and MAYBE found on disk (if Seekable)
 */
public interface Routable
{
    enum Domain
    {
        Key, Range;
        private static final Domain[] VALUES = Domain.values();

        public boolean isKey()
        {
            return this == Key;
        }

        public boolean isRange()
        {
            return this == Range;
        }

        public static Routable.Domain ofOrdinal(int ordinal)
        {
            return VALUES[ordinal];
        }

        public char shortName()
        {
            return this == Key ? 'K' : 'R';
        }
    }

    Domain domain();
    Unseekable toUnseekable();

    /**
     * Deterministically select a key that intersects this Routable and the provided Ranges
     */
    RoutingKey someIntersectingRoutingKey(@Nullable Ranges ranges);
}
