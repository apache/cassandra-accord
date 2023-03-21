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

import accord.api.Key;

/**
 * Something that can be found within the cluster AND found on disk, queried and returned
 */
public interface Seekable extends Routable
{
    /**
     * If this is a key, it will return itself, otherwise it throw an exception
     */
    Key asKey();

    /**
     * If this is a range, it will return itself, otherwise it throw an exception
     */
    Range asRange();

    /**
     * Returns a {@link Seekable} of the same type as this one truncated to the given range (intersection).
     * If the provided range does not intersect with this {@link Seekable}, an exception is thrown.
     */
    Seekable slice(Range range);
}
