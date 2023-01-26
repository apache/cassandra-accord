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

package accord.utils;

public interface IndexedFoldToLong<P1>
{
    /**
     * Apply some long->long merge function accepting a constant object parameter p1, a constant long parameter p2,
     * and the prior output of this function or the initial value, to some element of a collection,
     * with the index of the element provided.
     *
     * This function is used for efficiently folding over some subset of a collection.
     */
    long apply(P1 p1, long p2, long accumulate, int index);
}
