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

public interface IndexedRangeFoldToLong
{
    /**
     * Apply some long->long merge function accepting a constant parameter p1 and the prior output of this
     * function or an initial value, to some range of indexes referencing some other indexed collection.
     *
     * This function is used for folding over a filtered collection, where contiguous ranges are expected
     * and can be handled batch-wise.
     */
    long apply(long p1, long accumulator, int fromIndex, int toIndex);
}