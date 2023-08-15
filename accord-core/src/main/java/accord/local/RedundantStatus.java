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

import java.util.EnumMap;

import accord.utils.Invariants;

public enum RedundantStatus
{
    /** None of the relevant ranges are owned by the command store */
    NOT_OWNED,

    /** Some of the relevant ranges are owned by the command store and valid for execution */
    LIVE,

    /** The relevant owned ranges are part live and part pre-bootstrap */
    PARTIALLY_PRE_BOOTSTRAP,

    /** The relevant owned ranges are ALL pre-bootstrap, meaning we are fetching the transaction's entire result
     * from another node's snapshot already */
    PRE_BOOTSTRAP,

    /** The relevant owned ranges are fully redundant, meaning they are known applied or invalidated via a sync point
     * that has applied locally */
    LOCALLY_REDUNDANT,
    ;

    private EnumMap<RedundantStatus, RedundantStatus> merge;

    static
    {
        NOT_OWNED.merge = new EnumMap<>(RedundantStatus.class);
        NOT_OWNED.merge.put(NOT_OWNED, NOT_OWNED);
        NOT_OWNED.merge.put(LIVE, LIVE);
        NOT_OWNED.merge.put(PARTIALLY_PRE_BOOTSTRAP, PARTIALLY_PRE_BOOTSTRAP);
        NOT_OWNED.merge.put(PRE_BOOTSTRAP, PRE_BOOTSTRAP);
        NOT_OWNED.merge.put(LOCALLY_REDUNDANT, LOCALLY_REDUNDANT);
        LIVE.merge = new EnumMap<>(RedundantStatus.class);
        LIVE.merge.put(NOT_OWNED, LIVE);
        LIVE.merge.put(LIVE, LIVE);
        LIVE.merge.put(PARTIALLY_PRE_BOOTSTRAP, PARTIALLY_PRE_BOOTSTRAP);
        LIVE.merge.put(PRE_BOOTSTRAP, PARTIALLY_PRE_BOOTSTRAP);
        LIVE.merge.put(LOCALLY_REDUNDANT, LOCALLY_REDUNDANT);
        PARTIALLY_PRE_BOOTSTRAP.merge = new EnumMap<>(RedundantStatus.class);
        PARTIALLY_PRE_BOOTSTRAP.merge.put(NOT_OWNED, PARTIALLY_PRE_BOOTSTRAP);
        PARTIALLY_PRE_BOOTSTRAP.merge.put(LIVE, PARTIALLY_PRE_BOOTSTRAP);
        PARTIALLY_PRE_BOOTSTRAP.merge.put(PARTIALLY_PRE_BOOTSTRAP, PARTIALLY_PRE_BOOTSTRAP);
        PARTIALLY_PRE_BOOTSTRAP.merge.put(PRE_BOOTSTRAP, PARTIALLY_PRE_BOOTSTRAP);
        PARTIALLY_PRE_BOOTSTRAP.merge.put(LOCALLY_REDUNDANT, LOCALLY_REDUNDANT);
        PRE_BOOTSTRAP.merge = new EnumMap<>(RedundantStatus.class);
        PRE_BOOTSTRAP.merge.put(NOT_OWNED, PRE_BOOTSTRAP);
        PRE_BOOTSTRAP.merge.put(LIVE, PARTIALLY_PRE_BOOTSTRAP);
        PRE_BOOTSTRAP.merge.put(PARTIALLY_PRE_BOOTSTRAP, PARTIALLY_PRE_BOOTSTRAP);
        PRE_BOOTSTRAP.merge.put(PRE_BOOTSTRAP, PRE_BOOTSTRAP);
        PRE_BOOTSTRAP.merge.put(LOCALLY_REDUNDANT, LOCALLY_REDUNDANT);
        LOCALLY_REDUNDANT.merge = new EnumMap<>(RedundantStatus.class);
        LOCALLY_REDUNDANT.merge.put(NOT_OWNED, LOCALLY_REDUNDANT);
        LOCALLY_REDUNDANT.merge.put(LIVE, LOCALLY_REDUNDANT);
        LOCALLY_REDUNDANT.merge.put(PARTIALLY_PRE_BOOTSTRAP, LOCALLY_REDUNDANT);
        LOCALLY_REDUNDANT.merge.put(PRE_BOOTSTRAP, LOCALLY_REDUNDANT);
        LOCALLY_REDUNDANT.merge.put(LOCALLY_REDUNDANT, LOCALLY_REDUNDANT);
    }

    public RedundantStatus merge(RedundantStatus that)
    {
        RedundantStatus result = merge.get(that);
        Invariants.checkState(result != null, "Invalid RedundantStatus combination: " + this + " and " + that);
        return result;
    }
}
