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

package accord.impl.progresslog;

// the phase of the distributed state machine
public enum CoordinatePhase
{
    /**
     * This replica is not known to be a home shard of the transaction
     */
    NotInitialised,

    /**
     * not durably decided; if eligible to take over coordination, should see if coordination is stalled and if so
     * take over recovery to ensure an execution decision is reached
     */
    Undecided,

    /**
     * durably decided, but replicas may not be ready to execute; should wait until we can expect to successfully
     * execute the transaction before attempting recovery
     * TODO (expected): this state is not effectively used today, we only wait for the home shard to be ready to execute,
     * whereas we can perform a distributed wait using the WaitingState to ensure we can make progress
     */
    AwaitReadyToExecute,

    /**
     * some replicas of all shards ready to execute; we can expect that the transaction can be successfully executed,
     * and we should now try to ensure this happens when it is our turn to do so
     */
    ReadyToExecute,

    /**
     * The transaction has been durably executed at a majority of replicas of all shards (but not necessarily ourselves)
     */
    Done
    ;

    private static final CoordinatePhase[] lookup = values();

    boolean isAtMostReadyToExecute()
    {
        return compareTo(CoordinatePhase.ReadyToExecute) <= 0;
    }

    public static CoordinatePhase forOrdinal(int ordinal)
    {
        if (ordinal < 0 || ordinal > lookup.length)
            throw new IndexOutOfBoundsException(ordinal);
        return lookup[ordinal];
    }
}
