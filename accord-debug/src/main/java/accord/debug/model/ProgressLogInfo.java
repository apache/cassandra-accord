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

package accord.debug.model;

public class ProgressLogInfo
{
    public final String keyspaceName;
    public final String tableName;
    public final String tableId;
    public final int commandStoreId;
    public final String txnId;
    public final boolean contactEveryone;
    public final boolean waitingIsUninitialised;
    public final String waitingBlockedUntil;
    public final String waitingHomeSatisfies;
    public final String waitingProgress;
    public final int waitingRetryCounter;
    public final String waitingPackedKeyTrackerBits;
    public final long waitingScheduledAt;
    public final String homePhase;
    public final String homeProgress;
    public final int homeRetryCounter;
    public final long homeScheduledAt;
    
    public ProgressLogInfo(String keyspaceName, String tableName, String tableId, int commandStoreId, String txnId,
                           boolean contactEveryone, boolean waitingIsUninitialised, String waitingBlockedUntil,
                           String waitingHomeSatisfies, String waitingProgress, int waitingRetryCounter,
                           String waitingPackedKeyTrackerBits, long waitingScheduledAt, String homePhase,
                           String homeProgress, int homeRetryCounter, long homeScheduledAt)
    {
        this.keyspaceName = keyspaceName;
        this.tableName = tableName;
        this.tableId = tableId;
        this.commandStoreId = commandStoreId;
        this.txnId = txnId;
        this.contactEveryone = contactEveryone;
        this.waitingIsUninitialised = waitingIsUninitialised;
        this.waitingBlockedUntil = waitingBlockedUntil;
        this.waitingHomeSatisfies = waitingHomeSatisfies;
        this.waitingProgress = waitingProgress;
        this.waitingRetryCounter = waitingRetryCounter;
        this.waitingPackedKeyTrackerBits = waitingPackedKeyTrackerBits;
        this.waitingScheduledAt = waitingScheduledAt;
        this.homePhase = homePhase;
        this.homeProgress = homeProgress;
        this.homeRetryCounter = homeRetryCounter;
        this.homeScheduledAt = homeScheduledAt;
    }
}