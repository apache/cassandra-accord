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

public class RedundantBeforeInfo
{
    public final String keyspaceName;
    public final String tableName;
    public final String tableId;
    // TODO: group by token start / end in the UI
    public final String tokenStart;
    public final String tokenEnd;
    public final int commandStoreId;
    public final long startEpoch;
    public final long endEpoch;
    public final String gcBefore;
    public final String shardApplied;
    public final String quorumApplied;
    public final String locallyApplied;
    public final String locallyDurableToCommandStore;
    public final String locallyDurableToDataStore;
    public final String locallyRedundant;
    public final String locallySynced;
    public final String locallyWitnessed;
    public final String preBootstrap;
    public final String staleUntilAtLeast;
    
    public RedundantBeforeInfo(String keyspaceName, String tableName, String tableId, String tokenStart, String tokenEnd,
                               int commandStoreId, long startEpoch, long endEpoch, String gcBefore, String shardApplied,
                               String quorumApplied, String locallyApplied, String locallyDurableToCommandStore,
                               String locallyDurableToDataStore, String locallyRedundant, String locallySynced,
                               String locallyWitnessed, String preBootstrap, String staleUntilAtLeast)
    {
        this.keyspaceName = keyspaceName;
        this.tableName = tableName;
        this.tableId = tableId;
        this.tokenStart = tokenStart;
        this.tokenEnd = tokenEnd;
        this.commandStoreId = commandStoreId;
        this.startEpoch = startEpoch;
        this.endEpoch = endEpoch;
        this.gcBefore = gcBefore;
        this.shardApplied = shardApplied;
        this.quorumApplied = quorumApplied;
        this.locallyApplied = locallyApplied;
        this.locallyDurableToCommandStore = locallyDurableToCommandStore;
        this.locallyDurableToDataStore = locallyDurableToDataStore;
        this.locallyRedundant = locallyRedundant;
        this.locallySynced = locallySynced;
        this.locallyWitnessed = locallyWitnessed;
        this.preBootstrap = preBootstrap;
        this.staleUntilAtLeast = staleUntilAtLeast;
    }
}