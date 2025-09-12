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

package accord.debug.controller;

import java.util.List;

import accord.debug.model.*;

public interface Controller
{
    public List<NodeInfo> getNodes();
    public List<RedundantBeforeInfo> getRedundantBefore(String nodeId);
    public List<CoordinationInfo> getCoordinations(String nodeId);
    public List<TxnInfo> getTxn(String nodeId, String txnId);
    public List<TxnBlockedByInfo> getTxnBlockedBy(String nodeId, String txnId);
    public List<ProgressLogInfo> getProgressLog(String nodeId);
    public List<DurabilityServiceInfo> getDurabilityService(String nodeId);
    public List<CommandStoreInfo> getCommandStores(String nodeId);
    public List<DurableBeforeInfo> getDurableBefore(String nodeId);
    public List<TopologyInfo> getTopologies(String nodeId);
    public List<TxnInfo> getTransactions(String nodeId, int commandStoreId, String propertyFilter);
    public List<CommandsForKeyInfo> getCommandsForKey(String nodeId, String key);
    public TxnInfo getTransaction(String nodeId, int commandStoreId, String txnId);
    // TODO: CommandsForKeys
}