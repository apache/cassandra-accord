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

public class TxnBlockedByInfo
{
    public final String txnId;
    public final String keyspaceName;
    public final String tableName;
    public final int commandStoreId;
    public final int depth;
    public final String blockedBy;
    public final String reason;
    public final String saveStatus;
    public final String executeAt;
    public final String key;
    
    public TxnBlockedByInfo(String txnId, String keyspaceName, String tableName, int commandStoreId, int depth,
                            String blockedBy, String reason, String saveStatus, String executeAt, String key)
    {
        this.txnId = txnId;
        this.keyspaceName = keyspaceName;
        this.tableName = tableName;
        this.commandStoreId = commandStoreId;
        this.depth = depth;
        this.blockedBy = blockedBy;
        this.reason = reason;
        this.saveStatus = saveStatus;
        this.executeAt = executeAt;
        this.key = key;
    }
}