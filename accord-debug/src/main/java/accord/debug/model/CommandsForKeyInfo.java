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

public class CommandsForKeyInfo
{
    public final String key;
    public final int commandStoreId;
    public final String txnId;
    public final String ballot;
    public final String depsKnownBefore;
    public final String executeAt;
    public final String flags;
    public final String missing;
    public final String status;
    public final String statusOverrides;
    
    public CommandsForKeyInfo(String key, int commandStoreId, String txnId, String ballot,
                              String depsKnownBefore, String executeAt, String flags, String missing,
                              String status, String statusOverrides)
    {
        this.key = key;
        this.commandStoreId = commandStoreId;
        this.txnId = txnId;
        this.ballot = ballot;
        this.depsKnownBefore = depsKnownBefore;
        this.executeAt = executeAt;
        this.flags = flags;
        this.missing = missing;
        this.status = status;
        this.statusOverrides = statusOverrides;
    }
}