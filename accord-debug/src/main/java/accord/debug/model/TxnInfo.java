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

import java.util.List;

public class TxnInfo
{
    public final int commandStoreId;
    public final String txnId;
    public final String saveStatus;
    public final String route;
    public final String durability;
    public final String executeAt;
    public final String executesAtLeast;
    public final String txn;
    public final String deps;
    public final List<String> waitingOnKeys;
    public final List<String> waitingOnTxnIds;
    public final String writes;
    public final String result;
    public final String participantsOwns;
    public final String participantsTouches;
    public final String participantsHasTouched;
    public final String participantsExecutes;
    public final String participantsWaitsOn;
    
    public TxnInfo(int commandStoreId, String txnId, String saveStatus, String route, String durability,
                   String executeAt, String executesAtLeast, String txn, String deps,
                   List<String> waitingOnKeys, List<String> waitingOnTxnIds,
                   String writes, String result, String participantsOwns, String participantsTouches,
                   String participantsHasTouched, String participantsExecutes, String participantsWaitsOn)
    {
        this.commandStoreId = commandStoreId;
        this.txnId = txnId;
        this.saveStatus = saveStatus;
        this.route = route;
        this.durability = durability;
        this.executeAt = executeAt;
        this.executesAtLeast = executesAtLeast;
        this.txn = txn;
        this.deps = deps;
        this.waitingOnKeys = waitingOnKeys;
        this.waitingOnTxnIds = waitingOnTxnIds;
        this.writes = writes;
        this.result = result;
        this.participantsOwns = participantsOwns;
        this.participantsTouches = participantsTouches;
        this.participantsHasTouched = participantsHasTouched;
        this.participantsExecutes = participantsExecutes;
        this.participantsWaitsOn = participantsWaitsOn;
    }
}