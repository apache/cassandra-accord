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

package accord.impl;

import accord.local.Command;
import accord.local.SafeCommand;
import accord.local.StoreParticipants;
import accord.primitives.SaveStatus;
import accord.primitives.TxnId;
import accord.utils.Invariants;

import static accord.primitives.SaveStatus.Erased;
import static accord.primitives.SaveStatus.ErasedOrVestigial;
import static accord.primitives.Status.Durability.NotDurable;
import static accord.primitives.Status.Durability.UniversalOrInvalidated;

public class ErasedSafeCommand extends SafeCommand
{
    final Command erased;

    public ErasedSafeCommand(TxnId txnId, SaveStatus saveStatus)
    {
        super(txnId);
        this.erased = erased(txnId, saveStatus);
    }

    public static Command erased(TxnId txnId, SaveStatus saveStatus)
    {
        Invariants.checkArgument(saveStatus.compareTo(Erased) >= 0);
        return new Command.Truncated(txnId, saveStatus, saveStatus == ErasedOrVestigial ? NotDurable : UniversalOrInvalidated, StoreParticipants.empty(txnId), null, null, null);
    }

    @Override
    public Command current()
    {
        return erased;
    }

    @Override
    public void invalidate()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean invalidated()
    {
        return false;
    }

    @Override
    protected void set(Command command)
    {
        throw new UnsupportedOperationException();
    }
}
