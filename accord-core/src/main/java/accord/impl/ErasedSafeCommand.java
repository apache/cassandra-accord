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
import accord.local.SaveStatus;
import accord.primitives.TxnId;
import accord.utils.Invariants;

import static accord.local.SaveStatus.Erased;
import static accord.local.SaveStatus.ErasedOrInvalidOrVestigial;
import static accord.local.Status.Durability.NotDurable;
import static accord.local.Status.Durability.UniversalOrInvalidated;

public class ErasedSafeCommand extends SafeCommand
{
    final Command erased;

    public ErasedSafeCommand(TxnId txnId, SaveStatus saveStatus)
    {
        super(txnId);
        Invariants.checkArgument(saveStatus == Erased || saveStatus == ErasedOrInvalidOrVestigial);
        this.erased = new Command.Truncated(txnId, saveStatus, saveStatus == ErasedOrInvalidOrVestigial ? NotDurable : UniversalOrInvalidated, null, null, null, null);
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
