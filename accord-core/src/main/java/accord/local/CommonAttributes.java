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

import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.Status;
import accord.primitives.TxnId;
import accord.utils.Invariants;

import static accord.primitives.Status.Durability.NotDurable;

public interface CommonAttributes
{
    TxnId txnId();
    Status.Durability durability();
    StoreParticipants participants();
    default Route<?> route() { return participants().route(); }
    PartialTxn partialTxn();
    PartialDeps partialDeps();

    default Mutable mutable()
    {
        return new Mutable(this);
    }

    class Mutable implements CommonAttributes
    {
        private TxnId txnId;
        private Status.Durability durability;
        private StoreParticipants participants;
        private PartialTxn partialTxn;
        private PartialDeps partialDeps;

        public Mutable(TxnId txnId)
        {
            this.txnId = txnId;
            this.participants = StoreParticipants.empty(txnId);
            this.durability = NotDurable;
        }

        public Mutable(CommonAttributes attributes)
        {
            this.txnId = attributes.txnId();
            this.durability = attributes.durability();
            this.participants = Invariants.nonNull(attributes.participants());
            this.partialTxn = attributes.partialTxn();
            this.partialDeps = attributes.partialDeps();
        }

        @Override
        public Mutable mutable()
        {
            return this;
        }

        @Override
        public TxnId txnId()
        {
            return txnId;
        }

        public Mutable txnId(TxnId txnId)
        {
            this.txnId = txnId;
            return this;
        }

        @Override
        public Status.Durability durability()
        {
            return durability;
        }

        public Mutable durability(Status.Durability durability)
        {
            this.durability = durability;
            return this;
        }

        @Override
        public StoreParticipants participants()
        {
            return participants;
        }

        public Mutable updateParticipants(StoreParticipants participants)
        {
            this.participants = this.participants == null ? participants : participants.supplement(this.participants);
            return this;
        }

        public Mutable setParticipants(StoreParticipants participants)
        {
            this.participants = participants;
            return this;
        }

        @Override
        public PartialTxn partialTxn()
        {
            return partialTxn;
        }

        public Mutable partialTxn(PartialTxn partialTxn)
        {
            this.partialTxn = partialTxn;
            return this;
        }

        @Override
        public PartialDeps partialDeps()
        {
            return partialDeps;
        }

        public Mutable partialDeps(PartialDeps partialDeps)
        {
            this.partialDeps = partialDeps;
            return this;
        }
    }
}
