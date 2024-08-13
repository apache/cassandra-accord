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

import javax.annotation.Nullable;

import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.Seekables;
import accord.primitives.TxnId;
import accord.utils.Invariants;

public interface CommonAttributes
{
    TxnId txnId();
    Status.Durability durability();
    Route<?> route();
    // if we don't know a route, we may know some participants; if we know a route, this must return the same object as route()
    Participants<?> participants();
    PartialTxn partialTxn();
    @Nullable Seekables<?, ?> additionalKeysOrRanges();
    PartialDeps partialDeps();

    default Mutable mutable()
    {
        return new Mutable(this);
    }

    class Mutable implements CommonAttributes
    {
        private TxnId txnId;
        private Status.Durability durability;
        private Route<?> route;
        private Participants<?> participants; // if route != null, route == participants
        private PartialTxn partialTxn;
        private PartialDeps partialDeps;
        private Seekables<?, ?> additionalKeysOrRanges;

        public Mutable(TxnId txnId)
        {
            this.txnId = txnId;
        }

        public Mutable(CommonAttributes attributes)
        {
            this.txnId = attributes.txnId();
            this.durability = attributes.durability();
            this.route = attributes.route();
            this.participants = attributes.participants();
            Invariants.checkState(participants == route || route == null);
            this.partialTxn = attributes.partialTxn();
            this.partialDeps = attributes.partialDeps();
            this.additionalKeysOrRanges = attributes.additionalKeysOrRanges();
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
        public Route<?> route()
        {
            return route;
        }

        public Mutable route(Route<?> route)
        {
            this.route = route;
            this.participants = route;
            return this;
        }

        @Override
        public Participants<?> participants()
        {
            return participants;
        }

        public Mutable participants(Participants<?> participants)
        {
            Invariants.checkState(route == null);
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
            PartialTxn prev = this.partialTxn;
            this.partialTxn = partialTxn;
            if (prev != null || additionalKeysOrRanges != null)
            {
                Seekables<?, ?> removed = prev == null ? null : ((Seekables) prev.keys()).without(partialTxn.keys());
                if (prev != null && !removed.isEmpty())
                {
                    if (additionalKeysOrRanges == null) additionalKeysOrRanges = removed;
                    else additionalKeysOrRanges = ((Seekables)additionalKeysOrRanges).without(partialTxn.keys()).with(removed);
                }
                else if (additionalKeysOrRanges != null)
                {
                    additionalKeysOrRanges = ((Seekables)additionalKeysOrRanges).without(partialTxn.keys());
                }
            }
            return this;
        }

        public Mutable removePartialTxn()
        {
            this.partialTxn = null;
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

        public Seekables<?, ?> additionalKeysOrRanges()
        {
            return additionalKeysOrRanges;
        }

        public Mutable additionalKeysOrRanges(Seekables<?, ?> additionalKeysOrRanges)
        {
            this.additionalKeysOrRanges = additionalKeysOrRanges;
            return this;
        }
    }
}
