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

package accord.debug;

import accord.api.RoutingKey;
import accord.local.RedundantStatus;
import accord.primitives.Ballot;
import accord.primitives.Range;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Simplified representation of some of the entities in Accord protocol
public class Model
{
    public static class NodeInfo
    {
        public final int id;
        public final List<StoreInfo> stores;

        public NodeInfo(int id, List<StoreInfo> stores)
        {
            this.id = id;
            this.stores = stores;
        }
    }

    public static class StoreInfo
    {
        public final int storeId;
        public final List<Range> ranges;

        public StoreInfo(int storeId, List<Range> ranges)
        {
            this.storeId = storeId;
            this.ranges = ranges;
        }
    }

    public static class RedundantBeforeInfo
    {
        public static class MaxBounds
        {
            // Map of RedundantStatus$Property to TxnId
            public final Map<RedundantStatus.Property, TxnId> maxBounds;

            public MaxBounds()
            {
                this.maxBounds = new HashMap<>();
            }

            public void addProperty(RedundantStatus.Property property, TxnId maxBound)
            {
                if (maxBound != null && maxBound != TxnId.NONE)
                {
                    maxBounds.put(property, maxBound);
                }
            }
        }

        public static class Bounds
        {
            public final Range range;
            public final long startEpoch;
            public final long endEpoch;
            public final MaxBounds maxBounds;
            public final Timestamp staleUntilAtLeast;
            public final TxnId bootstrappedAt;
            public final TxnId gcBefore;

            public Bounds(Range range, long startEpoch, long endEpoch, Timestamp staleUntilAtLeast, MaxBounds maxBounds, TxnId bootstrappedAt, TxnId gcBefore)
            {
                this.range = range;
                this.startEpoch = startEpoch;
                this.endEpoch = endEpoch;
                this.staleUntilAtLeast = staleUntilAtLeast;
                this.maxBounds = maxBounds;
                this.bootstrappedAt = bootstrappedAt;
                this.gcBefore = gcBefore;
            }

            public static Bounds fromBounds(accord.local.RedundantBefore.Bounds bounds)
            {
                MaxBounds maxBounds = new MaxBounds();

                // Get max bounds for all properties
                for (RedundantStatus.Property property : RedundantStatus.Property.values())
                {
                    TxnId maxBound = bounds.maxBound(property);
                    maxBounds.addProperty(property, maxBound);
                }

                return new Bounds(bounds.range, bounds.startEpoch, bounds.endEpoch, bounds.staleUntilAtLeast, maxBounds, bounds.bootstrappedAt, bounds.gcBefore);
            }
        }

        public final Map<Range, Bounds> ranges;

        public RedundantBeforeInfo(Map<Range, Bounds> ranges)
        {
            this.ranges = ranges;
        }


        public static RedundantBeforeInfo asJson(accord.local.RedundantBefore redundantBefore)
        {
            if (redundantBefore == null || redundantBefore.isEmpty())
                return new RedundantBeforeInfo(Collections.emptyMap());

            Map<Range, Bounds> rangesMap = new HashMap<>();

            // Use foldl to iterate through all ranges and bounds
            redundantBefore.foldl((bounds, acc, p1, p2) -> {
                if (bounds != null)
                {
                    Bounds boundsJson = Bounds.fromBounds(bounds);
                    acc.put(boundsJson.range, boundsJson);
                }
                return acc;
            }, rangesMap, null, null, ignore -> false);

            return new RedundantBeforeInfo(rangesMap);
        }

    }

    public static class TxnInfo
    {
        public final TxnId txnId;
        public final String routingKey;
        public final String participants;
        public final SaveStatus saveStatus;
        public final Status.Durability durability;
        public final Timestamp executeAt;
        public final String promised;
        public final String acceptedOrCommitted;

        public final List<TxnId> partialDeps;
        public final List<TxnId> waitingOn;
        public final boolean satisfiesProperty;

        public TxnInfo(TxnId txnId, RoutingKey routingKey, String participants, SaveStatus saveStatus, Status.Durability durability,
                       Timestamp executeAt, Ballot promised, Ballot acceptedOrCommitted,
                       List<TxnId> partialDeps, List<TxnId> waitingOn)
        {
            this.txnId = txnId;
            this.routingKey = routingKey.toString();
            this.participants = participants;
            this.saveStatus = saveStatus;
            this.durability = durability;
            this.executeAt = executeAt;
            this.promised = promised == null ? null : promised.toStandardString();
            this.acceptedOrCommitted = acceptedOrCommitted == null ? null : acceptedOrCommitted.toStandardString();

            this.partialDeps = partialDeps;
            this.waitingOn = waitingOn;
            this.satisfiesProperty = true; // Default to true for backward compatibility
        }

        public TxnInfo(TxnId txnId, RoutingKey routingKey, String participants, SaveStatus saveStatus, Status.Durability durability,
                       Timestamp executeAt, Ballot promised, Ballot acceptedOrCommitted,
                       List<TxnId> partialDeps, List<TxnId> waitingOn, boolean satisfiesProperty)
        {
            this.txnId = txnId;
            this.routingKey = routingKey == null ? null : routingKey.toString();
            this.participants = participants;
            this.saveStatus = saveStatus;
            this.durability = durability;
            this.executeAt = executeAt;
            this.promised = promised == null ? null : promised.toStandardString();
            this.acceptedOrCommitted = acceptedOrCommitted == null ? null : acceptedOrCommitted.toStandardString();

            this.partialDeps = partialDeps;
            this.waitingOn = waitingOn;
            this.satisfiesProperty = satisfiesProperty;
        }
    }

    public static class CommandsForKeyInfo
    {
        public final String routingKey;
        public final List<CommandsForKeyTxnInfo> transactions;

        public CommandsForKeyInfo(RoutingKey routingKey, List<CommandsForKeyTxnInfo> transactions)
        {
            this.routingKey = routingKey.toString();
            this.transactions = transactions;
        }
    }

    public static class CommandsForKeyTxnInfo
    {
        public final String plainTxnId;
        public final String ballot;
        public final String depsKnownUntilExecuteAt;
        public final String flags;
        public final String plainExecuteAt;
        public final String missing;
        public final String status;
        public final String statusOverrides;

        public CommandsForKeyTxnInfo(String plainTxnId, String ballot, String depsKnownUntilExecuteAt,
                                     String flags, String plainExecuteAt, String missing,
                                     String status, String statusOverrides)
        {
            this.plainTxnId = plainTxnId;
            this.ballot = ballot;
            this.depsKnownUntilExecuteAt = depsKnownUntilExecuteAt;
            this.flags = flags;
            this.plainExecuteAt = plainExecuteAt;
            this.missing = missing;
            this.status = status;
            this.statusOverrides = statusOverrides;
        }
    }
}