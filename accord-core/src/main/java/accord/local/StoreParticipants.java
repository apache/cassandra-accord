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

import java.util.Objects;
import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.local.CommandStores.RangesForEpoch;
import accord.primitives.EpochSupplier;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Invariants;

import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Route.tryCastToRoute;

// TODO (desired): split into a single sub-class to save memory in typical case of owns==touches==hasTouched
public class StoreParticipants
{
    public static class SerializationSupport
    {
        public static StoreParticipants create(@Nullable Route<?> route, Participants<?> owns, Participants<?> touches, Participants<?> hasTouched)
        {
            return new StoreParticipants(route, owns, touches, hasTouched);
        }
    }

    private static final StoreParticipants EMPTY_KEYS = new StoreParticipants(null, RoutingKeys.EMPTY, RoutingKeys.EMPTY, RoutingKeys.EMPTY);
    private static final StoreParticipants EMPTY_RANGES = new StoreParticipants(null, Ranges.EMPTY, Ranges.EMPTY, Ranges.EMPTY);

    /**
     * The maximum known route
     */
    @Nullable final Route<?> route;

    /**
     * Keys that are _known_ to interact with the transaction, i.e. txnId.epoch() until committed, then txnId.epoch...executeAt.epoch
     */
    final Participants<?> owns;
    final Participants<?> touches;
    final Participants<?> hasTouched;

    private StoreParticipants(@Nullable Route<?> route, Participants<?> owns, Participants<?> touches, Participants<?> hasTouched)
    {
        this.route = route;
        this.owns = owns;
        this.touches = touches;
        this.hasTouched = hasTouched;
        Invariants.checkArgument(route != null || (!Route.isRoute(owns) && !Route.isRoute(touches) && !Route.isRoute(hasTouched)));
        Routable.Domain domain = owns.domain();
        Invariants.checkArgument(touches.containsAll(owns));
        Invariants.checkArgument(route == null || domain == route.domain());
        Invariants.checkArgument(domain == touches.domain());
        Invariants.checkArgument(domain == hasTouched.domain());
    }

    public final boolean hasFullRoute()
    {
        return Route.isFullRoute(route);
    }

    public final @Nullable Route<?> route()
    {
        return route;
    }

    /**
     * Everything that the replica is known by all other replicas to own and
     * participate in the coordination or execution of.
     */
    public final Participants<?> owns()
    {
        return owns;
    }

    public final Participants<?> touches()
    {
        return touches;
    }

    public final Participants<?> hasTouched()
    {
        return hasTouched;
    }

    public final boolean touches(RoutingKey key)
    {
        return touches.contains(key);
    }

    public final StoreParticipants supplement(Route<?> route)
    {
        route = Route.merge(this.route, (Route)route);
        if (route == this.route) return this;
        return new StoreParticipants(route, owns, touches, hasTouched);
    }

    public final StoreParticipants supplement(Participants<?> touches)
    {
        if (!hasTouched.isEmpty()) return this;
        Route<?> route = Route.merge(this.route, (Route)Route.tryCastToRoute(touches));
        return new StoreParticipants(route, owns, touches, touches);
    }

    public final StoreParticipants supplement(StoreParticipants that)
    {
        if (this == that) return this;
        return supplement(that.route, that.owns, that.hasTouched);
    }

    /**
     * Route, owns and hasTouched are merged
     * touches is left unchanged, as this varies and cannot safely be supplemented
     * (e.g. if one update touches more epochs than another, it will have suitable data for those epochs)
     */
    public final StoreParticipants supplement(@Nullable Route<?> route, @Nullable Participants<?> owns, @Nullable Participants<?> hasTouched)
    {
        route = Route.merge(this.route, (Route)route);
        owns = owns == null ? this.owns : this.owns.with((Participants) owns);
        Participants<?> touches = this.touches;
        hasTouched = Participants.merge(this.hasTouched, (Participants)hasTouched);
        if (owns != this.owns)
        {
            boolean equalTouches = hasTouched == touches;
            touches = Participants.merge((Participants)owns, touches);
            if (equalTouches) hasTouched = touches;
            else hasTouched = Participants.merge((Participants)touches, hasTouched);
        }
        return update(route, owns, touches, hasTouched);
    }

    private StoreParticipants update(Route<?> route, Participants<?> owns, Participants<?> touches, Participants<?> hasTouched)
    {
        return this.route == route && this.owns == owns && this.touches == touches && hasTouched == this.hasTouched
               ? this : new StoreParticipants(route, owns, touches, hasTouched);
    }

    private StoreParticipants update(Route<?> route, Participants<?> owns, Participants<?> touches, Participants<?> hasTouched, StoreParticipants other)
    {
        if (this.route == route && this.owns == owns && this.touches == touches && hasTouched == this.hasTouched)
            return this;

        return other.update(route, owns, touches, hasTouched);
    }

    public Ranges executeRanges(SafeCommandStore safeStore, TxnId txnId, Timestamp executeAt)
    {
        Ranges ranges = txnId.is(Txn.Kind.ExclusiveSyncPoint)
                        ? safeStore.ranges().all()
                        : safeStore.ranges().allAt(executeAt.epoch());

        return safeStore.redundantBefore().removePreBootstrap(txnId, ranges);
    }

    public Participants<?> executes(SafeCommandStore safeStore, TxnId txnId, Timestamp executeAt)
    {
        Invariants.checkState(Route.isFullRoute(route));
        if (route == owns)
            return route;

        if (txnId.is(Txn.Kind.ExclusiveSyncPoint))
            return route.slice(safeStore.ranges().all(), Minimal);

        if (txnId == executeAt || txnId.epoch() == executeAt.epoch())
            return owns;

        return owns.slice(safeStore.ranges().allAt(executeAt.epoch()), Minimal);
    }

    public static Route<?> touches(SafeCommandStore safeStore, TxnId txnId, EpochSupplier toEpoch, Route<?> route)
    {
        // TODO (required): remove pre-bootstrap?
        if (txnId.is(Txn.Kind.ExclusiveSyncPoint))
            return route.slice(safeStore.ranges().all(), Minimal);

        if (txnId == toEpoch || txnId.epoch() == toEpoch.epoch())
            return route;

        return route.slice(safeStore.ranges().allBetween(txnId.epoch(), toEpoch), Minimal);
    }

    public Participants<?> dependencyExecutesAtLeast(SafeCommandStore safeStore, Participants<?> participants, TxnId txnId, Timestamp executeAt)
    {
        if (txnId.is(Txn.Kind.ExclusiveSyncPoint))
            return participants.slice(safeStore.ranges().all(), Minimal);

        return participants.slice(safeStore.ranges().allAt(executeAt.epoch()), Minimal);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof StoreParticipants)) return false;
        if (obj == this) return true;
        StoreParticipants that = (StoreParticipants) obj;
        return Objects.equals(route, that.route) && owns.equals(that.owns) && touches.equals(that.touches);
    }

    public String toString()
    {
        return owns.toString() + (owns != touches ? "(" + touches.toString() + ")" : "");
    }

    public static StoreParticipants read(SafeCommandStore safeStore, Participants<?> participants, TxnId txnId)
    {
        Participants<?> owns = participants.slice(safeStore.ranges().allAt(txnId.epoch()), Minimal);
        return new StoreParticipants(tryCastToRoute(participants), owns, owns, owns);
    }

    public static StoreParticipants read(SafeCommandStore safeStore, Participants<?> participants, TxnId txnId, long epoch)
    {
        long txnIdEpoch = txnId.epoch();
        return read(safeStore, participants, txnId, Math.min(txnIdEpoch, epoch), Math.max(txnIdEpoch, epoch));
    }

    public static StoreParticipants execute(SafeCommandStore safeStore, Participants<?> participants, TxnId txnId, long executeAtEpoch)
    {
        long txnIdEpoch = txnId.epoch();
        Participants<?> owns = participants.slice(safeStore.ranges().allBetween(txnIdEpoch, executeAtEpoch), Minimal);
        return new StoreParticipants(tryCastToRoute(participants), owns, owns, owns);
    }

    public static StoreParticipants read(SafeCommandStore safeStore, Participants<?> participants, TxnId txnId, long minEpoch, long maxEpoch)
    {
        long txnIdEpoch = txnId.epoch();
        Participants<?> owns = participants.slice(safeStore.ranges().allAt(txnIdEpoch), Minimal);
        Participants<?> touches = minEpoch == maxEpoch && minEpoch == txnIdEpoch ? owns : participants.slice(safeStore.ranges().allBetween(minEpoch, maxEpoch), Minimal);
        return new StoreParticipants(tryCastToRoute(participants), owns, touches, touches);
    }

    public static StoreParticipants update(SafeCommandStore safeStore, Participants<?> participants, long minEpoch, TxnId txnId, long executeAtEpoch)
    {
        return update(safeStore, participants, minEpoch, txnId, executeAtEpoch, executeAtEpoch);
    }

    public static StoreParticipants update(SafeCommandStore safeStore, Participants<?> participants, long lowEpoch, TxnId txnId, long executeAtEpoch, long highEpoch)
    {
        RangesForEpoch storeRanges = safeStore.ranges();
        Ranges ownedRanges = storeRanges.allBetween(txnId.epoch(), executeAtEpoch);
        Participants<?> owns = participants.slice(ownedRanges, Minimal);
        Ranges touchesRanges = txnId.is(Txn.Kind.ExclusiveSyncPoint)
                               ? storeRanges.all()
                               : storeRanges.extend(ownedRanges, txnId.epoch(), executeAtEpoch, lowEpoch, highEpoch);
        Participants<?> touches = ownedRanges == touchesRanges || owns == participants ? owns : participants.slice(touchesRanges, Minimal);
        return new StoreParticipants(tryCastToRoute(participants), owns, touches, touches);
    }

    public static StoreParticipants invalidate(SafeCommandStore safeStore, Participants<?> participants, TxnId txnId)
    {
        RangesForEpoch storeRanges = safeStore.ranges();
        Ranges ownedRanges = storeRanges.allAt(txnId.epoch());
        Ranges touchesRanges = storeRanges.all();
        Participants<?> owns = participants.slice(ownedRanges, Minimal);
        Participants<?> touches = ownedRanges == touchesRanges || owns == participants ? owns : participants.slice(touchesRanges, Minimal);
        return new StoreParticipants(tryCastToRoute(participants), owns, touches, touches);
    }

    public static StoreParticipants empty(Routable.Domain domain)
    {
        return domain == Key ? EMPTY_KEYS : EMPTY_RANGES;
    }

    public static StoreParticipants empty(TxnId txnId)
    {
        return txnId.is(Key) ? EMPTY_KEYS : EMPTY_RANGES;
    }

    public static StoreParticipants empty(TxnId txnId, Route<?> route)
    {
        Participants<?> empty = txnId.is(Key) ? RoutingKeys.EMPTY : Ranges.EMPTY;
        return new StoreParticipants(route, empty, empty, empty);
    }

    public static StoreParticipants all(Route<?> route)
    {
        return new StoreParticipants(route, route, route, route);
    }
}
