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

import accord.api.RoutingKey;
import accord.api.VisibleForImplementation;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.Routables;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.primitives.Unseekables;
import accord.utils.Invariants;

import accord.utils.ReducingRangeMap;

import static accord.primitives.Timestamp.Flag.HLC_BOUND;

public class MaxDecidedRX extends ReducingRangeMap<MaxDecidedRX.DecidedRX>
{
    // TODO (desired): do we care about tracking both any and hlcBound? Should often be the same.
    //  NOTE: if we change this, we should definitely maintain a separate type to avoid mistaken
    //  usage when mixing with other txnId (which did happen)
    public static final class DecidedRX
    {
        static final DecidedRX NONE = new DecidedRX(TxnId.NONE, TxnId.NONE);
        static final DecidedRX MAX = new DecidedRX(TxnId.MAX, TxnId.MAX);

        public final TxnId any;
        public final TxnId hlcBound;

        public DecidedRX(TxnId any, TxnId hlcBound)
        {
            Invariants.nonNull(any);
            Invariants.nonNull(hlcBound);
            this.any = any;
            this.hlcBound = hlcBound;
        }

        @Override
        public String toString()
        {
            return "{any=" + any + ",hlcBound=" + hlcBound + '}';
        }

        public boolean includeDecided(TxnId txnId)
        {
            if (txnId.isSyncPoint())
                return txnId.compareTo(any) >= 0;
            return txnId.compareTo(hlcBound) >= 0;
        }

        public boolean includeDecided(long hlc)
        {
            return hlc >= hlcBound.hlc();
        }

        public boolean excludeDecided(long hlc)
        {
            return hlc < hlcBound.hlc();
        }

        public boolean excludeDecided(TxnId txnId)
        {
            return !includeDecided(txnId);
        }

        DecidedRX min(DecidedRX that)
        {
            TxnId any = TxnId.min(this.any, that.any);
            TxnId hlcBound = TxnId.mergeMin(this.hlcBound, that.hlcBound, TxnId::fromValues);
            return selectOrCreate(any, hlcBound, this, that);
        }

        static DecidedRX nonNullOrMin(DecidedRX a, DecidedRX b)
        {
            if (a == null || b == null)
                return a == null ? b : a;
            return a.min(b);
        }

        DecidedRX max(DecidedRX that)
        {
            TxnId any = TxnId.max(this.any, that.any);
            TxnId hlcBound = TxnId.max(this.hlcBound, that.hlcBound);
            return selectOrCreate(any, hlcBound, this, that);
        }

        static DecidedRX nonNullOrMax(DecidedRX a, DecidedRX b)
        {
            if (a == null || b == null)
                return a == null ? b : a;
            return a.max(b);
        }

        static DecidedRX selectOrCreate(TxnId any, TxnId hlcBound, DecidedRX a, DecidedRX b)
        {
            if (any == a.any && hlcBound == a.hlcBound)
                return a;
            if (any == b.any && hlcBound == b.hlcBound)
                return b;
            return new DecidedRX(any, hlcBound);
        }

        @Override
        public boolean equals(Object that)
        {
            return that instanceof DecidedRX && equals((DecidedRX) that);
        }

        public boolean equals(DecidedRX that)
        {
            return this.any.equals(that.any) && this.hlcBound.equals(that.hlcBound);
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }
    }

    public static final class SerializerSupport
    {
        public static MaxDecidedRX create(RoutingKey[] starts, DecidedRX[] values)
        {
            return new MaxDecidedRX(starts, values);
        }
    }

    public static final MaxDecidedRX EMPTY = new MaxDecidedRX();

    private MaxDecidedRX()
    {
        super();
    }

    private MaxDecidedRX(RoutingKey[] starts, DecidedRX[] values)
    {
        super(starts, values);
    }

    DecidedRX min(Routables<?> keysOrRanges)
    {
        DecidedRX result = foldlWithDefault(keysOrRanges, DecidedRX::nonNullOrMin, DecidedRX.NONE, null);
        return result == null ? DecidedRX.NONE : result;
    }

    DecidedRX max(Routables<?> keysOrRanges)
    {
        DecidedRX result = foldlWithDefault(keysOrRanges, DecidedRX::nonNullOrMax, DecidedRX.MAX, null);
        return result == null ? DecidedRX.NONE : result;
    }

    public DecidedRX forDeps(Unseekables<?> keysOrRanges, TxnId txnId)
    {
        Invariants.require(txnId.isSyncPoint());
        // first check max, as if this is later we don't know that we can safely filter
        DecidedRX max = max(keysOrRanges);
        if (max.any.compareTo(txnId) < 0)
            return min(keysOrRanges);
        return DecidedRX.NONE;
    }

    @VisibleForImplementation
    public @Nullable DecidedRX forDeps(Unseekable keyOrRange, TxnId txnId)
    {
        Invariants.require(txnId.isSyncPoint());
        if (keyOrRange.domain() == Domain.Key)
        {
            DecidedRX decidedRx = get((RoutingKey) keyOrRange);
            if (decidedRx.any.compareTo(txnId) < 0)
                return decidedRx;
        }
        else
        {
            // first check max, as if this is later we don't know that we can safely filter
            Range range = (Range) keyOrRange;
            Ranges ranges = Ranges.of(range);
            DecidedRX maxDecidedId = max(ranges);
            if (maxDecidedId.any.compareTo(txnId) < 0)
                return min(ranges);
        }
        return null;
    }

    @VisibleForImplementation
    public static @Nullable DecidedRX forDeps(MaxDecidedRX maxDecidedRX, Unseekables<?> keysOrRanges, TxnId txnId)
    {
        return maxDecidedRX == null ? null : maxDecidedRX.forDeps(keysOrRanges, txnId);
    }

    public DecidedRX get(RoutingKey key)
    {
        return getOrDefault(key, DecidedRX.NONE);
    }

    public MaxDecidedRX update(Unseekables<?> keysOrRanges, TxnId syncId)
    {
        DecidedRX update = new DecidedRX(syncId, syncId.is(HLC_BOUND) ? syncId : TxnId.NONE);
        if (keysOrRanges.isEmpty())
            return this;
        return merge(this, create(keysOrRanges, update, Builder::new), DecidedRX::max, Builder::new);
    }

    static class Builder extends AbstractBoundariesBuilder<RoutingKey, DecidedRX, MaxDecidedRX>
    {
        protected Builder(int capacity)
        {
            super(capacity);
        }

        @Override
        protected MaxDecidedRX buildInternal()
        {
            if (values.isEmpty())
                return EMPTY;

            return new MaxDecidedRX(starts.toArray(new RoutingKey[0]), values.toArray(new DecidedRX[0]));
        }
    }

}
