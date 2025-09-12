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

package accord.primitives;

import java.util.AbstractList;
import java.util.List;
import java.util.Objects;

import accord.api.RoutingKey;
import accord.utils.Invariants;
import accord.utils.SortedList.MergeCursor;

import static accord.primitives.Routables.Slice.Minimal;
import static accord.utils.Invariants.illegalArgument;

public class PartialDeps extends Deps
{
    public static final PartialDeps NONE = new PartialDeps(Ranges.EMPTY, KeyDeps.NONE, RangeDeps.NONE);

    public static Builder builder(Participants<?> covering, boolean buildRangeByTxnId)
    {
        return new Builder(covering, buildRangeByTxnId);
    }
    public static class Builder extends AbstractBuilder<PartialDeps>
    {
        final Participants<?> covering;
        public Builder(Participants<?> covering, boolean buildRangeByTxnId)
        {
            super(buildRangeByTxnId);
            this.covering = covering;
        }

        @Override
        public PartialDeps build()
        {
            return new PartialDeps(covering, keyBuilder.build(), rangeBuilder.build());
        }
    }

    public final Participants<?> covering; // set only if this is a range transaction, containing the minimal ranges of the original transaction that we cover
    public PartialDeps(Participants<?> covering, KeyDeps keyDeps, RangeDeps rangeDeps)
    {
        super(keyDeps, rangeDeps);
        this.covering = covering;
    }

    public boolean covers(Unseekables<?> participants)
    {
        return covering.containsAll(participants);
    }

    public boolean covers(RoutingKey key)
    {
        return covering.contains(key);
    }

    @Override
    public MergeCursor<TxnId, DepList> txnIds(RoutingKey key)
    {
        Invariants.requireArgument(covers(key), "%s is not covered by %s", key, this);
        return super.txnIds(key);
    }

    public Deps with(Deps that)
    {
        if (that instanceof PartialDeps)
            return with((PartialDeps) that);
        return super.with(that);
    }

    public PartialDeps with(PartialDeps that)
    {
        return new PartialDeps(
            this.covering.with((Participants)that.covering),
            this.keyDeps.with(that.keyDeps),
            this.rangeDeps.with(that.rangeDeps)
        );
    }

    @Override
    public boolean equals(Deps that)
    {
        return that instanceof PartialDeps
               && Objects.equals(covering, ((PartialDeps) that).covering)
               && super.equals(that);
    }

    @Override
    public PartialDeps intersecting(Participants<?> participants)
    {
        if (!covers(participants))
            throw illegalArgument("This PartialDeps does not cover the requested participants");
        return new PartialDeps(this.covering.intersecting(participants, Minimal), keyDeps.intersecting(participants), rangeDeps.intersecting(participants));
    }

    public PartialDeps intersectStable(Deps that, TxnId until)
    {
        return new PartialDeps(covering, keyDeps.withTxnIds(intersectStable(keyDeps.txnIds, that.keyDeps.txnIds, until)),
                               rangeDeps.withTxnIds(intersectStable(rangeDeps.txnIds, that.rangeDeps.txnIds, until)));
    }

    public Deps asFullUnsafe()
    {
        return new Deps(keyDeps, rangeDeps);
    }

    public List<TxnId> asListUnsafe()
    {
        return new AbstractList<>()
        {
            @Override
            public TxnId get(int index) { return txnId(index); }
            @Override
            public int size()
            {
                return txnIdCount();
            }
        };
    }

    public Deps reconstitute(FullRoute<?> route)
    {
        if (!covers(route.participants()))
            throw illegalArgument(covering + " does not cover " + route);
        return new Deps(keyDeps, rangeDeps);
    }

    // covering might cover a wider set of ranges, some of which may have no involved keys
    public PartialDeps reconstitutePartial(Participants<?> covering)
    {
        if (!covers(covering))
            throw illegalArgument(this.covering + " does not cover " + covering);

        if (covers(covering)) return this;
        else throw illegalArgument(this.covering + " does not cover " + covering);
    }
}
