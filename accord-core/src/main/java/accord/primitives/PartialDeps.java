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

import accord.utils.Invariants;

public class PartialDeps extends Deps
{
    public static final PartialDeps NONE = new PartialDeps(Ranges.EMPTY, KeyDeps.NONE, RangeDeps.NONE);

    public static Builder builder(Ranges covering)
    {
        return new Builder(covering);
    }
    public static class Builder extends AbstractBuilder<PartialDeps>
    {
        final Ranges covering;
        public Builder(Ranges covering)
        {
            this.covering = covering;
        }

        @Override
        public PartialDeps build()
        {
            return new PartialDeps(covering, keyBuilder.build(), rangeBuilder == null ? RangeDeps.NONE : rangeBuilder.build());
        }
    }

    // TODO (expected): we no longer need this if everyone has a FullRoute
    //      could also retain a simple bitset over the original FullRoute
    // TODO (required) remove this and related concepts, as can cause problems with topology changes for a single store
    //    where the store has some ranges that we participate in, and some we do not; we will not correctly construct covering in some cases
    public final Ranges covering;

    public PartialDeps(Ranges covering, KeyDeps keyDeps, RangeDeps rangeDeps)
    {
        super(keyDeps, rangeDeps);
        this.covering = covering;
        Invariants.checkState(covering.containsAll(keyDeps.keys));
        Invariants.checkState(rangeDeps.isCoveredBy(covering));
    }

    public boolean covers(Participants<?> participants)
    {
        return covering.containsAll(participants);
    }

    public boolean isEqualOrFuller(PartialDeps that)
    {
        // TODO (required): this is likely not correct: could have all txnIds but be missing some txnId<->key relations
        return covering.containsAll(that.covering)
            && keyDeps.txnIds().containsAll(that.keyDeps.txnIds())
            && rangeDeps.txnIds().containsAll(that.rangeDeps.txnIds());
    }

    public Deps with(Deps that)
    {
        if (that instanceof PartialDeps)
            return with((PartialDeps) that);
        return super.with(that);
    }

    public PartialDeps with(PartialDeps that)
    {
        Invariants.checkArgument((this.rangeDeps == null) == (that.rangeDeps == null));
        return new PartialDeps(that.covering.with(this.covering),
                this.keyDeps.with(that.keyDeps),
                this.rangeDeps == null ? null : this.rangeDeps.with(that.rangeDeps));
    }

    public Deps reconstitute(FullRoute<?> route)
    {
        if (!covers(route.participants()))
            throw new IllegalArgumentException();
        return new Deps(keyDeps, rangeDeps);
    }

    // covering might cover a wider set of ranges, some of which may have no involved keys
    public PartialDeps reconstitutePartial(Ranges covering)
    {
        if (!covers(covering))
            throw new IllegalArgumentException();

        if (covers(covering))
            return this;

        return new PartialDeps(covering, keyDeps, rangeDeps);
    }

    @Override
    public boolean equals(Object that)
    {
        return this == that || (that instanceof PartialDeps && equals((PartialDeps) that));
    }

    @Override
    public boolean equals(Deps that)
    {
        return that instanceof PartialDeps && equals((PartialDeps) that);
    }

    public boolean equals(PartialDeps that)
    {
        return that != null && this.covering.equals(that.covering) && super.equals(that);
    }

    @Override
    public String toString()
    {
        return covering + ":" + super.toString();
    }
}
