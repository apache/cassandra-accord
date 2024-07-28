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
    public static final PartialDeps NONE = new PartialDeps(Ranges.EMPTY, KeyDeps.NONE, RangeDeps.NONE, KeyDeps.NONE);

    public static Builder builder(Participants<?> covering)
    {
        return new Builder(covering);
    }
    public static class Builder extends AbstractBuilder<PartialDeps>
    {
        final Participants<?> covering;
        public Builder(Participants<?> covering)
        {
            this.covering = covering;
        }

        @Override
        public PartialDeps build()
        {
            return new PartialDeps(covering,
                                   keyBuilder.build(),
                                   rangeBuilder == null ? RangeDeps.NONE : rangeBuilder.build(),
                                   directKeyBuilder == null ? KeyDeps.NONE : directKeyBuilder.build());
        }
    }

    // TODO (expected): we no longer need this if everyone has a FullRoute
    //      could also retain a simple bitset over the original FullRoute
    // TODO (required) remove this and related concepts, as can cause problems with topology changes for a single store
    //    where the store has some ranges that we participate in, and some we do not; we will not correctly construct covering in some cases

    public final Participants<?> covering; // set only if this is a range transaction, containing the minimal ranges of the original transaction that we cover
    public PartialDeps(Participants<?> covering, KeyDeps keyDeps, RangeDeps rangeDeps, KeyDeps directKeyDeps)
    {
        super(keyDeps, rangeDeps, directKeyDeps);
        this.covering = covering;
    }

    public boolean covers(Unseekables<?> participants)
    {
        return covering.intersectsAll(participants);
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
            this.rangeDeps.with(that.rangeDeps),
            this.directKeyDeps.with(that.directKeyDeps)
        );
    }

    public Deps reconstitute(FullRoute<?> route)
    {
        if (!covers(route.participants()))
            throw new IllegalArgumentException();
        return new Deps(keyDeps, rangeDeps, directKeyDeps);
    }

    // covering might cover a wider set of ranges, some of which may have no involved keys
    public PartialDeps reconstitutePartial(Participants<?> covering)
    {
        if (!covers(covering))
            throw new IllegalArgumentException();

        if (covers(covering)) return this;
        else throw Invariants.illegalArgument(this + " does not cover " + covering);
    }
}
