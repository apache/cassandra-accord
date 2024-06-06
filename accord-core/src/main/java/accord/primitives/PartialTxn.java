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

import accord.api.Query;
import accord.api.Read;
import accord.api.Update;

import javax.annotation.Nullable;

import static accord.utils.Invariants.illegalState;

public interface PartialTxn extends Txn
{
    // TODO (expected): we no longer need this if everyone has a FullRoute
    Ranges covering();
    // TODO (low priority, efficiency): efficient merge when more than one input
    PartialTxn with(PartialTxn add);
    Txn reconstitute(FullRoute<?> route);
    PartialTxn reconstitutePartial(Ranges covering);

    default boolean covers(Route<?> route)
    {
        if (query() == null && route.contains(route.homeKey()))
            return false;

        return covers(route.participants());
    }

    default boolean covers(Participants<?> participants)
    {
        if (query() == null)
        {
            // The home shard is expected to store the query contents
            // So if the query is null, and we are being asked if we
            // cover a range that includes a home shard, we should say no
            Route<?> asRoute = Route.tryCastToRoute(participants);
            if (asRoute != null && asRoute.contains(asRoute.homeKey()))
                return false;
        }
        return covering().containsAll(participants);
    }

    static PartialTxn merge(@Nullable PartialTxn a, @Nullable PartialTxn b)
    {
        return a == null ? b : b == null ? a : a.with(b);
    }

    // TODO (low priority, clarity): override toString
    class InMemory extends Txn.InMemory implements PartialTxn
    {
        public final Ranges covering;

        public InMemory(Ranges covering, Kind kind, Seekables<?, ?> keys, Read read, Query query, Update update)
        {
            super(kind, keys, read, query, update);
            this.covering = covering;
        }

        @Override
        public Ranges covering()
        {
            return covering;
        }

        @Override
        public PartialTxn with(PartialTxn add)
        {
            if (!add.kind().equals(kind()))
                throw new IllegalArgumentException();

            Ranges covering = this.covering.with(add.covering());
            Seekables<?, ?> keys = ((Seekables)this.keys()).with(add.keys());
            Read read = this.read().merge(add.read());
            Query query = this.query() == null ? add.query() : this.query();
            Update update = this.update() == null ? null : this.update().merge(add.update());
            if (keys == this.keys())
            {
                if (covering == this.covering && read == this.read() && query == this.query() && update == this.update())
                    return this;
            }
            else if (keys == add.keys())
            {
                if (covering == add.covering() && read == add.read() && query == add.query() && update == add.update())
                    return add;
            }
            return new PartialTxn.InMemory(covering, kind(), keys, read, query, update);
        }

        public boolean covers(Ranges ranges)
        {
            return covering.containsAll(ranges);
        }

        @Override
        public Txn reconstitute(FullRoute<?> route)
        {
            if (!covers(route) || query() == null)
                throw illegalState("Incomplete PartialTxn: " + this + ", route: " + route);

            return new Txn.InMemory(kind(), keys(), read(), query(), update());
        }

        @Override
        public PartialTxn reconstitutePartial(Ranges covering)
        {
            if (!covers(covering))
                throw illegalState("Incomplete PartialTxn: " + this + ", covering: " + covering);

            if (this.covering.containsAll(covering))
                return this;

            return new PartialTxn.InMemory(covering, kind(), keys(), read(), query(), update());
        }
    }
}
