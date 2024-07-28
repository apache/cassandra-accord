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
    // TODO (low priority, efficiency): efficient merge when more than one input
    PartialTxn with(PartialTxn add);
    Txn reconstitute(FullRoute<?> route);
    PartialTxn reconstitutePartial(Participants<?> covering);

    default boolean covers(Route<?> route)
    {
        if (query() == null && route.contains(route.homeKey()))
            return false;

        return keys().intersectsAll(route);
    }

    default boolean covers(Unseekables<?> participants)
    {
        return keys().intersectsAll(participants);
    }

    static PartialTxn merge(@Nullable PartialTxn a, @Nullable PartialTxn b)
    {
        return a == null ? b : b == null ? a : a.with(b);
    }

    // TODO (low priority, clarity): override toString
    class InMemory extends Txn.InMemory implements PartialTxn
    {
        public InMemory(Kind kind, Seekables<?, ?> keys, Read read, Query query, Update update)
        {
            super(kind, keys, read, query, update);
        }

        @Override
        public PartialTxn with(PartialTxn add)
        {
            if (!add.kind().equals(kind()))
                throw new IllegalArgumentException();

            Seekables<?, ?> keys = ((Seekables)this.keys()).with(add.keys());
            Read read = this.read().merge(add.read());
            Query query = this.query() == null ? add.query() : this.query();
            Update update = this.update() == null ? null : this.update().merge(add.update());
            if (keys == this.keys())
            {
                if (read == this.read() && query == this.query() && update == this.update())
                    return this;
            }
            else if (keys == add.keys())
            {
                if (read == add.read() && query == add.query() && update == add.update())
                    return add;
            }
            return new PartialTxn.InMemory(kind(), keys, read, query, update);
        }

        @Override
        public Txn reconstitute(FullRoute<?> route)
        {
            if (!covers(route) || query() == null)
                throw illegalState("Incomplete PartialTxn: " + this + ", route: " + route);

            return new Txn.InMemory(kind(), keys(), read(), query(), update());
        }

        @Override
        public PartialTxn reconstitutePartial(Participants<?> covering)
        {
            if (!covers(covering))
                throw illegalState("Incomplete PartialTxn: " + this + ", covering: " + covering);

            if (this.keys().containsAll(covering))
                return this;

            return new PartialTxn.InMemory(kind(), keys(), read(), query(), update());
        }
    }
}
