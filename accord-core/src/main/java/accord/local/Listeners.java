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

import java.util.Collection;
import java.util.function.Predicate;

import accord.utils.DeterministicSet;

public class Listeners<L extends Command.Listener> extends DeterministicSet<L>
{
    public Listeners()
    {
    }

    public Listeners(Listeners<L> copy)
    {
        super(copy);
    }

    public static class Immutable extends Listeners<Command.DurableAndIdempotentListener>
    {
        public static final Immutable EMPTY = new Immutable();

        private Immutable()
        {
            super();
        }

        public Immutable(Listeners<Command.DurableAndIdempotentListener> listeners)
        {
            super(listeners);
        }

        Listeners<Command.DurableAndIdempotentListener> mutable()
        {
            return new Listeners<>(this);
        }

        @Override
        public boolean add(Command.DurableAndIdempotentListener item)
        {
            throw new UnsupportedOperationException("Cannot modify immutable set");
        }

        @Override
        public boolean remove(Object item)
        {
            throw new UnsupportedOperationException("Cannot modify immutable set");
        }

        @Override
        public boolean removeAll(Collection<?> c)
        {
            throw new UnsupportedOperationException("Cannot modify immutable set");
        }

        @Override
        public boolean addAll(Collection<? extends Command.DurableAndIdempotentListener> c)
        {
            throw new UnsupportedOperationException("Cannot modify immutable set");
        }

        @Override
        public boolean retainAll(Collection<?> c)
        {
            throw new UnsupportedOperationException("Cannot modify immutable set");
        }

        @Override
        public boolean removeIf(Predicate<? super Command.DurableAndIdempotentListener> filter)
        {
            throw new UnsupportedOperationException("Cannot modify immutable set");
        }
    }
}
