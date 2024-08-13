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

import java.util.Arrays;
import java.util.Collection;

import accord.api.RoutingKey;
import accord.utils.SortedArrays;

import static accord.utils.ArrayBuffers.cachedRoutingKeys;
import static accord.utils.Invariants.checkArgument;
import static accord.utils.SortedArrays.isSortedUnique;
import static accord.utils.SortedArrays.toUnique;

public class RoutingKeys extends AbstractUnseekableKeys implements Unseekables<RoutingKey>
{
    public static class SerializationSupport
    {
        public static RoutingKeys create(RoutingKey[] keys)
        {
            return new RoutingKeys(keys);
        }
    }

    public static final RoutingKeys EMPTY = new RoutingKeys(new RoutingKey[0]);

    private static final RoutingKey[] EMPTY_KEYS_ARRAY = new RoutingKey[0];

    RoutingKeys(RoutingKey[] keys)
    {
        super(keys);
    }

    public static RoutingKeys of(RoutingKey ... keys)
    {
        return new RoutingKeys(toUnique(sort(keys)));
    }

    public static RoutingKeys of(Collection<? extends RoutingKey> keys)
    {
        if (keys.isEmpty())
            return EMPTY;
        return of(keys.toArray(EMPTY_KEYS_ARRAY));
    }

    public static RoutingKeys ofSortedUnique(RoutingKey ... keys)
    {
        checkArgument(isSortedUnique(keys));
        return new RoutingKeys(keys);
    }

    @SuppressWarnings("unchecked")
    @Override
    public RoutingKeys with(Unseekables<RoutingKey> with)
    {
        return with((AbstractKeys<RoutingKey>) with);
    }

    @SuppressWarnings("unchecked")
    @Override
    public RoutingKeys with(Participants<RoutingKey> with)
    {
        return with((AbstractKeys<RoutingKey>) with);
    }

    @Override
    public Participants<RoutingKey> slice(int from, int to)
    {
        if (from == 0 && to == size())
            return this;
        return new RoutingKeys(Arrays.copyOfRange(keys, from, to));
    }

    private RoutingKeys with(AbstractKeys<RoutingKey> with)
    {
        return wrap(SortedArrays.linearUnion(keys, with.keys, cachedRoutingKeys()), with);
    }

    @Override
    public RoutingKeys with(RoutingKey with)
    {
        if (contains(with))
            return this;

        return wrap(toRoutingKeysArray(with, true));
    }

    @Override
    public UnseekablesKind kind()
    {
        return UnseekablesKind.RoutingKeys;
    }

    @Override
    public RoutingKeys slice(Ranges ranges)
    {
        return wrap(slice(ranges, RoutingKey[]::new));
    }

    @Override
    public RoutingKeys slice(Ranges ranges, Slice slice)
    {
        return slice(ranges);
    }

    private RoutingKeys wrap(RoutingKey[] wrap, AbstractKeys<RoutingKey> that)
    {
        return wrap == keys ? this : wrap == that.keys && that instanceof RoutingKeys ? (RoutingKeys)that : new RoutingKeys(wrap);
    }

    private RoutingKeys wrap(RoutingKey[] wrap)
    {
        return wrap == keys ? this : new RoutingKeys(wrap);
    }
}
