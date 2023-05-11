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

import accord.api.RoutingKey;
import accord.utils.SortedArrays;

import static accord.utils.ArrayBuffers.cachedRoutingKeys;
import static accord.utils.Invariants.checkArgument;
import static accord.utils.SortedArrays.isSortedUnique;
import static accord.utils.SortedArrays.toUnique;

public class RoutingKeys extends AbstractUnseekableKeys<AbstractUnseekableKeys<?>> implements Unseekables<RoutingKey, AbstractUnseekableKeys<?>>
{
    public static class SerializationSupport
    {
        public static RoutingKeys create(RoutingKey[] keys)
        {
            return new RoutingKeys(keys);
        }
    }

    public static final RoutingKeys EMPTY = new RoutingKeys(new RoutingKey[0]);

    RoutingKeys(RoutingKey[] keys)
    {
        super(keys);
    }

    public static RoutingKeys of(RoutingKey ... keys)
    {
        return new RoutingKeys(toUnique(sort(keys)));
    }

    public static RoutingKeys ofSortedUnique(RoutingKey ... keys)
    {
        checkArgument(isSortedUnique(keys));
        return new RoutingKeys(keys);
    }

    @Override
    public Unseekables<RoutingKey, ?> with(Unseekables<RoutingKey, ?> with)
    {
        AbstractKeys<RoutingKey, ?> that = (AbstractKeys<RoutingKey, ?>) with;
        return wrap(SortedArrays.linearUnion(keys, that.keys, cachedRoutingKeys()), that);
    }

    @Override
    public RoutingKeys with(RoutingKey with)
    {
        if (contains(with))
            return this;

        return wrap(toRoutingKeysArray(with));
    }

    public RoutingKeys without(RoutingKey without)
    {
        int index = indexOf(without);
        if (index < 0)
            return this;

        RoutingKey[] keys = new RoutingKey[size() - 1];
        System.arraycopy(keys, 0, keys, 0, index);
        System.arraycopy(keys, index + 1, keys, index, size() - (1 + index));
        return wrap(keys);
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

    private RoutingKeys wrap(RoutingKey[] wrap, AbstractKeys<RoutingKey, ?> that)
    {
        return wrap == keys ? this : wrap == that.keys && that instanceof RoutingKeys ? (RoutingKeys)that : new RoutingKeys(wrap);
    }

    private RoutingKeys wrap(RoutingKey[] wrap)
    {
        return wrap == keys ? this : new RoutingKeys(wrap);
    }
}
