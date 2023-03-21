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
import accord.primitives.Routable.Domain;

import static accord.primitives.Routables.Slice.Overlapping;

/**
 * Either a Route or a collection of Routable
 */
public interface Seekables<K extends Seekable, U extends Seekables<K, ?>> extends Routables<K, U>
{
    @Override
    default U slice(Ranges ranges) { return slice(ranges, Overlapping); }

    @Override
    U slice(Ranges ranges, Slice slice);
    Seekables<K, U> with(U with);

    Unseekables<?, ?> toUnseekables();

    FullRoute<?> toRoute(RoutingKey homeKey);
    
    static Seekables<?, ?> of(Seekable seekable)
    {
        return seekable.domain() == Domain.Range ? Ranges.of(seekable.asRange()) : Keys.of(seekable.asKey());
    }
}
