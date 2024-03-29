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

package accord.utils;

import java.util.function.BiFunction;
import java.util.function.Function;

public interface MapReduce<I, O> extends Function<I, O>
{
    // TODO (desired, safety): ensure mutual exclusivity when calling each of these methods
    @Override
    O apply(I in);
    O reduce(O o1, O o2);

    static <I, O> MapReduce<I, O> of(Function<I, O> map, BiFunction<O, O, O> reduce)
    {
        return new MapReduce<I, O>()
        {
            @Override
            public O apply(I in)
            {
                return map.apply(in);
            }

            @Override
            public O reduce(O o1, O o2)
            {
                return reduce.apply(o1, o2);
            }
        };
    }
}
