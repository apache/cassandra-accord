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

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface MapReduceConsume<I, O> extends MapReduce<I, O>, BiConsumer<O, Throwable>
{
    @Override
    void accept(O result, Throwable failure);

    static <I> MapReduceConsume<I, Void> forEach(Consumer<I> forEach, BiConsumer<Object, Throwable> consume)
    {
        return new MapReduceConsume<I, Void>() {
            @Override public void accept(Void result, Throwable failure) { consume.accept(result, failure); }
            @Override public Void apply(I in) { forEach.accept(in); return null; }
            @Override public Void reduce(Void o1, Void o2) {return null; }
        };
    }
}
