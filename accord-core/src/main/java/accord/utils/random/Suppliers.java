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

package accord.utils.random;

import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class Suppliers
{
    public interface Bools extends BooleanSupplier
    {
        boolean get();
        void cycle();
        default boolean getAsBoolean() { return get(); }
    }

    public interface Ints extends IntSupplier
    {
        int get();
        void cycle();
        default int getAsInt() { return get(); }
    }

    public interface Longs extends LongSupplier
    {
        long get();
        void cycle();
        default long getAsLong() { return get(); }
    }

    public interface Objects<T> extends Supplier<T>
    {
        T get();
        void cycle();
    }

}
