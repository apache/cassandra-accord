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

import java.util.function.BiPredicate;

import accord.primitives.Timestamp;

public class Timestamped<T>
{
    public final Timestamp timestamp;
    public final T data;

    public Timestamped(Timestamp timestamp, T data)
    {
        this.timestamp = timestamp;
        this.data = data;
    }

    public static <T> Timestamped<T> merge(Timestamped<T> a, Timestamped<T> b)
    {
        return a.timestamp.compareTo(b.timestamp) < 0 ? b : a;
    }

    public static <T> Timestamped<T> merge(Timestamped<T> a, Timestamped<T> b, BiPredicate<T, T> testPrefix, BiPredicate<T, T> testEquality)
    {
        int c = a.timestamp.compareTo(b.timestamp);
        if (c == 0)
        {
            Invariants.checkArgument(testEquality.test(a.data, b.data));
            return a;
        }
        else if (c < 0)
        {
            Invariants.checkArgument(testPrefix.test(a.data, b.data));
            return b;
        }
        else
        {
            Invariants.checkArgument(testPrefix.test(b.data, a.data));
            return a;
        }
    }

    public static <T> Timestamped<T> mergeEqual(Timestamped<T> a, Timestamped<T> b, BiPredicate<T, T> testEquality)
    {
        int c = a.timestamp.compareTo(b.timestamp);
        Invariants.checkState(c == 0 && testEquality.test(a.data, b.data));
        return a;
    }

    @Override
    public String toString()
    {
        return data.toString();
    }
}
