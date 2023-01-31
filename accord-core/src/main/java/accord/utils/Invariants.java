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

import net.nicoulaj.compilecommand.annotations.Inline;

import java.util.function.Predicate;

public class Invariants
{
    private static final boolean PARANOID = true;
    private static final boolean DEBUG = true;

    public static boolean isParanoid()
    {
        return PARANOID;
    }
    public static boolean debug()
    {
        return DEBUG;
    }

    public static <T1, T2 extends T1> T2 checkType(T1 cast)
    {
        return (T2)cast;
    }

    public static <T1, T2 extends T1> T2 checkType(Class<T2> to, T1 cast)
    {
        if (cast != null && !to.isInstance(cast))
            throw new IllegalStateException();
        return (T2)cast;
    }

    public static <T1, T2 extends T1> T2 checkType(Class<T2> to, T1 cast, String msg)
    {
        if (cast != null && !to.isInstance(cast))
            throw new IllegalStateException(msg);
        return (T2)cast;
    }

    public static void paranoid(boolean condition)
    {
        if (PARANOID && !condition)
            throw new IllegalStateException();
    }

    public static void checkState(boolean condition)
    {
        if (!condition)
            throw new IllegalStateException();
    }

    public static void checkState(boolean condition, String msg)
    {
        if (!condition)
            throw new IllegalStateException(msg);
    }

    public static <T> T nonNull(T param)
    {
        if (param == null)
            throw new NullPointerException();
        return param;
    }

    public static int isNatural(int input)
    {
        if (input < 0)
            throw new IllegalStateException();
        return input;
    }

    public static long isNatural(long input)
    {
        if (input < 0)
            throw new IllegalStateException();
        return input;
    }

    public static void checkArgument(boolean condition)
    {
        if (!condition)
            throw new IllegalArgumentException();
    }

    public static void checkArgument(boolean condition, String msg)
    {
        if (!condition)
            throw new IllegalArgumentException(msg);
    }

    public static <T> T checkArgument(T param, boolean condition)
    {
        if (!condition)
            throw new IllegalArgumentException();
        return param;
    }

    public static <T> T checkArgument(T param, boolean condition, String msg)
    {
        if (!condition)
            throw new IllegalArgumentException(msg);
        return param;
    }

    @Inline
    public static <T> T checkArgument(T param, Predicate<T> condition)
    {
        if (!condition.test(param))
            throw new IllegalArgumentException();
        return param;
    }

    @Inline
    public static <T> T checkArgument(T param, Predicate<T> condition, String msg)
    {
        if (!condition.test(param))
            throw new IllegalArgumentException(msg);
        return param;
    }
}
