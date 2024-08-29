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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;

import accord.local.Node;
import accord.primitives.Timestamp;

public class ReflectionUtils
{
    /**
     * Types that should not walk the fields in the type.
     */
    private static final Set<Class<?>> NO_RECURSIVE = ImmutableSet.of(String.class,
                                                                      // numbers store primitives but reflection converts to a boxed value; this will cause a recursive check over the same thing over and over again...
                                                                      Byte.class,
                                                                      Character.class,
                                                                      Short.class,
                                                                      Integer.class,
                                                                      Long.class,
                                                                      Float.class,
                                                                      Double.class,
                                                                      // accord primitives
                                                                      Timestamp.class,
                                                                      Node.Id.class);

    public static String toString(Object o)
    {
        if (o == null) return "null";
        if (!o.getClass().isArray())
            return o.toString();
        if (o instanceof Object[])
            return Stream.of((Object[]) o).map(ReflectionUtils::toString).collect(Collectors.toList()).toString();
        if (o instanceof byte[])
            return Arrays.toString((byte[]) o);
        if (o instanceof short[])
            return Arrays.toString((short[]) o);
        if (o instanceof int[])
            return Arrays.toString((int[]) o);
        if (o instanceof long[])
            return Arrays.toString((long[]) o);
        if (o instanceof float[])
            return Arrays.toString((float[]) o);
        if (o instanceof double[])
            return Arrays.toString((double[]) o);
        if (o instanceof char[])
            return Arrays.toString((char[]) o);
        if (o instanceof boolean[])
            return Arrays.toString((boolean[]) o);
        throw new UnsupportedOperationException("Unknown array type: " + o.getClass());
    }

    public static List<Difference<?>> recursiveEquals(Object lhs, Object rhs)
    {
        if (Objects.equals(lhs, rhs)) return Collections.emptyList();
        // there is a difference... find it...
        List<Difference<?>> accum = new ArrayList<>();
        DeterministicIdentitySet<Object> seenLhs = new DeterministicIdentitySet<>();
        DeterministicIdentitySet<Object> seenRhs = new DeterministicIdentitySet<>();
        recursiveEquals(".", seenLhs, lhs, seenRhs, rhs, accum);
        return accum;
    }

    private static void recursiveEquals(String path, DeterministicIdentitySet<Object> seenLhs, Object lhs, DeterministicIdentitySet<Object> seenRhs, Object rhs, List<Difference<?>> accum)
    {
        if (Objects.equals(lhs, rhs)) return;
        if (lhs == null || rhs == null)
        {
            // one side is null, so doesn't make sense to look further
            accum.add(new Difference<>(path, lhs, rhs));
            return;
        }
        if (!(seenLhs.add(lhs) && seenRhs.add(rhs)))
        {
            // seen pointer before, unsafe to keep walking
            accum.add(new Difference<>(path, lhs, rhs));
            return;
        }
        if (!lhs.getClass().equals(rhs.getClass()))
        {
            // when types don't match the field walking won't walk... we know this isn't a match, so just return early
            accum.add(new Difference<>(path, lhs, rhs));
            return;
        }
        List<Field> fields = getFields(lhs.getClass());
        if (fields.isEmpty())
        {
            if (lhs instanceof Object[])
            {
                Object[] left = (Object[]) lhs;
                Object[] right = (Object[]) rhs;
                if (left.length != right.length)
                    accum.add(new Difference<>(path + "length", left.length, right.length));
                path = path.substring(0, path.length() - 1); // remove the last '.'
                for (int i = 0, size = Math.min(left.length, right.length); i < size; i++)
                {
                    Object l = left[i];
                    Object r = right[i];
                    recursiveEquals(path + "[" + i + "].", seenLhs,  l, seenRhs, r, accum);
                }
                return;
            }
            accum.add(new Difference<>(path, lhs, rhs));
            return;
        }
        int startSize = accum.size();
        for (Field f : fields)
        {
            f.setAccessible(true);
            try
            {
                recursiveEquals(path + f.getName() + '.', seenLhs, f.get(lhs), seenRhs, f.get(rhs), accum);
            }
            catch (IllegalAccessException e)
            {
                throw new AssertionError(e);
            }
        }
        // no field difference found, but the type defined equals as different, so add this level
        if (startSize == accum.size())
            accum.add(new Difference<>(path, lhs, rhs));
    }
    private static boolean checkFields(Class<?> klass)
    {
        return !(klass.isEnum() || NO_RECURSIVE.contains(klass));
    }

    private static List<Field> getFields(Class<?> aClass)
    {
        if (!checkFields(aClass))
            return Collections.emptyList();
        List<Field> fields = new ArrayList<>();
        for (Class<?> klass = aClass; klass != null; klass = klass.getSuperclass())
        {
            Field[] fs = klass.getDeclaredFields();
            if (fs != null && fs.length > 0)
            {
                for (Field f : fs)
                {
                    if (Modifier.isStatic(f.getModifiers()) || Modifier.isTransient(f.getModifiers())) continue;
                    fields.add(f);
                }
            }
        }
        return fields;
    }

    public static class Difference<T>
    {
        public final String path;
        public final T lhs, rhs;

        public Difference(String path, T lhs, T rhs)
        {
            this.path = path;
            this.lhs = lhs;
            this.rhs = rhs;
        }

        @Override
        public String toString()
        {
            return "Diff{" +
                   "path='" + path + '\'' +
                   ", lhs=" + ReflectionUtils.toString(lhs) +
                   ", rhs=" + ReflectionUtils.toString(rhs) +
                   '}';
        }
    }
}
