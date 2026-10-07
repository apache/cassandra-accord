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

import java.lang.reflect.Array;

public class ArrayUtils
{
    public static <T> T[] concat(T[] first, T[] second)
    {
        T[] array = (T[]) Array.newInstance(first.getClass().getComponentType(), first.length + second.length);
        System.arraycopy(first, 0, array, 0, first.length);
        System.arraycopy(second, 0, array, first.length, second.length);
        return array;
    }

    public static void reverse(int[] array)
    {
        for (int i = 0; i < array.length / 2; i++)
        {
            int tmp = array[i];
            array[i] = array[array.length- 1 - i];
            array[array.length - 1 - i] = tmp;
        }
    }

    public static void shuffle(int[] array, RandomSource rs)
    {
        for (int i = array.length; i > 1; i--)
        {
            int j = i - 1;
            int k = rs.nextInt(i);
            int tmp = array[j];
            array[j] = array[k];
            array[k] = tmp;
        }
    }

    public static void shuffle(Object[] array, RandomSource rs)
    {
        for (int i = array.length; i > 1; i--)
        {
            int j = i - 1;
            int k = rs.nextInt(i);
            Object tmp = array[j];
            array[j] = array[k];
            array[k] = tmp;
        }
    }
}
