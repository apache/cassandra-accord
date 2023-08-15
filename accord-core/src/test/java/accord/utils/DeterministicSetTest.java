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

import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

class DeterministicSetTest
{
    @Test
    public void removePrevious()
    {
        DeterministicSet<Integer> set = of(1, 2, 3);
        Iterator<Integer> it = set.iterator();
        assertThat(next(it)).isEqualTo(3);
        set.remove(3);
        assertThat(next(it)).isEqualTo(2);
        assertThat(next(it)).isEqualTo(1);
        assertThat(it).isExhausted();
    }

    @Test
    public void removeCurrent()
    {
        DeterministicSet<Integer> set = of(1, 2, 3);
        Iterator<Integer> it = set.iterator();
        assertThat(next(it)).isEqualTo(3);
        set.remove(2);
        assertThat(next(it)).isEqualTo(1);
        assertThat(it).isExhausted();
    }

    @Test
    public void removeNext()
    {
        DeterministicSet<Integer> set = of(1, 2, 3);
        Iterator<Integer> it = set.iterator();
        assertThat(next(it)).isEqualTo(3);
        set.remove(1);
        assertThat(next(it)).isEqualTo(2);
        assertThat(it).isExhausted();
    }

    @Test
    public void removeFirst()
    {
        DeterministicSet<Integer> set = of(1, 2, 3);
        Iterator<Integer> it = set.iterator();
        set.remove(3);
        assertThat(next(it)).isEqualTo(2);
        assertThat(next(it)).isEqualTo(1);
        assertThat(it).isExhausted();
    }

    private static <T> DeterministicSet<T> of(T... values)
    {
        DeterministicSet<T> set = new DeterministicSet<>();
        for (T t : values)
            set.add(t);
        return set;
    }

    private static <T> T next(Iterator<T> it)
    {
        assertThat(it).hasNext();
        return it.next();
    }
}