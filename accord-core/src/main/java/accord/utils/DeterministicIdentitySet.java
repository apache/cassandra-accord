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

import com.google.common.collect.Iterables;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class DeterministicIdentitySet<T> extends AbstractSet<T>
{
    static class Entry<T>
    {
        final T item;
        Entry<T> prev;
        Entry<T> next;

        Entry(T item)
        {
            this.item = item;
        }
    }

    // TODO (low priority): an identity hash map that doesn't mind concurrent modification / iteration
    final IdentityHashMap<T, Entry<T>> lookup;
    final Entry<T> head = new Entry<T>(null);

    public DeterministicIdentitySet()
    {
        this(0);
    }

    public DeterministicIdentitySet(int size)
    {
        head.prev = head.next = head;
        lookup = new IdentityHashMap<>(size);
    }

    public DeterministicIdentitySet(DeterministicIdentitySet<T> copy)
    {
        this(copy.size());
        copy.forEach(this::addInternal);
    }

    @Override
    public Iterator<T> iterator()
    {
        return new Iterator<T>()
        {
            Entry<T> next = head.next;
            @Override
            public boolean hasNext()
            {
                return next != head;
            }

            @Override
            public T next()
            {
                if (!hasNext())
                    throw new NoSuchElementException();
                T result = next.item;
                next = next.next;
                return result;
            }
        };
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof Iterable))
            return false;

        return Iterables.elementsEqual(this, (Iterable<?>) o);
    }

    @Override
    public int hashCode()
    {
        int result = 1;

        for (T element : this)
            result = 31 * result + (element == null ? 0 : element.hashCode());

        return result;
    }

    @Override
    public int size()
    {
        return lookup.size();
    }

    private boolean addInternal(T item)
    {
        Entry<T> entry = lookup.computeIfAbsent(item, Entry::new);
        if (entry.prev != null)
            return false;
        entry.prev = head;
        entry.next = head.next;
        head.next = entry;
        entry.next.prev = entry;
        return true;
    }
    // we add to the front, and iterate in reverse order, so that we can add and remove while iterating without modifying the set we iterate over
    @Override
    public boolean add(T item)
    {
        return addInternal(item);
    }

    @Override
    public boolean remove(Object item)
    {
        Entry<T> entry = lookup.remove(item);
        if (entry == null)
            return false;
        Entry<T> prev = entry.prev, next = entry.next;
        prev.next = next;
        next.prev = prev;
        entry.next = null; // so deletes can be detected during enumeration
        return true;
    }

    @Override
    public void forEach(Consumer<? super T> consumer)
    {
        Entry<T> cur = head.next;
        while (cur != head)
        {
            consumer.accept(cur.item);
            while (cur.next == null)
                cur = cur.prev;
            cur = cur.next;
        }
    }

    public <P> void forEach(BiConsumer<? super P, ? super T> consumer, P parameter)
    {
        Entry<T> cur = head.next;
        while (cur != head)
        {
            consumer.accept(parameter, cur.item);
            while (cur.next == null)
                cur = cur.prev;
            cur = cur.next;
        }
    }
}
