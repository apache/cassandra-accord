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

import java.util.Iterator;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Spliterators.spliteratorUnknownSize;

/**
 * A simple intrusive double-linked list for maintaining a list of tasks,
 * useful for invalidating queued ordered tasks
 *
 * TODO (low priority): COPIED FROM CASSANDRA
 */

@SuppressWarnings("unchecked")
public class IntrusiveLinkedList<O extends IntrusiveLinkedListNode> extends IntrusiveLinkedListNode implements Iterable<O>
{
    public IntrusiveLinkedList()
    {
        prev = next = this;
    }

    public void addFirst(O add)
    {
        if (add.next != null)
            throw new IllegalStateException();
        add(this, add, next);
    }

    public void addLast(O add)
    {
        if (add.next != null)
            throw new IllegalStateException();
        add(prev, add, this);
    }

    private void add(IntrusiveLinkedListNode after, IntrusiveLinkedListNode add, IntrusiveLinkedListNode before)
    {
        add.next = before;
        add.prev = after;
        before.prev = add;
        after.next = add;
    }

    public O poll()
    {
        IntrusiveLinkedListNode next = this.next;
        if (next == this)
            return null;
        next.remove();
        return (O) next;
    }

    public O peek()
    {
        IntrusiveLinkedListNode next = this.next;
        if (next == this)
            return null;
        return (O) next;
    }

    public boolean isEmpty()
    {
        return next == this;
    }

    @Override
    public Iterator<O> iterator()
    {
        return new Iterator<>()
        {
            IntrusiveLinkedListNode next = IntrusiveLinkedList.this.next;

            @Override
            public boolean hasNext()
            {
                return next != IntrusiveLinkedList.this;
            }

            @Override
            public O next()
            {
                O result = (O)next;
                if (result.next == null)
                    throw new NullPointerException();
                next = result.next;
                return result;
            }
        };
    }

    public Stream<O> stream()
    {
        return StreamSupport.stream(spliteratorUnknownSize(iterator(), Spliterator.IMMUTABLE), false);
    }
}

