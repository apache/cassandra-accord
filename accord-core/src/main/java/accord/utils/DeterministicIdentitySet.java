package accord.utils;

import java.util.AbstractSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
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

    // TODO: an identity hash map that doesn't mind concurrent modification / iteration
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
    public int size()
    {
        return lookup.size();
    }

    // we add to the front, and iterate in reverse order, so that we can add and remove while iterating without modifying the set we iterate over
    public boolean add(T item)
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

    public boolean remove(Object item)
    {
        Entry<T> entry = lookup.remove(item);
        if (entry == null)
            return false;
        Entry<T> prev = entry.prev, next = entry.next;
        prev.next = next;
        next.prev = prev;
        return true;
    }

    public void forEach(Consumer<? super T> consumer)
    {
        Entry<T> next = head.next;
        while (next != head)
        {
            consumer.accept(next.item);
            next = next.next;
        }
    }
}
