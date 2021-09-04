package accord.utils;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;

public class WrapAroundSet<T> extends AbstractSet<T>
{
    final Map<T, Integer> lookup;
    final WrapAroundList<T> list;

    public WrapAroundSet(Map<T, Integer> lookup, WrapAroundList<T> list)
    {
        this.lookup = lookup;
        this.list = list;
    }

    @Override
    public boolean contains(Object o)
    {
        Integer i = lookup.get(o);
        if (i == null) return false;
        if (list.end > list.start)
            return i >= list.start && i < list.end;
        else
            return i >= list.start || i < list.end;
    }

    @Override
    public Iterator<T> iterator()
    {
        return list.iterator();
    }

    @Override
    public int size()
    {
        return list.size;
    }
}
