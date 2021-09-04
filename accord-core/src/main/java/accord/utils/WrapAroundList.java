package accord.utils;

import java.util.AbstractList;

public class WrapAroundList<T> extends AbstractList<T>
{
    final T[] contents;
    final int start, end, size;

    public WrapAroundList(T[] contents, int start, int end)
    {
        this.contents = contents;
        this.start = start;
        this.end = end;
        this.size = end > start ? end - start : end + (contents.length - start);
    }


    @Override
    public T get(int index)
    {
        int i = start + index;
        if (i >= contents.length) i -= contents.length;
        return contents[i];
    }

    @Override
    public int size()
    {
        return size;
    }
}