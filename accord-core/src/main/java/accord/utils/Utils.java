package accord.utils;

import java.util.*;
import java.util.function.IntFunction;

// TODO: remove when jdk8 support is dropped
public class Utils
{
    // reimplements Collection#toArray
    public static <T> T[] toArray(Collection<T> src, IntFunction<T[]> factory)
    {
        T[] dst = factory.apply(src.size());
        int i = 0;
        for (T item : src)
            dst[i++] = item;
        return dst;
    }

    // reimplements Collection#toArray
    public static <T> T[] toArray(List<T> src, IntFunction<T[]> factory)
    {
        T[] dst = factory.apply(src.size());
        for (int i=0; i<dst.length; i++)
            dst[i] = src.get(i);
        return dst;
    }

    // reimplements List#of
    public static <T> List<T> listOf(T... src)
    {
        List<T> dst = new ArrayList<>(src.length);
        for (int i=0; i<src.length; i++)
            dst.add(src[i]);
        return dst;
    }
}
