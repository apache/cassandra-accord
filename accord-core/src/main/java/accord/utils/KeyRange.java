package accord.utils;

import accord.api.Key;

public class KeyRange<K extends Key<K>>
{
    public final K start;
    public final K end;

    private KeyRange(K start, K end)
    {
        this.start = start;
        this.end = end;
    }

    public static <K extends Key<K>> KeyRange<K> of(K start, K end)
    {
        return new KeyRange<>(start, end);
    }
}
