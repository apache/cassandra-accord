package accord.impl.list;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import accord.api.Key;
import accord.api.Data;
import accord.api.Update;

public class ListUpdate extends TreeMap<Key, Integer> implements Update
{
    @Override
    public ListWrite apply(Data read)
    {
        ListWrite write = new ListWrite();
        Map<Key, int[]> data = (ListData)read;
        for (Map.Entry<Key, Integer> e : entrySet())
            write.put(e.getKey(), append(data.get(e.getKey()), e.getValue()));
        return write;
    }

    private static int[] append(int[] to, int append)
    {
        to = Arrays.copyOf(to, to.length + 1);
        to[to.length - 1] = append;
        return to;
    }

    @Override
    public String toString()
    {
        return entrySet().stream()
                         .map(e -> e.getKey() + ":" + e.getValue())
                         .collect(Collectors.joining(", ", "{", "}"));
    }
}
