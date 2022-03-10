package accord.impl.list;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import accord.api.Data;
import accord.api.Key;

public class ListData extends TreeMap<Key, int[]> implements Data
{
    @Override
    public Data merge(Data data)
    {
        if (data != null)
            this.putAll(((ListData)data));
        return this;
    }

    @Override
    public String toString()
    {
        return entrySet().stream()
                         .map(e -> e.getKey() + "=" + Arrays.toString(e.getValue()))
                         .collect(Collectors.joining(", ", "{", "}"));
    }
}
