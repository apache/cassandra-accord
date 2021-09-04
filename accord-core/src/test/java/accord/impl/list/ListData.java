package accord.impl.list;

import java.util.TreeMap;

import accord.api.Data;
import accord.api.Key;

public class ListData extends TreeMap<Key, int[]> implements Data
{
    @Override
    public Data merge(Data data)
    {
        this.putAll(((ListData)data));
        return this;
    }
}
