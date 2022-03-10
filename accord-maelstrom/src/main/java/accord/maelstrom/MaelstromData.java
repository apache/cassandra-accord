package accord.maelstrom;

import java.util.TreeMap;

import accord.api.Data;
import accord.api.Key;

public class MaelstromData extends TreeMap<Key, Value> implements Data
{
    @Override
    public Data merge(Data data)
    {
        if (data != null)
            this.putAll(((MaelstromData)data));
        return this;
    }
}
