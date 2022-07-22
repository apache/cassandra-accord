package accord.maelstrom;

import java.util.Map;
import java.util.TreeMap;

import accord.api.Key;
import accord.api.Data;
import accord.api.Update;
import accord.primitives.KeyRanges;
import accord.primitives.Keys;

public class MaelstromUpdate extends TreeMap<Key, Value> implements Update
{
    @Override
    public Keys keys()
    {
        return new Keys(keySet());
    }

    @Override
    public MaelstromWrite apply(Data read)
    {
        MaelstromWrite write = new MaelstromWrite();
        Map<Key, Value> data = (MaelstromData)read;
        for (Map.Entry<Key, Value> e : entrySet())
            write.put(e.getKey(), data.get(e.getKey()).append(e.getValue()));
        return write;
    }

    @Override
    public Update slice(KeyRanges ranges)
    {
        MaelstromUpdate result = new MaelstromUpdate();
        for (Map.Entry<Key, Value> e : entrySet())
        {
            if (ranges.contains(e.getKey()))
                result.put(e.getKey(), e.getValue());
        }
        return result;
    }

    @Override
    public Update merge(Update other)
    {
        MaelstromUpdate result = new MaelstromUpdate();
        result.putAll(this);
        result.putAll((MaelstromUpdate) other);
        return result;
    }
}
