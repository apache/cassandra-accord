package accord.maelstrom;

import java.io.IOException;

import accord.api.Key;
import accord.maelstrom.Datum.Kind;
import accord.primitives.KeyRange;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

public class MaelstromKey implements Key
{
    public static class Range extends KeyRange.EndInclusive
    {
        public Range(Key start, Key end)
        {
            super(start, end);
        }

        @Override
        public KeyRange subRange(Key start, Key end)
        {
            return new Range((MaelstromKey) start, (MaelstromKey) end);
        }
    }

    final Datum datum;

    public MaelstromKey(Kind kind, Object value)
    {
        datum = new Datum(kind, value);
    }

    public MaelstromKey(Double value)
    {
        datum = new Datum(value);
    }

    @Override
    public int compareTo(Key that)
    {
        return datum.compareTo(((MaelstromKey) that).datum);
    }

    public static MaelstromKey read(JsonReader in) throws IOException
    {
        return Datum.read(in, MaelstromKey::new);
    }

    public static final TypeAdapter<MaelstromKey> GSON_ADAPTER = new TypeAdapter<>()
    {
        @Override
        public void write(JsonWriter out, MaelstromKey value) throws IOException
        {
            value.datum.write(out);
        }

        @Override
        public MaelstromKey read(JsonReader in) throws IOException
        {
            return MaelstromKey.read(in);
        }
    };

    @Override
    public int routingHash()
    {
        return datum.hashCode();
    }
}
