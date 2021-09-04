package accord.maelstrom;

import java.io.IOException;

import accord.api.Key;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

public class MaelstromKey extends Datum<MaelstromKey> implements Key<MaelstromKey>
{
    public MaelstromKey(Kind kind, Object value)
    {
        super(kind, value);
    }

    public MaelstromKey(String value)
    {
        super(value);
    }

    public MaelstromKey(Long value)
    {
        super(value);
    }

    public MaelstromKey(Double value)
    {
        super(value);
    }

    @Override
    public int compareTo(MaelstromKey that)
    {
        return compareTo((Datum) that);
    }

    public static MaelstromKey read(JsonReader in) throws IOException
    {
        return read(in, MaelstromKey::new);
    }

    public static final TypeAdapter<MaelstromKey> GSON_ADAPTER = new TypeAdapter<>()
    {
        @Override
        public void write(JsonWriter out, MaelstromKey value) throws IOException
        {
            value.write(out);
        }

        @Override
        public MaelstromKey read(JsonReader in) throws IOException
        {
            return MaelstromKey.read(in);
        }
    };
 }
