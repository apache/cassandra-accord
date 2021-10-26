package accord.maelstrom;

import java.io.IOException;

import accord.api.Key;
import accord.api.KeyRange;
import accord.topology.KeyRanges;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

public class MaelstromKey extends Datum<MaelstromKey> implements Key<MaelstromKey>
{
    public static class Range extends KeyRange.EndInclusive<MaelstromKey>
    {
        public Range(MaelstromKey start, MaelstromKey end)
        {
            super(start, end);
        }

        @Override
        public KeyRange<MaelstromKey> subRange(MaelstromKey start, MaelstromKey end)
        {
            return new Range(start, end);
        }

        @Override
        public KeyRanges split(int count)
        {
            Preconditions.checkArgument(start().kind == Kind.HASH);
            Preconditions.checkArgument(end().kind == Kind.HASH);
            long startHash = ((Hash) start().value).hash;
            long endHash = end().value != null ? ((Hash) end().value).hash : Integer.MAX_VALUE;
            long currentSize = endHash - startHash;
            if (currentSize < count)
                return new KeyRanges(new KeyRange[]{this});
            long interval =  currentSize / count;

            long subEnd = 0;
            KeyRange[] ranges = new KeyRange[count];
            for (int i=0; i<count; i++)
            {
                long subStart = i > 0 ? subEnd : startHash;
                subEnd = i < count - 1 ? subStart + interval : endHash;
                ranges[i] = new Range(new MaelstromKey(Kind.HASH, new Hash(Ints.checkedCast(subStart))),
                                      i < count - 1 ? new MaelstromKey(Kind.HASH, new Hash(Ints.checkedCast(subEnd))) : end());
            }
            return new KeyRanges(ranges);
        }
    }

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
