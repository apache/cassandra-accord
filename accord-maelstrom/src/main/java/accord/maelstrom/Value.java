package accord.maelstrom;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

// TODO: abstract
public class Value
{
    public static final Value EMPTY = new Value();

    final Datum[] contents;

    private Value()
    {
        this.contents = new Datum[0];
    }

    public Value(Datum contents)
    {
        this.contents = new Datum[] { contents };
    }

    public Value(Datum[] contents)
    {
        this.contents = contents;
    }

    public Value append(Datum datum)
    {
        Datum[] contents = Arrays.copyOf(this.contents, this.contents.length + 1);
        contents[contents.length - 1] = datum;
        return new Value(contents);
    }

    public Value append(Value data)
    {
        Datum[] contents = Arrays.copyOf(this.contents, this.contents.length + data.contents.length);
        for (int i = contents.length - data.contents.length ; i < contents.length ; ++i)
            contents[i] = data.contents[data.contents.length + i - contents.length];
        return new Value(contents);
    }

    public void write(JsonWriter out) throws IOException
    {
        if (contents.length == 1 && contents[0].isSimple())
        {
            contents[0].write(out);
        }
        else
        {
            out.beginArray();
            for (Datum datum : contents)
                datum.write(out);
            out.endArray();
        }
    }

    public void writeVerbose(JsonWriter out) throws IOException
    {
        out.beginArray();
        for (Datum datum : contents)
            datum.write(out);
        out.endArray();
    }

    public static Value read(JsonReader in) throws IOException
    {
        return read(in, Value::new);
    }

    protected static <V> V read(JsonReader in, Function<Datum[], V> constructor) throws IOException
    {
        if (in.peek() == JsonToken.NULL)
        {
            in.nextNull();
            return null;
        }

        if (in.peek() == JsonToken.BEGIN_ARRAY)
        {
            List<Datum> buffer = new ArrayList<>();
            in.beginArray();
            while (in.hasNext())
                buffer.add(Datum.read(in));
            in.endArray();
            return constructor.apply(buffer.toArray(Datum[]::new));
        }

        return constructor.apply(new Datum[] { Datum.read(in) });
    }

    public static final TypeAdapter<Value> GSON_ADAPTER = new TypeAdapter<>()
    {
        @Override
        public void write(JsonWriter out, Value value) throws IOException
        {
            if (value == null) out.nullValue();
            else value.write(out);
        }

        @Override
        public Value read(JsonReader in) throws IOException
        {
            return Value.read(in);
        }
    };
}
