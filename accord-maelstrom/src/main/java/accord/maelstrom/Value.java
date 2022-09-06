/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

import static accord.utils.Utils.toArray;

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
            return constructor.apply(toArray(buffer, Datum[]::new));
        }

        return constructor.apply(new Datum[] { Datum.read(in) });
    }

    public static final TypeAdapter<Value> GSON_ADAPTER = new TypeAdapter<Value>()
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
