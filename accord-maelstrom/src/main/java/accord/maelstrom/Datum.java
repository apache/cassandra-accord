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
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.zip.CRC32;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

public class Datum implements Comparable<Datum>
{
    public static final boolean COMPARE_BY_HASH = true;

    public static class Hash implements Comparable<Hash>
    {
        final int hash;
        public Hash(int hash)
        {
            this.hash = hash;
        }

        @Override
        public int compareTo(Hash that)
        {
            return Integer.compare(this.hash, that.hash);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Hash hash1 = (Hash) o;
            return hash == hash1.hash;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(hash);
        }

        public String toString()
        {
            return "#" + hash;
        }
    }

    public enum Kind
    {
        STRING, LONG, DOUBLE, HASH;

        public MaelstromKey.Routing[] split(int count)
        {
            if (count <= 1)
                throw new IllegalArgumentException();

            MaelstromKey.Routing[] result = new MaelstromKey.Routing[count];
            if (this != Kind.HASH)
                throw new UnsupportedOperationException();

            int delta = 2 * (Integer.MAX_VALUE / count);
            int start = Integer.MIN_VALUE;
            for (int i = 0; i < count; ++i)
                result[i] = new MaelstromKey.Routing(start + i * delta);

            return result;
        }

        private static final int CHARS = 63;

        private static String toString(long v)
        {
            if (v == 0) return "";
            --v;
            char[] buf = new char[4];
            for (int i = 3 ; i >= 0 ; --i)
            {
                buf[i] = toChar(v % CHARS);
                v /= CHARS;
            }
            return new String(buf);
        }

        private static char toChar(long v)
        {
            if (v == 0) return ' ';
            v -= 1;
            if (v < 10) return (char) ('0' + v);
            v -= 10;
            if (v < 26) return (char) ('A' + v);
            v -= 26;
            return (char) ('a' + v);
        }

    }

    public final Kind kind;
    public final Object value;

    Datum(Kind kind, Object value)
    {
        this.kind = kind;
        this.value = value;
    }

    public Datum(String value)
    {
        this(Kind.STRING, value);
    }

    public Datum(Long value)
    {
        this(Kind.LONG, value);
    }

    public Datum(Double value)
    {
        this(Kind.DOUBLE, value);
    }

    public Datum(Hash value)
    {
        this(Kind.HASH, value);
    }

    @Override
    public int hashCode()
    {
        return value == null ? 0 : hash(value);
    }

    @Override
    public boolean equals(Object that)
    {
        return that == this || (that instanceof Datum && equals((Datum) that));
    }

    public boolean equals(Datum that)
    {
        return this.kind.equals(that.kind) && Objects.equals(this.value, that.value);
    }

    @Override
    public String toString()
    {
        return value == null ? kind + ":+Inf" : value.toString();
    }

    @Override
    public int compareTo(Datum that)
    {
        int c = 0;
        if (COMPARE_BY_HASH)
            c = Integer.compare(hash(this.value), hash(that.value));
        if (c == 0) c = this.kind.compareTo(that.kind);
        if (c != 0) return c;
        if (this.value == null || that.value == null)
        {
            if (this.value == null && that.value == null)
                return 0;
            return this.value == null ? 1 : -1;
        }
        return ((Comparable)this.value).compareTo(that.value);
    }

    static int hash(Object object)
    {
        if (object == null)
            return Integer.MAX_VALUE;

        if (object instanceof Hash)
            return ((Hash) object).hash;

        CRC32 crc32c = new CRC32();
        int i = object.hashCode();
        crc32c.update(i);
        crc32c.update(i >> 8);
        crc32c.update(i >> 16);
        crc32c.update(i >> 24);
        return (int)crc32c.getValue();
    }

    public static Datum read(JsonReader in) throws IOException
    {
        return read(in, Datum::new);
    }

    public void write(JsonWriter out) throws IOException
    {
        if (!isSimple())
        {
            out.beginArray();
            out.value(kind.toString());
            if (kind == Kind.HASH)
            {
                out.value(value != null);
                if (value != null)
                    out.value(((Hash)value).hash);
            }
            out.endArray();
            return;
        }
        switch (kind)
        {
            default: throw new IllegalStateException();
            case LONG: out.value((Long) value); break;
            case DOUBLE: out.value((Double) value); break;
            case STRING: out.value((String) value); break;
        }
    }

    public boolean isSimple()
    {
        return value != null && kind != Kind.HASH;
    }

    protected static <V> V read(JsonReader in, BiFunction<Kind, Object, V> constructor) throws IOException
    {
        Datum.Kind type;
        Object value;
        switch (in.peek())
        {
            default:
                throw new IllegalStateException();
            case BEGIN_ARRAY:
                in.beginArray();
                type = Kind.valueOf(in.nextString());
                if (type == Kind.HASH && in.nextBoolean()) value = new Hash(in.nextInt());
                else value = null;
                in.endArray();
                break;
            case STRING:
                value = in.nextString();
                type = Datum.Kind.STRING;
                break;
            case NUMBER:
                try { value = in.nextLong(); type = Datum.Kind.LONG; }
                catch (IllegalArgumentException iae) { value = in.nextDouble(); type = Datum.Kind.DOUBLE; }
                break;
        }
        return constructor.apply(type, value);
    }

    public static final TypeAdapter<Datum> GSON_ADAPTER = new TypeAdapter<Datum>()
    {
        @Override
        public void write(JsonWriter out, Datum value) throws IOException
        {
            value.write(out);
        }

        @Override
        public Datum read(JsonReader in) throws IOException
        {
            return Datum.read(in);
        }
    };
}
