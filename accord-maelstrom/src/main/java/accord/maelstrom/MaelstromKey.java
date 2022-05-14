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

import accord.api.Key;
import accord.api.RoutingKey;
import accord.maelstrom.Datum.Kind;
import accord.primitives.KeyRange;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import javax.annotation.Nonnull;

public class MaelstromKey implements Key
{
    public static class Range extends KeyRange.EndInclusive
    {
        public Range(RoutingKey start, RoutingKey end)
        {
            super(start, end);
        }

        @Override
        public KeyRange subRange(RoutingKey start, RoutingKey end)
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
    public int compareTo(@Nonnull RoutingKey that)
    {
        if (that instanceof InfiniteRoutingKey)
            return -that.compareTo(this);
        return datum.compareTo(((MaelstromKey) that).datum);
    }

    public static MaelstromKey read(JsonReader in) throws IOException
    {
        return Datum.read(in, MaelstromKey::new);
    }

    public static final TypeAdapter<MaelstromKey> GSON_ADAPTER = new TypeAdapter<MaelstromKey>()
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

    @Override
    public Key toRoutingKey()
    {
        return this;
    }
}
