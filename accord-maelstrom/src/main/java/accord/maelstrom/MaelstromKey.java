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

import accord.api.RoutingKey;

import accord.local.ShardDistributor;
import accord.primitives.RoutableKey;
import accord.utils.Invariants;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import javax.annotation.Nonnull;

public class MaelstromKey implements RoutableKey
{
    public static class Splitter implements ShardDistributor.EvenSplit.Splitter<Long>
    {
        private static long hash(RoutingKey routingKey)
        {
            Datum.Hash hash = ((Datum.Hash)((MaelstromKey)routingKey).datum.value);
            if (hash == null)
                return Integer.MAX_VALUE;
            return hash.hash;
        }

        @Override
        public Long sizeOf(accord.primitives.Range range)
        {
            return hash(range.end()) - hash(range.start());
        }

        @Override
        public accord.primitives.Range subRange(accord.primitives.Range range, Long start, Long end)
        {
            Invariants.checkState(end - start <= Integer.MAX_VALUE);
            long startHash = hash(range.start());
            Invariants.checkArgument(startHash + end <= hash(range.end()));
            return range.subRange(
                    new Routing(Datum.Kind.HASH, new Datum.Hash((int) (startHash + start))),
                    new Routing(Datum.Kind.HASH, new Datum.Hash((int) (startHash + end)))
            );
        }

        @Override
        public Long zero()
        {
            return 0L;
        }

        @Override
        public Long add(Long a, Long b)
        {
            return a + b;
        }

        @Override
        public Long subtract(Long a, Long b)
        {
            return a - b;
        }

        @Override
        public Long divide(Long a, int i)
        {
            return a / i;
        }

        @Override
        public Long multiply(Long a, int i)
        {
            return a * i;
        }

        @Override
        public int min(Long v, int i)
        {
            return (int)Math.min(v, i);
        }

        @Override
        public int compare(Long a, Long b)
        {
            return a.compareTo(b);
        }
    }

    public static class Key extends MaelstromKey implements accord.api.Key
    {
        public Key(Datum.Kind kind, Object value)
        {
            super(kind, value);
        }

        public Key(Double value)
        {
            super(value);
        }
    }

    public static class Routing extends MaelstromKey implements accord.api.RoutingKey
    {
        public Routing(Datum.Kind kind, Object value)
        {
            super(kind, value);
        }

        public Routing(Double value)
        {
            super(value);
        }
    }

    public static class Range extends accord.primitives.Range.EndInclusive
    {
        public Range(RoutingKey start, RoutingKey end)
        {
            super(start, end);
        }

        @Override
        public accord.primitives.Range subRange(RoutingKey start, RoutingKey end)
        {
            return new Range(start, end);
        }
    }

    final Datum datum;

    public MaelstromKey(Datum.Kind kind, Object value)
    {
        datum = new Datum(kind, value);
    }

    public MaelstromKey(Double value)
    {
        datum = new Datum(value);
    }

    @Override
    public int compareTo(@Nonnull RoutableKey that)
    {
        return datum.compareTo(((MaelstromKey) that).datum);
    }

    public static Key readKey(JsonReader in) throws IOException
    {
        return Datum.read(in, Key::new);
    }

    public static Routing readRouting(JsonReader in) throws IOException
    {
        return Datum.read(in, Routing::new);
    }

    public static final TypeAdapter<Key> GSON_KEY_ADAPTER = new TypeAdapter<Key>()
    {
        @Override
        public void write(JsonWriter out, Key value) throws IOException
        {
            value.datum.write(out);
        }

        @Override
        public Key read(JsonReader in) throws IOException
        {
            return MaelstromKey.readKey(in);
        }
    };

    public static final TypeAdapter<Routing> GSON_ROUTING_ADAPTER = new TypeAdapter<Routing>()
    {
        @Override
        public void write(JsonWriter out, Routing value) throws IOException
        {
            value.datum.write(out);
        }

        @Override
        public Routing read(JsonReader in) throws IOException
        {
            return MaelstromKey.readRouting(in);
        }
    };

    @Override
    public RoutingKey toUnseekable()
    {
        if (this instanceof Routing)
            return (Routing)this;
        return new Routing(datum.kind, datum.value);
    }

    @Override
    public String toString()
    {
        return datum.toString();
    }
}
