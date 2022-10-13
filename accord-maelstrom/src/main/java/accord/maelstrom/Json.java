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
import java.util.*;

import accord.local.Node;
import accord.api.Result;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import accord.local.Node.Id;
import accord.api.Key;
import accord.primitives.Deps;
import accord.txn.Txn;
import accord.primitives.TxnId;
import accord.txn.Writes;
import accord.primitives.Ballot;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.messages.ReadData.ReadOk;

public class Json
{
    public static final Gson GSON;
    public static final TypeAdapter<Object> DEFAULT_ADAPTER = new TypeAdapter<Object>()
    {
        @Override
        public void write(JsonWriter out, Object value)
        {
            GSON.toJson(value, Object.class, out);
        }

        @Override
        public Object read(JsonReader in)
        {
            return GSON.fromJson(in, Object.class);
        }
    };

    public static final TypeAdapter<Id> ID_ADAPTER = new TypeAdapter<Id>()
    {
        @Override
        public void write(JsonWriter out, Id value) throws IOException
        {
            if (value.id == 0) out.nullValue();
            else out.value(Json.toString(value));
        }

        @Override
        public Id read(JsonReader in) throws IOException
        {
            if (in.peek() == JsonToken.NULL)
            {
                in.nextNull();
                return Id.NONE;
            }

            return parseId(in.nextString());
        }
    };

    public static Id parseId(String id)
    {
        switch (id.charAt(0))
        {
            case 'c': return new Id(-Long.parseLong(id.substring(1)));
            case 'n':return  new Id( Long.parseLong(id.substring(1)));
            default: throw new IllegalStateException();
        }
    }

    public static String toString(Id id)
    {
        if (id.id < 0) return "c" + id.id;
        else return "n" + id.id;
    }

    public static final TypeAdapter<Timestamp> TIMESTAMP_ADAPTER = new TypeAdapter<Timestamp>()
    {
        @Override
        public void write(JsonWriter out, Timestamp value) throws IOException
        {
            if (value == null) out.nullValue();
            else writeTimestamp(out, value);
        }

        @Override
        public Timestamp read(JsonReader in) throws IOException
        {
            if (in.peek() == JsonToken.NULL)
            {
                in.nextNull();
                return null;
            }
            return readTimestamp(in, Timestamp::new);
        }
    };

    private interface TimestampFactory<T>
    {
        T create(long epoch, long real, int logical, Id node);
    }

    private static <T> T readTimestamp(JsonReader in, TimestampFactory<T> factory) throws IOException
    {
        in.beginArray();
        long epoch = in.nextLong();
        long real = in.nextLong();
        int logical = in.nextInt();
        Id node = ID_ADAPTER.read(in);
        in.endArray();
        return factory.create(epoch, real, logical, node);
    }

    private static void writeTimestamp(JsonWriter out, Timestamp timestamp) throws IOException
    {
        out.beginArray();
        out.value(timestamp.epoch);
        out.value(timestamp.real);
        out.value(timestamp.logical);
        ID_ADAPTER.write(out, timestamp.node);
        out.endArray();
    }

    public static final TypeAdapter<TxnId> TXNID_ADAPTER = new TypeAdapter<TxnId>()
    {
        @Override
        public void write(JsonWriter out, TxnId value) throws IOException
        {
            writeTimestamp(out, value);
        }

        @Override
        public TxnId read(JsonReader in) throws IOException
        {
            return readTimestamp(in, TxnId::new);
        }
    };

    public static final TypeAdapter<Ballot> BALLOT_ADAPTER = new TypeAdapter<Ballot>()
    {
        @Override
        public void write(JsonWriter out, Ballot value) throws IOException
        {
            writeTimestamp(out, value);
        }

        @Override
        public Ballot read(JsonReader in) throws IOException
        {
            return readTimestamp(in, Ballot::new);
        }
    };


    public static final TypeAdapter<Keys> KEYS_ADAPTER = new TypeAdapter<Keys>()
    {
        @Override
        public void write(JsonWriter out, Keys value) throws IOException
        {
            out.beginArray();
            for (Key key : value)
                ((MaelstromKey)key).datum.write(out);
            out.endArray();
        }

        @Override
        public Keys read(JsonReader in) throws IOException
        {
            List<Key> keys = new ArrayList<>();
            in.beginArray();
            while (in.hasNext())
                keys.add(MaelstromKey.read(in));
            in.endArray();
            return Keys.of(keys);
        }
    };

    public static final TypeAdapter<Txn> TXN_ADAPTER = new TypeAdapter<Txn>()
    {
        @Override
        public void write(JsonWriter out, Txn txn) throws IOException
        {
            if (txn == null)
            {
                out.nullValue();
                return;
            }

            Keys keys = txn.keys();
            MaelstromRead read = (MaelstromRead) txn.read();
            MaelstromUpdate update = (MaelstromUpdate) txn.update();

            out.beginObject();
            out.name("r");
            out.beginArray();
            for (int i = 0 ; i < keys.size() ; ++i)
            {
                MaelstromKey key = (MaelstromKey) keys.get(i);
                if (read.keys.indexOf(key) >= 0)
                {
                    key.datum.write(out);
                }
            }
            out.endArray();
            out.name("append");
            out.beginArray();
            for (int i = 0 ; i < keys.size() ; ++i)
            {
                MaelstromKey key = (MaelstromKey) keys.get(i);
                if (update != null && update.containsKey(key))
                {
                    out.beginArray();
                    key.datum.write(out);
                    update.get(key).write(out);
                    out.endArray();
                }
            }
            out.endArray();
            out.name("client");
            out.value(((MaelstromQuery)txn.query()).client.id);
            out.name("requestId");
            out.value(((MaelstromQuery)txn.query()).requestId);
            out.endObject();
        }

        @Override
        public Txn read(JsonReader in) throws IOException
        {
            if (in.peek() == JsonToken.NULL)
                return null;

            NavigableSet<Key> buildReadKeys = new TreeSet<>();
            NavigableSet<Key> buildKeys = new TreeSet<>();
            MaelstromUpdate update = new MaelstromUpdate();

            Node.Id client = null;
            long requestId = Long.MIN_VALUE;
            in.beginObject();
            while (in.hasNext())
            {
                String kind = in.nextName();
                switch (kind)
                {
                    default: throw new IllegalStateException("Invalid kind: " + kind);
                    case "r":
                        in.beginArray();
                        while (in.hasNext())
                            buildReadKeys.add(MaelstromKey.read(in));
                        in.endArray();
                        break;
                    case "append":
                        in.beginArray();
                        while (in.hasNext())
                        {
                            in.beginArray();
                            Key key = MaelstromKey.read(in);
                            buildKeys.add(key);
                            Value append = Value.read(in);
                            update.put(key, append);
                            in.endArray();
                        }
                        in.endArray();
                        break;
                    case "client":
                        client = ID_ADAPTER.read(in);
                        break;
                    case "requestId":
                        requestId = in.nextLong();
                        break;
                }
            }
            in.endObject();

            if (client == null)
                throw new IllegalStateException();

            buildKeys.addAll(buildReadKeys);
            Keys readKeys = new Keys(buildReadKeys);
            Keys keys = new Keys(buildKeys);
            MaelstromRead read = new MaelstromRead(keys, readKeys);
            MaelstromQuery query = new MaelstromQuery(client, requestId);

            return new Txn.InMemory(keys, read, query, update);
        }
    };

    public static final TypeAdapter<Deps> DEPS_ADAPTER = new TypeAdapter<Deps>()
    {
        @Override
        public void write(JsonWriter out, Deps value) throws IOException
        {
            out.beginArray();
            for (Map.Entry<Key, TxnId> e : value)
            {
                out.beginArray();
                ((MaelstromKey)e.getKey()).datum.write(out);
                GSON.toJson(e.getValue(), TxnId.class, out);
                out.endArray();
            }
            out.endArray();
        }

        @Override
        public Deps read(JsonReader in) throws IOException
        {
            in.beginArray();
            if (!in.hasNext())
            {
                in.endArray();
                return Deps.NONE;
            }

            TreeMap<Key, List<TxnId>> byKey = new TreeMap<>();
            while (in.hasNext())
            {
                in.beginArray();
                Key key = MaelstromKey.read(in);
                TxnId txnId = GSON.fromJson(in, TxnId.class);
                byKey.computeIfAbsent(key, ignore -> new ArrayList<>()).add(txnId);
                in.endArray();
            }
            in.endArray();

            try (Deps.OrderedBuilder builder = Deps.orderedBuilder(true))
            {
                byKey.forEach((key, txnIds) -> {
                    builder.nextKey(key);
                    txnIds.forEach(builder::add);
                });
                return builder.build();
            }
        }
    };

    public static final TypeAdapter<Writes> TXN_WRITES_ADAPTER = new TypeAdapter<Writes>()
    {
        @Override
        public void write(JsonWriter out, Writes value) throws IOException
        {
            if (value == null)
            {
                out.nullValue();
                return;
            }
            out.beginObject();
            out.name("executeAt");
            GSON.toJson(value.executeAt, Timestamp.class, out);
            out.name("keys");
            Keys keys = value.keys;
            KEYS_ADAPTER.write(out, keys);
            out.name("writes");
            MaelstromWrite write = (MaelstromWrite) value.write;
            out.beginArray();
            for (Key key : keys)
            {
                Value append = write.get(key);
                if (append == null) out.nullValue();
                else append.write(out);
            }
            out.endArray();
            out.endObject();
        }

        @Override
        public Writes read(JsonReader in) throws IOException
        {
            if (in.peek() == JsonToken.NULL)
                return null;

            in.beginObject();
            Timestamp executeAt = null;
            Keys keys = null;
            List<Value> writes = null;
            while (in.hasNext())
            {
                switch (in.nextName())
                {
                    default: throw new IllegalStateException();
                    case "executeAt":
                        executeAt = GSON.fromJson(in, Timestamp.class);
                        break;
                    case "keys":
                        keys = KEYS_ADAPTER.read(in);
                        break;
                    case "writes":
                        writes = new ArrayList<>();
                        in.beginArray();
                        while (in.hasNext())
                            writes.add(Value.read(in));
                        in.endArray();
                        break;
                }
            }
            in.endObject();

            MaelstromWrite write = new MaelstromWrite();
            if (writes != null)
            {
                for (int i = 0 ; i < writes.size() ; ++i)
                {
                    if (writes.get(i) != null)
                        write.put(keys.get(i), writes.get(i));
                }
            }
            return new Writes(executeAt, keys, write);
        }
    };

    public static final TypeAdapter<ReadOk> READ_OK_ADAPTER = new TypeAdapter<ReadOk>()
    {
        @Override
        public void write(JsonWriter out, ReadOk value) throws IOException
        {
            out.beginArray();
            for (Map.Entry<Key, Value> e : ((MaelstromData)value.data).entrySet())
            {
                out.beginArray();
                ((MaelstromKey)e.getKey()).datum.write(out);
                e.getValue().write(out);
                out.endArray();
            }
            out.endArray();
        }

        @Override
        public ReadOk read(JsonReader in) throws IOException
        {
            MaelstromData result = new MaelstromData();
            in.beginArray();
            while (in.hasNext())
            {
                in.beginArray();
                MaelstromKey key = MaelstromKey.read(in);
                Value value = Value.read(in);
                result.put(key, value);
                in.endArray();
            }
            in.endArray();
            return new ReadOk(result);
        }
    };

    static final TypeAdapter FAIL = new TypeAdapter()
    {
        @Override
        public void write(JsonWriter out, Object value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object read(JsonReader in)
        {
            throw new UnsupportedOperationException();
        }
    };

    static
    {
        // TODO: Maelstrom hooks should be registered at run-time to permit separate tree
        GSON = new GsonBuilder().registerTypeAdapter(Packet.class, Packet.GSON_ADAPTER)
                                .registerTypeAdapter(Id.class, ID_ADAPTER)
                                .registerTypeAdapter(Txn.class, TXN_ADAPTER)
                                .registerTypeAdapter(Ballot.class, BALLOT_ADAPTER)
                                .registerTypeAdapter(TxnId.class, TXNID_ADAPTER)
                                .registerTypeAdapter(Timestamp.class, TIMESTAMP_ADAPTER)
                                .registerTypeAdapter(Datum.class, Datum.GSON_ADAPTER)
                                .registerTypeAdapter(MaelstromKey.class, MaelstromKey.GSON_ADAPTER)
                                .registerTypeAdapter(Value.class, Value.GSON_ADAPTER)
                                .registerTypeAdapter(Writes.class, TXN_WRITES_ADAPTER)
                                .registerTypeAdapter(MaelstromResult.class, MaelstromResult.GSON_ADAPTER)
                                .registerTypeAdapter(ReadOk.class, READ_OK_ADAPTER)
                                .registerTypeAdapter(Deps.class, Json.DEPS_ADAPTER)
                                .registerTypeAdapter(Keys.class, KEYS_ADAPTER)
                                .registerTypeAdapter(Body.class, Body.FAIL_READ)
                                .registerTypeAdapter(Result.class, MaelstromResult.GSON_ADAPTER)
                                .registerTypeAdapter(MaelstromRequest.class, Body.FAIL_READ)
                                .registerTypeAdapter(MaelstromReply.class, Body.FAIL_READ)
                                .create();
    }

}
