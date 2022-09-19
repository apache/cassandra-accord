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
import java.util.NavigableMap;
import java.util.TreeMap;

import accord.local.Node;
import accord.api.Key;
import accord.messages.MessageType;
import accord.primitives.Keys;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import accord.maelstrom.Packet.Type;
import accord.messages.Reply;

public class MaelstromReply extends Body implements Reply
{
    final MaelstromResult result;

    public MaelstromReply(long in_reply_to, MaelstromResult result)
    {
        super(Type.txn_ok, SENTINEL_MSG_ID, in_reply_to);
        this.result = result;
    }

    @Override
    public MessageType type()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    void writeBody(JsonWriter out) throws IOException
    {
        super.writeBody(out);
        out.name("txn");
        Keys keys = result.keys;
        Value[] reads = result.read;
        MaelstromUpdate update = result.update;
        out.beginArray();
        for (int i = 0 ; i < keys.size() ; ++i)
        {
            MaelstromKey key = (MaelstromKey) keys.get(i);
            if (reads[i] != null)
            {
                out.beginArray();
                out.value("r");
                key.datum.write(out);
                reads[i].writeVerbose(out);
                out.endArray();
            }
            if (update != null && update.containsKey(key))
            {
                for (Datum append : update.get(key).contents)
                {
                    out.beginArray();
                    out.value("append");
                    key.datum.write(out);
                    append.write(out);
                    out.endArray();
                }
            }
        }
        out.endArray();
    }

    public static MaelstromResult readResultExternal(JsonReader in, Node.Id client, long requestId) throws IOException
    {
        if (in.peek() == JsonToken.NULL)
            return null;

        NavigableMap<Key, Value> reads = new TreeMap<>();
        MaelstromUpdate update = new MaelstromUpdate();
        in.beginArray();
        while (in.hasNext())
        {
            in.beginArray();
            String op = in.nextString();
            Key key = MaelstromKey.readKey(in);
            switch (op)
            {
                default: throw new IllegalStateException("Invalid op: " + op);
                case "r":
                {
                    Value value = Value.read(in);
                    reads.put(key, value);
                    break;
                }
                case "append":
                    Datum value = Datum.read(in);
                    update.merge(key, new Value(value), Value::append);
            }
            in.endArray();
        }
        in.endArray();

        for (Key key : update.keySet())
            reads.putIfAbsent(key, null);

        Keys keys = new Keys(reads.navigableKeySet());
        Value[] values = reads.values().toArray(new Value[0]);

        return new MaelstromResult(client, requestId, keys, values, update);
    }

}
