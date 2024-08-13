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
import java.util.NavigableSet;
import java.util.TreeSet;

import accord.api.Key;
import accord.local.Node;
import accord.local.Node.Id;
import accord.maelstrom.Packet.Type;
import accord.messages.MessageType;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.primitives.Keys;
import accord.primitives.Txn;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import static accord.utils.Invariants.illegalState;

public class MaelstromRequest extends Body implements Request
{
    final Txn txn;

    public MaelstromRequest(long msg_id, Txn txn)
    {
        super(Type.txn, msg_id, SENTINEL_MSG_ID);
        this.txn = txn;
    }

    @Override
    public void process(Node node, Id client, ReplyContext replyContext)
    {
        node.coordinate(txn).addCallback((success, fail) -> {
            Reply reply = success != null ? new MaelstromReply(MaelstromReplyContext.messageIdFor(replyContext), (MaelstromResult) success) : null;
            node.reply(client, replyContext, reply, fail);
        });
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
        writeTxnExternal(out, txn);
    }

    static void writeTxnExternal(JsonWriter out, Txn txn) throws IOException
    {
        if (txn == null)
        {
            out.nullValue();
            return;
        }

        out.beginArray();
        Keys keys = (Keys)txn.keys();
        MaelstromRead read = (MaelstromRead) txn.read();
        MaelstromUpdate update = (MaelstromUpdate) txn.update();
        for (int i = 0 ; i < keys.size() ; ++i)
        {
            MaelstromKey.Key key = (MaelstromKey.Key) keys.get(i);
            if (read.readKeys.indexOf(key) >= 0)
            {
                out.beginArray();
                out.value("r");
                key.datum.write(out);
                out.nullValue();
                out.endArray();
            }
            if (update.containsKey(key))
            {
                out.beginArray();
                out.value("append");
                key.datum.write(out);
                update.get(key).write(out);
                out.endArray();
            }
        }
        out.endArray();
    }

    public static Txn readTxnExternal(JsonReader in, Node.Id client, long requestId) throws IOException
    {
        if (in.peek() == JsonToken.NULL)
            return null;

        NavigableSet<Key> buildReadKeys = new TreeSet<>();
        NavigableSet<Key> buildKeys = new TreeSet<>();
        MaelstromUpdate update = new MaelstromUpdate();
        in.beginArray();
        while (in.hasNext())
        {
            in.beginArray();
            String op = in.nextString();
            Key key = MaelstromKey.readKey(in);
            switch (op)
            {
                default: throw illegalState("Invalid op: " + op);
                case "r":
                    in.nextNull();
                    buildReadKeys.add(key);
                    break;
                case "append":
                    Datum value = Datum.read(in);
                    buildKeys.add(key);
                    update.merge(key, new Value(value), Value::append);
            }
            in.endArray();
        }
        in.endArray();

        buildKeys.addAll(buildReadKeys);
        Keys readKeys = new Keys(buildReadKeys);
        Keys keys = new Keys(buildKeys);
        MaelstromRead read = new MaelstromRead(readKeys, keys);
        MaelstromQuery query = new MaelstromQuery(client, requestId);

        return new Txn.InMemory(keys, read, query, update);
    }

}
