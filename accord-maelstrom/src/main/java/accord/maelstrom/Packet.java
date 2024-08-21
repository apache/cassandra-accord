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
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import accord.messages.*;
import com.google.gson.JsonObject;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import accord.local.Node.Id;

import static accord.utils.Invariants.illegalState;

public class Packet implements ReplyContext
{

    public enum Type
    {
        init(MaelstromInit.class, MaelstromInit.GSON_ADAPTER),
        init_ok(Body.class, Body.GSON_ADAPTER),
        txn(MaelstromRequest.class, MaelstromRequest.GSON_ADAPTER),
        txn_ok(MaelstromReply.class, MaelstromReply.GSON_ADAPTER),
        error(Error.class, Error.GSON_ADAPTER),
        PreAccept(accord.messages.PreAccept.class),
        PreAcceptOk(accord.messages.PreAccept.PreAcceptOk.class),
        PreAcceptNack(accord.messages.PreAccept.PreAcceptNack.class),
        Accept(accord.messages.Accept.class),
        AcceptReply(accord.messages.Accept.AcceptReply.class),
        Commit(accord.messages.Commit.class),
        Apply(Apply.class),
        ApplyReply(Apply.ApplyReply.class),
        Read(ReadData.class),
        ReadOk(ReadData.ReadOk.class),
        ReadNack(ReadData.CommitOrReadNack.class),
        WaitOnCommit(accord.messages.WaitOnCommit.class),
        WaitOnCommitOk(accord.messages.WaitOnCommit.WaitOnCommitOk.class),
        Recover(BeginRecovery.class),
        RecoverOk(BeginRecovery.RecoverOk.class),
        RecoverNack(BeginRecovery.RecoverNack.class),
        CheckStatus(CheckStatus.class),
        CheckStatusOk(CheckStatus.CheckStatusOk.class),
        CheckStatusOkFull(CheckStatus.CheckStatusOkFull.class),
        InformOfTxnId(InformOfTxnId.class, Json.DEFAULT_ADAPTER),
        SimpleReply(accord.messages.SimpleReply.class, Json.DEFAULT_ADAPTER),
        FailureReply(Reply.FailureReply.class)
        ;

        private static final Map<Class<?>, Type> LOOKUP_MAP = Arrays.stream(Type.values())
                .filter(t -> t.type != null)
                .<Map<Class<?>, Type>>collect(HashMap::new, (m, t) -> m.put(t.type, t), Map::putAll);

        public final Class<?> type;
        public final TypeAdapter<?> adapter;

        Type(Class<?> type, TypeAdapter<?> adapter)
        {
            this.type = type;
            this.adapter = adapter;
        }

        Type(Class<?> type)
        {
            this(type, Json.DEFAULT_ADAPTER);
        }

        public static Type lookup(Class<?> klass)
        {
            Type value = LOOKUP_MAP.get(klass);
            if (value == null)
                throw new NullPointerException("Unable to lookup for class " + klass);
            return value;
        }
    }

    final Id src;
    final Id dest;
    final Body body;

    public Packet(Id src, Id dest, Body body)
    {
        this.src = src;
        this.dest = dest;
        this.body = body;
    }

    public Packet(Id src, Id dest, long messageId, Request body)
    {
        this.src = src;
        this.dest = dest;
        this.body = new Wrapper(Type.lookup(body.getClass()), messageId, Body.SENTINEL_MSG_ID, body);
    }

    public Packet(Id src, Id dest, long replyId, Reply body)
    {
        this.src = src;
        this.dest = dest;
        this.body = body instanceof Body ? (Body) body : new Wrapper(Type.lookup(body.getClass()), Body.SENTINEL_MSG_ID, replyId, body);
    }

    public static Packet parse(String str)
    {
        return Json.GSON.fromJson(str, Packet.class);
    }

    public String toString()
    {
        return Json.GSON.toJson(this);
    }

    public static final TypeAdapter<Packet> GSON_ADAPTER = new TypeAdapter<Packet>()
    {
        @Override
        public void write(JsonWriter out, Packet value) throws IOException
        {
            out.beginObject();
            out.name("src");
            out.value(value.src.id);
            out.name("dest");
            out.value(value.dest.id);
            out.name("body");
            Json.GSON.toJson(value.body, Body.class, out);
            out.endObject();
        }

        @Override
        public Packet read(JsonReader in) throws IOException
        {
            in.beginObject();
            Id src = null, dest = null;
            Body body = null;
            String deferredBody = null;
            while (in.hasNext())
            {
                String field = in.nextName();
                switch (field)
                {
                    case "src": src = Json.ID_ADAPTER.read(in); break;
                    case "dest": dest = Json.ID_ADAPTER.read(in); break;
                    case "body":
                        if (src == null) deferredBody = Json.GSON.fromJson(in, JsonObject.class).toString();
                        else body = Body.read(in, src);
                        break;
                    case "id": in.nextLong(); break;
                    default:
                        throw illegalState("Unexpected field " + field);
                }
            }
            in.endObject();
            if (body == null && deferredBody != null)
                body = Body.read(new JsonReader(new StringReader(deferredBody)), src);
            return new Packet(src, dest, body);
        }
    };

}
