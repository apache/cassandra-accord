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

import accord.maelstrom.Packet.Type;
import accord.messages.MessageType;
import accord.messages.Reply;
import com.google.gson.stream.JsonWriter;

public class Error extends Body implements Reply
{
    final int code;
    final String text;

    public Error(long in_reply_to, int code, String text)
    {
        super(Type.error, SENTINEL_MSG_ID, in_reply_to);
        this.code = code;
        this.text = text;
    }

    @Override
    void writeBody(JsonWriter out) throws IOException
    {
        super.writeBody(out);
        out.name("code");
        out.value(code);
        out.name("text");
        out.value(text);
    }

    @Override
    public MessageType type()
    {
        throw new UnsupportedOperationException();
    }
}
