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
import com.google.gson.stream.JsonWriter;
import accord.local.Node.Id;

public class MaelstromInit extends Body
{
    final Id self;
    final Id[] cluster;

    public MaelstromInit(long msg_id, Id self, Id[] cluster)
    {
        super(Type.init, msg_id, SENTINEL_MSG_ID);
        this.self = self;
        this.cluster = cluster;
    }

    @Override
    void writeBody(JsonWriter out) throws IOException
    {
        super.writeBody(out);
        out.name("node_id");
        out.value(self.id);
        out.name("node_ids");
        out.beginArray();
        for (Id node : cluster)
            out.value(node.id);
        out.endArray();
    }
}
