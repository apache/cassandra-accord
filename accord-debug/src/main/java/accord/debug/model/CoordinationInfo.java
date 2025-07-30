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

package accord.debug.model;

import java.util.ArrayList;
import java.util.List;

import accord.local.Node;

public class CoordinationInfo
{
    public final String txnId;
    public final String kind;
    public final long coordinationId;
    public final String description;
    public final String nodes;
    public final String nodesInflight;
    public final String nodesContacted;
    public final String participants;
    public final String replies;
    public final String tracker;
    
    public CoordinationInfo(String txnId, String kind, long coordinationId, String description, String nodes,
                            String nodesInflight, String nodesContacted, String participants, String replies, String tracker)
    {
        this.txnId = txnId;
        this.kind = kind;
        this.coordinationId = coordinationId;
        this.description = description;
        this.nodes = nodes;
        this.nodesInflight = nodesInflight;
        this.nodesContacted = nodesContacted;
        this.participants = participants;
        this.replies = replies;
        this.tracker = tracker;
    }

    public static List<CoordinationInfo> getCoordinations(Node node)
    {
        List<CoordinationInfo> coordinations = new ArrayList<>();
        
        // Note: The Coordination interface/class referenced in the Cassandra implementation
        // may not be available in the current Accord codebase. This method provides a
        // framework for when the Coordination API becomes available.
        
        // TODO: Once the Coordination API is available, implement the following pattern:
        // 
        // Coordinations nodeCoordinations = node.coordinations();
        // for (Coordination c : nodeCoordinations)
        // {
        //     coordinations.add(new CoordinationInfo(
        //         toStringOrNull(c.txnId()),
        //         c.kind().toString(),
        //         c.coordinationId(),
        //         c.describe(),
        //         toStringOrNull(c.nodes()),
        //         toStringOrNull(c.inflight()),
        //         toStringOrNull(c.contacted()),
        //         toStringOrNull(c.scope()),
        //         summarise(c.replies()),
        //         summarise(c.tracker())
        //     ));
        // }
        
        return coordinations;
    }
    
    private static String toStringOrNull(Object obj)
    {
        return obj == null ? null : obj.toString();
    }
    
    private static String summarise(Object obj)
    {
        // TODO: Implement proper summarization logic based on the object type
        return obj == null ? null : obj.toString();
    }
}