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

package accord.messages;

import accord.local.Node;
import accord.local.Node.Id;

public interface Request extends Message
{
    default long waitForEpoch() { return 0; }
    default long knownEpoch() { return waitForEpoch(); }
    void preProcess(Node on, Id from, ReplyContext replyContext);
    void process(Node on, Id from, ReplyContext replyContext);

    /**
     * Process the request without replying back
     */
    default void process(Node on)
    {
        throw new UnsupportedOperationException();
    }
}
