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

import static accord.messages.MessageType.Kind.LOCAL;
import static accord.messages.MessageType.Kind.REMOTE;

/**
 * Meant to assist implementations with mapping accord messages to their own messaging systems.
 */
public enum MessageType
{
    SIMPLE_RSP                         (REMOTE, false),
    FAILURE_RSP                        (REMOTE, false),
    PRE_ACCEPT_REQ                     (REMOTE, true ),
    PRE_ACCEPT_RSP                     (REMOTE, false),
    ACCEPT_REQ                         (REMOTE, true ),
    ACCEPT_RSP                         (REMOTE, false),
    ACCEPT_INVALIDATE_REQ              (REMOTE, true ),
    GET_DEPS_REQ                       (REMOTE, false),
    GET_DEPS_RSP                       (REMOTE, false),
    COMMIT_MINIMAL_REQ                 (REMOTE, true ),
    COMMIT_MAXIMAL_REQ                 (REMOTE, true ),
    COMMIT_INVALIDATE_REQ              (REMOTE, true ),
    APPLY_MINIMAL_REQ                  (REMOTE, true ),
    APPLY_MAXIMAL_REQ                  (REMOTE, true ),
    APPLY_RSP                          (REMOTE, false),
    READ_REQ                           (REMOTE, false),
    READ_RSP                           (REMOTE, false),
    BEGIN_RECOVER_REQ                  (REMOTE, true ),
    BEGIN_RECOVER_RSP                  (REMOTE, false),
    BEGIN_INVALIDATE_REQ               (REMOTE, true ),
    BEGIN_INVALIDATE_RSP               (REMOTE, false),
    WAIT_ON_COMMIT_REQ                 (REMOTE, false),
    WAIT_ON_COMMIT_RSP                 (REMOTE, false),
    WAIT_UNTIL_APPLIED_REQ             (REMOTE, false),
    INFORM_OF_TXN_REQ                  (REMOTE, true ),
    INFORM_DURABLE_REQ                 (REMOTE, true ),
    INFORM_HOME_DURABLE_REQ            (REMOTE, true ),
    CHECK_STATUS_REQ                   (REMOTE, false),
    CHECK_STATUS_RSP                   (REMOTE, false),
    FETCH_DATA_REQ                     (REMOTE, false),
    FETCH_DATA_RSP                     (REMOTE, false),
    SET_SHARD_DURABLE_REQ              (REMOTE, true ),
    SET_GLOBALLY_DURABLE_REQ           (REMOTE, true ),
    QUERY_DURABLE_BEFORE_REQ           (REMOTE, false),
    QUERY_DURABLE_BEFORE_RSP           (REMOTE, false),
    APPLY_AND_WAIT_UNTIL_APPLIED_REQ   (REMOTE, true ),

    PROPAGATE_PRE_ACCEPT_MSG           (LOCAL,  true ),
    PROPAGATE_COMMIT_MSG               (LOCAL,  true ),
    PROPAGATE_APPLY_MSG                (LOCAL,  true ),
    PROPAGATE_OTHER_MSG                (LOCAL,  true ),
    ;

    public enum Kind { LOCAL, REMOTE }

    /**
     * LOCAL messages are not sent to remote nodes.
     */
    private final Kind kind;

    /**
     * If true, indicates that processing of the message has important side effects.
     */
    private final boolean hasSideEffects;

    MessageType(Kind kind, boolean hasSideEffects)
    {
        this.hasSideEffects = hasSideEffects;
        this.kind = kind;
    }

    public boolean isLocal()
    {
        return kind == LOCAL;
    }

    public boolean isRemote()
    {
        return kind == REMOTE;
    }

    public boolean hasSideEffects()
    {
        return hasSideEffects;
    }
}
