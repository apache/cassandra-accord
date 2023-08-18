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

/**
 * Meant to assist implementations map accord messages to their own messaging systems.
 */
public enum MessageType
{
    SIMPLE_RSP               (false),
    PRE_ACCEPT_REQ           (true ),
    PRE_ACCEPT_RSP           (false),
    ACCEPT_REQ               (true ),
    ACCEPT_RSP               (false),
    ACCEPT_INVALIDATE_REQ    (true ),
    GET_DEPS_REQ             (false),
    GET_DEPS_RSP             (false),
    COMMIT_REQ               (true ),
    COMMIT_INVALIDATE_REQ    (true ),
    APPLY_REQ                (true ),
    APPLY_RSP                (false),
    READ_REQ                 (false),
    READ_RSP                 (false),
    BEGIN_RECOVER_REQ        (true ),
    BEGIN_RECOVER_RSP        (false),
    BEGIN_INVALIDATE_REQ     (true ),
    BEGIN_INVALIDATE_RSP     (false),
    WAIT_ON_COMMIT_REQ       (false),
    WAIT_ON_COMMIT_RSP       (false),
    WAIT_ON_APPLY_REQ        (false),
    INFORM_OF_TXN_REQ        (true ),
    INFORM_DURABLE_REQ       (true ),
    INFORM_HOME_DURABLE_REQ  (true ),
    CHECK_STATUS_REQ         (false),
    CHECK_STATUS_RSP         (false),
    FETCH_DATA_REQ           (false),
    FETCH_DATA_RSP           (false),
    SET_SHARD_DURABLE_REQ    (true ),
    SET_GLOBALLY_DURABLE_REQ (true ),
    QUERY_DURABLE_BEFORE_REQ (false),
    QUERY_DURABLE_BEFORE_RSP (false),
    FAILURE_RSP              (false),
    ;

    /**
     * If true, indicates that processing of the message has important side effects.
     */
    public final boolean hasSideEffects;

    MessageType(boolean hasSideEffects)
    {
        this.hasSideEffects = hasSideEffects;
    }
}
