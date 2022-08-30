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
 * meant to assist implementations map accord messages to their own messaging systems
 */
public enum MessageType
{
    PREACCEPT_REQ,
    PREACCEPT_RSP,
    ACCEPT_REQ,
    ACCEPT_INVALIDATE_REQ,
    ACCEPT_RSP,
    COMMIT_REQ,
    COMMIT_INVALIDATE,
    APPLY_REQ,
    APPLY_RSP,
    APPLY_AND_CHECK_REQ,
    APPLY_AND_CHECK_RSP,
    READ_REQ,
    READ_RSP,
    BEGIN_RECOVER_REQ,
    BEGIN_RECOVER_RSP,
    BEGIN_INVALIDATE_REQ,
    BEGIN_INVALIDATE_RSP,
    WAIT_ON_COMMIT_REQ,
    WAIT_ON_COMMIT_RSP,
    INFORM_REQ,
    INFORM_RSP,
    INFORM_PERSISTED_REQ,
    INFORM_PERSISTED_RSP,
    CHECK_STATUS_REQ,
    CHECK_STATUS_RSP
}
