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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;

import com.google.common.collect.ImmutableList;

import static accord.messages.MessageType.Kind.REMOTE;
import static accord.messages.MessageType.Kind.LOCAL;

/**
 * Meant to assist implementations with mapping accord messages to their own messaging systems.
 *
 * TODO (expected): no longer need hasSideEffects parameter. Also, why are we not using enums?
 */
public class MessageType
{
    public static final MessageType SIMPLE_RSP                        = remote("SIMPLE_RSP",                        false);
    public static final MessageType FAILURE_RSP                       = remote("FAILURE_RSP",                       false);
    public static final MessageType PRE_ACCEPT_REQ                    = remote("PRE_ACCEPT_REQ",                    true );
    public static final MessageType PRE_ACCEPT_RSP                    = remote("PRE_ACCEPT_RSP",                    false);
    public static final MessageType ACCEPT_REQ                        = remote("ACCEPT_REQ",                        true );
    public static final MessageType ACCEPT_RSP                        = remote("ACCEPT_RSP",                        false);
    public static final MessageType ACCEPT_INVALIDATE_REQ             = remote("ACCEPT_INVALIDATE_REQ",             true );
    public static final MessageType CALCULATE_DEPS_REQ                = remote("CALCULATE_DEPS_REQ",                false);
    public static final MessageType CALCULATE_DEPS_RSP                = remote("CALCULATE_DEPS_RSP",                false);
    public static final MessageType GET_EPHEMERAL_READ_DEPS_REQ       = remote("GET_EPHEMERAL_READ_DEPS_REQ",       false);
    public static final MessageType GET_EPHEMERAL_READ_DEPS_RSP       = remote("GET_EPHEMERAL_READ_DEPS_RSP",       false);
    public static final MessageType GET_MAX_CONFLICT_REQ              = remote("GET_MAX_CONFLICT_REQ",              false);
    public static final MessageType GET_MAX_CONFLICT_RSP              = remote("GET_MAX_CONFLICT_RSP",              false);
    public static final MessageType COMMIT_SLOW_PATH_REQ              = remote("COMMIT_SLOW_PATH_REQ",              true);
    public static final MessageType COMMIT_MAXIMAL_REQ                = remote("COMMIT_MAXIMAL_REQ",                true );
    public static final MessageType STABLE_FAST_PATH_REQ              = remote("STABLE_FAST_PATH_REQ",              true);
    public static final MessageType STABLE_SLOW_PATH_REQ              = remote("STABLE_SLOW_PATH_REQ",              true);
    public static final MessageType STABLE_MAXIMAL_REQ                = remote("STABLE_MAXIMAL_REQ",                true );
    public static final MessageType COMMIT_INVALIDATE_REQ             = remote("COMMIT_INVALIDATE_REQ",             true );
    public static final MessageType APPLY_MINIMAL_REQ                 = remote("APPLY_MINIMAL_REQ",                 true );
    public static final MessageType APPLY_MAXIMAL_REQ                 = remote("APPLY_MAXIMAL_REQ",                 true );
    public static final MessageType APPLY_RSP                         = remote("APPLY_RSP",                         false);
    public static final MessageType READ_REQ                          = remote("READ_REQ",                          false);
    public static final MessageType READ_EPHEMERAL_REQ                = remote("READ_EPHEMERAL_REQ",                false);
    public static final MessageType READ_RSP                          = remote("READ_RSP",                          false);
    public static final MessageType BEGIN_RECOVER_REQ                 = remote("BEGIN_RECOVER_REQ",                 true );
    public static final MessageType BEGIN_RECOVER_RSP                 = remote("BEGIN_RECOVER_RSP",                 false);
    public static final MessageType BEGIN_INVALIDATE_REQ              = remote("BEGIN_INVALIDATE_REQ",              true );
    public static final MessageType BEGIN_INVALIDATE_RSP              = remote("BEGIN_INVALIDATE_RSP",              false);
    public static final MessageType AWAIT_REQ                         = remote("AWAIT_REQ",                         false);
    public static final MessageType AWAIT_RSP                         = remote("AWAIT_RSP",                         false);
    public static final MessageType ASYNC_AWAIT_COMPLETE_REQ          = remote("ASYNC_AWAIT_COMPLETE_RSP",          false);
    public static final MessageType WAIT_UNTIL_APPLIED_REQ            = remote("WAIT_UNTIL_APPLIED_REQ",            false);
    public static final MessageType INFORM_DURABLE_REQ                = remote("INFORM_DURABLE_REQ",                true );
    public static final MessageType CHECK_STATUS_REQ                  = remote("CHECK_STATUS_REQ",                  false);
    public static final MessageType CHECK_STATUS_RSP                  = remote("CHECK_STATUS_RSP",                  false);
    public static final MessageType FETCH_DATA_REQ                    = remote("FETCH_DATA_REQ",                    false);
    public static final MessageType FETCH_DATA_RSP                    = remote("FETCH_DATA_RSP",                    false);
    public static final MessageType SET_SHARD_DURABLE_REQ             = remote("SET_SHARD_DURABLE_REQ",             true );
    public static final MessageType SET_GLOBALLY_DURABLE_REQ          = remote("SET_GLOBALLY_DURABLE_REQ",          true );
    public static final MessageType QUERY_DURABLE_BEFORE_REQ          = remote("QUERY_DURABLE_BEFORE_REQ",          false);
    public static final MessageType QUERY_DURABLE_BEFORE_RSP          = remote("QUERY_DURABLE_BEFORE_RSP",          false);
    public static final MessageType APPLY_THEN_WAIT_UNTIL_APPLIED_REQ = remote("APPLY_THEN_WAIT_UNTIL_APPLIED_REQ", true );

    /**
     * LOCAL messages are not sent to remote nodes.
     */
    public enum Kind { LOCAL, REMOTE }

    public static final List<MessageType> values;

    static
    {
        ImmutableList.Builder<MessageType> builder = ImmutableList.builder();
        for (Field f : MessageType.class.getDeclaredFields())
        {
            if (f.getType().equals(MessageType.class) && Modifier.isStatic(f.getModifiers()))
            {
                try
                {
                    builder.add((MessageType) f.get(null));
                }
                catch (IllegalAccessException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }
        values = builder.build();
    }

    protected static MessageType local(String name, boolean hasSideEffects)
    {
        return new MessageType(name, LOCAL, hasSideEffects);
    }

    protected static MessageType remote(String name, boolean hasSideEffects)
    {
        return new MessageType(name, REMOTE, hasSideEffects);
    }

    private final String name;
    private final Kind kind;

    /**
     * If true, indicates that processing of the message has important side effects.
     */
    private final boolean hasSideEffects;

    protected MessageType(String name, Kind kind, boolean hasSideEffects)
    {
        this.name = name;
        this.kind = kind;
        this.hasSideEffects = hasSideEffects;
    }

    public String name()
    {
        return name;
    }

    @Override
    public String toString()
    {
        return name();
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
