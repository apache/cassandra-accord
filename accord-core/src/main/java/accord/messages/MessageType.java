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
 */
public class MessageType
{
    public static final MessageType SIMPLE_RSP                       = mt(REMOTE, false);
    public static final MessageType FAILURE_RSP                      = mt(REMOTE, false);
    public static final MessageType PRE_ACCEPT_REQ                   = mt(REMOTE, true );
    public static final MessageType PRE_ACCEPT_RSP                   = mt(REMOTE, false);
    public static final MessageType ACCEPT_REQ                       = mt(REMOTE, true );
    public static final MessageType ACCEPT_RSP                       = mt(REMOTE, false);
    public static final MessageType ACCEPT_INVALIDATE_REQ            = mt(REMOTE, true );
    public static final MessageType GET_DEPS_REQ                     = mt(REMOTE, false);
    public static final MessageType GET_DEPS_RSP                     = mt(REMOTE, false);
    public static final MessageType COMMIT_MINIMAL_REQ               = mt(REMOTE, true );
    public static final MessageType COMMIT_MAXIMAL_REQ               = mt(REMOTE, true );
    public static final MessageType COMMIT_INVALIDATE_REQ            = mt(REMOTE, true );
    public static final MessageType APPLY_MINIMAL_REQ                = mt(REMOTE, true );
    public static final MessageType APPLY_MAXIMAL_REQ                = mt(REMOTE, true );
    public static final MessageType APPLY_RSP                        = mt(REMOTE, false);
    public static final MessageType READ_REQ                         = mt(REMOTE, false);
    public static final MessageType READ_RSP                         = mt(REMOTE, false);
    public static final MessageType BEGIN_RECOVER_REQ                = mt(REMOTE, true );
    public static final MessageType BEGIN_RECOVER_RSP                = mt(REMOTE, false);
    public static final MessageType BEGIN_INVALIDATE_REQ             = mt(REMOTE, true );
    public static final MessageType BEGIN_INVALIDATE_RSP             = mt(REMOTE, false);
    public static final MessageType WAIT_ON_COMMIT_REQ               = mt(REMOTE, false);
    public static final MessageType WAIT_ON_COMMIT_RSP               = mt(REMOTE, false);
    public static final MessageType WAIT_UNTIL_APPLIED_REQ           = mt(REMOTE, false);
    public static final MessageType INFORM_OF_TXN_REQ                = mt(REMOTE, true );
    public static final MessageType INFORM_DURABLE_REQ               = mt(REMOTE, true );
    public static final MessageType INFORM_HOME_DURABLE_REQ          = mt(REMOTE, true );
    public static final MessageType CHECK_STATUS_REQ                 = mt(REMOTE, false);
    public static final MessageType CHECK_STATUS_RSP                 = mt(REMOTE, false);
    public static final MessageType FETCH_DATA_REQ                   = mt(REMOTE, false);
    public static final MessageType FETCH_DATA_RSP                   = mt(REMOTE, false);
    public static final MessageType SET_SHARD_DURABLE_REQ            = mt(REMOTE, true );
    public static final MessageType SET_GLOBALLY_DURABLE_REQ         = mt(REMOTE, true );
    public static final MessageType QUERY_DURABLE_BEFORE_REQ         = mt(REMOTE, false);
    public static final MessageType QUERY_DURABLE_BEFORE_RSP         = mt(REMOTE, false);
    public static final MessageType APPLY_AND_WAIT_UNTIL_APPLIED_REQ = mt(REMOTE, true );

    public static final MessageType PROPAGATE_PRE_ACCEPT_MSG         = mt(LOCAL,  true );
    public static final MessageType PROPAGATE_COMMIT_MSG             = mt(LOCAL,  true );
    public static final MessageType PROPAGATE_APPLY_MSG              = mt(LOCAL,  true );
    public static final MessageType PROPAGATE_OTHER_MSG              = mt(LOCAL,  true );


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

    protected static MessageType mt(Kind kind, boolean hasSideEffects)
    {
        return new MessageType(kind, hasSideEffects);
    }

    /**
     * LOCAL messages are not sent to remote nodes.
     */
    private final Kind kind;

    /**
     * If true, indicates that processing of the message has important side effects.
     */
    private final boolean hasSideEffects;

    protected MessageType(Kind kind, boolean hasSideEffects)
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
