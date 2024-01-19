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

package accord.impl.list;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import accord.local.Node.Id;
import accord.api.Result;
import accord.messages.MessageType;
import accord.primitives.Keys;
import accord.messages.Reply;
import accord.primitives.Seekables;
import accord.primitives.TxnId;

import static accord.utils.Invariants.illegalState;

public class ListResult implements Result, Reply
{
    public enum Status
    { Applied, RecoveryApplied, HeartBeat, Invalidated, Lost, Other, Truncated, Failure }
    public final Id client;
    public final long requestId;
    public final TxnId txnId;
    public final Seekables<?, ?> readKeys;
    public final Keys responseKeys;
    public final int[][] read; // equal in size to keys.size()
    public final ListUpdate update;
    private final Status status;

    public ListResult(Status status, Id client, long requestId, TxnId txnId, Seekables<?, ?> readKeys, Keys responseKeys, int[][] read, ListUpdate update)
    {
        this.client = client;
        this.requestId = requestId;
        this.txnId = txnId;
        this.readKeys = readKeys;
        this.responseKeys = responseKeys;
        this.read = read;
        this.update = update;
        this.status = status;
    }

    private ListResult(Id client, long requestId, TxnId txnId, Status status)
    {
        this.client = client;
        this.requestId = requestId;
        this.txnId = txnId;
        this.readKeys = null;
        this.responseKeys = null;
        this.read = null;
        this.update = null;
        this.status = status;
    }

    public static ListResult heartBeat(Id client, long requestId, TxnId txnId)
    {
        return new ListResult(client, requestId, txnId, Status.HeartBeat);
    }

    public static ListResult invalidated(Id client, long requestId, TxnId txnId)
    {
        return new ListResult(client, requestId, txnId, Status.Invalidated);
    }

    public static ListResult lost(Id client, long requestId, TxnId txnId)
    {
        return new ListResult(client, requestId, txnId, Status.Lost);
    }

    public static ListResult other(Id client, long requestId, TxnId txnId)
    {
        return new ListResult(client, requestId, txnId, Status.Other);
    }

    public static ListResult truncated(Id client, long requestId, TxnId txnId)
    {
        return new ListResult(client, requestId, txnId, Status.Truncated);
    }

    public static ListResult failure(Id client, long requestId, TxnId txnId)
    {
        return new ListResult(client, requestId, txnId, Status.Failure);
    }

    @Override
    public MessageType type()
    {
        return null;
    }

    public boolean isSuccess()
    {
        return status.compareTo(Status.RecoveryApplied) <= 0;
    }

    public Status status()
    {
        return status;
    }

    @Override
    public String toString()
    {
        return "{client:" + client + ", "
               + "requestId:" + requestId + ", "
               + "txnId:" + txnId + ", "
               + (responseKeys == null
                  ? (read == null ? "invalidated!}" : read.length == 0 ? "unknown}" : "lost}")
                  : "reads:" + IntStream.range(0, responseKeys.size())
                                      .mapToObj(i -> responseKeys.get(i) + ":" + Arrays.toString(read[i]))
                                      .collect(Collectors.joining(", ", "{", "}")) + ", "
                    + "writes:" + update + "}");
    }
}
