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

package accord.coordinate;

import java.util.function.BiConsumer;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.Unseekables;
import accord.primitives.TxnId;

/**
 * Find the homeKey of a txnId with some known keys
 */
public class FindHomeKey extends CheckShards
{
    final BiConsumer<RoutingKey, Throwable> callback;
    FindHomeKey(Node node, TxnId txnId, Unseekables<?, ?> unseekables, BiConsumer<RoutingKey, Throwable> callback)
    {
        super(node, txnId, unseekables, txnId.epoch(), IncludeInfo.No);
        this.callback = callback;
    }

    public static FindHomeKey findHomeKey(Node node, TxnId txnId, Unseekables<?, ?> unseekables, BiConsumer<RoutingKey, Throwable> callback)
    {
        FindHomeKey findHomeKey = new FindHomeKey(node, txnId, unseekables, callback);
        findHomeKey.start();
        return findHomeKey;
    }

    @Override
    protected boolean isSufficient(CheckStatusOk ok)
    {
        return ok.homeKey != null;
    }

    @Override
    protected void onDone(Success success, Throwable failure)
    {
        if (failure != null) callback.accept(null, failure);
        else callback.accept(merged == null ? null : merged.homeKey, null);
    }
}
