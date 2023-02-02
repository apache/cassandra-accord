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
import accord.local.Status;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.RoutingKeys;
import accord.primitives.TxnId;
import accord.utils.Invariants;

import static accord.primitives.Route.castToFullRoute;
import static accord.primitives.Route.isFullRoute;

/**
 * A result of null indicates the transaction is globally persistent
 * A result of CheckStatusOk indicates the maximum status found for the transaction, which may be used to assess progress
 */
public class RecoverWithHomeKey extends CheckShards implements BiConsumer<Object, Throwable>
{
    final RoutingKey homeKey;
    final BiConsumer<Outcome, Throwable> callback;
    final Status witnessedByInvalidation;

    RecoverWithHomeKey(Node node, TxnId txnId, RoutingKey homeKey, Status witnessedByInvalidation, BiConsumer<Outcome, Throwable> callback)
    {
        super(node, txnId, RoutingKeys.of(homeKey), txnId.epoch(), IncludeInfo.Route);
        this.witnessedByInvalidation = witnessedByInvalidation;
        // if witnessedByInvalidation == AcceptedInvalidate then we cannot assume its definition was known, and our comparison with the status is invalid
        Invariants.checkState(witnessedByInvalidation != Status.AcceptedInvalidate);
        // if witnessedByInvalidation == Invalidated we should anyway not be recovering
        Invariants.checkState(witnessedByInvalidation != Status.Invalidated);
        this.homeKey = homeKey;
        this.callback = callback;
    }

    public static RecoverWithHomeKey recover(Node node, TxnId txnId, RoutingKey homeKey, Status witnessedByInvalidation, BiConsumer<Outcome, Throwable> callback)
    {
        RecoverWithHomeKey maybeRecover = new RecoverWithHomeKey(node, txnId, homeKey, witnessedByInvalidation, callback);
        maybeRecover.start();
        return maybeRecover;
    }

    @Override
    public void accept(Object unused, Throwable fail)
    {
        callback.accept(null, fail);
    }

    @Override
    protected boolean isSufficient(CheckStatusOk ok)
    {
        return ok.route != null;
    }

    @Override
    protected void onDone(Success success, Throwable fail)
    {
        if (fail != null)
        {
            callback.accept(null, fail);
        }
        else if (merged == null || !isFullRoute(merged.route))
        {
            switch (success)
            {
                default: throw new IllegalStateException();
                case Success:
                    // home shard must know full Route. Our success criteria is that the response contained a route,
                    // so reaching here without a response or one without a full Route is a bug.
                    callback.accept(null, new IllegalStateException());
                    return;
                case Quorum:
                    if (witnessedByInvalidation != null && witnessedByInvalidation.compareTo(Status.PreAccepted) > 0)
                        throw new IllegalStateException("We previously invalidated, finding a status that should be recoverable");
                    Invalidate.invalidate(node, txnId, contact.with(homeKey), true, callback);
            }
        }
        else
        {
            // start recovery
            RecoverWithRoute.recover(node, txnId, castToFullRoute(merged.route), witnessedByInvalidation, callback);
        }
    }
}
