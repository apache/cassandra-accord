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

import accord.local.Node;
import accord.local.Status.Known;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.IncludeInfo;
import accord.messages.CheckStatus.WithQuorum;
import accord.primitives.Route;
import accord.primitives.Unseekables;
import accord.primitives.TxnId;

import static accord.local.Status.Known.Nothing;

/**
 * Find the homeKey of a txnId with some known keys
 */
public class FindSomeRoute extends CheckShards<Unseekables<?>>
{
    static class Result
    {
        public final Route<?> route;
        public final Known known;
        public final WithQuorum withQuorum;

        Result(Route<?> route, Known known, WithQuorum withQuorum)
        {
            this.route = route;
            this.known = known;
            this.withQuorum = withQuorum;
        }
    }

    final BiConsumer<Result, Throwable> callback;
    FindSomeRoute(Node node, TxnId txnId, Unseekables<?> unseekables, BiConsumer<Result, Throwable> callback)
    {
        super(node, txnId, unseekables, IncludeInfo.Route);
        this.callback = callback;
    }

    public static FindSomeRoute findSomeRoute(Node node, TxnId txnId, Unseekables<?> unseekables, BiConsumer<Result, Throwable> callback)
    {
        FindSomeRoute findSomeRoute = new FindSomeRoute(node, txnId, unseekables, callback);
        findSomeRoute.start();
        return findSomeRoute;
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
        else if (merged == null) callback.accept(new Result(null, Nothing, success.withQuorum), null);
        else callback.accept(new Result(merged.route, merged.finish(this.route, success.withQuorum).knownFor(this.route), success.withQuorum), null);
    }
}
