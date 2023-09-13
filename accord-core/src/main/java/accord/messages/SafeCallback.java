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

import java.util.Objects;

import accord.coordinate.Timeout;
import accord.local.AgentExecutor;
import accord.local.Node;

public class SafeCallback<T extends Reply>
{
    private final AgentExecutor executor;
    private final Callback<T> callback;

    public SafeCallback(AgentExecutor executor, Callback<T> callback)
    {
        this.executor = Objects.requireNonNull(executor, "executor");
        this.callback = Objects.requireNonNull(callback, "callback");
    }

    public void success(Node.Id src, T reply)
    {
        safeCall(src, reply, Callback::onSuccess);
    }

    public void slowResponse(Node.Id src)
    {
        safeCall(src, null, (callback, id, ignore) -> callback.onSlowResponse(id));
    }

    public void failure(Node.Id to, Throwable t)
    {
        safeCall(to, t, Callback::onFailure);
    }

    public void timeout(Node.Id to)
    {
        failure(to, new Timeout(null, null));
    }

    public void onCallbackFailure(Node.Id to, Throwable t)
    {
        safeCall(to, t, Callback::onCallbackFailure);
    }

    private interface SafeCall<T, P>
    {
        void accept(Callback<T> callback, Node.Id id, P param) throws Throwable;
    }

    private <P> void safeCall(Node.Id src, P param, SafeCall<T, P> call)
    {
        // TODO (low priority, correctness): if the executor is shutdown this propgates the exception to the network stack
        executor.execute(() -> {
            try
            {
                call.accept(callback, src, param);
            }
            catch (Throwable t)
            {
                try
                {
                    callback.onCallbackFailure(src, t);
                }
                catch (Throwable t2)
                {
                    t.addSuppressed(t2);
                    executor.agent().onUncaughtException(t);
                }
            }
        });
    }

    @Override
    public String toString()
    {
        return callback.toString();
    }
}
