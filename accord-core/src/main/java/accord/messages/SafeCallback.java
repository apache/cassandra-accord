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
import accord.local.CommandStore;
import accord.local.Node;

public class SafeCallback<T extends Reply>
{
    private final CommandStore commandStore;
    private final Callback<T> callback;

    public SafeCallback(CommandStore commandStore, Callback<T> callback)
    {
        this.commandStore = Objects.requireNonNull(commandStore, "commandStore");
        this.callback = Objects.requireNonNull(callback, "callback");
    }

    public void success(Node.Id src, T reply)
    {
        safeCall(src, () -> callback.onSuccess(src, reply));
    }

    public void slowResponse(Node.Id src)
    {
        safeCall(src, () -> callback.onSlowResponse(src));
    }

    public void failure(Node.Id to, Throwable t)
    {
        safeCall(to, () -> callback.onFailure(to, t));
    }

    public void timeout(Node.Id to)
    {
        failure(to, new Timeout(null, null));
    }

    private void safeCall(Node.Id src, Runnable fn)
    {
        commandStore.execute(() -> {
            try
            {
                fn.run();
            }
            catch (Throwable t)
            {
                try
                {
                    callback.onCallbackFailure(src, t);
                }
                finally
                {
                    commandStore.agent().onUncaughtException(t);
                }
            }
        });
    }
}
