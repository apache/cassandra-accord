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
import javax.annotation.Nullable;

import accord.api.AsyncExecutor;
import accord.local.Node;
import accord.local.Node.Id;

/**
 * Represents some execution for handling responses from messages a node has sent.
 */
public interface Callback<R>
{
    void onSuccess(Id from, R reply);
    default void onSlow(Id from) {}
    // null to be interpreted as Timeout
    void onFailure(Id from, @Nullable Throwable failure);
    // return true if the failure was handled/propagated
    default boolean onCallbackFailure(Id from, Throwable failure) { return false; }

    interface CallbackExclusive<R>
    {
        private static void replyMaybeImmediately(AsyncExecutor executor, boolean doNotReplyImmediately, Runnable run)
        {
            if (doNotReplyImmediately || !executor.tryExecuteImmediately(run))
                executor.execute(run);
        }

        static <R> Runnable runOnSuccess(CallbackExclusive<R> callback, Node.Id from, R success)
        {
            return () -> {
                try { callback.onSuccessExclusive(from, success); }
                catch (Throwable t) { callback.onCallbackFailureExclusive(from, t); }
            };
        }

        static <R> void onSuccess(AsyncExecutor executor, boolean doNotReplyImmediately, CallbackExclusive<R> callback, Node.Id from, R success)
        {
            replyMaybeImmediately(executor, doNotReplyImmediately, runOnSuccess(callback, from, success));
        }


        static <R> Runnable runOnFailure(CallbackExclusive<R> callback, Node.Id from, Throwable failure)
        {
            return () -> {
                try { callback.onFailureExclusive(from, failure); }
                catch (Throwable t)
                {
                    if (failure != null)
                    {
                        try { t.addSuppressed(failure);}
                        catch (Throwable ignore) { /* best effort */ }
                    }
                    callback.onCallbackFailureExclusive(from, t);
                }
            };
        }

        static <R> void onFailure(AsyncExecutor executor, boolean doNotReplyImmediately, CallbackExclusive<R> callback, Node.Id from, Throwable failure)
        {
            replyMaybeImmediately(executor, doNotReplyImmediately, runOnFailure(callback, from, failure));
        }

        static <R> Runnable runOnSlow(CallbackExclusive<R> callback, Node.Id from)
        {
            return () -> {
                try { callback.onSlowExclusive(from); }
                catch (Throwable t) { callback.onCallbackFailureExclusive(from, t); }
            };
        }

        static <R> void onSlow(AsyncExecutor executor, boolean unsafeToReplyImmediately, CallbackExclusive<R> callback, Node.Id from)
        {
            replyMaybeImmediately(executor, unsafeToReplyImmediately, runOnSlow(callback, from));
        }

        void onSuccessExclusive(Node.Id from, R reply);
        void onFailureExclusive(Node.Id from, @Nullable Throwable failure);
        void onCallbackFailureExclusive(Node.Id from, Throwable failure);
        default void onSlowExclusive(Node.Id from) {}
    }

    abstract class ConcreteCallbackExclusive<R extends Reply> implements Callback<R>, CallbackExclusive<R>
    {
        protected final AsyncExecutor executor;

        public ConcreteCallbackExclusive(AsyncExecutor executor)
        {
            this.executor = Objects.requireNonNull(executor, "executor");
        }

        @Override
        public final void onSuccess(Node.Id from, R reply)
        {
            CallbackExclusive.onSuccess(executor, true, this, from, reply);
        }

        @Override
        public final void onSlow(Node.Id from)
        {
            CallbackExclusive.onSlow(executor, true, this, from);
        }

        @Override
        public final void onFailure(Node.Id from, Throwable failure)
        {
            CallbackExclusive.onFailure(executor, true, this, from, failure);
        }
    }
}
