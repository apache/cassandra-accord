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
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import org.junit.jupiter.api.Test;

import accord.local.CommandStores;
import accord.local.MapReduceConsumeCommandStores;
import accord.local.Node;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.async.Cancellable;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// this file was authored by LLM
class AwaitCancellationTest
{
    private static class ObservableAwait extends Await
    {
        final AtomicBoolean cleanupCalled = new AtomicBoolean();

        ObservableAwait()
        {
            super(new TxnId(1, 1, Txn.Kind.Write, Routable.Domain.Range, new Node.Id(1)), Ranges.EMPTY, Until.IsApplied, 1, 1, -1, false);
        }

        @Override
        protected void cleanupOnFailure(@Nullable Cancellable cancel)
        {
            super.cleanupOnFailure(cancel);
            cleanupCalled.set(true);
        }
    }

    private static Cancellable process(Await await, Cancellable work, boolean cancelDuringSubmit)
    {
        Node node = mock(Node.class);
        CommandStores stores = mock(CommandStores.class);
        when(node.commandStores()).thenReturn(stores);
        doAnswer(invocation -> {
            if (cancelDuringSubmit)
                await.cancel();
            return work;
        }).when(stores).mapReduceConsume(anyLong(), anyLong(), any(MapReduceConsumeCommandStores.class));
        return await.process(node, new Node.Id(2), mock(ReplyContext.class));
    }

    @Test
    void cancellingRequestCleansUpState()
    {
        ObservableAwait await = new ObservableAwait();
        AtomicBoolean workCancelled = new AtomicBoolean();
        Cancellable exposed = process(await, () -> workCancelled.set(true), false);
        assertSame(await, exposed);
        assertFalse(workCancelled.get());

        exposed.cancel();
        assertTrue(workCancelled.get());
        assertTrue(await.cleanupCalled.get());
        assertTrue(await.isCancelled());
    }

    @Test
    void cancellingDuringSubmitCancelsWork()
    {
        ObservableAwait await = new ObservableAwait();
        AtomicBoolean workCancelled = new AtomicBoolean();
        process(await, () -> workCancelled.set(true), true);
        assertTrue(await.cleanupCalled.get());
        assertTrue(workCancelled.get());
    }

    /**
     * KeyBarriers.awaitLocal processes a synchronous Await with a null ReplyContext; if any store must wait,
     * accept() must not dereference the (absent) reply context to register an expiry
     */
    @Test
    void localAwaitWithoutReplyContextMayWait() throws Exception
    {
        AtomicBoolean replied = new AtomicBoolean();
        Await await = new Await(new TxnId(1, 1, Txn.Kind.Write, Routable.Domain.Range, new Node.Id(1)), Ranges.EMPTY, Await.Until.IsApplied, 1, 1, -1, false)
        {
            @Override
            protected void reply(AwaitOk reply, Throwable failure)
            {
                replied.set(true);
            }
        };
        Field waitingOn = Await.class.getDeclaredField("synchronouslyWaitingOn");
        waitingOn.setAccessible(true);

        Node node = mock(Node.class);
        CommandStores stores = mock(CommandStores.class);
        when(node.commandStores()).thenReturn(stores);
        doAnswer(invocation -> {
            waitingOn.setInt(await, 1); // simulate one store registering a listener
            assertDoesNotThrow(() -> await.accept(null, null));
            return null;
        }).when(stores).mapReduceConsume(anyLong(), anyLong(), any(MapReduceConsumeCommandStores.class));

        await.process(node, null, null);
        assertFalse(replied.get());
        assertFalse(await.isDone());
    }
}
