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

package accord.impl;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.impl.InMemoryCommandStore.GlobalCommandsForKey;
import accord.impl.InMemoryCommandStore.InMemoryCommandStoreCaches;
import accord.impl.InMemoryCommandStore.InMemorySafeStore;
import accord.local.ExecutionContext;
import accord.local.cfk.SafeCommandsForKey;
import accord.primitives.RoutableKey;
import accord.primitives.RoutingKeys;

import static accord.local.ExecutionContext.unsequencedIdempotentIncrementalWrite;

public class AbstractSafeCommandStoreTest
{
    @Test
    public void testCanExecuteWithUninitialisedKeys()
    {
        InMemorySafeCommandsForKey first = new GlobalCommandsForKey(IntKey.routing(1)).createSafeReference();
        InMemorySafeCommandsForKey second = new GlobalCommandsForKey(IntKey.routing(2)).createSafeReference();
        ExecutionContext context = unsequencedIdempotentIncrementalWrite(RoutingKeys.of(first.key(), second.key()), "test");
        Map<RoutableKey, InMemorySafeCommandsForKey> references = new HashMap<>();
        InMemorySafeStore store = new InMemorySafeStore(null, context, null, new HashMap<>(), references)
        {
            @Override
            protected InMemoryCommandStoreCaches tryGetCaches()
            {
                throw new AssertionError("Retained references must not be acquired again");
            }

            @Override
            protected SafeCommandsForKey maybeCleanup(SafeCommandsForKey safeCfk)
            {
                return safeCfk;
            }
        };

        // Insert after construction to retain references outside the initialised batch.
        references.put(first.key(), first);
        references.put(second.key(), second);
        Assertions.assertNull(store.get(first.key()));
        Assertions.assertNull(store.get(second.key()));
        Assertions.assertNull(store.canExecute(context));
        Assertions.assertTrue(first.isUninitialised());
        Assertions.assertTrue(second.isUninitialised());

        second.preExecute();
        ExecutionContext executable = store.canExecute(context);
        Assertions.assertNotNull(executable);
        Assertions.assertEquals(RoutingKeys.of(second.key()), executable.keys());
        Assertions.assertSame(second, store.get(second.key()));
        Assertions.assertNull(store.get(first.key()));
        Assertions.assertTrue(first.isUninitialised());
        Assertions.assertNull(store.canExecute(unsequencedIdempotentIncrementalWrite(RoutingKeys.of(first.key()), "test")));

        first.preExecute();
        Assertions.assertSame(context, store.canExecute(context));
        Assertions.assertSame(first, store.get(first.key()));
        Assertions.assertSame(second, store.get(second.key()));
    }
}
