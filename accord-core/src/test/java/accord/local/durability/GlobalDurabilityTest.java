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


package accord.local.durability;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import accord.local.Node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

// this file was authored by LLM
class GlobalDurabilityTest
{
    private static void startGlobalSync(GlobalDurability durability) throws Exception
    {
        Method method = GlobalDurability.class.getDeclaredMethod("startGlobalSync");
        method.setAccessible(true);
        method.invoke(durability);
    }

    private static int inflight(GlobalDurability durability) throws Exception
    {
        Field field = GlobalDurability.class.getDeclaredField("inflight");
        field.setAccessible(true);
        return field.getInt(durability);
    }

    @Test
    void failedLaunchRefundsReservedPermits() throws Exception
    {
        GlobalDurability durability = new GlobalDurability(mock(Node.class));
        AtomicInteger attempts = new AtomicInteger();
        durability.setMaxConcurrency(2);
        durability.setSelectSendTo(topology -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("injected launch failure");
        });

        startGlobalSync(durability);
        assertEquals(0, inflight(durability));
        startGlobalSync(durability);
        assertEquals(2, attempts.get(), "second cycle must not be suppressed");
        assertEquals(0, inflight(durability));
    }
}
