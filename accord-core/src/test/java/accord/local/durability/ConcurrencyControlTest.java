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

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// this file was authored by LLM
class ConcurrencyControlTest
{
    @Test
    void immediateSupplierFailureReleasesSlot()
    {
        ConcurrencyControl control = new ConcurrencyControl(1);
        assertThrows(IllegalStateException.class, () -> control.<Void>submit(() -> { throw new IllegalStateException("injected supplier failure"); }));

        AtomicBoolean started = new AtomicBoolean();
        AsyncResult<Void> next = control.<Void>submit(() -> {
            started.set(true);
            return AsyncChains.success(null);
        }).beginAsResult();

        assertTrue(started.get(), "next supplier must start");
        assertTrue(next.isSuccess());
        assertEquals(0, control.inflight);
        assertEquals(0, control.pending.size());
    }

    @Test
    void queuedSupplierFailureReleasesSlot()
    {
        ConcurrencyControl control = new ConcurrencyControl(1);
        AsyncResult.Settable<Void> first = AsyncResults.settable();
        control.<Void>submit(first::chain).begin((s, f) -> {});

        AsyncResult<Void> failing = control.<Void>submit(() -> { throw new IllegalStateException("injected supplier failure"); }).beginAsResult();
        AsyncResult<Void> next = control.<Void>submit(() -> AsyncChains.success(null)).beginAsResult();
        assertEquals(2, control.pending.size());
        assertFalse(failing.isDone());

        first.setSuccess(null);
        assertTrue(failing.isDone());
        assertFalse(failing.isSuccess());
        assertTrue(next.isSuccess(), "next supplier must start and complete");
        assertEquals(0, control.inflight);
        assertEquals(0, control.pending.size());
    }
}
