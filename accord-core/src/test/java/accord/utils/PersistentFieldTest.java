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


package accord.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

// this file was authored by LLM
class PersistentFieldTest
{
    private static PersistentField<Integer, Integer> field(AtomicInteger installed, BiFunction<Integer, Integer, AsyncResult<?>> persist, Function<Integer, AsyncResult<?>> image)
    {
        PersistentField.Persister<Integer, Integer> persister = new PersistentField.Persister<>()
        {
            @Override
            public AsyncResult<?> persist(Integer delta, Integer newValue)
            {
                return persist.apply(delta, newValue);
            }

            @Override
            public AsyncResult<?> persist(Integer newValue)
            {
                return image.apply(newValue);
            }

            @Override
            public Integer load()
            {
                return 0;
            }
        };
        return new PersistentField<>("", installed::get, Integer::sum, Integer::sum, persister, installed::set, Runnable::run);
    }

    @Test
    void synchronousPersistFailureDoesNotPoisonLaterSaves()
    {
        AtomicInteger installed = new AtomicInteger();
        AtomicInteger calls = new AtomicInteger();
        PersistentField<Integer, Integer> field = field(installed, (delta, newValue) -> {
            if (calls.getAndIncrement() == 0)
                throw new IllegalStateException("injected persistence failure");
            return AsyncResults.success(null);
        }, newValue -> { throw new UnsupportedOperationException(); });

        AsyncResult<?> first = null;
        try { first = field.save(1); }
        catch (IllegalStateException ignore) {} // permitted; the failure must not affect later saves
        if (first != null)
        {
            assertTrue(first.isDone());
            assertFalse(first.isSuccess());
        }

        AsyncResult<?> second = field.save(2);
        assertTrue(second.isSuccess());
        assertEquals(2, installed.get());

        AsyncResult<?> third = field.save(4);
        assertTrue(third.isSuccess());
        assertEquals(6, installed.get());
    }

    @Test
    void asynchronousPersistFailureIsNotReportedAsSuccess()
    {
        AtomicInteger installed = new AtomicInteger();
        PersistentField<Integer, Integer> field = field(installed, (delta, newValue) -> AsyncResults.failure(new RuntimeException("disk failed")), (newValue) -> AsyncResults.failure(new RuntimeException("disk failed")));

        AsyncResult<?> result = field.save(1);
        assertTrue(result.isDone());
        assertFalse(result.isSuccess());
        assertEquals(0, installed.get());
    }

    @Test
    void deltaFailureRecoveredByImage()
    {
        AtomicInteger installed = new AtomicInteger();
        List<Integer> images = new ArrayList<>();
        PersistentField<Integer, Integer> field = field(installed, (delta, newValue) -> AsyncResults.failure(new RuntimeException("disk failed")),
                                                        newValue -> {
                                                            images.add(newValue);
                                                            if (images.size() > 1) throw new AssertionError("re-imaged " + images);
                                                            return AsyncResults.success(null);
                                                        });

        AsyncResult<?> result = field.save(1);
        assertEquals(List.of(1), images);
        assertTrue(result.isSuccess());
        assertEquals(1, installed.get());
    }

    @Test
    void laterSavesWaitForImageOfEarlierFailure()
    {
        AtomicInteger installed = new AtomicInteger();
        List<AsyncResult.Settable<Void>> deltas = new ArrayList<>();
        List<AsyncResult.Settable<Void>> images = new ArrayList<>();
        PersistentField<Integer, Integer> field = field(installed,
                                                        (delta, newValue) -> { AsyncResult.Settable<Void> r = AsyncResults.settable(); deltas.add(r); return r; },
                                                        newValue -> { AsyncResult.Settable<Void> r = AsyncResults.settable(); images.add(r); return r; });

        AsyncResult<?> first = field.save(1);
        AsyncResult<?> second = field.save(2);
        deltas.get(1).setSuccess(null);
        assertFalse(second.isDone());
        deltas.get(0).setFailure(new RuntimeException("disk failed"));
        assertEquals(1, images.size());
        assertFalse(first.isDone());
        assertFalse(second.isDone());

        images.get(0).setSuccess(null);
        assertEquals(1, images.size());
        assertTrue(first.isSuccess());
        assertTrue(second.isSuccess());
        assertEquals(3, installed.get());
    }

    @Test
    void imageFailureFailsWaitersAndNextSaveIsImaged()
    {
        AtomicInteger installed = new AtomicInteger();
        List<AsyncResult.Settable<Void>> deltas = new ArrayList<>();
        List<Integer> imageValues = new ArrayList<>();
        List<AsyncResult.Settable<Void>> images = new ArrayList<>();
        PersistentField<Integer, Integer> field = field(installed,
                                                        (delta, newValue) -> { AsyncResult.Settable<Void> r = AsyncResults.settable(); deltas.add(r); return r; },
                                                        newValue -> { AsyncResult.Settable<Void> r = AsyncResults.settable(); imageValues.add(newValue); images.add(r); return r; });

        AsyncResult<?> first = field.save(1);
        AsyncResult<?> second = field.save(2);
        deltas.get(1).setSuccess(null);
        deltas.get(0).setFailure(new RuntimeException("disk failed"));

        images.get(0).setFailure(new RuntimeException("image failed"));
        assertTrue(first.isDone());
        assertFalse(first.isSuccess());
        assertFalse(second.isDone());
        assertEquals(List.of(1, 3), imageValues);

        images.get(1).setSuccess(null);
        assertTrue(second.isSuccess());
        assertEquals(3, installed.get());

        AsyncResult<?> third = field.save(4);
        deltas.get(2).setSuccess(null);
        assertTrue(third.isSuccess());
        assertEquals(2, imageValues.size());
        assertEquals(7, installed.get());
    }
}
