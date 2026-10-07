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

package accord.utils.async;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.impl.AbstractAsyncExecutor;
import accord.utils.Reduce;
import org.assertj.core.api.AbstractThrowableAssert;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static accord.utils.Invariants.illegalState;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AsyncChainsTest
{
    private static class ResultCallback<V> implements BiConsumer<V, Throwable>
    {
        private static class Result<V>
        {
            final V value;
            final Throwable failure;

            public Result(V value, Throwable failure)
            {
                this.value = value;
                this.failure = failure;
            }
        }

        private final AtomicReference<Result<V>> state = new AtomicReference<>(null);

        @Override
        public void accept(V result, Throwable failure)
        {
            boolean set = state.compareAndSet(null, new Result<>(result, failure));
            Assertions.assertTrue(set);
        }

        public V value()
        {
            Result<V> result = state.get();
            Assertions.assertTrue(result != null);
            Assertions.assertTrue(result.failure == null);
            return result.value;
        }
    }

    @Test
    void basicTest()
    {
        ResultCallback<Integer> finalCallback = new ResultCallback<>();
        ResultCallback<Integer> intermediateCallback = new ResultCallback<>();

        AsyncChain<Integer> chain = AsyncChains.chain(() -> 5);
        chain = chain.invoke(intermediateCallback);

        chain = chain.map(i -> i + 2);

        chain.begin(finalCallback);

        Assertions.assertEquals(5, intermediateCallback.value());
        Assertions.assertEquals(7, finalCallback.value());
    }

    /**
     * Test immediate chains cannot be reused
     */
    @Test
    void immediateTest()
    {
        AsyncChain<Integer> success = AsyncChains.success(5);
        success.map(i -> i + 2);
        boolean failed = false;
        try
        {
            success.map(i -> i + 2);
        }
        catch (Throwable t)
        {
            failed = true;
        }
        Assertions.assertTrue(failed);
    }

    @Test
    void allTest()
    {
        AsyncChain<Integer> chain1 = AsyncChains.success(1);
        AsyncChain<Integer> chain2 = AsyncChains.success(2);
        AsyncChain<Integer> chain3 = AsyncChains.success(3);
        AsyncChain<List<Integer>> reduced = AsyncChains.allOf(Lists.newArrayList(chain1, chain2, chain3));
        ResultCallback<List<Integer>> callback = new ResultCallback<>();
        reduced.begin(callback);
        Assertions.assertEquals(Arrays.asList(1, 2, 3), callback.value());
    }

    @Test
    void reduceTest()
    {
        AsyncChain<Integer> chain1 = AsyncChains.success(1);
        AsyncChain<Integer> chain2 = AsyncChains.success(2);
        AsyncChain<Integer> chain3 = AsyncChains.success(3);
        AsyncChain<Integer> reduced = AsyncChains.reduce(Lists.newArrayList(chain1, chain2, chain3), (a, b) -> a + b);
        ResultCallback<Integer> callback = new ResultCallback<>();
        reduced.begin(callback);
        Assertions.assertEquals(6, callback.value());
    }

    @Test
    void beginSeesException()
    {
        AbstractAsyncExecutor rejecting = ignore -> { throw new RejectedExecutionException(); };
        rejecting.chain(() -> 42)
                .map(i -> i + 1)
                .begin((success, failure) -> {
                    if (failure == null)
                        throw illegalState("Should see failure");
                });

        rejecting.chain(() -> {})
                .map(ignore -> 1)
                .beginAsResult()
                .invoke((success, failure) -> {
                    if (failure == null)
                        throw illegalState("Expected to fail");
                });
    }

    @Test
    void headRejectsSecondBegin()
    {
        AsyncChain<String> chain = new AsyncChains.Head<String>() {
            @Override
            protected Cancellable start(BiConsumer<? super String, Throwable> callback) {
                callback.accept("success", null);
                return null;
            }
        };

        chain.begin((i1, i2) -> {});
        assertThrows(() -> chain.begin((i1, i2) -> {}));
    }

    @Test
    void chainRejectsSecondBegin()
    {
        AsyncChain<String> chain = new AsyncChains.Head<String>() {
            @Override
            protected Cancellable start(BiConsumer<? super String, Throwable> callback) {
                callback.accept("success", null);
                return null;
            }
        };
        chain = chain.map(s -> s + " is true");
        chain.begin((i1, i2) -> {});
        AsyncChain<String> finalChain = chain;
        assertThrows(() -> finalChain.begin((i1, i2) -> {}));
    }

    private static void assertThrows(Runnable fn)
    {
        try
        {
            fn.run();
            Assertions.fail("Should have been rejected");
        }
        catch (AssertionError e)
        {
            if ("Should have been rejected".equals(e.getMessage())) throw e;
        }
        catch (Throwable t)
        {
            // expected
        }
    }

    @Test
    void test3()
    {
        AtomicReference<Boolean> sawFailure = new AtomicReference<>(null);
        AtomicBoolean sawCallback = new AtomicBoolean(false);
        AsyncChains.failure(new NullPointerException("just kidding"))
                .beginAsResult()
                .invokeIfSuccess(() -> sawCallback.set(true))
                .invoke((success, failure) -> {
                    if (failure != null) sawFailure.set(true);
                    else sawFailure.set(false);
                });
        Assertions.assertEquals(Boolean.TRUE, sawFailure.get());
        Assertions.assertFalse(sawCallback.get());
    }

    @Test
    void simpleHeadChain() throws ExecutionException, InterruptedException {
        AsyncChain<Integer> chain = new AsyncChains.Head<Integer>() {
            @Override
            protected Cancellable start(BiConsumer<? super Integer, Throwable> callback) {
                callback.accept(0, null);
                return null;
            }
        };
        chain = chain.map(i -> i + 1)
                     .map(i -> i + 2)
                     .map(i -> i + 3)
                     .map(i -> i + 4)
                     .map(i -> i + 5);

        Assertions.assertEquals(15, AsyncChainUtils.getBlocking(chain));
    }

    private static void assertCombinerSize(int size, AsyncChain<?> chain)
    {
        Assertions.assertTrue(chain instanceof AsyncChains.AccumulatingReducer, () -> String.format("%s is not an instance of AsyncChains.AccumulatingReducer", chain));
        AsyncChains.AccumulatingReducer<?> combiner = (AsyncChains.AccumulatingReducer<?>) chain;
        Assertions.assertEquals(size, combiner.size());
    }

    @Test
    void appendingReduceTest()
    {
        AsyncChain<Integer> chain1 = AsyncChains.success(1);
        AsyncChain<Integer> chain2 = AsyncChains.success(2);
        AsyncChain<Integer> chain3 = AsyncChains.success(3);
        Reduce<Integer, Integer> add = (a, b) -> a + b;
        AsyncChain<Integer> reduction1 = AsyncChains.reduce(chain1, chain2, add);
        assertCombinerSize(2, reduction1);
        AsyncChain<Integer> reduction2 = AsyncChains.reduce(reduction1, chain3, add);
        assertCombinerSize(3, reduction2);
        Assertions.assertSame(reduction1, reduction2);

        ResultCallback<Integer> callback = new ResultCallback<>();
        reduction2.begin(callback);
        Assertions.assertEquals(6, callback.value());
    }

    @Test
    void uncombinableReduce()
    {
        AsyncChain<Integer> chain1 = AsyncChains.success(1);
        AsyncChain<Integer> chain2 = AsyncChains.success(2);
        AsyncChain<Integer> chain3 = AsyncChains.success(3);
        Reduce<Integer, Integer> add = (a, b) -> a + b;
        Reduce<Integer, Integer> mult = (a, b) -> a * b;
        AsyncChain<Integer> reduction1 = AsyncChains.reduce(chain1, chain2, add);
        assertCombinerSize(2, reduction1);
        AsyncChain<Integer> reduction2 = AsyncChains.reduce(reduction1, chain3, mult);
        assertCombinerSize(2, reduction2);
        Assertions.assertNotSame(reduction1, reduction2);

        ResultCallback<Integer> callback = new ResultCallback<>();
        reduction2.begin(callback);
        Assertions.assertEquals(9, callback.value());
    }

    @Test
    void exceptionHandling()
    {
        List<Supplier<? extends AsyncChain<? extends Object>>> topLevel = new ArrayList<>();
        topLevel.add(() -> AsyncChains.success(42));
        topLevel.add(() -> new AsyncChains.Head<Integer>()
        {
            @Override
            protected Cancellable start(BiConsumer<? super Integer, Throwable> callback)
            {
                callback.accept(42, null);
                return null;
            }
        });
        topLevel.add(() -> {
            AsyncResult.Settable<Integer> settable = AsyncResults.settable();
            settable.setSuccess(42);
            return settable.chain();
        });
        topLevel.add(() -> AsyncChains.allOf(Arrays.asList(AsyncChains.success(0), AsyncChains.success(0), AsyncChains.success(42))));
        topLevel.add(() -> AsyncChains.reduce(AsyncChains.success(1), AsyncChains.success(1), (a, b) -> a + b));
        topLevel.add(() -> AsyncChains.<Integer>failure(new NullPointerException()).recover(ignore -> AsyncChains.success(42)));

        for (Supplier<? extends AsyncChain<? extends Object>> start : topLevel)
        {
            assertWillSeeFailure(start.get().map(ignore -> {throw new UserFailure();})).isInstanceOf(UserFailure.class);
            assertWillSeeFailure(start.get().map(i -> i.toString()).map(ignore -> {throw new UserFailure();})).isInstanceOf(UserFailure.class);
            assertWillSeeFailure(start.get().flatMap(i -> AsyncChains.success(i)).map(ignore -> {throw new UserFailure();})).isInstanceOf(UserFailure.class);

            assertWillSeeFailure(start.get().flatMap(ignore -> {throw new UserFailure();})).isInstanceOf(UserFailure.class);
            assertWillSeeFailure(start.get().map(i -> i.toString()).flatMap(ignore -> {throw new UserFailure();})).isInstanceOf(UserFailure.class);
            assertWillSeeFailure(start.get().flatMap(i -> AsyncChains.success(i)).flatMap(ignore -> {throw new UserFailure();})).isInstanceOf(UserFailure.class);

            assertThatThrownBy(() -> start.get().begin((s, f) -> {throw new UserFailure();})).isInstanceOf(UserFailure.class);
            assertThatThrownBy(() -> start.get().map(i -> i.toString()).begin((s, f) -> {throw new UserFailure();})).isInstanceOf(UserFailure.class);
            assertThatThrownBy(() -> start.get().flatMap(i -> AsyncChains.success(i)).begin((s, f) -> {throw new UserFailure();})).isInstanceOf(UserFailure.class);
        }
    }

    private static class UserFailure extends RuntimeException
    {

    }

    private static <T> AbstractThrowableAssert<?, ? extends Throwable> assertWillSeeFailure(AsyncChain<T> chain)
    {
        BiConsumer<? super T, Throwable> mock = Mockito.mock(BiConsumer.class);
        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        Mockito.doNothing().when(mock).accept(Mockito.isNull(), captor.capture());
        chain.begin(mock);
        return org.assertj.core.api.Assertions.assertThat(captor.getValue());
    }

    private static Throwable failure(AsyncResult<?> result)
    {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        result.invoke((success, fail) -> failure.set(fail));
        return failure.get();
    }

    @Test
    void intermediateCallbackExceptionFailsDownstream()
    {
        RuntimeException boom = new RuntimeException("callback");
        AtomicReference<Throwable> reported = new AtomicReference<>();
        Thread thread = Thread.currentThread();
        Thread.UncaughtExceptionHandler old = thread.getUncaughtExceptionHandler();
        AsyncResult.Settable<Integer> source = AsyncResults.settable();
        AsyncResult<Integer> downstream = source.chain().invoke((value, failure) -> { throw boom; }).beginAsResult();
        try
        {
            thread.setUncaughtExceptionHandler((ignored, failure) -> reported.set(failure));
            source.setSuccess(1);
        }
        finally
        {
            thread.setUncaughtExceptionHandler(old);
        }

        Assertions.assertNull(reported.get());
        Assertions.assertTrue(downstream.isDone());
        Assertions.assertSame(boom, failure(downstream));
    }

    @Test
    void intermediateCallbackExceptionSuppressesOriginalFailure()
    {
        RuntimeException original = new RuntimeException("original");
        RuntimeException boom = new RuntimeException("callback");
        AsyncResult<Integer> downstream = AsyncChains.<Integer>failure(original).invoke((value, failure) -> { throw boom; }).beginAsResult();

        Assertions.assertSame(boom, failure(downstream));
        Assertions.assertSame(original, boom.getSuppressed()[0]);
        Assertions.assertEquals(0, original.getSuppressed().length); // the (possibly shared) upstream failure is not mutated
    }

    @Test
    void intermediateCallbackRethrowingFailureIsNotSelfSuppressed()
    {
        RuntimeException original = new RuntimeException("original");
        AsyncResult<Integer> downstream = AsyncChains.<Integer>failure(original).invoke((value, failure) -> { throw (RuntimeException) failure; }).beginAsResult();
        Assertions.assertSame(original, failure(downstream));
        Assertions.assertEquals(0, original.getSuppressed().length);
    }

    @Test
    void asyncCallbackExceptionFailsDownstream()
    {
        RuntimeException boom = new RuntimeException("callback");
        AbstractAsyncExecutor immediate = Runnable::run;
        AsyncResult<Integer> downstream = AsyncChains.success(1).invoke((value, failure) -> { throw boom; }, immediate).beginAsResult();
        Assertions.assertSame(boom, failure(downstream));
    }

    @Test
    void asyncCallbackRejectionFailsDownstream()
    {
        RejectedExecutionException rejected = new RejectedExecutionException();
        AbstractAsyncExecutor rejecting = ignore -> { throw rejected; };
        AsyncResult<Integer> downstream = AsyncChains.success(1).invoke((value, failure) -> {}, rejecting).beginAsResult();
        Assertions.assertTrue(downstream.isDone());
        Assertions.assertSame(rejected, failure(downstream));
    }
}
