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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

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

        public Throwable failure()
        {
            Result<V> result = state.get();
            Assertions.assertTrue(result != null);
            Assertions.assertTrue(result.failure != null);
            return result.failure;
        }
    }

    @Test
    void basicTest()
    {
        ResultCallback<Integer> finalCallback = new ResultCallback<>();
        ResultCallback<Integer> intermediateCallback = new ResultCallback<>();

        AsyncChain<Integer> chain = AsyncChains.ofCallable(MoreExecutors.directExecutor(), () -> 5);
        chain = chain.addCallback(intermediateCallback);

        chain = chain.map(i -> i + 2);
        chain.begin(finalCallback);

        Assertions.assertEquals(5, intermediateCallback.value());
        Assertions.assertEquals(7, finalCallback.value());
    }

    /**
     * Test immediate chains can be reused
     */
    @Test
    void immediateTest()
    {
        AsyncChain<Integer> success = AsyncChains.success(5);
        AsyncChain<Integer> chain1 = success.map(i -> i + 2);
        AsyncChain<Integer> chain2 = success.map(i -> i + 2);
        chain2 = chain2.map(i -> i + 2);

        ResultCallback<Integer> firstCallback = new ResultCallback<>();
        ResultCallback<Integer> secondCallback = new ResultCallback<>();
        ResultCallback<Integer> immediateCallback = new ResultCallback<>();

        chain1.begin(firstCallback);
        chain2.begin(secondCallback);
        success.begin(immediateCallback);

        Assertions.assertEquals(firstCallback.value(), 7);
        Assertions.assertEquals(secondCallback.value(), 9);
        Assertions.assertEquals(immediateCallback.value(), 5);
    }

    @Test
    void allTest()
    {
        AsyncChain<Integer> chain1 = AsyncChains.success(1);
        AsyncChain<Integer> chain2 = AsyncChains.success(2);
        AsyncChain<Integer> chain3 = AsyncChains.success(3);
        AsyncChain<List<Integer>> reduced = AsyncChains.all(Lists.newArrayList(chain1, chain2, chain3));
        ResultCallback<List<Integer>> callback = new ResultCallback<>();
        reduced.begin(callback);
        Assertions.assertEquals(Lists.newArrayList(1, 2, 3), callback.value());
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

    private static void assertCombinerSize(int size, AsyncChain<?> chain)
    {
        Assertions.assertTrue(chain instanceof AsyncChainCombiner, () -> String.format("%s is not an instance of AsyncChainCombiner", chain));
        AsyncChainCombiner<?, ?> combiner = (AsyncChainCombiner<?, ?>) chain;
        Assertions.assertEquals(size, combiner.size());
    }

    @Test
    void appendingReduceTest()
    {
        AsyncChain<Integer> chain1 = AsyncChains.success(1);
        AsyncChain<Integer> chain2 = AsyncChains.success(2);
        AsyncChain<Integer> chain3 = AsyncChains.success(3);
        BiFunction<Integer, Integer, Integer> add = (a, b) -> a + b;
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
        BiFunction<Integer, Integer, Integer> add = (a, b) -> a + b;
        BiFunction<Integer, Integer, Integer> mult = (a, b) -> a * b;
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
    void beginSeesException()
    {
        AsyncChains.ofCallable(ignore -> {
                    throw new RejectedExecutionException();
                }, () -> 42)
                .map(i -> i + 1)
                .begin((success, failure) -> {
                    if (failure == null)
                        throw new IllegalStateException("Should see failure");
                });

        AsyncChains.ofRunnable(ignore -> {
                    throw new RejectedExecutionException();
                }, () -> {})
                .map(ignore -> 1)
                .beginAsResult()
                .addCallback((success, failure) -> {
                    if (failure == null)
                        throw new IllegalStateException("Expected to fail");
                });

        AsyncChains.<Integer>ofCallable(fn -> fn.run(), () -> {
                    throw new RuntimeException("Unchecked");
                }).map(i -> i + 1).map(i -> i + 1)
                .begin((success, failure) -> {
                    if (failure == null)
                        throw new IllegalStateException("Should see failure");
                });

        AsyncChains.ofCallable(fn -> fn.run(), () -> 42
                ).map(i -> i + 1)
                .map(ignore -> {
                    throw new RuntimeException("Unchecked");
                })
                .begin((success, failure) -> {
                    if (failure == null)
                        throw new IllegalStateException("Should see failure");
                });
    }

    @Test
    void headRejectsSecondBegin()
    {
        AsyncChain<String> chain = new AsyncChains.Head<String>() {
            @Override
            protected void start(BiConsumer<? super String, Throwable> callback) {
                callback.accept("success", null);
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
            protected void start(BiConsumer<? super String, Throwable> callback) {
                callback.accept("success", null);
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
                .addCallback(() -> sawCallback.set(true))
                .begin((success, failure) -> {
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
            protected void start(BiConsumer<? super Integer, Throwable> callback) {
                callback.accept(0, null);
            }
        };
        chain = chain.map(i -> i + 1)
                     .map(i -> i + 2)
                     .map(i -> i + 3)
                     .map(i -> i + 4)
                     .map(i -> i + 5);

        Assertions.assertEquals(15, AsyncChains.getBlocking(chain));
    }
}
