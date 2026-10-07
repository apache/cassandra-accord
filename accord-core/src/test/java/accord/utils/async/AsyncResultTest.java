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

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.impl.AbstractAsyncExecutor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AsyncResultTest
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
    void immediateSuccessTest()
    {
        AsyncResult<Integer> result = AsyncResults.success(42);
        
        Assertions.assertTrue(result.isDone());
        Assertions.assertTrue(result.isSuccess());
        
        ResultCallback<Integer> callback = new ResultCallback<>();
        result.invoke(callback);
        Assertions.assertEquals(42, callback.value());
    }

    @Test
    void immediateFailureTest()
    {
        RuntimeException exception = new RuntimeException("test failure");
        AsyncResult<Integer> result = AsyncResults.failure(exception);
        
        Assertions.assertTrue(result.isDone());
        Assertions.assertFalse(result.isSuccess());
        
        ResultCallback<Integer> callback = new ResultCallback<>();
        result.invoke(callback);
        Assertions.assertEquals(exception, callback.failure());
    }

    @Test
    void settableResultTest()
    {
        AsyncResult.Settable<String> settable = AsyncResults.settable();
        
        Assertions.assertFalse(settable.isDone());
        Assertions.assertFalse(settable.isSuccess());
        
        settable.setSuccess("test value");
        
        Assertions.assertTrue(settable.isDone());
        Assertions.assertTrue(settable.isSuccess());
        
        ResultCallback<String> callback = new ResultCallback<>();
        settable.invoke(callback);
        Assertions.assertEquals("test value", callback.value());
    }

    @Test
    void settableResultFailureTest()
    {
        AsyncResult.Settable<String> settable = AsyncResults.settable();
        RuntimeException exception = new RuntimeException("settable failure");
        
        settable.setFailure(exception);
        
        Assertions.assertTrue(settable.isDone());
        Assertions.assertFalse(settable.isSuccess());
        
        ResultCallback<String> callback = new ResultCallback<>();
        settable.invoke(callback);
        Assertions.assertEquals(exception, callback.failure());
    }

    @Test
    void settableResultDoubleSetRejection()
    {
        AsyncResult.Settable<String> settable = AsyncResults.settable();
        settable.setSuccess("first");
        
        assertThatThrownBy(() -> settable.setSuccess("second")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> settable.setFailure(new RuntimeException())).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void mapTransformationTest()
    {
        AsyncResult<Integer> source = AsyncResults.success(42);
        AsyncResult<String> mapped = source.map(i -> "value: " + i);
        
        Assertions.assertTrue(mapped.isDone());
        Assertions.assertTrue(mapped.isSuccess());
        
        ResultCallback<String> callback = new ResultCallback<>();
        mapped.invoke(callback);
        Assertions.assertEquals("value: 42", callback.value());
    }

    @Test
    void mapWithExecutorTest()
    {
        AtomicBoolean executorUsed = new AtomicBoolean(false);
        AbstractAsyncExecutor testExecutor = task -> {
            executorUsed.set(true);
            task.run();
        };
        
        AsyncResult<Integer> source = AsyncResults.success(42);
        AsyncResult<String> mapped = source.map(i -> "mapped: " + i, testExecutor);
        
        ResultCallback<String> callback = new ResultCallback<>();
        mapped.invoke(callback);
        
        Assertions.assertTrue(executorUsed.get());
        Assertions.assertEquals("mapped: 42", callback.value());
    }

    @Test
    void flatMapTest()
    {
        AsyncResult<Integer> source = AsyncResults.success(3);
        AsyncResult<String> flatMapped = source.flatMap(i -> AsyncResults.success("result: " + (i * 2)));
        
        Assertions.assertTrue(flatMapped.isDone());
        Assertions.assertTrue(flatMapped.isSuccess());
        
        ResultCallback<String> callback = new ResultCallback<>();
        flatMapped.invoke(callback);
        Assertions.assertEquals("result: 6", callback.value());
    }

    @Test
    void flatMapWithFailureTest()
    {
        RuntimeException exception = new RuntimeException("flatmap failure");
        AsyncResult<Integer> source = AsyncResults.success(3);
        AsyncResult<String> flatMapped = source.flatMap(i -> AsyncResults.failure(exception));
        
        Assertions.assertTrue(flatMapped.isDone());
        Assertions.assertFalse(flatMapped.isSuccess());
        
        ResultCallback<String> callback = new ResultCallback<>();
        flatMapped.invoke(callback);
        Assertions.assertEquals(exception, callback.failure());
    }

    @Test
    void recoverFromFailureTest()
    {
        RuntimeException originalException = new RuntimeException("original failure");
        AsyncResult<String> failed = AsyncResults.failure(originalException);
        AsyncResult<String> recovered = failed.recover(ex -> AsyncResults.success("recovered"));
        
        Assertions.assertTrue(recovered.isDone());
        Assertions.assertTrue(recovered.isSuccess());
        
        ResultCallback<String> callback = new ResultCallback<>();
        recovered.invoke(callback);
        Assertions.assertEquals("recovered", callback.value());
    }

    @Test
    void recoverFromSuccessTest()
    {
        AsyncResult<String> success = AsyncResults.success("original success");
        AsyncResult<String> recovered = success.recover(ex -> AsyncResults.success("should not be called"));
        
        Assertions.assertTrue(recovered.isDone());
        Assertions.assertTrue(recovered.isSuccess());
        
        ResultCallback<String> callback = new ResultCallback<>();
        recovered.invoke(callback);
        Assertions.assertEquals("original success", callback.value());
    }

    @Test
    void invokeIfSuccessTest()
    {
        AtomicBoolean successCallbackCalled = new AtomicBoolean(false);
        AsyncResult<Integer> success = AsyncResults.success(42);
        
        success.invokeIfSuccess(() -> successCallbackCalled.set(true));
        Assertions.assertTrue(successCallbackCalled.get());
        
        AtomicBoolean failureCallbackCalled = new AtomicBoolean(false);
        AsyncResult<Integer> failure = AsyncResults.failure(new RuntimeException());
        
        failure.invokeIfSuccess(() -> failureCallbackCalled.set(true));
        Assertions.assertFalse(failureCallbackCalled.get());
    }

    @Test
    void invokeIfSuccessWithValueTest()
    {
        AtomicInteger capturedValue = new AtomicInteger(0);
        AsyncResult<Integer> success = AsyncResults.success(42);
        
        success.invokeIfSuccess(capturedValue::set);
        Assertions.assertEquals(42, capturedValue.get());
    }

    @Test
    void multipleCallbacksTest()
    {
        AsyncResult.Settable<String> settable = AsyncResults.settable();
        
        AtomicInteger callbackCount = new AtomicInteger(0);
        AtomicReference<String> callback1Value = new AtomicReference<>();
        AtomicReference<String> callback2Value = new AtomicReference<>();
        
        settable.invoke((value, ex) -> {
            callbackCount.incrementAndGet();
            callback1Value.set(value);
        });
        
        settable.invoke((value, ex) -> {
            callbackCount.incrementAndGet();
            callback2Value.set(value);
        });
        
        settable.setSuccess("shared value");
        
        Assertions.assertEquals(2, callbackCount.get());
        Assertions.assertEquals("shared value", callback1Value.get());
        Assertions.assertEquals("shared value", callback2Value.get());
    }

    @Test
    void chainConversionTest()
    {
        AsyncResult<Integer> result = AsyncResults.success(42);
        AsyncChain<Integer> chain = result.chain();
        
        ResultCallback<Integer> callback = new ResultCallback<>();
        chain.begin(callback);
        
        Assertions.assertEquals(42, callback.value());
    }

    @Test
    void chainImmediatelyElseTest()
    {
        AsyncResult<Integer> completedResult = AsyncResults.success(42);
        AsyncChain<Integer> chain = completedResult.chainImmediatelyElse(null);
        
        ResultCallback<Integer> callback = new ResultCallback<>();
        chain.begin(callback);
        
        Assertions.assertEquals(42, callback.value());
    }

    @Test
    void runnableResultTest()
    {
        AsyncResults.RunnableResult<String> runnableResult = AsyncResults.runnableResult(() -> "computed value");
        
        Assertions.assertFalse(runnableResult.isDone());
        
        runnableResult.run();
        
        Assertions.assertTrue(runnableResult.isDone());
        Assertions.assertTrue(runnableResult.isSuccess());
        
        ResultCallback<String> callback = new ResultCallback<>();
        runnableResult.invoke(callback);
        Assertions.assertEquals("computed value", callback.value());
    }

    @Test
    void runnableExceptionTest()
    {
        RuntimeException exception = new RuntimeException("callable exception");
        AsyncResults.RunnableResult<String> runnableResult = AsyncResults.runnableResult(() -> {
            throw exception;
        });
        
        runnableResult.run();
        
        Assertions.assertTrue(runnableResult.isDone());
        Assertions.assertFalse(runnableResult.isSuccess());
        
        ResultCallback<String> callback = new ResultCallback<>();
        runnableResult.invoke(callback);
        Assertions.assertEquals(exception, callback.failure());
    }

    @Test
    void propagateExceptionsTest()
    {
        AbstractAsyncExecutor rejecting = ignore -> { throw new RejectedExecutionException(); };
        AsyncResult<Integer> source = AsyncResults.success(42);
        
        AsyncResult<String> mapped = source.map(i -> "value: " + i, rejecting);
        
        Assertions.assertTrue(mapped.isDone());
        Assertions.assertFalse(mapped.isSuccess());
        
        ResultCallback<String> callback = new ResultCallback<>();
        mapped.invoke(callback);
        Assertions.assertTrue(callback.failure() instanceof RejectedExecutionException);
    }

    @Test
    void callbackExceptionTest()
    {
        AsyncResult<Integer> result = AsyncResults.success(42);
        AtomicReference<Throwable> reported = new AtomicReference<>();
        Thread.UncaughtExceptionHandler old = Thread.currentThread().getUncaughtExceptionHandler();
        Thread.currentThread().setUncaughtExceptionHandler((t, e) -> reported.set(e));
        try
        {
            result.invoke((value, ex) -> {
                throw new RuntimeException("callback exception");
            });
            assertThat(reported.get()).isInstanceOf(RuntimeException.class);
        }
        finally
        {
            Thread.currentThread().setUncaughtExceptionHandler(old);
        }
    }

    @Test
    void nullSuccessValueTest()
    {
        AsyncResult<String> nullResult = AsyncResults.success(null);
        
        Assertions.assertTrue(nullResult.isDone());
        Assertions.assertTrue(nullResult.isSuccess());
        
        ResultCallback<String> callback = new ResultCallback<>();
        nullResult.invoke(callback);
        Assertions.assertNull(callback.value());
    }

    private static Throwable failure(AsyncResult<?> result)
    {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        result.invoke((success, fail) -> failure.set(fail));
        return failure.get();
    }

    private static void withUncaughtHandler(AtomicReference<Throwable> reported, Runnable run)
    {
        Thread thread = Thread.currentThread();
        Thread.UncaughtExceptionHandler old = thread.getUncaughtExceptionHandler();
        thread.setUncaughtExceptionHandler((ignore, failure) -> reported.set(failure));
        try { run.run(); }
        finally { thread.setUncaughtExceptionHandler(old); }
    }

    @Test
    void reducerExceptionFailsCombinedResult()
    {
        RuntimeException boom = new RuntimeException("reducer");
        AtomicReference<Throwable> reported = new AtomicReference<>();
        AsyncResult.Settable<Integer> left = AsyncResults.settable();
        AsyncResult.Settable<Integer> right = AsyncResults.settable();
        AsyncResult<Integer> combined = AsyncResults.reduce(Arrays.asList(left, right), (a, b) -> { throw boom; });

        withUncaughtHandler(reported, () -> {
            left.setSuccess(1);
            right.setSuccess(2);
        });

        Assertions.assertNull(reported.get());
        Assertions.assertTrue(combined.isDone());
        Assertions.assertSame(boom, failure(combined));
    }

    @Test
    void zeroInputProcessExceptionFailsCombinedResult()
    {
        RuntimeException boom = new RuntimeException("process");
        AsyncResult<Integer> combined = new AsyncCombiner.ResultCombiner<Object, Integer>(Collections.emptyList())
        {
            @Override
            Integer process(Object[] inputs)
            {
                throw boom;
            }
        }.beginAsResult();

        Assertions.assertTrue(combined.isDone());
        Assertions.assertSame(boom, failure(combined));
    }

    @Test
    void nestedMapperExceptionFailsResult()
    {
        RuntimeException boom = new RuntimeException("mapper");
        AtomicReference<Throwable> reported = new AtomicReference<>();
        AsyncResult.Settable<Integer> source = AsyncResults.settable();
        AsyncResult<String> nested = NestedAsyncResult.flatMap(source, value -> { throw boom; });

        withUncaughtHandler(reported, () -> source.setSuccess(1));

        Assertions.assertNull(reported.get());
        Assertions.assertTrue(nested.isDone());
        Assertions.assertSame(boom, failure(nested));
        Assertions.assertEquals("Done", nested.toString());
    }

    @Test
    void listenerRethrowingFailureDoesNotPreventLaterListeners()
    {
        RuntimeException original = new RuntimeException("original");
        AtomicReference<Throwable> reported = new AtomicReference<>();
        AtomicBoolean secondNotified = new AtomicBoolean();
        AsyncResult.Settable<Integer> source = AsyncResults.settable();
        source.invoke((success, fail) -> { throw (RuntimeException) fail; });
        source.invoke((success, fail) -> secondNotified.set(true));

        withUncaughtHandler(reported, () -> source.setFailure(original));

        Assertions.assertTrue(secondNotified.get());
        Assertions.assertSame(original, reported.get());
        Assertions.assertEquals(0, original.getSuppressed().length);

        // immediate (already completed) path
        reported.set(null);
        withUncaughtHandler(reported, () -> AsyncResults.failure(original).invoke((success, fail) -> { throw (RuntimeException) fail; }));
        Assertions.assertSame(original, reported.get());
    }
}
