package accord.coordinate;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import accord.local.Node;
import accord.api.Result;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.txn.Ballot;

public class Coordinate
{
    private static CompletionStage<Result> andThenExecute(Node node, CompletionStage<Agreed> agree)
    {
        DebugCompletionStage<Result> result = new DebugCompletionStage<>(agree);
        result.wrapped = agree.thenCompose(agreed -> {
            CompletionStage<Result> execute = Execute.execute(node, agreed);
            result.debug2 = execute;
            return execute;
        });
        return result;
    }

    public static CompletionStage<Result> execute(Node node, TxnId txnId, Txn txn)
    {
        return andThenExecute(node, Agree.agree(node, txnId, txn));
    }

    public static CompletionStage<Result> recover(Node node, TxnId txnId, Txn txn)
    {
        return andThenExecute(node, new Recover(node, new Ballot(node.uniqueNow()), txnId, txn));
    }

    private static class DebugCompletionStage<T> implements CompletionStage<T>
    {
        final Object debug1;
        Object debug2;
        CompletionStage<T> wrapped;

        @Override
        public <U> CompletionStage<U> thenApply(Function<? super T, ? extends U> fn)
        {
            return wrapped.thenApply(fn);
        }

        @Override
        public <U> CompletionStage<U> thenApplyAsync(Function<? super T, ? extends U> fn)
        {
            return wrapped.thenApplyAsync(fn);
        }

        @Override
        public <U> CompletionStage<U> thenApplyAsync(Function<? super T, ? extends U> fn, Executor executor)
        {
            return wrapped.thenApplyAsync(fn, executor);
        }

        @Override
        public CompletionStage<Void> thenAccept(Consumer<? super T> action)
        {
            return wrapped.thenAccept(action);
        }

        @Override
        public CompletionStage<Void> thenAcceptAsync(Consumer<? super T> action)
        {
            return wrapped.thenAcceptAsync(action);
        }

        @Override
        public CompletionStage<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor)
        {
            return wrapped.thenAcceptAsync(action, executor);
        }

        @Override
        public CompletionStage<Void> thenRun(Runnable action)
        {
            return wrapped.thenRun(action);
        }

        @Override
        public CompletionStage<Void> thenRunAsync(Runnable action)
        {
            return wrapped.thenRunAsync(action);
        }

        @Override
        public CompletionStage<Void> thenRunAsync(Runnable action, Executor executor)
        {
            return wrapped.thenRunAsync(action, executor);
        }

        @Override
        public <U, V> CompletionStage<V> thenCombine(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn)
        {
            return wrapped.thenCombine(other, fn);
        }

        @Override
        public <U, V> CompletionStage<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn)
        {
            return wrapped.thenCombineAsync(other, fn);
        }

        @Override
        public <U, V> CompletionStage<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn, Executor executor)
        {
            return wrapped.thenCombineAsync(other, fn, executor);
        }

        @Override
        public <U> CompletionStage<Void> thenAcceptBoth(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action)
        {
            return wrapped.thenAcceptBoth(other, action);
        }

        @Override
        public <U> CompletionStage<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action)
        {
            return wrapped.thenAcceptBothAsync(other, action);
        }

        @Override
        public <U> CompletionStage<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action, Executor executor)
        {
            return wrapped.thenAcceptBothAsync(other, action, executor);
        }

        @Override
        public CompletionStage<Void> runAfterBoth(CompletionStage<?> other, Runnable action)
        {
            return wrapped.runAfterBoth(other, action);
        }

        @Override
        public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action)
        {
            return wrapped.runAfterBothAsync(other, action);
        }

        @Override
        public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor)
        {
            return wrapped.runAfterBothAsync(other, action, executor);
        }

        @Override
        public <U> CompletionStage<U> applyToEither(CompletionStage<? extends T> other, Function<? super T, U> fn)
        {
            return wrapped.applyToEither(other, fn);
        }

        @Override
        public <U> CompletionStage<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn)
        {
            return wrapped.applyToEitherAsync(other, fn);
        }

        @Override
        public <U> CompletionStage<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn, Executor executor)
        {
            return wrapped.applyToEitherAsync(other, fn, executor);
        }

        @Override
        public CompletionStage<Void> acceptEither(CompletionStage<? extends T> other, Consumer<? super T> action)
        {
            return wrapped.acceptEither(other, action);
        }

        @Override
        public CompletionStage<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action)
        {
            return wrapped.acceptEitherAsync(other, action);
        }

        @Override
        public CompletionStage<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action, Executor executor)
        {
            return wrapped.acceptEitherAsync(other, action, executor);
        }

        @Override
        public CompletionStage<Void> runAfterEither(CompletionStage<?> other, Runnable action)
        {
            return wrapped.runAfterEither(other, action);
        }

        @Override
        public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action)
        {
            return wrapped.runAfterEitherAsync(other, action);
        }

        @Override
        public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor)
        {
            return wrapped.runAfterEitherAsync(other, action, executor);
        }

        @Override
        public <U> CompletionStage<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn)
        {
            return wrapped.thenCompose(fn);
        }

        @Override
        public <U> CompletionStage<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn)
        {
            return wrapped.thenComposeAsync(fn);
        }

        @Override
        public <U> CompletionStage<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn, Executor executor)
        {
            return wrapped.thenComposeAsync(fn, executor);
        }

        @Override
        public <U> CompletionStage<U> handle(BiFunction<? super T, Throwable, ? extends U> fn)
        {
            return wrapped.handle(fn);
        }

        @Override
        public <U> CompletionStage<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn)
        {
            return wrapped.handleAsync(fn);
        }

        @Override
        public <U> CompletionStage<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn, Executor executor)
        {
            return wrapped.handleAsync(fn, executor);
        }

        @Override
        public CompletionStage<T> whenComplete(BiConsumer<? super T, ? super Throwable> action)
        {
            return wrapped.whenComplete(action);
        }

        @Override
        public CompletionStage<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action)
        {
            return wrapped.whenCompleteAsync(action);
        }

        @Override
        public CompletionStage<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action, Executor executor)
        {
            return wrapped.whenCompleteAsync(action, executor);
        }

        @Override
        public CompletionStage<T> exceptionally(Function<Throwable, ? extends T> fn)
        {
            return wrapped.exceptionally(fn);
        }

        @Override
        public CompletableFuture<T> toCompletableFuture()
        {
            return wrapped.toCompletableFuture();
        }

        private DebugCompletionStage(Object debug1)
        {
            this.debug1 = debug1;
        }
    }

}
