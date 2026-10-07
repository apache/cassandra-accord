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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.ProtocolModifiers;
import accord.api.RateLimitedLogStatement;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

// A helper class for implementing fields that needs to be asynchronously persisted and concurrent updates
// need to be merged and ordered
public class PersistentField<Input, Saved>
{
    private static final Logger logger = LoggerFactory.getLogger(PersistentField.class);
    private static final RateLimitedLogStatement failedToPersistDelta = ProtocolModifiers.rateLimitedLogStatementFactory.error("Failed to persist {} {}", logger);
    private static final RateLimitedLogStatement persistingImage = ProtocolModifiers.rateLimitedLogStatementFactory.info("Persisting {} image {} after earlier failure", logger);
    private static final RateLimitedLogStatement failedToPersistImage = ProtocolModifiers.rateLimitedLogStatementFactory.error("Failed to {} {} image {}; discarding save record", logger);

    public interface Persister<Input, Saved>
    {
        AsyncResult<?> persist(Input addValue, Saved newValue);
        AsyncResult<?> persist(Saved newValue);
        Saved load();
    }

    private static class PendingInput<Input>
    {
        final AsyncResult.Settable<Void> done = AsyncResults.settable();
        final Input input;

        private PendingInput(Input input)
        {
            this.input = input;
        }
    }

    private static class PendingSave<Saved>
    {
        final int id;
        final Saved saving;
        final List<AsyncResult.Settable<Void>> notifyOnDone;
        boolean failed;

        private PendingSave(int id, Saved saving, List<AsyncResult.Settable<Void>> notifyOnDone)
        {
            this.id = id;
            this.saving = saving;
            this.notifyOnDone = notifyOnDone;
        }
    }

    private final String name;
    private final Supplier<Saved> currentValue;
    private final BiFunction<Input, Input, Input> mergeInputs;
    private final BiFunction<Input, Saved, Saved> mergeToSave;
    private final Persister<Input, Saved> persister;
    private final Consumer<Saved> set;
    private final Executor mergeExecutor;

    private Saved latestSave;
    private int nextId;
    private boolean requireImage;
    private final List<PendingInput<Input>> inputBuffer = new ArrayList<>();
    private final ArrayDeque<PendingInput<Input>> inputs = new ArrayDeque<>();
    private final ArrayDeque<PendingSave<Saved>> saves = new ArrayDeque<>();
    private final TreeSet<Integer> complete = new TreeSet<>();
    private final Lock mergeLock = new ReentrantLock();

    public PersistentField(String name, @Nonnull Supplier<Saved> currentValue, @Nonnull BiFunction<Input, Input, Input> mergeInputs, @Nonnull BiFunction<Input, Saved, Saved> mergeToSave, @Nonnull Persister<Input, Saved> persister, Consumer<Saved> set, Executor mergeExecutor)
    {
        this.name = name;
        Invariants.nonNull(currentValue, "currentValue cannot be null");
        Invariants.nonNull(mergeInputs, "mergeInputs cannot be null");
        Invariants.nonNull(mergeToSave, "mergeToSave cannot be null");
        Invariants.nonNull(persister, "persist cannot be null");
        Invariants.nonNull(set, "set cannot be null");
        this.currentValue = currentValue;
        this.mergeInputs = mergeInputs;
        this.mergeToSave = mergeToSave;
        this.persister = persister;
        this.set = set;
        this.mergeExecutor = mergeExecutor;
    }

    public void load()
    {
        set.accept(persister.load());
    }

    public AsyncResult<?> save(@Nonnull Input inputValue)
    {
        Invariants.nonNull(inputValue, "inputValue cannot be null");
        PendingInput<Input> submit = new PendingInput<>(inputValue);
        synchronized (this)
        {
            inputs.add(submit);
        }
        trySave();
        return submit.done;
    }

    private void trySave()
    {
        if (mergeLock.tryLock())
        {
            try { save(); }
            finally { mergeLock.unlock(); }

            synchronized (this)
            {
                if (!inputs.isEmpty())
                    mergeExecutor.execute(this::trySave);
            }
        }
    }

    @GuardedBy("mergeLock")
    private void save()
    {
        Saved startingValue;
        synchronized (this)
        {
            if (inputs.isEmpty())
                return;

            inputBuffer.clear();
            inputBuffer.addAll(inputs);
            inputs.clear();

            startingValue = latestSave;
            if (startingValue == null)
            {
                Invariants.require(saves.isEmpty());
                startingValue = currentValue.get();
            }
        }

        Input inputValue = inputBuffer.get(0).input;
        for (int i = 1; i < inputBuffer.size() ; ++i)
            inputValue = mergeInputs.apply(inputValue, inputBuffer.get(i).input);

        Saved newValue = mergeToSave.apply(inputValue, startingValue);
        if (newValue == startingValue)
        {
            inputBuffer.forEach(i -> i.done.setSuccess(null));
            inputBuffer.clear();
            return;
        }

        final List<AsyncResult.Settable<Void>> notifyOnDone = new ArrayList<>(inputBuffer.size());
        for (PendingInput<?> pending : inputBuffer)
            notifyOnDone.add(pending.done);
        inputBuffer.clear();

        // submit persistence before taking our slot, so we can report a failure without interfering with other work
        AsyncResult<?> persist;
        try
        {
            persist = persister.persist(inputValue, newValue);
        }
        catch (Throwable t)
        {
            notifyOnDone.forEach(i -> i.setFailure(t));
            return;
        }

        PendingSave<Saved> save;
        synchronized (this)
        {
            this.latestSave = newValue;
            save = new PendingSave<>(++nextId, newValue, notifyOnDone);
            saves.addLast(save);
        }

        persist.invoke((success, fail) -> persisted(save, fail));
    }

    private synchronized void persisted(PendingSave<Saved> save, Throwable fail)
    {
        if (fail != null)
        {
            failedToPersistDelta.log(name, save.id, fail);
            save.failed = true;
        }
        complete.add(save.id);
        postPersisted();
    }

    private synchronized void persistedImage(@Nonnull PendingSave<Saved> save, Throwable fail)
    {
        Invariants.expect(saves.peekFirst() == save);
        if (fail != null)
        {
            failedToPersistImage.log("persist", name, save.id, fail);
            saves.pollFirst();
            save.notifyOnDone.forEach(i -> i.setFailure(fail));
        }
        else
        {
            requireImage = false;
            save.failed = false;
            complete.add(save.id);
        }
        postPersisted();
    }

    private synchronized void postPersisted()
    {
        Saved latest = null;
        while (!complete.isEmpty())
        {
            PendingSave<Saved> next = Invariants.nonNull(saves.peekFirst());
            if (complete.first() != next.id)
                break;

            complete.pollFirst();
            if (next.failed)
                requireImage = true;

            if (requireImage)
            {
                persistingImage.log(name, next.id);
                AsyncResult<?> writeImage;
                try { writeImage = persister.persist(next.saving); }
                catch (Throwable t)
                {
                    failedToPersistImage.log("initiate persist of", name, next.id, t);
                    next.notifyOnDone.forEach(i -> i.setFailure(t));
                    saves.pollFirst();
                    continue;
                }

                if (latest != null)
                    set.accept(latest);
                writeImage.invoke((s2, f2) -> persistedImage(next, f2));
                return;
            }

            saves.pollFirst();
            next.notifyOnDone.forEach(i -> i.setSuccess(null));
            latest = next.saving;
        }

        if (latest != null)
            set.accept(latest);
    }
}
