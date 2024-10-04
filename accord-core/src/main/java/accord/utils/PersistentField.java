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
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

// A helper class for implementing fields that needs to be asynchronously persisted and concurrent updates
// need to be merged and ordered
public class PersistentField<Input, Saved>
{
    public interface Persister<Input, Saved>
    {
        AsyncResult<?> persist(Input addValue, Saved newValue);
        Saved load();
    }

    private static class Pending<Saved>
    {
        final int id;
        final Saved saving;

        private Pending(int id, Saved saving)
        {
            this.id = id;
            this.saving = saving;
        }
    }

    @Nonnull
    private final Supplier<Saved> currentValue;
    @Nonnull
    private final BiFunction<Input, Saved, Saved> merge;
    @Nonnull
    private final Persister<Input, Saved> persister;
    @Nonnull
    private final Consumer<Saved> set;

    private Saved latestPending;
    private int nextId;
    private final ArrayDeque<Pending<Saved>> pending = new ArrayDeque<>();
    private final TreeSet<Integer> complete = new TreeSet<>();

    public PersistentField(@Nonnull Supplier<Saved> currentValue, @Nonnull BiFunction<Input, Saved, Saved> merge, @Nonnull Persister<Input, Saved> persister, @Nullable Consumer<Saved> set)
    {
        Invariants.nonNull(currentValue, "currentValue cannot be null");
        Invariants.nonNull(persister, "persist cannot be null");
        Invariants.nonNull(set, "set cannot be null");
        this.currentValue = currentValue;
        this.merge = merge;
        this.persister = persister;
        this.set = set;
    }

    public void load()
    {
        set.accept(persister.load());
    }

    public synchronized AsyncResult<?> mergeAndUpdate(@Nonnull Input inputValue)
    {
        Invariants.nonNull(merge, "merge cannot be null");
        Invariants.nonNull(inputValue, "inputValue cannot be null");
        return mergeAndUpdate(inputValue, merge);
    }

    private AsyncResult<?> mergeAndUpdate(@Nullable Input inputValue, @Nonnull BiFunction<Input, Saved, Saved> merge)
    {
        Invariants.nonNull(merge, "merge cannot be null");
        AsyncResult.Settable<Void> result = AsyncResults.settable();
        Saved startingValue = latestPending;
        if (startingValue == null)
        {
            Invariants.checkState(pending.isEmpty());
            startingValue = currentValue.get();
        }
        Saved newValue = merge.apply(inputValue, startingValue);
        this.latestPending = newValue;
        int id = ++nextId;
        pending.add(new Pending<>(id, newValue));

        AsyncResult<?> pendingWrite = persister.persist(inputValue, newValue);
        pendingWrite.addCallback((success, fail) -> {
            synchronized (this)
            {
                complete.add(id);
                boolean upd = false;
                Saved latest = null;
                while (!complete.isEmpty() && pending.peek().id == complete.first())
                {
                    latest = pending.poll().saving;
                    complete.pollFirst();
                    upd = true;
                }
                if (upd) set.accept(latest);
            }
        });

        return result;
    }
}
