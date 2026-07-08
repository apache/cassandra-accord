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

package accord.local;

import javax.annotation.Nullable;

import accord.api.Tracing;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Unseekables;
import accord.utils.MapReduce;
import accord.utils.async.AsyncChain;

import static accord.primitives.Routables.Slice.Minimal;

public abstract class MapReduceCommandStores<P extends Participants<?>, O> implements ExecutionContext, MapReduce<SafeCommandStore, O>
{
    public final P scope;
    private Tracing tracing;

    protected MapReduceCommandStores(P scope)
    {
        this.scope = scope;
    }

    public final O apply(SafeCommandStore safeStore)
    {
        if (refuses(safeStore))
        {
            if (tracing != null)
                tracing.trace(safeStore.commandStore(), "Refused");
            return refuseInternal(safeStore);
        }
        if (tracing != null)
            tracing.trace(safeStore.commandStore(), "Processing");

        return applyInternal(safeStore);
    }

    public final AsyncChain<O> applyAsync(Ranges ranges, CommandStore commandStore)
    {
        return applyAsyncInternal(ranges, commandStore);
    }

    protected AsyncChain<O> applyAsyncInternal(Ranges ranges, CommandStore commandStore)
    {
        return commandStore.chain(slice(ranges, Minimal), this);
    }

    protected boolean supportsPartialRefusal()
    {
        return false;
    }

    private boolean refuses(SafeCommandStore safeStore)
    {
        if (supportsPartialRefusal()) return safeStore.refusesAllOwnedOf(scope);
        else return safeStore.refusesAnyOf(scope);
    }

    protected O refuseInternal(SafeCommandStore safeStore)
    {
        throw new LogUnavailableException();
    }

    protected abstract O applyInternal(SafeCommandStore safeStore);

    @Override
    public Unseekables<?> keys()
    {
        return scope;
    }

    public void setTracing(Tracing tracing)
    {
        this.tracing = tracing;
    }

    public final @Nullable Tracing tracing()
    {
        return tracing;
    }
}
