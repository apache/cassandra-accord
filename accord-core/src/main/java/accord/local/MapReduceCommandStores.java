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

import accord.primitives.Participants;
import accord.primitives.Unseekables;
import accord.utils.MapReduce;
import accord.utils.async.AsyncChain;

public abstract class MapReduceCommandStores<P extends Participants<?>, O> implements PreLoadContext, MapReduce<SafeCommandStore, O>
{
    public final P scope;

    protected MapReduceCommandStores(P scope)
    {
        this.scope = scope;
    }

    public final O apply(SafeCommandStore safeStore)
    {
        if (refuses(safeStore))
            return refuseInternal(safeStore);
        return applyInternal(safeStore);
    }

    public final AsyncChain<O> applyAsync(CommandStore commandStore)
    {
        return applyAsyncInternal(commandStore);
    }

    AsyncChain<O> applyAsyncInternal(CommandStore commandStore)
    {
        return commandStore.chain(this, this);
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
}
