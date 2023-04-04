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

package accord.impl.list;

import java.util.function.Supplier;

import accord.local.CommandStore;
import accord.local.PreLoadContext;
import accord.utils.async.AsyncChain;

public interface ListExecutor
{
    <T> AsyncChain<T> submit(Supplier<T> fn);

    AsyncChain<Void> execute(Runnable fn);

    static ListExecutor fromStore(CommandStore store)
    {
        return new ListExecutor()
        {
            @Override
            public <T> AsyncChain<T> submit(Supplier<T> fn)
            {
                return store.submit(PreLoadContext.empty(), ignore -> fn.get());
            }

            @Override
            public AsyncChain<Void> execute(Runnable fn)
            {
                return store.execute(PreLoadContext.empty(), ignore -> fn.run());
            }
        };
    }
}
