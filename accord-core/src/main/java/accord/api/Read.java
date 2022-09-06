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

package accord.api;

import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.local.CommandStore;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * A read to be performed on potentially multiple shards, the inputs of which may be fed to a {@link Query}
 *
 * TODO: support splitting the read into per-shard portions
 */
public interface Read
{
    Keys keys();
    Future<Data> read(Key key, CommandStore commandStore, Timestamp executeAt, DataStore store);

    class ReadFuture extends AsyncPromise<Data> implements BiConsumer<Data, Throwable>
    {
        public final Keys keyScope;
        private Data result = null;
        private int pending = 0;

        public ReadFuture(Keys keyScope, List<Future<Data>> futures)
        {
            this.keyScope = keyScope;
            pending = futures.size();
            listen(futures);
        }

        private synchronized void listen(List<Future<Data>> futures)
        {
            for (int i=0, mi=futures.size(); i<mi; i++)
                futures.get(i).addCallback(this);
        }

        @Override
        public synchronized void accept(Data data, Throwable throwable)
        {
            if (isDone())
                return;

            if (throwable != null)
                tryFailure(throwable);

            result = result != null ? result.merge(data) : data;
            if (--pending == 0)
                trySuccess(result);
        }
    }
}
