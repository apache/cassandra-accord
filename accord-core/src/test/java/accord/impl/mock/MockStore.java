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

package accord.impl.mock;

import accord.api.Data;
import accord.api.Key;
import accord.api.Query;
import accord.api.Read;
import accord.api.Result;
import accord.api.DataStore;
import accord.api.Update;
import accord.api.Write;
import accord.primitives.Keys;
import accord.primitives.Timestamp;

public class MockStore implements DataStore
{
    public static final Data DATA = new Data() {
        @Override
        public Data merge(Data data)
        {
            return DATA;
        }
    };

    public static final Result RESULT = new Result() {};
    public static final Query QUERY = (data, read, update) -> RESULT;
    public static final Write WRITE = (key, executeAt, store) -> {};

    public static Read read(Keys keys)
    {
        return new Read()
        {
            @Override
            public Keys keys()
            {
                return keys;
            }

            @Override
            public Data read(Key key, Timestamp executeAt, DataStore store)
            {
                return DATA;
            }
        };
    }

    public static Update update(Keys keys)
    {
        return new Update()
        {
            @Override
            public Keys keys()
            {
                return keys;
            }

            @Override
            public Write apply(Data data)
            {
                return WRITE;
            }
        };
    }
}
