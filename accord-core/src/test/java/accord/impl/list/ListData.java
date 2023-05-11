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

import java.util.Arrays;
import java.util.TreeMap;
import java.util.stream.Collectors;

import accord.api.Data;
import accord.api.Key;
import accord.utils.Timestamped;

public class ListData extends TreeMap<Key, Timestamped<int[]>> implements Data
{
    @Override
    public Data merge(Data data)
    {
        if (data != null)
        {
            ((ListData)data).forEach((k, v) -> {
                merge(k, v, (a, b) -> a == null ? b : b == null ? a : Timestamped.mergeEqual(a, b, Arrays::equals));
            });
        }
        return this;
    }

    @Override
    public String toString()
    {
        return entrySet().stream()
                         .map(e -> e.getKey() + "=" + Arrays.toString(e.getValue().data))
                         .collect(Collectors.joining(", ", "{", "}"));
    }
}
