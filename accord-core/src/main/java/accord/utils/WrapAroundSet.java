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

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;

public class WrapAroundSet<T> extends AbstractSet<T>
{
    final Map<T, Integer> lookup;
    final WrapAroundList<T> list;

    public WrapAroundSet(Map<T, Integer> lookup, WrapAroundList<T> list)
    {
        this.lookup = lookup;
        this.list = list;
    }

    @Override
    public boolean contains(Object o)
    {
        Integer i = lookup.get(o);
        if (i == null) return false;
        if (list.end > list.start)
            return i >= list.start && i < list.end;
        else
            return i >= list.start || i < list.end;
    }

    @Override
    public Iterator<T> iterator()
    {
        return list.iterator();
    }

    @Override
    public int size()
    {
        return list.size;
    }
}
