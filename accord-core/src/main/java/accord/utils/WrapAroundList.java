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

import java.util.AbstractList;

public class WrapAroundList<T> extends AbstractList<T>
{
    final T[] contents;
    final int start, end, size;

    public WrapAroundList(T[] contents, int start, int end)
    {
        this.contents = contents;
        this.start = start;
        this.end = end;
        this.size = end > start ? end - start : end + (contents.length - start);
    }


    @Override
    public T get(int index)
    {
        int i = start + index;
        if (i >= contents.length) i -= contents.length;
        return contents[i];
    }

    @Override
    public int size()
    {
        return size;
    }
}