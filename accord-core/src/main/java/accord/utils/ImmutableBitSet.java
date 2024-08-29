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

public class ImmutableBitSet extends SimpleBitSet
{
    public static class SerializationSupport
    {
        public static ImmutableBitSet construct(long[] bits)
        {
            return new ImmutableBitSet(bits);
        }
    }

    public static final ImmutableBitSet EMPTY = new ImmutableBitSet();

    private ImmutableBitSet()
    {
        super(0);
    }

    public ImmutableBitSet(int size)
    {
        super(size);
    }

    public ImmutableBitSet(int size, boolean set)
    {
        super(size, set);
    }

    public ImmutableBitSet(SimpleBitSet copy)
    {
        super(copy);
    }

    public ImmutableBitSet(SimpleBitSet copy, boolean share)
    {
        super(copy, share);
    }

    ImmutableBitSet(long[] bits)
    {
        super(bits);
    }

    @Override
    public boolean set(int i)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRange(int from, int to)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean unset(int i)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return "ImmutableBitSet{" +
               "count=" + count +
               '}';
    }
}
