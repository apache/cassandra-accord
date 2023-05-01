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

public class NBitSet
{
    private int capacity;
    private final int bitsPerValue;
    private long[] data;
    private final long mask;

    private int wordsInUse = 0;

    public NBitSet(int capacity, int bitsPerValue)
    {
        if (bitsPerValue < 1 || bitsPerValue > 31)
        {
            throw new IllegalArgumentException("Bits per value must be in between 1 and 31");
        }
        this.capacity = capacity;
        this.bitsPerValue = bitsPerValue;
        this.data = new long[(int) Math.ceil((double) capacity * bitsPerValue / Long.SIZE)];
        this.mask = (1 << bitsPerValue) - 1;
    }

    public int get(int index)
    {
        if (index < 0 || index >= capacity)
        {
            throw new IndexOutOfBoundsException("Index must be between 0 and " + (capacity - 1));
        }
        int bitIndex = index * bitsPerValue;
        int dataIndex = bitIndex / Long.SIZE;
        int offset = bitIndex % Long.SIZE;
        int value = (int) ((data[dataIndex] >>> offset) & mask);
        if (Long.SIZE - offset < bitsPerValue)
        {
            value |= (data[dataIndex + 1] & ((1L << (bitsPerValue - (Long.SIZE - offset))) - 1)) << (Long.SIZE - offset);
        }
        return value;
    }

    public void set(int index, int value)
    {
        if (index < 0)
        {
            throw new IndexOutOfBoundsException("Index must be between 0 and " + (capacity - 1));
        }
        if (value < 0 || value > mask)
        {
            throw new IllegalArgumentException("Value must be between 0 and " + mask);
        }

        if (index >= capacity)
        {
            resize(index + 1);
        }

        int bitIndex = index * bitsPerValue;
        int dataIndex = bitIndex / Long.SIZE;
        int offset = bitIndex % Long.SIZE;
        long orValue = (((long)value) << offset);
        long startValue = data[dataIndex];
        data[dataIndex] = (data[dataIndex] & ~(mask << offset)) | orValue;
        long endValue = data[dataIndex];
        if (startValue != endValue & (startValue == 0 | endValue == 0))
            wordsInUse = endValue == 0 ? wordsInUse - 1 : wordsInUse + 1;
        if (Long.SIZE - offset < bitsPerValue)
        {
            int remainingBits = bitsPerValue - (Long.SIZE - offset);
            startValue = data[dataIndex + 1];
            data[dataIndex + 1] = (data[dataIndex + 1]) & ~((1L << remainingBits) - 1) | (value >>> (Long.SIZE - offset));
            endValue = data[dataIndex + 1];
            if (startValue != endValue & (startValue == 0 | endValue == 0))
                wordsInUse = endValue == 0 ? wordsInUse - 1 : wordsInUse + 1;
        }
    }

    /**
     * Decrements unless it is already zero which silently succeeds
     */
    public void decrementSkipZero(int index)
    {
        int value = get(index);
        if (value == 0)
            return;
        set(index, value - 1);
    }

    public boolean isEmpty()
    {
        return wordsInUse == 0;
    }

    private void resize(int newCapacity)
    {
        int newSize = (int) Math.ceil((double) newCapacity * bitsPerValue / Long.SIZE);
        long[] newData = new long[newSize];
        System.arraycopy(data, 0, newData, 0, data.length);
        data = newData;
        capacity = newCapacity;
    }
}
