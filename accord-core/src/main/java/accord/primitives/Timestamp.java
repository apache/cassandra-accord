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

package accord.primitives;

import accord.local.Node.Id;

public class Timestamp implements Comparable<Timestamp>
{
    public static final Timestamp NONE = new Timestamp(0, 0, 0, Id.NONE);

    public final long epoch;
    public final long real;
    public final int logical;
    public final Id node;

    public Timestamp(long epoch, long real, int logical, Id node)
    {
        this.epoch = epoch;
        this.real = real;
        this.logical = logical;
        this.node = node;
    }

    public Timestamp(Timestamp copy)
    {
        this.epoch = copy.epoch;
        this.real = copy.real;
        this.logical = copy.logical;
        this.node = copy.node;
    }

    public Timestamp withMinEpoch(long minEpoch)
    {
        return minEpoch <= epoch ? this : new Timestamp(minEpoch, real, logical, node);
    }

    public Timestamp logicalNext(Id node)
    {
        return new Timestamp(epoch, real, logical + 1, node);
    }

    @Override
    public int compareTo(Timestamp that)
    {
        if (this == that) return 0;
        int c = Long.compare(this.epoch, that.epoch);
        if (c == 0) c = Long.compare(this.real, that.real);
        if (c == 0) c = Integer.compare(this.logical, that.logical);
        if (c == 0) c = this.node.compareTo(that.node);
        return c;
    }

    @Override
    public int hashCode()
    {
        return (int) (((((epoch * 31) + real) * 31) + node.hashCode()) * 31 + logical);
    }

    public boolean equals(Timestamp that)
    {
        return this.epoch == that.epoch && this.real == that.real && this.logical == that.logical && this.node.equals(that.node);
    }

    @Override
    public boolean equals(Object that)
    {
        return that instanceof Timestamp && equals((Timestamp) that);
    }

    public static <T extends Timestamp> T max(T a, T b)
    {
        return a.compareTo(b) >= 0 ? a : b;
    }

    public static <T extends Timestamp> T min(T a, T b)
    {
        return a.compareTo(b) <= 0 ? a : b;
    }

    @Override
    public String toString()
    {
        return "[" + epoch + ',' + real + ',' + logical + ',' + node + ']';
    }

}
