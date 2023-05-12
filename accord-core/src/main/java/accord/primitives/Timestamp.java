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
import accord.utils.Invariants;

import static accord.utils.Invariants.checkArgument;

import javax.annotation.Nonnull;

public class Timestamp implements Comparable<Timestamp>
{
    private static final int REJECTED_FLAG = 0x8000;

    /**
     * The set of flags we want to retain as we merge timestamps (e.g. when taking mergeMax).
     * Today this is only the REJECTED_FLAG, but we may include additional flags in future (such as Committed, Applied..)
     * which we may also want to retain when merging in other contexts (such as in Deps).
     */
    private static final int MERGE_FLAGS = 0x8000;

    public static Timestamp fromBits(long msb, long lsb, Id node)
    {
        return new Timestamp(msb, lsb, node);
    }

    public static Timestamp fromValues(long epoch, long hlc, Id node)
    {
        return new Timestamp(epoch, hlc, 0, node);
    }

    public static Timestamp fromValues(long epoch, long hlc, int flags, Id node)
    {
        return new Timestamp(epoch, hlc, flags, node);
    }

    public static Timestamp maxForEpoch(long epoch)
    {
        return new Timestamp(epochMsb(epoch) | 0x7fff, Long.MAX_VALUE, Id.MAX);
    }

    public static Timestamp minForEpoch(long epoch)
    {
        return new Timestamp(epochMsb(epoch), 0, Id.NONE);
    }

    // TODO (expected): we can only use 63 bits for HLC as we don't support negative timestamps, so can use 49 bits for epoch
    public static final long MAX_EPOCH = (1L << 48) - 1;
    private static final long HLC_INCR = 1L << 16;
    private static final long MAX_FLAGS = HLC_INCR - 1;
    public static final Timestamp NONE = new Timestamp(0, 0, 0, Id.NONE);

    public final long msb;
    public final long lsb;
    public final Id node;

    Timestamp(long epoch, long hlc, int flags, Id node)
    {
        Invariants.checkArgument(hlc >= 0);
        Invariants.checkArgument(epoch <= MAX_EPOCH);
        Invariants.checkArgument(flags <= MAX_FLAGS);
        this.msb = epochMsb(epoch) | hlcMsb(hlc);
        this.lsb = hlcLsb(hlc) | flags;
        this.node = node;
    }

    Timestamp(long msb, long lsb, Id node)
    {
        this.msb = msb;
        this.lsb = lsb;
        this.node = node;
    }

    public Timestamp(Timestamp copy)
    {
        this.msb = copy.msb;
        this.lsb = copy.lsb;
        this.node = copy.node;
    }

    Timestamp(Timestamp copy, int flags)
    {
        checkArgument(flags <= MAX_FLAGS);
        this.msb = copy.msb;
        this.lsb = notFlags(copy.lsb) | flags;
        this.node = copy.node;
    }

    public long epoch()
    {
        return epoch(msb);
    }

    /**
     * A hybrid logical clock with implementation-defined resolution
     */
    public long hlc()
    {
        return highHlc(msb) | lowHlc(lsb);
    }

    public int flags()
    {
        return flags(lsb);
    }

    public boolean isRejected()
    {
        return (lsb & REJECTED_FLAG) != 0;
    }

    public Timestamp asRejected()
    {
        return withExtraFlags(REJECTED_FLAG);
    }

    public Timestamp withEpochAtLeast(long minEpoch)
    {
        return minEpoch <= epoch() ? this : new Timestamp(minEpoch, hlc(), flags(), node);
    }

    public Timestamp withExtraFlags(int flags)
    {
        checkArgument(flags <= MAX_FLAGS);
        long newLsb = lsb | flags;
        if (lsb == newLsb)
            return this;
        return new Timestamp(msb, newLsb, node);
    }

    public Timestamp mergeFlags(Timestamp mergeFlags)
    {
        long newLsb = lsb | (mergeFlags.lsb & MERGE_FLAGS);
        if (lsb == newLsb)
            return this;
        return new Timestamp(msb, newLsb, node);
    }

    public Timestamp logicalNext(Id node)
    {
        long lsb = this.lsb + HLC_INCR;
        long msb = this.msb;
        if (lowHlc(lsb) == 0)
            ++msb; // overflow of lsb
        return new Timestamp(msb, lsb, node);
    }

    @Override
    public int compareTo(@Nonnull Timestamp that)
    {
        if (this == that) return 0;
        int c = Long.compareUnsigned(this.msb, that.msb);
        if (c == 0) c = Long.compare(lowHlc(this.lsb), lowHlc(that.lsb));
        if (c == 0) c = this.node.compareTo(that.node);
        return c;
    }

    public int compareToStrict(@Nonnull Timestamp that)
    {
        if (this == that) return 0;
        int c = Long.compareUnsigned(this.msb, that.msb);
        if (c == 0) c = Long.compareUnsigned(this.lsb, that.lsb);
        if (c == 0) c = this.node.compareTo(that.node);
        return c;
    }

    @Override
    public int hashCode()
    {
        return (int) (((msb * 31) + lowHlc(lsb)) * 31) + node.hashCode();
    }

    public boolean equals(Timestamp that)
    {
        return this.msb == that.msb && lowHlc(this.lsb) == lowHlc(that.lsb) && this.node.equals(that.node);
    }

    /**
     * Include flag bits in identity
     */
    public boolean equalsStrict(Timestamp that)
    {
        return this.msb == that.msb && this.lsb == that.lsb && this.node.equals(that.node);
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

    /**
     * Take the maximum of the two, but merge any mergeable flags
     */
    public static Timestamp mergeMax(Timestamp a, Timestamp b)
    {
        return a.compareTo(b) >= 0 ? a.mergeFlags(b) : b.mergeFlags(a);
    }

    public static <T extends Timestamp> T rejectedOrMax(T a, T b)
    {
        return a.compareTo(b) >= 0 ? a : b;
    }

    public static Timestamp rejectStale(Timestamp maybeReject, Timestamp ifOnOrBefore)
    {
        if (maybeReject.compareTo(ifOnOrBefore) > 0)
            return maybeReject;
        return maybeReject.asRejected();
    }

    public static <T extends Timestamp> T nonNullOrMax(T a, T b)
    {
        return a == null ? b : b == null ? a : max(a, b);
    }

    public static <T extends Timestamp> T min(T a, T b)
    {
        return a.compareTo(b) <= 0 ? a : b;
    }

    private static long epoch(long msb)
    {
        return msb >>> 15;
    }

    private static long epochMsb(long epoch)
    {
        return epoch << 15;
    }

    private static long hlcMsb(long hlc)
    {
        return hlc >>> 48;
    }

    private static long hlcLsb(long hlc)
    {
        return hlc << 16;
    }

    private static long highHlc(long msb)
    {
        return (msb & 0x7fff) << 48;
    }

    private static long lowHlc(long lsb)
    {
        return lsb >>> 16;
    }

    private static int flags(long lsb)
    {
        return (int) (lsb & MAX_FLAGS);
    }

    private static long notFlags(long lsb)
    {
        return lsb & ~MAX_FLAGS;
    }

    public Timestamp merge(Timestamp that)
    {
        return merge(this, that, Timestamp::fromBits);
    }

    interface Constructor<T>
    {
        T construct(long msb, long lsb, Id node);
    }

    static <T extends Timestamp> T merge(Timestamp a, Timestamp b, Constructor<T> constructor)
    {
        checkArgument(a.msb == b.msb);
        checkArgument(lowHlc(a.lsb) == lowHlc(b.lsb));
        checkArgument(a.node.equals(b.node));
        return constructor.construct(a.msb, a.lsb | b.lsb, a.node);
    }

    @Override
    public String toString()
    {
        return "[" + epoch() + ',' + hlc() + ',' + flags() + ',' + node + ']';
    }
}
