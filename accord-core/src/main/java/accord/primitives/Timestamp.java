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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import accord.local.Node.Id;
import accord.utils.Invariants;
import javax.annotation.Nonnull;

import static accord.primitives.Timestamp.Flag.HLC_BOUND;
import static accord.primitives.Timestamp.Flag.REJECTED;
import static accord.primitives.Timestamp.Flag.SHARD_BOUND;
import static accord.primitives.Timestamp.Flag.SOFT_REJECT;
import static accord.primitives.Timestamp.Flag.UNSTABLE;
import static accord.utils.Invariants.illegalArgument;

/**
 * TxnId flag bits:
 *      [0..1)    - TxnId Domain
 *      [1..4)    - TxnId Kind
 *      [4..5)    - TxnId Cardinality
 *      [5..7)    - TxnId uses FastPath
 *      [7..8)    - TxnId uses Medium Path
 *      [11..12)  - TxnId is unstable - used by Dep handling only
 *
 * Timestamp flag bits
 *      [11..12)  - isRejected
 *
 */
public class Timestamp implements Comparable<Timestamp>, EpochSupplier
{
    public static final Timestamp MAX = new Timestamp(Long.MAX_VALUE, Long.MAX_VALUE, Id.MAX);
    public static final Timestamp NONE = new Timestamp(0, 0, 0, Id.NONE);

    public interface ValueFactory<T extends Timestamp>
    {
        T create(long epoch, long hlc, int flags, Id id);
    }

    public interface RawFactory<T extends Timestamp>
    {
        T create(long msb, long lsb, Id id);
    }

    public enum Flag
    {
        /**
         * To be used only by executeAt responses during PreAccept. Can never be taken at the same time as UNSTABLE.
         */
        REJECTED(0x0800),

        /**
         * To be used only by executeAt responses during PreAccept. Can never be taken at the same time as HLC_BOUND.
         */
        SOFT_REJECT(0x0400),

        /**
         * To be used only by TxnId inside of Dep collections. Can never be taken at the same time as REJECTED.
         */
        UNSTABLE(0x0800),

        /**
         * An ExclusiveSyncPoint that marks its executeAt with this flag can be used to cleanup HLCs.
         * This means it did not witness any conflicts in a future epoch, and so all transactions that
         * might take a lower HLC must have been witnessed.
         */
        HLC_BOUND(0x0400),

        /**
         * An ExclusiveSyncPoint that has applied at the shard and is reported as a lower dependency bound has this bit set. If
         * the TxnId is included as a dependency receiving replicas can use this bound to filter other transactions,
         * but more importantly replicas processing another RX for transitive dependencies MUST report this (or a higher)
         * SHARD_BOUND in their own dependency calculations.
         */
        SHARD_BOUND(0x0200),
        ;
        public final int bit;

        Flag(int bit)
        {
            this.bit = bit;
        }
    }

    /**
     * The set of flags we want to retain as we merge timestamps (e.g. when taking mergeMax).
     */
    static final int MERGE_FLAGS = REJECTED.bit | SOFT_REJECT.bit | UNSTABLE.bit | HLC_BOUND.bit | SHARD_BOUND.bit;
    public static final long IDENTITY_LSB = 0xFFFFFFFF_FFFF00FFL;
    public static final int IDENTITY_FLAGS = 0x00000000_000000FF;
    public static final int NON_IDENTITY_FLAGS = 0x00000000_0000FF00;
    public static final int NON_IDENTITY_FLAGS_SHIFT = Integer.bitCount(IDENTITY_FLAGS);
    public static final int KIND_AND_DOMAIN_FLAGS = 0x00000000_0000000F;
    public static final long MAX_EPOCH = (1L << 48) - 1;
    private static final long HLC_INCR = 1L << 16;
    static final int MAX_FLAGS = (int) (HLC_INCR - 1);

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
        return maxForEpoch(epoch, Timestamp::fromBits);
    }

    public static <T extends Timestamp> T maxForEpoch(long epoch, RawFactory<T> factory)
    {
        return factory.create(epochMsb(epoch) | 0x7fff, Long.MAX_VALUE, Id.MAX);
    }

    public static Timestamp minForEpoch(long epoch)
    {
        return minForEpoch(epoch, Timestamp::fromBits);
    }

    public static <T extends Timestamp> T minForEpoch(long epoch, RawFactory<T> factory)
    {
        return factory.create(epochMsb(epoch), 0, Id.NONE);
    }

    public final long msb;
    public final long lsb;
    public final Id node;

    Timestamp(long epoch, long hlc, int flags, Id node)
    {
        Invariants.requireArgument(hlc >= 0, "hlc must be positive or zero; given %d", hlc);
        Invariants.requireArgument(epoch <= MAX_EPOCH, "epoch %d > MAX_EPOCH %d", epoch, MAX_EPOCH);
        Invariants.requireArgument(flags <= MAX_FLAGS, "flags %d > MAX_FLAGS %d", flags, MAX_FLAGS);
        Invariants.requireArgument(flags >= 0, "flags %d < 0", flags);
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

    public Timestamp(Timestamp copy, Id node)
    {
        this.msb = copy.msb;
        this.lsb = copy.lsb;
        this.node = node;
    }

    Timestamp(Timestamp copy, int flags)
    {
        Invariants.requireArgument(flags <= MAX_FLAGS);
        Invariants.requireArgument(flags >= 0);
        this.msb = copy.msb;
        this.lsb = notFlags(copy.lsb) | flags;
        this.node = copy.node;
    }

    @Override
    public final long epoch()
    {
        return epoch(msb);
    }

    /**
     * A hybrid logical clock with implementation-defined resolution
     */
    public final long hlc()
    {
        return highHlc(msb) | lowHlc(lsb);
    }

    public long uniqueHlc() { return hlc(); }

    public final boolean hasDistinctHlcAndUniqueHlc()
    {
        return getClass() == TimestampWithUniqueHlc.class;
    }

    public final int flags()
    {
        return flags(lsb);
    }

    public final boolean hasNonIdentityFlags()
    {
        return (flagsUnmasked() & NON_IDENTITY_FLAGS) != 0;
    }

    // if caller masks no need to mask before returning
    public final int flagsUnmasked()
    {
        return (int)lsb;
    }

    public boolean is(Flag flag)
    {
        return 0 != (flagsUnmasked() & flag.bit);
    }

    public Timestamp asRejected()
    {
        return addFlag(REJECTED);
    }

    public Timestamp withNextHlc(long hlcAtLeast)
    {
        Invariants.requireArgument(hlcAtLeast >= 0);
        return new Timestamp(epoch(), Math.max(hlcAtLeast, hlc() + 1), flags(), node);
    }

    public Timestamp withEpochAtLeast(long minEpoch)
    {
        return minEpoch <= epoch() ? this : new Timestamp(minEpoch, hlc(), flags(), node);
    }

    public static <T extends Timestamp> T withEpochAtLeast(long minEpoch, T ts, ValueFactory<T> factory)
    {
        return minEpoch <= ts.epoch() ? ts : factory.create(minEpoch, ts.hlc(), ts.flags(), ts.node);
    }

    public static <T extends Timestamp> T withEpochAtMost(long maxEpoch, T ts, ValueFactory<T> factory)
    {
        return maxEpoch >= ts.epoch() ? ts : factory.create(maxEpoch, ts.hlc(), ts.flags(), ts.node);
    }

    public Timestamp withEpoch(long epoch)
    {
        return epoch == epoch() ? this : new Timestamp(epoch, hlc(), flags(), node);
    }

    public Timestamp withNode(Id node)
    {
        return this.node.equals(node) ? this : new Timestamp(this, node);
    }

    public Timestamp addFlags(int flags)
    {
        Invariants.requireArgument(flags <= MAX_FLAGS);
        return addFlags(this, flags, Timestamp::new);
    }

    public Timestamp addFlag(Flag flag)
    {
        return addFlags(flag.bit);
    }

    public Timestamp removeFlag(Flag flag)
    {
        return removeFlags(this, flag.bit, Timestamp::fromBits);
    }

    public Timestamp withoutNonIdentityFlags()
    {
        return removeFlags(this, NON_IDENTITY_FLAGS, Timestamp::fromBits);
    }

    public Timestamp withoutFlags()
    {
        return removeFlags(this, MAX_FLAGS, Timestamp::fromBits);
    }

    public final Timestamp addFlags(Timestamp merge)
    {
        return addFlags(this, merge, Timestamp::new);
    }

    static <T extends Timestamp> T addFlags(T to, T from, RawFactory<T> factory)
    {
        long newLsb = to.lsb | (from.lsb & MERGE_FLAGS);
        if (to.lsb == newLsb)
            return to;
        if (from.lsb == newLsb && to.msb == from.msb && to.node.equals(from.node))
            return from;
        return factory.create(to.msb, newLsb, to.node);
    }

    static <T extends Timestamp> T addFlags(T to, int addFlags, RawFactory<T> factory)
    {
        long newLsb = to.lsb | addFlags;
        if (to.lsb == newLsb)
            return to;
        return factory.create(to.msb, newLsb, to.node);
    }

    static <T extends Timestamp> T removeFlags(T to, int removeFlags, RawFactory<T> factory)
    {
        long newLsb = to.lsb & ~removeFlags;
        if (to.lsb == newLsb)
            return to;
        return factory.create(to.msb, newLsb, to.node);
    }

    public Timestamp logicalNext(Id node)
    {
        long lsb = this.lsb + HLC_INCR;
        long msb = this.msb;
        if (lowHlc(lsb) == 0)
            ++msb; // overflow of lsb
        return new Timestamp(msb, lsb, node);
    }

    public Timestamp logicalPrev(Id node)
    {
        long lsb = this.lsb - HLC_INCR;
        long msb = this.msb;
        if (lowHlc(this.lsb) == 0)
            --msb; // overflow of lsb
        return new Timestamp(msb, lsb, node);
    }

    public Timestamp next()
    {
        if (node.id < Integer.MAX_VALUE)
            return new Timestamp(msb, lsb, new Id(node.id + 1));
        return logicalNext(Id.NONE);
    }

    public Timestamp prev()
    {
        if (node.id > 0)
            return new Timestamp(msb, lsb, new Id(node.id - 1));
        return logicalPrev(Id.MAX);
    }

    @Override
    public int compareTo(@Nonnull Timestamp that)
    {
        if (this == that) return 0;
        int c = compareMsb(this.msb, that.msb);
        if (c == 0) c = compareLsb(this.lsb, that.lsb);
        if (c == 0) c = this.node.compareTo(that.node);
        return c;
    }

    // a non-zero result means that EPOCH and HLC are BOTH greater/less-equal,
    // and at least one of (epoch, hlc, node, flag) is strictly greater/less.
    public final int compareSimultaneousEpochAndHlc(@Nonnull Timestamp that)
    {
        if (this == that) return 0;
        int cmpEpoch = Long.compare(this.epoch(), that.epoch());
        int cmpHlc = Long.compare(this.hlc(), that.hlc());
        if (cmpEpoch != 0)
        {
            return cmpEpoch < 0 ? cmpHlc <= 0 ? -1 : 0
                                : cmpHlc >= 0 ? 1 : 0;
        }
        else if (cmpHlc != 0)
        {
            return cmpHlc;
        }
        int c = Long.compare(this.lsb & IDENTITY_FLAGS, that.lsb & IDENTITY_FLAGS);
        if (c == 0) c = this.node.compareTo(that.node);
        return c;
    }

    public static int compareMsb(long msbA, long msbB)
    {
        return Long.compareUnsigned(msbA, msbB);
    }

    public static int compareLsb(long lsbA, long lsbB)
    {
        int c = Long.compare(lowHlc(lsbA), lowHlc(lsbB));
        return c != 0 ? c : Long.compare(lsbA & IDENTITY_FLAGS, lsbB & IDENTITY_FLAGS);
    }

    public int compareToWithoutEpoch(@Nonnull Timestamp that)
    {
        if (this == that) return 0;
        int c = Long.compare(highHlc(this.msb), highHlc(that.msb));
        if (c == 0) c = compareLsb(this.lsb, that.lsb);
        if (c == 0) c = this.node.compareTo(that.node);
        return c;
    }

    public int compareToStrict(@Nonnull Timestamp that)
    {
        if (this == that) return 0;
        int c = Long.compareUnsigned(this.msb, that.msb);
        if (c == 0) c = Long.compare(lowHlc(this.lsb), lowHlc(that.lsb));
        if (c == 0) c = Long.compare(this.uniqueHlc(), that.uniqueHlc());
        if (c == 0) c = Long.compare(this.flags(), that.flags());
        if (c == 0) c = this.node.compareTo(that.node);
        return c;
    }

    public Timestamp flattenUniqueHlc()
    {
        if (!hasDistinctHlcAndUniqueHlc())
            return this;
        return new Timestamp(epoch(), uniqueHlc(), flags(), node);
    }

    @Override
    public int hashCode()
    {
        return (int) (((msb * 31) + lowHlc(lsb)) * 31) + node.hashCode();
    }

    public boolean equals(Timestamp that)
    {
        if (that == this)
            return true;

        return that != null && this.msb == that.msb
               && ((this.lsb ^ that.lsb) & IDENTITY_LSB) == 0
               && this.node.equals(that.node);
    }

    /**
     * Include flag bits in identity and any uniqueHlc
     */
    public boolean equalsStrict(Timestamp that)
    {
        return this.msb == that.msb && this.lsb == that.lsb && this.node.equals(that.node) && uniqueHlc() == that.uniqueHlc();
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
     * The resulting timestamp will have max(a.hlc,b.hlc) and max(a.epoch, b.epoch),
     * and any mergeable flag in either a or b
     */
    public static Timestamp mergeMaxAndFlags(Timestamp a, Timestamp b)
    {
        // Note: it is not safe to take the highest HLC while retaining the current node;
        //       however, it is safe to take the highest epoch, as the originating node will always advance the hlc()
        return a.compareToWithoutEpoch(b) >= 0 ? a.addFlags(b).withEpochAtLeast(b.epoch())
                                               : b.addFlags(a).withEpochAtLeast(a.epoch());
    }

    /**
     * The resulting timestamp will have max(a.hlc,b.hlc) and max(a.epoch, b.epoch)
     */
    public static Timestamp mergeMax(Timestamp a, Timestamp b)
    {
        // Note: it is not safe to take the highest HLC while retaining the current node;
        //       however, it is safe to take the highest epoch, as the originating node will always advance the hlc()
        return a.compareToWithoutEpoch(b) >= 0 ? a.withEpochAtLeast(b.epoch())
                                               : b.withEpochAtLeast(a.epoch());
    }

    /**
     * The resulting timestamp will have max(a.hlc,b.hlc) and max(a.epoch, b.epoch)
     */
    public static <T extends Timestamp> T mergeMax(T a, T b, ValueFactory<T> factory)
    {
        // Note: it is not safe to take the highest HLC while retaining the current node;
        //       however, it is safe to take the highest epoch, as the originating node will always advance the hlc()
        return a.compareToWithoutEpoch(b) >= 0 ? withEpochAtLeast(b.epoch(), a, factory)
                                               : withEpochAtLeast(a.epoch(), b, factory);
    }

    /**
     * The resulting timestamp will have min(a.hlc,b.hlc) and min(a.epoch, b.epoch)
     */
    public static <T extends Timestamp> T mergeMin(T a, T b, ValueFactory<T> factory)
    {
        // Note: it is not safe to take the lowest HLC while retaining the current node;
        //       however, it is safe to take the highest epoch, as the originating node will always advance the hlc()
        return a.compareToWithoutEpoch(b) <= 0 ? withEpochAtMost(b.epoch(), a, factory)
                                               : withEpochAtMost(a.epoch(), b, factory);
    }

    public static <T extends Timestamp> T nonNullOrMax(T a, T b)
    {
        return a == null ? b : b == null ? a : max(a, b);
    }

    public static <T extends Timestamp> T nonNullOrMin(T a, T b)
    {
        return a == null ? b : b == null ? a : min(a, b);
    }

    public static <T extends Timestamp> T min(T a, T b)
    {
        return a.compareTo(b) <= 0 ? a : b;
    }

    private static long epoch(long msb)
    {
        return msb >>> 15;
    }

    public static long epochMsb(long epoch)
    {
        return epoch << 15;
    }

    public static long hlcMsb(long hlc)
    {
        return hlc >>> 48;
    }

    public static long hlcLsb(long hlc)
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
        Invariants.requireArgument(a.msb == b.msb);
        Invariants.requireArgument(lowHlc(a.lsb) == lowHlc(b.lsb));
        Invariants.requireArgument(a.node.equals(b.node));
        return constructor.construct(a.msb, a.lsb | b.lsb, a.node);
    }

    @Override
    public String toString()
    {
        return toStandardString();
    }

    public String toStandardString()
    {
        long hlc = hlc();
        long uniqueHlc = uniqueHlc();
        return "[" + epoch() + ',' + hlc + (hlc == uniqueHlc ? "" : "+" + (uniqueHlc - hlc)) + ',' + flags() + ',' + node + ']';
    }

    private static final Pattern PARSE = Pattern.compile("\\[(?<epoch>[0-9]+),(?<hlc>[0-9]+)(\\+(?<uniqueHlc>[0-9]+>))?,(?<flags>[0-9]+)(?<kind>\\([KR][REWXV]\\))?,(?<node>[0-9]+)]");
    public static Timestamp parse(String timestampString)
    {
        Matcher m = PARSE.matcher(timestampString);
        if (!m.matches())
            throw illegalArgument("Invalid Timestamp string: " + timestampString);
        return fromMatcher(timestampString, m, true);
    }

    public static Timestamp tryParse(String timestampString)
    {
        Matcher m = PARSE.matcher(timestampString);
        if (!m.matches())
            return null;
        return fromMatcher(timestampString, m, false);
    }

    private static Timestamp fromMatcher(String timestampString, Matcher m, boolean failIfInvalid)
    {
        String uniqueHlc = m.group("uniqueHlc");
        String kind = m.group("kind");
        if (uniqueHlc != null && kind != null)
            return failOrNull(timestampString, failIfInvalid);

        long epoch = Long.parseLong(m.group("epoch"));
        long hlc = Long.parseLong(m.group("hlc"));
        int flags = Integer.parseInt(m.group("flags"));
        if (flags > 0xffff)
            return failOrNull(timestampString, failIfInvalid);

        Id node = new Id(Integer.parseInt(m.group("node")));
        if (uniqueHlc != null)
            return new TimestampWithUniqueHlc(epoch, hlc, Long.parseLong(uniqueHlc), flags, node);

        if (kind != null)
        {
            // TODO (expected): validate kind vs flags
            return TxnId.fromValues(epoch, hlc, flags, node);
        }

        return fromValues(epoch, hlc, flags, node);
    }

    private static <T extends Timestamp> T failOrNull(String timestampString, boolean fail)
    {
        if (fail)
            throw illegalArgument("Invalid Timestamp string: " + timestampString);
        return null;
    }
}
