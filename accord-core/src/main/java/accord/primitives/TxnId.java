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

import javax.annotation.Nullable;

import accord.local.Node.Id;
import accord.primitives.Routable.Domain;
import accord.primitives.Txn.Kind;
import accord.primitives.Txn.Kind.Kinds;
import accord.utils.Invariants;
import accord.utils.TinyEnumSet;

import static accord.primitives.Timestamp.Flag.UNSTABLE;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.primitives.Txn.Kind.Read;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.Cardinality.Any;
import static accord.primitives.TxnId.MediumPath.NoMediumPath;
import static accord.utils.Invariants.illegalArgument;

public class TxnId extends Timestamp
{
    public enum FastPath
    {
        Unoptimised,
        PrivilegedCoordinatorWithoutDeps,
        PrivilegedCoordinatorWithDeps;

        private static final int SHIFT = 5;
        private static final FastPath[] VALUES = values();

        public final int bits;

        FastPath()
        {
            this.bits = ordinal() << SHIFT;
        }

        final boolean is(int flagsUnmasked)
        {
            return is(flagsUnmasked, this);
        }

        private static boolean is(int flagsUnmasked, FastPath test)
        {
            return ((flagsUnmasked >>> SHIFT) & 3) == test.ordinal();
        }

        private static FastPath forFlags(int flagsUnmasked)
        {
            return forOrdinal(ordinalFromFlags(flagsUnmasked));
        }

        private static int ordinalFromFlags(int flagsUnmasked)
        {
            return (flagsUnmasked >>> SHIFT) & 3;
        }

        public static FastPath forOrdinal(int ordinal)
        {
            return VALUES[ordinal];
        }

        static boolean hasPrivilegedCoordinator(int flagsUnmasked)
        {
            return 0 != ordinalFromFlags(flagsUnmasked);
        }

        public FastPath toPermitted(FastPaths permitted)
        {
            if (permitted.contains(this))
                return this;
            if (this == Unoptimised)
                return this;
            FastPath alt = this == PrivilegedCoordinatorWithDeps ? PrivilegedCoordinatorWithoutDeps : PrivilegedCoordinatorWithDeps;
            if (permitted.contains(alt))
                return alt;
            return Unoptimised;
        }
    }

    public static class FastPaths extends TinyEnumSet<FastPath>
    {
        public FastPaths(FastPath... values) { super(values); }

        public boolean hasPrivilegedCoordinator() { return bitset > 1; }
    }

    public enum MediumPath
    {
        NoMediumPath,
        TrackStable;

        private static final int SHIFT = 7;
        public final int bit() { return ordinal() << SHIFT; }

        final boolean is(int flagsUnmasked)
        {
            return is(flagsUnmasked, this);
        }

        private static boolean is(int flagsUnmasked, MediumPath test)
        {
            return ((flagsUnmasked >>> SHIFT) & 1) == test.ordinal();
        }

        public static MediumPath forFlags(int flagsUnmasked)
        {
            return (flagsUnmasked & (1 << SHIFT)) == 0 ? NoMediumPath : TrackStable;
        }

        public static MediumPath forOrdinal(int ordinal)
        {
            return ordinal == 0 ? NoMediumPath : TrackStable;
        }
    }

    public enum Cardinality
    {
        SingleKey,
        Any;

        private static final int SHIFT = 4;
        public final int bit()
        {
            return ordinal() << SHIFT;
        }

        final boolean is(int flagsUnmasked)
        {
            return is(flagsUnmasked, this);
        }

        public static Cardinality cardinality(Routables<?> routables)
        {
            return routables.size() == 1 && routables.domain() == Domain.Key ? SingleKey : Any;
        }

        public static Cardinality cardinality(Domain domain, Routables<?> routables)
        {
            return domain == Domain.Key && routables.size() == 1 ? SingleKey : Any;
        }

        private static boolean is(int flagsUnmasked, Cardinality test)
        {
            return ((flagsUnmasked >>> SHIFT) & 1) == test.ordinal();
        }

        private static Cardinality forFlags(int flagsUnmasked)
        {
            return (flagsUnmasked & (1 << SHIFT)) == 0 ? SingleKey : Any;
        }

        public static Cardinality forOrdinal(int ordinal)
        {
            return ordinal == 0 ? SingleKey : Any;
        }
    }

    private static final int DOMAIN_AND_KIND_AND_CARDINALITY_MASK = 0x1f;
    public static final TxnId[] NO_TXNIDS = new TxnId[0];

    public static final TxnId NONE = new TxnId(0, 0, Id.NONE);
    public static final TxnId MAX = new TxnId(Long.MAX_VALUE, Long.MAX_VALUE, Id.MAX);

    public static TxnId fromBits(long msb, long lsb, Id node)
    {
        return new TxnId(msb, lsb, node);
    }

    public static TxnId fromValues(long epoch, long hlc, Id node)
    {
        return new TxnId(epoch, hlc, 0, node);
    }

    public static TxnId fromValues(long epoch, long hlc, int flags, Id node)
    {
        return new TxnId(epoch, hlc, flags, node);
    }

    public static TxnId fromValues(long epoch, long hlc, int flags, int node)
    {
        return new TxnId(epoch, hlc, flags, new Id(node));
    }

    public TxnId(Timestamp timestamp, Kind rw, Domain domain)
    {
        this(timestamp, rw, domain, Any);
    }

    public TxnId(Timestamp timestamp, Kind rw, Domain domain, Cardinality cardinality)
    {
        this(timestamp, 0, rw, domain, cardinality);
    }

    public TxnId(Timestamp timestamp, int flags, Kind rw, Domain domain, Cardinality cardinality)
    {
        super(timestamp, flags(flags, rw, domain, cardinality));
    }

    public TxnId(TxnId copy)
    {
        super(copy.msb, copy.lsb, copy.node);
    }

    public TxnId(long epoch, long hlc, Kind rw, Domain domain, Id node)
    {
        this(epoch, hlc, rw, domain, Any, node);
    }

    public TxnId(long epoch, long hlc, Kind rw, Domain domain, Cardinality cardinality, Id node)
    {
        this(epoch, hlc, 0, rw, domain, cardinality, node);
    }

    public TxnId(long epoch, long hlc, int flags, Kind rw, Domain domain, Id node)
    {
        this(epoch, hlc, flags, rw, domain, Any, node);
    }

    public TxnId(long epoch, long hlc, int flags, Kind rw, Domain domain, Cardinality cardinality, Id node)
    {
        this(epoch, hlc, flags(flags, rw, domain, cardinality), node);
    }

    private TxnId(long epoch, long hlc, int flags, Id node)
    {
        super(epoch, hlc, flags, node);
    }

    private TxnId(long msb, long lsb, Id node)
    {
        super(msb, lsb, node);
    }

    public final boolean isWrite()
    {
        return is(Write);
    }

    public final boolean isSomeRead()
    {
        return is(Read) || is(EphemeralRead);
    }

    public final Kind kind()
    {
        return kind(flagsUnmasked());
    }

    public final boolean is(Kinds kinds)
    {
        return kinds.testOrdinal(kindOrdinal(flagsUnmasked()));
    }

    public final boolean is(Kind kind)
    {
        return kindOrdinal(flagsUnmasked()) == kind.ordinal();
    }

    public final boolean is(FastPath fastPath)
    {
        return fastPath.is(flagsUnmasked());
    }

    public final boolean is(MediumPath mediumPath)
    {
        return mediumPath.is(flagsUnmasked());
    }

    public final boolean is(Cardinality cardinality)
    {
        return cardinality.is(flagsUnmasked());
    }

    public final TxnId withoutNonIdentityFlags()
    {
        return (flags() & ~IDENTITY_FLAGS) == 0 ? this : new TxnId(msb, lsb & IDENTITY_LSB, node);
    }

    public final int nonIdentityFlags()
    {
        return flags() >>> NON_IDENTITY_FLAGS_SHIFT;
    }

    public final boolean hasPrivilegedCoordinator()
    {
        return FastPath.hasPrivilegedCoordinator(flagsUnmasked());
    }

    public final boolean hasMediumPath()
    {
        return !MediumPath.is(flagsUnmasked(), NoMediumPath);
    }

    public final boolean isVisible()
    {
        return Kind.isVisible(kindOrdinal(flagsUnmasked()));
    }

    public final boolean isSyncPoint()
    {
        return Kind.isSyncPoint(kindOrdinal(flagsUnmasked()));
    }

    public final boolean isSystemTxn()
    {
        return Kind.isSystemTxn(kindOrdinal(flagsUnmasked()));
    }

    public final boolean awaitsOnlyDeps()
    {
        return Kind.awaitsOnlyDeps(kindOrdinal(flagsUnmasked()));
    }

    public final boolean awaitsPreviouslyOwned()
    {
        return Kind.awaitsPreviouslyOwned(kindOrdinal(flagsUnmasked()));
    }

    public final boolean is(Domain domain)
    {
        return domainOrdinal(flagsUnmasked()) == domain.ordinal();
    }

    public final Kinds witnesses()
    {
        return kind().witnessesKinds();
    }

    public final boolean witnesses(TxnId txnId)
    {
        return Kind.witnesses(kindOrdinal(flagsUnmasked()), kindOrdinal(txnId.flagsUnmasked()));
    }

    public final boolean witnessedBy(TxnId txnId)
    {
        return txnId.witnesses(this);
    }

    public final Kinds witnessedBy()
    {
        return kind().witnessedByKinds();
    }

    public final Domain domain()
    {
        return domain(flagsUnmasked());
    }

    public TxnId as(Kind kind)
    {
        return as(kind, domain());
    }

    public TxnId as(Domain domain)
    {
        return as(kind(), domain);
    }

    public TxnId as(Kind kind, Domain domain)
    {
        return new TxnId(epoch(), hlc(), flagsWithoutDomainOrKindOrCardinality(), kind, domain, cardinality(), node);
    }

    private int flagsWithoutDomainOrKindOrCardinality()
    {
        return flags() & ~DOMAIN_AND_KIND_AND_CARDINALITY_MASK;
    }

    public TxnId addFlags(int flags)
    {
        Invariants.requireArgument(flags <= MAX_FLAGS);
        return addFlags(this, flags, TxnId::new);
    }

    public TxnId addFlag(Flag flag)
    {
        return addFlags(flag.bit);
    }

    public TxnId propagateUnstable(@Nullable TxnId ifNullOrUnstable)
    {
        return ifNullOrUnstable == null || ifNullOrUnstable.is(UNSTABLE) ? addFlag(UNSTABLE) : this;
    }

    public final TxnId addFlags(TxnId merge)
    {
        return addFlags(this, merge, TxnId::new);
    }

    public TxnId withEpoch(long epoch)
    {
        return epoch == epoch() ? this : new TxnId(epoch, hlc(), flags(), node);
    }

    @Override
    public TxnId merge(Timestamp that)
    {
        return merge(this, that, TxnId::fromBits);
    }

    @Override
    public String toString()
    {
        return "[" + epoch() + ',' + hlc() + ',' + flags() + '(' + domain().shortName() + kind().shortName() + ')' + ',' + node + ']';
    }

    private static int flags(int flags, Kind rw, Domain domain, Cardinality cardinality)
    {
        Invariants.require(cardinality == Any || domain == Domain.Key);
        int paramFlags = flags(rw) | flags(domain) | cardinality.bit();
        Invariants.require((flags & paramFlags) == 0);
        return paramFlags | flags;
    }

    private static int flags(Kind rw)
    {
        return rw.ordinal() << 1;
    }

    private static int flags(Domain domain)
    {
        return domain.ordinal();
    }

    private static Kind kind(int flags)
    {
        return Kind.forOrdinal(kindOrdinal(flags));
    }

    private static Domain domain(int flags)
    {
        return Domain.forOrdinal(domainOrdinal(flags));
    }

    public static int kindOrdinal(int flags)
    {
        return (flags >>> 1) & 7;
    }

    public final int kindOrdinal()
    {
        return kindOrdinal(flagsUnmasked());
    }

    public FastPath fastPath()
    {
        return FastPath.forFlags(flagsUnmasked());
    }

    public MediumPath mediumPath()
    {
        return MediumPath.forFlags(flagsUnmasked());
    }

    public Cardinality cardinality()
    {
        return Cardinality.forFlags(flagsUnmasked());
    }

    public boolean hasOnlyIdentityFlags()
    {
        return (flags() & ~IDENTITY_FLAGS) == 0;
    }

    public static int domainOrdinal(int flags)
    {
        return flags & 1;
    }

    public static TxnId maxForEpoch(long epoch)
    {
        return new TxnId(epochMsb(epoch) | 0x7fff, Long.MAX_VALUE, Id.MAX);
    }

    public static TxnId minForEpoch(long epoch)
    {
        return new TxnId(epochMsb(epoch), 0, Id.NONE);
    }

    public static TxnId noneIfNull(TxnId id)
    {
        return id == null ? NONE : id;
    }

    public static TxnId maxIfNull(TxnId id)
    {
        return id == null ? MAX : id;
    }

    private static final Pattern PARSE = Pattern.compile("\\[(?<epoch>[0-9]+),(?<hlc>[0-9]+),(?<flags>[0-9]+)\\([KR][REWXV]\\),(?<node>[0-9]+)]");
    public static TxnId parse(String txnIdString)
    {
        Matcher m = PARSE.matcher(txnIdString);
        if (!m.matches())
            throw illegalArgument("Invalid TxnId string: " + txnIdString);
        return fromValues(Long.parseLong(m.group("epoch")), Long.parseLong(m.group("hlc")), Integer.parseInt(m.group("flags")), new Id(Integer.parseInt(m.group("node"))));
    }

    public static TxnId tryParse(String txnIdString)
    {
        Matcher m = PARSE.matcher(txnIdString);
        if (!m.matches())
            return null;
        return fromValues(Long.parseLong(m.group("epoch")), Long.parseLong(m.group("hlc")), Integer.parseInt(m.group("flags")), new Id(Integer.parseInt(m.group("node"))));
    }

    public static boolean equalsStrict(TxnId[] a, TxnId[] b)
    {
        if (a.length != b.length)
            return false;

        for (int i = 0 ; i < a.length ; ++i)
        {
            if (!a[i].equalsStrict(b[i]))
                return false;
        }
        return true;
    }
}
