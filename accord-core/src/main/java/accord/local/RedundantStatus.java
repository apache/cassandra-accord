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

package accord.local;

import accord.utils.Invariants;
import accord.utils.TinyEnumSet;
import accord.utils.UnhandledEnum;

import static accord.local.RedundantStatus.Cmp.LT;
import static accord.local.RedundantStatus.Cmp.LE;
import static accord.local.RedundantStatus.Coverage.ALL;
import static accord.local.RedundantStatus.Coverage.SOME;
import static accord.local.RedundantStatus.Property.*;
import static accord.local.RedundantStatus.Property.GC_BEFORE;
import static accord.local.RedundantStatus.Property.LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.LOCALLY_SYNCED;
import static accord.local.RedundantStatus.Property.LOCALLY_WITNESSED;
import static accord.local.RedundantStatus.Property.MAJORITY_APPLIED;
import static accord.local.RedundantStatus.Property.NOT_OWNED;
import static accord.local.RedundantStatus.Property.PRE_BOOTSTRAP;
import static accord.local.RedundantStatus.Property.PRE_BOOTSTRAP_OR_STALE;
import static accord.local.RedundantStatus.Property.REVERSE_PROPERTIES;
import static accord.local.RedundantStatus.Property.SHARD_APPLIED;
import static accord.local.RedundantStatus.Property.WAS_OWNED;

// TODO (testing): validate that we never lose a status previously held,
//  i.e. once any particular property holds for a TxnId, it should continue to hold in perpetuity
public class RedundantStatus
{
    public enum Coverage
    {
        NONE(0),
        SOME(1),
        ALL(0x10001);

        final int mask;

        Coverage(int mask)
        {
            this.mask = mask;
        }

        public boolean atLeast(Coverage coverage)
        {
            return compareTo(coverage) >= 0;
        }
    }

    enum Cmp
    {
        LE, LT
    }

    public enum Property
    {
        // applied or pre-bootstrap or stale or was owned
        // (DEFUNCT | APPLIED)
        LOCALLY_REDUNDANT                  (false,  true,  LT),

        // pre-bootstrap or stale or was owned
        // (WAS_OWNED | PRE_BOOTSTRAP_OR_STALE)
        LOCALLY_DEFUNCT                    (false,  true,  LT, LOCALLY_REDUNDANT),

        /**
         * We can bootstrap ranges at different times, and have a transaction that participates in both ranges -
         * in this case one of the portions of the transaction may be totally unordered with respect to other transactions
         * in that range because both occur prior to the bootstrappedAt point, so their (local) dependencies are entirely erased.
         * We can also re-bootstrap the same range because bootstrap failed, and leave dangling transactions to execute
         * which then execute in an unordered fashion.
         *
         * See also {@link SafeCommandStore#safeToReadAt()}.
         * TODO (expected): do we need to distinguish this case from DEFUNCT?
         */
        PRE_BOOTSTRAP_OR_STALE             (true, true,  LT, LOCALLY_DEFUNCT),
        PRE_BOOTSTRAP                      (true, true,  LT, PRE_BOOTSTRAP_OR_STALE),

        LOCALLY_WITNESSED                  (false,  true,  LE),
        // we've applied a sync point locally covering the transaction, but the transaction itself may not have applied
        LOCALLY_SYNCED                     (false,  true,  LE, LOCALLY_REDUNDANT),
        LOCALLY_APPLIED                    (true, false, LE, LOCALLY_SYNCED),

        /**
         * We have fully executed until across all a majority of replicas for the range in question,
         * but not necessarily ourselves.
         */
        MAJORITY_APPLIED                   (false,  true,  LE),

        /**
         * We have fully executed until across all healthy non-bootstrapping replicas for the range in question,
         * but not necessarily ourselves.
         */
        SHARD_APPLIED                      (false, true, LE, MAJORITY_APPLIED),

        TRUNCATE_BEFORE                    (false,  true,  LT, SHARD_APPLIED, LOCALLY_SYNCED),
        GC_BEFORE                          (false,  true,  LT, TRUNCATE_BEFORE),

        // not persisted
        WAS_OWNED                          (false,  false, LT, LOCALLY_DEFUNCT),
        NOT_OWNED                          (false,  false, LT),

        /**
         * Ranges can be marked as LOCALLY_LOST if the node has lost was forced by operator to give up knowledge
         * of transactional history for a specific range.
         */
        LOCALLY_LOST                       (false, false, LT, PRE_BOOTSTRAP_OR_STALE),
        ;


        static final Property[] PROPERTIES = values();
        static final Property[] REVERSE_PROPERTIES = values();
        static
        {
            // we have 32 integer bits to use, and we use 2 bits per property. If we exceed this number of properties we need to bump to long.
            Invariants.require(PROPERTIES[PROPERTIES.length - 1].ordinal() < 16);
            for (int i = 0 ; i < REVERSE_PROPERTIES.length / 2 ; ++i)
            {
                REVERSE_PROPERTIES[i] = REVERSE_PROPERTIES[REVERSE_PROPERTIES.length - (1 + i)];
                REVERSE_PROPERTIES[REVERSE_PROPERTIES.length - (1 + i)] = PROPERTIES[i];
            }
        }

        final boolean overrideWasOwned;
        final boolean mergeWithPreBootstrapOrStale;
        final Cmp cmp;
        final int compareLessEqual;
        final Property[] implies;

        Property(boolean overrideWasOwned, boolean mergeWithPreBootstrapOrStale, Cmp cmp, Property ... implies)
        {
            this.overrideWasOwned = overrideWasOwned;
            this.mergeWithPreBootstrapOrStale = mergeWithPreBootstrapOrStale;
            this.cmp = cmp;
            this.compareLessEqual = cmp == Cmp.LT ? -1 : 0;
            this.implies = implies;
        }

        final int shift()
        {
            return ordinal();
        }
    }

    public static class SomeStatus
    {
        public static final SomeStatus NONE = new SomeStatus((short)0);

        public static final SomeStatus PRE_BOOTSTRAP_ONLY = oneSlow(PRE_BOOTSTRAP);
        public static final SomeStatus LOCALLY_WITNESSED_ONLY = oneSlow(LOCALLY_WITNESSED);
        public static final SomeStatus LOCALLY_APPLIED_ONLY = oneSlow(LOCALLY_APPLIED);
        public static final SomeStatus MAJORITY_APPLIED_ONLY = oneSlow(MAJORITY_APPLIED);
        public static final SomeStatus SHARD_APPLIED_ONLY = oneSlow(SHARD_APPLIED);
        public static final SomeStatus GC_BEFORE_AND_LOCALLY_APPLIED = multi(GC_BEFORE, LOCALLY_APPLIED);
        public static final SomeStatus LOCALLY_INCOMPLETE_ONLY = oneSlow(LOCALLY_LOST);

        final short encoded;
        public SomeStatus(short encoded)
        {
            this.encoded = encoded;
        }

        public boolean is(Property property)
        {
            return 0 != (1 & (encoded >>> property.shift()));
        }
    }

    private static final Coverage[] COVERAGE_TO_STRING = new Coverage[] { ALL, SOME };
    public static final RedundantStatus NONE = new RedundantStatus(0);
    public static final RedundantStatus PRE_BOOTSTRAP_OR_STALE_ONLY = toAll(oneSlow(PRE_BOOTSTRAP_OR_STALE));
    public static final RedundantStatus NOT_OWNED_ONLY = toAll(oneSlow(NOT_OWNED));

    public static final RedundantStatus WAS_OWNED_ONLY = toAll(oneSlow(WAS_OWNED));
    public static final RedundantStatus WAS_OWNED_SYNCED = toAll(multi(WAS_OWNED, LOCALLY_SYNCED));
    public static final RedundantStatus WAS_OWNED_RETIRED = toAll(multi(WAS_OWNED, GC_BEFORE));

    final int encoded;
    RedundantStatus(int encoded)
    {
        this.encoded = encoded;
    }

    public RedundantStatus mergeShards(RedundantStatus that)
    {
        int merged = mergeShards(this.encoded, that.encoded);
        return selectOrCreate(merged, this, that);
    }

    public RedundantStatus add(RedundantStatus that)
    {
        int bits = this.encoded | that.encoded;
        return selectOrCreate(bits, this, that);
    }

    public RedundantStatus subtract(RedundantStatus that)
    {
        int bits = this.encoded & ~that.encoded;
        return selectOrCreate(bits, this, that);
    }

    static RedundantStatus addHistory(RedundantStatus add, RedundantStatus history)
    {
        int addEncoded = add.encoded;
        if (history.any(PRE_BOOTSTRAP_OR_STALE))
            addEncoded &= PRE_BOOTSTRAP_MERGE_MASK;
        int encoded = addEncoded | history.encoded;
        return selectOrCreate(encoded, history, add);
    }

    static short addHistory(short add, short history)
    {
        if (decode(history, PRE_BOOTSTRAP_OR_STALE) != Coverage.NONE)
            add &= PRE_BOOTSTRAP_MERGE_MASK;
        return (short) (add | history);
    }

    static RedundantStatus selectOrCreate(int encoded, RedundantStatus a, RedundantStatus b)
    {
        if (encoded == a.encoded) return a;
        if (encoded == b.encoded) return b;
        return new RedundantStatus(encoded);
    }

    static SomeStatus selectOrCreate(short encoded, SomeStatus a, SomeStatus b)
    {
        if (encoded == a.encoded) return a;
        if (encoded == b.encoded) return b;
        return new SomeStatus(encoded);
    }

    public short encodedPart(Coverage coverage)
    {
        switch (coverage)
        {
            default: throw new UnhandledEnum(coverage);
            case NONE: return (short)0;
            case SOME: return (short)(encoded & 0xff);
            case ALL:  return (short)((encoded >>> 16) & 0xff);
        }
    }

    public Coverage get(Property property)
    {
        return get(encoded, property);
    }

    public static Coverage get(int encoded, Property property)
    {
        return decode(encoded, property);
    }

    public boolean all(Property property)
    {
        return all(encoded, property);
    }

    public boolean all(Property a, Property b)
    {
        return all(encoded, a, b);
    }

    public static boolean all(int encoded, Property property)
    {
        return get(encoded, property) == ALL;
    }

    public static boolean all(int encoded, Property a, Property b)
    {
        return all(encoded, a) && all(encoded, b);
    }

    public boolean none(Property property)
    {
        return none(encoded, property);
    }

    public static boolean none(int encoded, Property property)
    {
        return get(encoded, property) == Coverage.NONE;
    }

    public boolean any(Property property)
    {
        return any(encoded, property);
    }

    public static boolean any(int encoded, Property property)
    {
        return get(encoded, property) != Coverage.NONE;
    }

    public static boolean matchesMask(int encoded, int propertyMask)
    {
        return (encoded & propertyMask) == propertyMask;
    }

    public static int mask(Property property, Coverage coverage)
    {
        int mask = coverage.mask;
        return mask << property.shift();
    }

    public static Coverage decode(long encoded, Property property)
    {
        int coverage = (int)((encoded >>> property.shift()) & ALL.mask);
        switch (coverage)
        {
            default: throw new IllegalStateException("Invalid Coverage value encoded for " + property + ": " + coverage);
            case 0: return Coverage.NONE;
            case 1: return SOME;
            case 0x10001: return ALL;
        }
    }

    private static final int ALL_BITS = 0xFFFF0000;
    private static final int ANY_BITS = 0x0000FFFF;
    private static final int WAS_OWNED_OVERRIDE_MASK;
    static final short PRE_BOOTSTRAP_MERGE_MASK;
    static final short ONLY_LE_MASK;
    private static final int WAS_ALL_OWNED_MASK = ALL.mask << WAS_OWNED.shift();
    private static final int NOT_OWNED_MASK = ALL.mask << NOT_OWNED.shift();
    static
    {
        int wasOwnedMask = 0;
        short preBootstrapMask = 0, leMask = 0;
        for (Property property : Property.values())
        {
            if (!property.overrideWasOwned)
                wasOwnedMask |= encodeAll(property);
            if (property.mergeWithPreBootstrapOrStale)
                preBootstrapMask |= encodeAny(property);
            if (property.cmp == LE)
                leMask |= encodeAny(property);
        }
        WAS_OWNED_OVERRIDE_MASK = wasOwnedMask;
        PRE_BOOTSTRAP_MERGE_MASK = preBootstrapMask;
        ONLY_LE_MASK = leMask;
    }

    public static int mergeShards(int a, int b)
    {
        int either = a | b;
        Invariants.require((either & NOT_OWNED_MASK) == 0);
        int all = (a & b) & ALL_BITS;
        int any = either & ANY_BITS;
        int result = all | any;
        if ((either & WAS_ALL_OWNED_MASK) == WAS_ALL_OWNED_MASK)
        {
            if ((a & WAS_ALL_OWNED_MASK) != WAS_ALL_OWNED_MASK)
                result |= a & WAS_OWNED_OVERRIDE_MASK;
            if ((b & WAS_ALL_OWNED_MASK) != WAS_ALL_OWNED_MASK)
                result |= b & WAS_OWNED_OVERRIDE_MASK;
        }
        return result;
    }

    public static SomeStatus oneSlow(Property property)
    {
        return new SomeStatus(transitiveClosure(property));
    }

    private static SomeStatus multi(Property ... properties)
    {
        short encoded = 0;
        for (Property property : properties)
            encoded |= transitiveClosure(property);
        return new SomeStatus(encoded);
    }

    private static short transitiveClosure(Property property)
    {
        short encoded = encodeAny(property);
        for (Property implied : property.implies)
            encoded |= transitiveClosure(implied);
        return encoded;
    }

    static short encodeAny(Property property)
    {
        return (short) (1 << property.shift());
    }

    static int encodeAll(Property property)
    {
        return ALL.mask << property.shift();
    }

    static int toAll(short encodedKeyStatus)
    {
        return encodedKeyStatus | (((int)encodedKeyStatus)<<16);
    }

    static RedundantStatus toAll(SomeStatus key)
    {
        return new RedundantStatus(toAll(key.encoded));
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("{");
        boolean firstCoverage = true;
        for (Coverage coverage : COVERAGE_TO_STRING)
        {
            if (!firstCoverage) builder.append(',');
            firstCoverage = false;
            builder.append(coverage);
            builder.append(":[");
            int implied = 0;
            boolean firstProperty = true;
            for (Property property : REVERSE_PROPERTIES)
            {
                if (TinyEnumSet.contains(implied, property))
                {
                    implied |= TinyEnumSet.encode(property.implies);
                }
                else if (get(property) == coverage)
                {
                    if (!firstProperty) builder.append(",");
                    firstProperty = false;
                    builder.append(property);
                }
            }
            builder.append("]");
        }
        builder.append("}");
        return builder.toString();
    }

    public boolean equals(Object that)
    {
        return that != null && that.getClass() == RedundantStatus.class && encoded == ((RedundantStatus) that).encoded;
    }
}
