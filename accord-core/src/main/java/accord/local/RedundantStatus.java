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

import static accord.local.RedundantStatus.Cmp.LT;
import static accord.local.RedundantStatus.Cmp.LE;
import static accord.local.RedundantStatus.Coverage.ALL;
import static accord.local.RedundantStatus.Coverage.SOME;
import static accord.local.RedundantStatus.Property.GC_BEFORE;
import static accord.local.RedundantStatus.Property.LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_COMMAND_STORE;
import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_DATA_STORE;
import static accord.local.RedundantStatus.Property.LOCALLY_SYNCED;
import static accord.local.RedundantStatus.Property.LOCALLY_WITNESSED;
import static accord.local.RedundantStatus.Property.LOG_INCOMPLETE;
import static accord.local.RedundantStatus.Property.LOG_UNAVAILABLE;
import static accord.local.RedundantStatus.Property.QUORUM_APPLIED;
import static accord.local.RedundantStatus.Property.NOT_OWNED;
import static accord.local.RedundantStatus.Property.UNREADY;
import static accord.local.RedundantStatus.Property.REVERSE_PROPERTIES;
import static accord.local.RedundantStatus.Property.SHARD_APPLIED;
import static accord.local.RedundantStatus.Property.WAS_OWNED;

// TODO (testing): validate that we never lose a status previously held,
//  i.e. once any particular property holds for a TxnId, it should continue to hold in perpetuity
public class RedundantStatus
{
    public enum Coverage
    {
        NONE(0L),
        SOME(1L),
        ALL(0x100000001L);

        final long mask;

        Coverage(long mask)
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

    // note: while many of these properties are implied by other ones, the reason we track them as separate bits
    //  is so that the implied properties are correctly tracked across shards when we merge.
    public enum Property
    {
        // applied or pre-bootstrap or stale or was owned
        // (DEFUNCT | APPLIED)
        LOCALLY_REDUNDANT                  (false,  true,  LT),

        // pre-bootstrap or stale or was owned
        // (WAS_OWNED | UNREADY)
        LOCALLY_DEFUNCT                    (false,  true,  LT, LOCALLY_REDUNDANT),

        /**
         * We can bootstrap ranges at different times, and have a transaction that participates in both ranges -
         * in this case one of the portions of the transaction may be totally unordered with respect to other transactions
         * in that range because both occur prior to the readyAt point, so their (local) dependencies are entirely erased.
         * We can also re-bootstrap the same range because bootstrap failed, and leave dangling transactions to execute
         * which then execute in an unordered fashion.
         *
         * See also {@link SafeCommandStore#safeToReadAt()}.
         * Being UNREADY on one epoch doesn't override another epoch where we have lost ownership
         * (a sync point may still be waiting for it to apply)
         */
        UNREADY                            (false, true, LT, LOCALLY_DEFUNCT),

        SHARD_APPLIED_HLC_BOUND            (false, true, LE),

        /**
         * A point before which we do not know OUR OWN log
         */
        LOG_UNAVAILABLE                    (true, false, LT, UNREADY),

        // have applied a VisibilitySyncPoint locally, syncing deps for active transactions
        LOCALLY_WITNESSED                  (false,  true,  LE),

        // we've applied a sync point locally covering the transaction, but the transaction itself may not have applied
        LOCALLY_SYNCED                     (false,  true,  LE, LOCALLY_REDUNDANT),
        LOCALLY_APPLIED                    ( true, false,  LE, LOCALLY_SYNCED),

        /**
         * We have applied the preceding transactions durably to the store, so that we can safely truncate the Write
         * information as we will not need to replay it to the store
         */
        LOCALLY_DURABLE_TO_DATA_STORE      (false, true,  LT, LOCALLY_APPLIED),

        /**
         * We have applied the preceding transactions durably to all summary structures, so that on restart we do
         * not need to replay the transaction to restore any internal state.
         */
        LOCALLY_DURABLE_TO_COMMAND_STORE   (false, false,  LT, LOCALLY_APPLIED),

        /**
         * We have fully executed until across all a majority of replicas for the range in question,
         * but not necessarily ourselves.
         */
        QUORUM_APPLIED                     (false,  true,  LE),

        /**
         * We have fully executed until across all healthy non-bootstrapping replicas for the range in question,
         * but not necessarily ourselves.
         */
        SHARD_APPLIED                      (false,  true,  LE, QUORUM_APPLIED),

        // TODO (expected): this is effectively defunct (== GC_BEFORE). Presumably intended to apply prior to LOCALLY_DURABLE_..
        //  to permit TRUNCATE_WITH_OUTCOME to be applied by compaction more eagerly
        TRUNCATE_BEFORE                    (false,  true,  LT, SHARD_APPLIED, LOCALLY_SYNCED),

        // TODO (desired): separate GC_BEFORE with HLC_BOUND and without
        GC_BEFORE                          (false,  true,  LT, TRUNCATE_BEFORE, SHARD_APPLIED_HLC_BOUND),

        /**
         * A point before which OUR OWN log is perhaps incomplete - we may answer definitive queries (i.e. >= STABLE)
         * but an absent or undecided record will throw a log fault
         */
        LOG_INCOMPLETE                     ( true, false,  LT, UNREADY),

        // not persisted
        WAS_OWNED                          (false,  false, LT, LOCALLY_DEFUNCT),
        NOT_OWNED                          (false,  false, LT),
        ;

        static final Property[] PROPERTIES = values();
        static final Property[] REVERSE_PROPERTIES = values();
        static
        {
            // we now have room for 32 property ordinals, but the integration serializer assumes up to 16;
            // for now we verify that no more than 16 unique PERSISTENT properties exist. WAS_OWNED and NOT_OWNED are derived.
            Invariants.require(WAS_OWNED.ordinal() <= 16);
            for (int i = 0 ; i < REVERSE_PROPERTIES.length / 2 ; ++i)
            {
                REVERSE_PROPERTIES[i] = REVERSE_PROPERTIES[REVERSE_PROPERTIES.length - (1 + i)];
                REVERSE_PROPERTIES[REVERSE_PROPERTIES.length - (1 + i)] = PROPERTIES[i];
            }
        }

        final boolean overrideWasOwned;
        final boolean mergeWithUnready;
        final Cmp cmp;
        final int compareLessEqual;
        final Property[] implies;

        Property(boolean overrideWasOwned, boolean mergeWithUnready, Cmp cmp, Property ... implies)
        {
            this.overrideWasOwned = overrideWasOwned;
            this.mergeWithUnready = mergeWithUnready;
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
        public static final SomeStatus NONE = new SomeStatus(0);

        public static final SomeStatus UNREADY_ONLY = oneSlow(UNREADY);
        public static final SomeStatus LOCALLY_WITNESSED_ONLY = oneSlow(LOCALLY_WITNESSED);
        public static final SomeStatus LOCALLY_APPLIED_ONLY = oneSlow(LOCALLY_APPLIED);
        public static final SomeStatus QUORUM_APPLIED_ONLY = oneSlow(QUORUM_APPLIED);
        public static final SomeStatus SHARD_APPLIED_ONLY = oneSlow(SHARD_APPLIED);
        public static final SomeStatus LOG_UNAVAILABLE_ONLY = oneSlow(LOG_UNAVAILABLE);
        public static final SomeStatus LOG_INCOMPLETE_ONLY = oneSlow(LOG_INCOMPLETE);
        public static final SomeStatus LOCALLY_DURABLE_TO_DATA_STORE_ONLY = oneSlow(LOCALLY_DURABLE_TO_DATA_STORE);
        public static final SomeStatus LOCALLY_DURABLE_TO_COMMAND_STORE_ONLY = oneSlow(LOCALLY_DURABLE_TO_COMMAND_STORE);
        public static final SomeStatus GC_BEFORE_AND_LOCALLY_DURABLE = multi(GC_BEFORE, LOCALLY_DURABLE_TO_DATA_STORE, LOCALLY_DURABLE_TO_COMMAND_STORE);

        final int encoded;
        public SomeStatus(int encoded)
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
    public static final RedundantStatus NOT_OWNED_ONLY = toAll(oneSlow(NOT_OWNED));

    public static final RedundantStatus WAS_OWNED_ONLY = toAll(oneSlow(WAS_OWNED));
    public static final RedundantStatus WAS_OWNED_SYNCED = toAll(multi(WAS_OWNED, LOCALLY_SYNCED));
    public static final RedundantStatus WAS_OWNED_RETIRED = toAll(multi(WAS_OWNED, GC_BEFORE));

    final long encoded;
    RedundantStatus(long encoded)
    {
        this.encoded = encoded;
    }

    public RedundantStatus mergeShards(RedundantStatus that)
    {
        long merged = mergeShards(this.encoded, that.encoded);
        return selectOrCreate(merged, this, that);
    }

    public RedundantStatus add(RedundantStatus that)
    {
        long bits = this.encoded | that.encoded;
        return selectOrCreate(bits, this, that);
    }

    public RedundantStatus subtract(RedundantStatus that)
    {
        long bits = this.encoded & ~that.encoded;
        return selectOrCreate(bits, this, that);
    }

    static RedundantStatus addHistory(RedundantStatus add, RedundantStatus history)
    {
        long addEncoded = add.encoded;
        if (history.any(UNREADY))
            addEncoded &= UNREADY_MERGE_MASK;
        long encoded = addEncoded | history.encoded;
        return selectOrCreate(encoded, history, add);
    }

    static long addHistory(long add, long history)
    {
        if (!none(history, UNREADY))
            add &= UNREADY_MERGE_MASK;
        return add | history;
    }

    static RedundantStatus selectOrCreate(long encoded, RedundantStatus a, RedundantStatus b)
    {
        if (encoded == a.encoded) return a;
        if (encoded == b.encoded) return b;
        return new RedundantStatus(encoded);
    }

    static SomeStatus selectOrCreate(int encoded, SomeStatus a, SomeStatus b)
    {
        if (encoded == a.encoded) return a;
        if (encoded == b.encoded) return b;
        return new SomeStatus(encoded);
    }

    public boolean is(Property property, Coverage coverage)
    {
        return is(encoded, property, coverage);
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
        return all(encoded & 0xFFFFFFFFL, property);
    }

    public static boolean all(long encoded, Property property)
    {
        return is(encoded, property, ALL);
    }

    public static boolean all(int encoded, Property a, Property b)
    {
        throw new IllegalArgumentException("Impossible to match ALL on int encoded");
    }

    public static boolean all(long encoded, Property a, Property b)
    {
        return all(encoded, a) && all(encoded, b);
    }

    public boolean none(Property property)
    {
        return none(encoded, property);
    }

    public static boolean none(int encoded, Property property)
    {
        return none(encoded & 0xFFFFFFFFL, property);
    }

    public static boolean none(long encoded, Property property)
    {
        return !is(encoded, property, SOME);
    }

    public boolean any(Property property)
    {
        return any(encoded, property);
    }

    public static boolean any(int encoded, Property property)
    {
        return any(encoded & 0xFFFFFFFFL, property);
    }

    public static boolean any(long encoded, Property property)
    {
        return is(encoded, property, SOME);
    }

    // propertyMask in this case must be SOME, not ALL
    public static boolean matchesMask(int encoded, long propertyMask)
    {
        Invariants.require((propertyMask & ~0xFFFFFFFL) == 0);
        return matchesMask(encoded & 0xFFFFFFFFL, propertyMask);
    }

    public static boolean matchesMask(long encoded, long propertyMask)
    {
        return (encoded & propertyMask) == propertyMask;
    }

    public static long shiftedMask(Property property, Coverage coverage)
    {
        long mask = coverage.mask;
        return mask << property.shift();
    }

    public static boolean is(long encoded, Property property, Coverage coverage)
    {
        return matchesMask(encoded, shiftedMask(property, coverage));
    }

    private static final long ALL_BITS = 0xFFFFFFFF00000000L;
    private static final long ANY_BITS = 0x00000000FFFFFFFFL;
    private static final long WAS_OWNED_OVERRIDE_MASK;
    static final long UNREADY_MERGE_MASK;
    static final long ONLY_LE_MASK;
    private static final long WAS_ALL_OWNED_MASK = ALL.mask << WAS_OWNED.shift();
    private static final long NOT_OWNED_MASK = ALL.mask << NOT_OWNED.shift();
    static
    {
        long wasOwnedMask = 0;
        long unreadyMask = 0, leMask = 0;
        for (Property property : Property.values())
        {
            if (property.overrideWasOwned)
                wasOwnedMask |= encodeAll(property);
            if (property.mergeWithUnready)
                unreadyMask |= encodeAny(property);
            if (property.cmp == LE)
                leMask |= encodeAny(property);
        }
        WAS_OWNED_OVERRIDE_MASK = wasOwnedMask;
        UNREADY_MERGE_MASK = unreadyMask;
        ONLY_LE_MASK = leMask;
    }

    public static long mergeShards(long a, long b)
    {
        long either = a | b;
        Invariants.require((either & NOT_OWNED_MASK) == 0);
        long all = (a & b) & ALL_BITS;
        long any = either & ANY_BITS;
        long result = all | any;
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
        int encoded = 0;
        for (Property property : properties)
            encoded |= transitiveClosure(property);
        return new SomeStatus(encoded);
    }

    private static int transitiveClosure(Property property)
    {
        int encoded = encodeAny(property);
        for (Property implied : property.implies)
            encoded |= transitiveClosure(implied);
        return encoded;
    }

    static int encodeAny(Property property)
    {
        return 1 << property.shift();
    }

    static long encodeAll(Property property)
    {
        return ALL.mask << property.shift();
    }

    static long toAll(long encodedKeyStatus)
    {
        Invariants.require(0 == (encodedKeyStatus & ~0xFFFFFFFFL));
        return encodedKeyStatus | (encodedKeyStatus<<32);
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
                else if (is(property, coverage))
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
