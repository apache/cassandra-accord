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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import accord.api.RoutingKey;
import accord.coordinate.CollectLatestDeps;
import accord.coordinate.CoordinationAdapter;
import accord.local.Node;
import accord.api.ExclusiveAsyncExecutor;
import accord.primitives.Known.KnownDeps;
import accord.topology.SelectShards;
import accord.utils.Invariants;
import accord.utils.ReducingIntervalMap;
import accord.utils.ReducingRangeMap;
import accord.utils.TriFunction;
import accord.utils.UnhandledEnum;

import static accord.messages.Accept.Kind.SLOW;
import static accord.primitives.Known.KnownDeps.DepsCommitted;
import static accord.primitives.Known.KnownDeps.DepsErased;
import static accord.primitives.Known.KnownDeps.DepsFromCoordinator;
import static accord.primitives.Known.KnownDeps.DepsKnown;
import static accord.primitives.Known.KnownDeps.DepsProposed;
import static accord.primitives.Known.KnownDeps.DepsProposedFixed;
import static accord.primitives.Known.KnownDeps.DepsUnknown;

public class LatestDeps extends ReducingRangeMap<LatestDeps.LatestEntry>
{
    public static final LatestDeps EMPTY = new LatestDeps();

    public static class SerializerSupport
    {
        public static LatestDeps create(RoutingKey[] starts, LatestEntry[] values)
        {
            return new LatestDeps(starts, values);
        }
    }

    public static void withCommitted(CoordinationAdapter<?> adapter, Node node, ExclusiveAsyncExecutor executor, Merge merge, FullRoute<?> route, Ballot ballot, TxnId txnId, Timestamp executeAt, Txn txn, BiConsumer<?, Throwable> failureCallback, Consumer<Deps> withDeps)
    {
        if (!node.topology().active().hasAtLeastEpoch(executeAt.epoch()))
        {
            node.withEpochAtLeast(executeAt.epoch(), executor, failureCallback, () -> withCommitted(adapter, node, executor, merge, route, ballot, txnId, executeAt, txn, failureCallback, withDeps));
            return;
        }

        LatestDeps.MergedCommitResult merged = merge.mergeCommitOrStable(null, DepsCommitted);
        Route<?> missing = route.without(merged.sufficientFor);
        Deps committed = merged.deps;
        if (missing.isEmpty()) withDeps.accept(merged.deps);
        else
        {
            // we include the committed deps in our proposal so that if we contact a replica that participates in one of the committed shards we include any deps it should see
            Deps propose = merge.mergeProposal(missing).with(committed);
            adapter.proposeOnly(node, executor, missing, missing, route, SLOW, ballot, txnId, txn, executeAt, propose, (success, fail) -> {
                if (fail != null) failureCallback.accept(null, fail);
                else
                {
                    success = success.intersecting(missing).asFullUnsafe();
                    withDeps.accept(success.with(committed));
                }
            });
        }
    }

    public static void withStable(CoordinationAdapter<?> adapter, Node node, ExclusiveAsyncExecutor executor, Merge merge, Deps alreadyStableDeps, Route<?> require, @Nullable Route<?> sendTo, @Nullable SelectShards selectSendTo, FullRoute<?> route, Ballot ballot, TxnId txnId, Timestamp executeAt, Txn txn, BiConsumer<?, ? super Throwable> failureCallback, Consumer<Deps> withDeps)
    {
        Invariants.require(sendTo == null || selectSendTo != null);
        if (!node.topology().active().hasAtLeastEpoch(executeAt.epoch()))
        {
            node.withEpochAtLeast(executeAt.epoch(), executor, failureCallback, () -> withStable(adapter, node, executor, merge, alreadyStableDeps, require, sendTo, selectSendTo, route, ballot, txnId, executeAt, txn, failureCallback, withDeps));
            return;
        }

        LatestDeps.MergedCommitResult mergedStable = merge.mergeCommitOrStable(null, DepsKnown);
        Deps stable = mergedStable.deps.with(alreadyStableDeps);
        Route<?> stabilise = require.without(mergedStable.sufficientFor);
        if (stabilise.isEmpty()) withDeps.accept(stable);
        else
        {
            LatestDeps.MergedCommitResult mergedCommitted = merge.mergeCommitOrStable(stabilise, DepsCommitted);
            Route<?> propose = stabilise.without(mergedCommitted.sufficientFor);
            // we merge with stable to make sure we can send a full Commit to any replica that overlaps the stable and unstable ranges
            Deps committed = stable.with(mergedCommitted.deps);
            if (propose.isEmpty())
            {
                stabilise(adapter, node, executor, committed, stabilise, sendTo, selectSendTo, route, ballot, txnId, executeAt, txn, failureCallback, withDeps);
            }
            else
            {
                // we merge with committed to make sure we can send a full Commit to any replica that overlaps the stable and unstable ranges
                Deps notaccepted = committed.with(merge.mergeProposal(propose));
                adapter.proposeOnly(node, executor, propose, sendTo == null ? propose : sendTo, route, SLOW, ballot, txnId, txn, executeAt, notaccepted, (success, fail) -> {
                    if (fail != null) failureCallback.accept(null, fail);
                    else
                    {
                        // TODO (desired): do we need to slice to those we were proposing, or can we just stabilise whatever we get back?
                        success = success.intersecting(propose).asFullUnsafe();
                        stabilise(adapter, node, executor, success.with(committed), stabilise, sendTo, selectSendTo, route, ballot, txnId, executeAt, txn, failureCallback, withDeps);
                    }
                });
            }
        }
    }

    public static void stabilise(CoordinationAdapter<?> adapter, Node node, ExclusiveAsyncExecutor executor, Deps deps, Route<?> stabilise, @Nullable Route<?> sendTo, SelectShards selectSendTo, FullRoute<?> route, Ballot ballot, TxnId txnId, Timestamp executeAt, Txn txn, BiConsumer<?, ? super Throwable> failureCallback, Consumer<Deps> withDeps)
    {
        Invariants.require(sendTo == null || selectSendTo != null);
        adapter.stabiliseOnly(node, executor, stabilise, sendTo == null ? stabilise : sendTo, route, ballot, txnId, txn, executeAt, deps, (success, fail) -> {
            if (fail != null) failureCallback.accept(null, fail);
            else withDeps.accept(deps);
        });
    }

    public static void withStable(CoordinationAdapter<?> adapter, Node node, ExclusiveAsyncExecutor executor, TxnId txnId, Timestamp executeAt, Txn txn, Deps alreadyStableDeps, Route<?> require, @Nullable Route<?> sendTo, SelectShards selectSendTo, FullRoute<?> route, BiConsumer<?, ? super Throwable> failureCallback, Consumer<Deps> withDeps)
    {
        Invariants.require(sendTo == null || selectSendTo != null);
        if (!node.topology().active().hasAtLeastEpoch(executeAt.epoch()))
        {
            node.withEpochAtLeast(executeAt.epoch(), executor, failureCallback, () -> withStable(adapter, node, executor, txnId, executeAt, txn, alreadyStableDeps, require, sendTo, selectSendTo, route, failureCallback, withDeps));
        }
        else if (require.isEmpty())
        {
            withDeps.accept(alreadyStableDeps);
        }
        else
        {
            Ballot ballot = node.uniqueTimestamp(Ballot::fromValues);
            CollectLatestDeps.withLatestDeps(node, txnId, route, require, ballot, executeAt, (extraDeps, fail) -> {
                if (fail != null)
                {
                    failureCallback.accept(null, fail);
                }
                else
                {
                    Merge merge = merge(extraDeps, i -> i);
                    LatestDeps.withStable(adapter, node, executor, merge, alreadyStableDeps, require, sendTo, selectSendTo, route, ballot, txnId, executeAt, txn, failureCallback, withDeps);
                }
            });
        }
    }

    public static class MergedCommitResult
    {
        public final Deps deps;
        public final Ranges sufficientFor;

        public MergedCommitResult(Deps deps, Ranges sufficientFor)
        {
            this.deps = deps;
            this.sufficientFor = sufficientFor;
        }
    }

    public static class AbstractEntry
    {
        public final KnownDeps known;
        public final Ballot ballot;
        public final @Nullable Deps coordinatedDeps;

        private AbstractEntry(KnownDeps known, Ballot ballot, @Nullable Deps coordinatedDeps)
        {
            this.known = known;
            this.ballot = ballot;
            this.coordinatedDeps = coordinatedDeps;
        }

        // merge's first parameter should be the one with higher phase or ballot (as appropriate);
        // the second parameter will be used only for merging any localDeps
        static <T extends AbstractEntry> T reduce(T a, T b, BiFunction<T, T, T> merge)
        {
            if (a.known == DepsErased || b.known == DepsErased)
                return a.known.compareTo(b.known) >= 0 ? a : b;

            int c = a.known.phase.compareTo(b.known.phase);
            if (c == 0 && a.known.phase.tieBreakWithBallot) c = a.ballot.compareTo(b.ballot);
            if (c < 0)
            {
                T tmp = a;
                a = b;
                b = tmp;
            }

            if (a.known.compareTo(DepsProposed) <= 0)
                return merge.apply(a, b);
            // note that it is *not* necessarily guaranteed that stable deps will be the same - only that they will imply the same relations once filtered
            // so it is not safe to test equality of deps when merging two stable deps, as one might include additional TxnId that will not impact execution
            return a;
        }
    }

    public static class LatestEntry extends AbstractEntry
    {
        // set only if DepsUnknown or DepsProposed
        public final @Nullable Deps localDeps;

        public LatestEntry(KnownDeps known, Ballot ballot, Deps coordinatedDeps, Deps localDeps)
        {
            super(known, ballot, coordinatedDeps);
            this.localDeps = localDeps;
        }

        static LatestEntry reduce(LatestEntry a, LatestEntry b)
        {
            return reduce(a, b, (v1, v2) -> new LatestEntry(v1.known, v1.ballot, v1.coordinatedDeps,
                                                            v1.localDeps == null ? v2.localDeps
                                                                                 : v2.localDeps == null ? v1.localDeps
                                                                                                        : v1.localDeps.with(v2.localDeps)));
        }

        static LatestEntry slice(RoutingKey start, RoutingKey end, LatestEntry v)
        {
            Deps coordinatedDeps = slice(start, end, v.coordinatedDeps);
            Deps localDeps = slice(start, end, v.localDeps);

            if (coordinatedDeps == v.coordinatedDeps && localDeps == v.localDeps)
                return v;

            return new LatestEntry(v.known, v.ballot, coordinatedDeps, localDeps);
        }

        private static Deps slice(RoutingKey start, RoutingKey end, @Nullable Deps deps)
        {
            if (deps == null) return null;
            KeyDeps keyDeps = deps.keyDeps;
            RangeDeps rangeDeps = deps.rangeDeps;
            RoutingKeys keys = deps.keyDeps.keys;

            boolean slice = keys.indexOf(start) != -1 || keys.indexOf(end) != -1 - keys.size();
            if (!slice) slice = rangeDeps.indexOfStart(start) != -1 || rangeDeps.indexOfStart(end) != -1 - rangeDeps.rangeCount();
            if (!slice) return deps;

            Ranges ranges = Ranges.of(start.rangeFactory().newRange(start, end));
            return new Deps(keyDeps.slice(ranges), rangeDeps.slice(ranges));
        }

        public String toString()
        {
            return known + "," + ballot
                   + (localDeps == null ? "" : ",local:" + localDeps.keyDeps.toBriefString() + "/" + localDeps.rangeDeps.toBriefString())
                   + (coordinatedDeps == null ? "" : ",coordinated:" + coordinatedDeps.keyDeps.toBriefString() + "/" + coordinatedDeps.rangeDeps.toBriefString());
        }

        @Override
        public boolean equals(Object obj)
        {
            if (!(obj instanceof LatestEntry))
                return false;

            LatestEntry that = (LatestEntry) obj;
            return this.known == that.known
                   && Objects.equals(this.ballot, that.ballot)
                   && Objects.equals(this.localDeps, that.localDeps)
                   && Objects.equals(this.coordinatedDeps, that.coordinatedDeps);
        }
    }

    private LatestDeps()
    {
        super(RoutingKeys.EMPTY_KEYS_ARRAY, new LatestEntry[0]);
    }

    private LatestDeps(RoutingKey[] starts, LatestEntry[] values)
    {
        super(starts, values);
    }

    public Deps merge()
    {
        return Deps.merge(values, values.length, (array, i) -> array[i], d -> d.coordinatedDeps);
    }

    public static LatestDeps merge(LatestDeps a, LatestDeps b)
    {
        return ReducingIntervalMap.mergeIntervals(a, b, Builder::new);
    }

    public static LatestDeps create(Participants<?> participants, KnownDeps knownDeps, Ballot ballot, Deps coordinatedDeps, Deps localDeps)
    {
        if (participants.isEmpty())
            return new LatestDeps();

        Builder builder = new Builder(participants.size() * 2);
        for (int i = 0 ; i < participants.size() ; ++i)
        {
            Range cur = participants.get(i).asRange();
            Ranges slice = Ranges.of(cur);
            builder.append(cur.start(), cur.end(), new LatestEntry(knownDeps, ballot, slice(slice, coordinatedDeps), slice(slice, localDeps)));
        }
        return builder.build();
    }

    private static Deps slice(Ranges ranges, @Nullable Deps deps)
    {
        if (deps == null)
            return null;

        return new Deps(deps.keyDeps.slice(ranges), deps.rangeDeps.slice(ranges));
    }

    static class Builder extends AbstractIntervalBuilder<RoutingKey, LatestEntry, LatestDeps>
    {
        protected Builder(int capacity)
        {
            super(capacity);
        }

        @Override
        protected LatestEntry slice(RoutingKey start, RoutingKey end, LatestEntry v)
        {
            return LatestEntry.slice(start, end, v);
        }

        @Override
        protected LatestEntry reduce(LatestEntry a, LatestEntry b)
        {
            return LatestEntry.reduce(a, b);
        }

        @Override
        protected LatestDeps buildInternal()
        {
            return new LatestDeps(starts.toArray(new RoutingKey[0]), values.toArray(new LatestEntry[0]));
        }
    }

    public static <T> Deps mergeProposal(List<T> list, Function<T, LatestDeps> getter)
    {
        // merge merge merge
        Merge merge = merge(list, getter);
        return merge.mergeProposal();
    }

    public static <T> Merge merge(List<T> list, Function<T, LatestDeps> getter)
    {
        Merge merge = Merge.EMPTY;
        for (int i = 0, size = list.size() ; i < size ; ++i)
        {
            LatestDeps cur = getter.apply(list.get(i));
            if (cur == null) continue;
            merge = Merge.merge(merge, new Merge(cur));
        }
        // merge merge merge
        return merge;
    }

    // build a merge-intention without actually merging any deps, to save time merging ones we discover we don't need to
    public static class Merge extends ReducingRangeMap<Merge.MergeEntry>
    {
        private static final Merge EMPTY = new Merge();

        static class MergeEntry extends AbstractEntry
        {
            final List<Deps> merge;

            MergeEntry(KnownDeps known, Ballot ballot, Deps coordinatedDeps, List<Deps> merge)
            {
                super(known, ballot, coordinatedDeps);
                this.merge = merge;
            }

            MergeEntry(LatestEntry convert)
            {
                super(convert.known, convert.ballot, convert.coordinatedDeps);
                this.merge = convert.localDeps == null ? ImmutableList.of() : ImmutableList.of(convert.localDeps);
            }

            static MergeEntry reduce(MergeEntry a, MergeEntry b)
            {
                return reduce(a, b, (v1, v2) -> new MergeEntry(v1.known, v1.ballot, v1.coordinatedDeps,
                                                               ImmutableList.<Deps>builder().addAll(v1.merge).addAll(v2.merge).build()));
            }
        }

        private Merge()
        {
            super();
        }

        private Merge(LatestDeps convert)
        {
            super(convert.starts, convert(convert.values));
        }

        private Merge(RoutingKey[] starts, MergeEntry[] values)
        {
            super(starts, values);
        }

        static Merge merge(Merge a, Merge b)
        {
            return mergeIntervals(a, b, MergeBuilder::new);
        }

        private static MergeEntry[] convert(LatestEntry[] convert)
        {
            MergeEntry[] result = new MergeEntry[convert.length];
            for (int i = 0 ; i < convert.length ; ++i)
            {
                if (convert[i] == null)
                    continue;

                result[i] = new MergeEntry(convert[i]);
            }
            return result;
        }

        public Participants<?> notAccepted(Participants<?> participants)
        {
            for (int i = 0 ; i < values.length ; ++i)
            {
                if (values[i] != null && values[i].known != DepsUnknown && values[i].known != DepsFromCoordinator)
                    participants = participants.without(Ranges.of(starts[i].rangeFactory().newRange(starts[i], starts[i + 1])));
            }
            return participants;
        }

        public Deps mergeProposal()
        {
            return mergeProposal(null);
        }

        Deps mergeProposal(Participants<?> intersecting)
        {
            if (size() == 0)
                return Deps.NONE;

            KeyDeps keyDeps =  KeyDeps.merge(stream(intersecting, Merge::forProposal, (d, r) -> slice(d.keyDeps, r, intersecting)));
            RangeDeps rangeDeps =  RangeDeps.merge(stream(intersecting, Merge::forProposal, (d, r) -> slice(d.rangeDeps, r, intersecting)));
            return new Deps(keyDeps, rangeDeps);
        }

        MergedCommitResult mergeCommitOrStable(@Nullable Participants<?> intersecting, KnownDeps atLeast)
        {
            return mergeCommitOrStable(intersecting, atLeast == DepsProposedFixed ? test -> test.compareTo(DepsKnown) >= 0 || test == DepsProposedFixed
                                                                                  : test -> test.compareTo(atLeast) >= 0);
        }

        MergedCommitResult mergeCommitOrStable(@Nullable Participants<?> intersecting, Predicate<KnownDeps> atLeast)
        {
            if (size() == 0)
                return new MergedCommitResult(Deps.NONE, Ranges.EMPTY);

            SuccessCollector sufficientFor = new SuccessCollector(true);
            KeyDeps keyDeps =  KeyDeps.merge(stream(intersecting, forCommitOrStable(atLeast, sufficientFor), (d, r) -> slice(d.keyDeps, r, intersecting)));
            RangeDeps rangeDeps = RangeDeps.merge(stream(intersecting, forCommitOrStable(atLeast, sufficientFor), (d, r) -> slice(d.rangeDeps, r, intersecting)));
            return new MergedCommitResult(new Deps(keyDeps, rangeDeps), Ranges.of(sufficientFor.toArray(new Range[0])));
        }

        private static KeyDeps slice(KeyDeps keyDeps, Ranges ranges, @Nullable Participants<?> intersecting)
        {
            keyDeps = keyDeps.slice(ranges);
            return intersecting == null ? keyDeps : keyDeps.intersecting(intersecting);
        }

        private static RangeDeps slice(RangeDeps rangeDeps, Ranges ranges, @Nullable Participants<?> intersecting)
        {
            rangeDeps = rangeDeps.slice(ranges);
            return intersecting == null ? rangeDeps : rangeDeps.intersecting(intersecting);
        }

        private <V> Stream<V> stream(@Nullable Participants<?> intersecting, TriFunction<Ranges, MergeEntry, BiFunction<Deps, Ranges, V>, Stream<V>> selector, BiFunction<Deps, Ranges, V> getter)
        {
            RangeFactory rangeFactory = starts[0].rangeFactory();
            return IntStream.range(0, size())
                            .filter(i -> values[i] != null)
                            .mapToObj(i -> {
                                Range range = rangeFactory.newRange(starts[i], starts[i+1]);
                                if (intersecting != null && !intersecting.intersects(range))
                                    return Stream.<V>of();
                                return selector.apply(Ranges.of(range), values[i], getter);
                            })
                            .flatMap(v -> v);
        }

        private static <V> Stream<V> forProposal(Ranges slice, MergeEntry e, BiFunction<Deps, Ranges, V> getter)
        {
            switch (e.known)
            {
                default: throw new UnhandledEnum(e.known);
                case DepsUnknown:
                    return e.merge.stream().map(d -> getter.apply(d, slice));
                case DepsFromCoordinator:
                    Stream<V> merge = e.merge.stream().map(d -> getter.apply(d, slice));
                    return Stream.concat(Stream.of(getter.apply(e.coordinatedDeps, slice)), merge);
                case DepsProposedFixed: case DepsProposed:
                    return Stream.of(getter.apply(e.coordinatedDeps, slice));
                case DepsKnown: case DepsErased: case NoDeps: case DepsCommitted:
                    throw UnhandledEnum.invalid(e.known);
            }
        }

        static class SuccessCollector extends ArrayList<Range>
        {
            final boolean hasSeenCommit;
            int hasMedium = 0;

            SuccessCollector(boolean hasSeenCommit)
            {
                this.hasSeenCommit = hasSeenCommit;
            }

            void add(Range range, KnownDeps known)
            {
                add(range);
                if (!hasSeenCommit)
                {
                    switch (known)
                    {
                        default: throw new UnhandledEnum(known);
                        case DepsUnknown:
                        case DepsFromCoordinator:
                        case DepsProposed:
                        case DepsCommitted:
                            Invariants.require(hasMedium <= 0, "Highest ballot for %s had %s, but another range had DepsProposedFixed", range, known);
                            hasMedium = -1;
                            break;
                        case DepsProposedFixed:
                            Invariants.require(hasMedium >= 0, "Highest ballot for %s had DepsProposedFixed, but another range had a slow path or no proposal", range);
                            hasMedium = 1;
                            break;
                        case DepsKnown:
                            break;
                    }
                }
            }
        }

        private static <V> TriFunction<Ranges, MergeEntry, BiFunction<Deps, Ranges, V>, Stream<V>> forCommitOrStable(Predicate<KnownDeps> test, SuccessCollector success)
        {
            return (Ranges ranges, MergeEntry e, BiFunction<Deps, Ranges, V> getter) -> {
                if (e.known == DepsErased || !test.test(e.known))
                    return Stream.empty();

                success.add(ranges.get(0), e.known);
                switch (e.known)
                {
                    default: throw new UnhandledEnum(e.known);
                    case DepsUnknown:
                    case DepsFromCoordinator:
                        return e.merge.stream().map(d -> getter.apply(d, ranges));

                    case DepsProposedFixed:
                        return Stream.of(getter.apply(e.coordinatedDeps, ranges));

                    case DepsProposed:
                        // we may encounter DepsProposed for any interrupted commit. This might be a fast-path commit that
                        // was partially recovered by a prior recovery coordinator, or a slow-path commit that was interrupted.
                        // For the former we would only require the previously proposed deps as these would represent a complete set of deps
                        // However in the latter case to skip re-proposing for this shard  IFF txnId == executeAt we combine the coordinated/accepted deps
                        // with the computed deps from each response, as this is equivalent to the committed deps the prior coordinator
                        // would have committed using the accept responses.
                        return Stream.concat(Stream.of(getter.apply(e.coordinatedDeps, ranges)), e.merge.stream().map(d -> getter.apply(d, ranges)));

                    case DepsCommitted:
                    case DepsKnown:
                        return Stream.of(getter.apply(e.coordinatedDeps, ranges));
                    case NoDeps:
                    throw new AssertionError("Invalid KnownDeps for commit: " + e.known);
                }
            };
        }

        static class MergeBuilder extends AbstractIntervalBuilder<RoutingKey, MergeEntry, Merge>
        {
            protected MergeBuilder(int capacity)
            {
                super(capacity);
            }

            @Override
            protected MergeEntry slice(RoutingKey start, RoutingKey end, MergeEntry v)
            {
                return v;
            }

            @Override
            protected MergeEntry reduce(MergeEntry a, MergeEntry b)
            {
                return MergeEntry.reduce(a, b);
            }

            @Override
            protected MergeEntry tryMergeEqual(MergeEntry a, MergeEntry b)
            {
                if (a == null || b == null)
                    return null;

                if (a.coordinatedDeps != b.coordinatedDeps)
                    return null;

                if (a.merge.size() != b.merge.size())
                    return null;

                for (int i = 0 ; i < a.merge.size() ; ++i)
                {
                    if (a.merge.get(i) != b.merge.get(i))
                        return null;
                }

                return a;
            }

            @Override
            protected Merge buildInternal()
            {
                return new Merge(starts.toArray(new RoutingKey[0]), values.toArray(new MergeEntry[0]));
            }
        }
    }

}
