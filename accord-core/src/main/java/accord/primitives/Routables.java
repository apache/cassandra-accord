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

import accord.api.RoutingKey;
import accord.primitives.Routable.Domain;
import accord.utils.*;
import net.nicoulaj.compilecommand.annotations.Inline;

import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static accord.utils.SortedArrays.Search.FLOOR;

/**
 * A collection of either Seekable or Unseekable
 */
public interface Routables<K extends Routable> extends Iterable<K>
{
    /**
     * How to slice an input range that partially overlaps a slice range.
     * This modifier applies only when both input collections (to either {@link #slice} or {@link #intersecting} contain Ranges)
     */
    enum Slice
    {
        /** (Default) Overlapping ranges are returned unmodified */
        Overlapping,
        /** Overlapping ranges are split/shrunk to the intersection of the overlaps */
        Minimal,
        /** Overlapping ranges are extended to the union of the overlaps */
        Maximal
    }

    K get(int i);
    int size();

    boolean isEmpty();
    default Stream<K> stream()
    {
        return StreamSupport.stream(spliterator(), false);
    }
    boolean intersects(AbstractRanges ranges);
    boolean intersects(AbstractKeys<?> keys);
    default boolean intersects(Routables<?> routables)
    {
        switch (routables.domain())
        {
            default: throw new AssertionError();
            case Key: return intersects((AbstractKeys<?>) routables);
            case Range: return intersects((AbstractRanges) routables);
        }
    }

    boolean contains(RoutableKey key);
    boolean containsAll(Routables<?> keysOrRanges);
    boolean intersectsAll(Unseekables<?> keysOrRanges);

    Routables<?> slice(int from, int to);
    Routables<?> slice(Ranges ranges);
    Routables<K> slice(Ranges ranges, Slice slice);
    Routables<?> intersecting(Unseekables<?> intersecting);
    Routables<K> intersecting(Unseekables<?> intersecting, Slice slice);

    /**
     * Search forwards from {code thisIndex} and {@code withIndex} to find the first entries in each collection
     * that intersect with each other. Return their position packed in a long, with low bits representing
     * the resultant {@code thisIndex} and high bits {@code withIndex}.
     */
    long findNextIntersection(int thisIndex, AbstractRanges with, int withIndex);

    /**
     * Search forwards from {code thisIndex} and {@code withIndex} to find the first entries in each collection
     * that intersect with each other. Return their position packed in a long, with low bits representing
     * the resultant {@code thisIndex} and high bits {@code withIndex}.
     */
    long findNextIntersection(int thisIndex, AbstractKeys<?> with, int withIndex);

    /**
     * Search forwards from {code thisIndex} and {@code withIndex} to find the first entries in each collection
     * that intersect with each other. Return their position packed in a long, with low bits representing
     * the resultant {@code thisIndex} and high bits {@code withIndex}.
     */
    long findNextIntersection(int thisIndex, Routables<K> with, int withIndex);

    /**
     * Perform {@link SortedArrays#exponentialSearch} from {@code thisIndex} looking for {@code find} with behaviour of {@code search}
     */
    int findNext(int thisIndex, Range find, SortedArrays.Search search);

    /**
     * Perform {@link SortedArrays#exponentialSearch} from {@code thisIndex} looking for {@code find} with behaviour of {@code search}
     */
    int findNext(int thisIndex, RoutableKey find, SortedArrays.Search search);

    /**
     * Perform {@link SortedArrays#exponentialSearch} from {@code thisIndex} looking for {@code find} with behaviour of {@code search}
     */
    default int findNext(int thisIndex, Routable find, SortedArrays.Search search)
    {
        switch (find.domain())
        {
            default: throw new AssertionError("Unhandled domain: " + find.domain());
            case Key: return findNext(thisIndex, (RoutableKey) find, search);
            case Range: return findNext(thisIndex, (Range) find, search);
        }
    }

    Domain domain();

    /**
     * Fold-left over the {@code inputs} that intersect with {@code matching} in ascending order.
     * Terminate once we hit {@code terminalValue}.
     */
    @Inline
    static <Input extends Routable, T> T foldl(Routables<Input> inputs, AbstractRanges matching, IndexedFold<? super Input, T> fold, T initialValue)
    {
        return Helper.foldl(Routables::findNextIntersection, Helper::findLimit, inputs, matching, fold, initialValue);
    }

    /**
     * Fold-left over the {@code inputs} that intersect with {@code matching} in ascending order.
     * If the inputs are ranges, narrow them to the parts the intersect with {@code matching}, so that we never visit
     * any portion of a {@code Range} that is not in {@code matching} (See {@link Slice#Minimal}).
     * Terminate once we hit {@code terminalValue}.
     */
    @Inline
    static <T> T foldlMinimal(Seekables<?, ?> inputs, AbstractRanges matching, IndexedFold<? super Seekable, T> fold, T initialValue)
    {
        return Helper.foldlMinimal(inputs, matching, fold, initialValue);
    }

    /**
     * Fold-left over the {@code inputs} that intersect with {@code matching} in ascending order.
     * Terminate once we hit {@code terminalValue}.
     */
    @Inline
    static <Input extends RoutableKey, T> T foldl(AbstractKeys<Input> inputs, AbstractRanges matching, IndexedFold<? super Input, T> fold, T initialValue)
    {
        return Helper.foldl(AbstractKeys::findNextIntersection, Helper::findLimit, inputs, matching, fold, initialValue);
    }

    /**
     * Fold-left over the {@code inputs} that intersect with {@code matching} in ascending order.
     * Terminate once we hit {@code terminalValue}.
     */
    @Inline
    static <T> T foldl(AbstractRanges inputs, Routables<?> matching, IndexedFold<? super Range, T> fold, T initialValue)
    {
        switch (matching.domain())
        {
            default: throw new AssertionError();
            case Key: return Helper.foldl(AbstractRanges::findNextIntersection, Helper::findLimit, inputs, (AbstractKeys<?>)matching, fold, initialValue);
            case Range: return Helper.foldl(AbstractRanges::findNextIntersection, Helper::findLimit, inputs, (AbstractRanges)matching, fold, initialValue);
        }
    }

    /**
     * Fold-left over the {@code inputs} that intersect with {@code matching} in ascending order.
     * Terminate once we hit {@code terminalValue}.
     */
    @Inline
    static <Input extends RoutableKey, T> T foldl(AbstractKeys<Input> inputs, Routables<?> matching, IndexedFold<? super Input, T> fold, T initialValue)
    {
        switch (matching.domain())
        {
            default: throw new AssertionError();
            case Key: return Helper.foldl(AbstractKeys::findNextIntersection, Helper::findLimit, inputs, (AbstractKeys<?>)matching, fold, initialValue);
            case Range: return Helper.foldl(AbstractKeys::findNextIntersection, Helper::findLimit, inputs, (AbstractRanges)matching, fold, initialValue);
        }
    }

    @Inline
    static <P1, P2, Input extends RoutableKey, T> T foldl(AbstractKeys<Input> inputs, Routables<?> matching, IndexedTriFold<P1, P2, ? super Input, T> fold, P1 p1, P2 p2, T initialValue, Predicate<T> terminate)
    {
        switch (matching.domain())
        {
            default: throw new AssertionError();
            case Key: return Helper.foldl(AbstractKeys::findNextIntersection, Helper::findLimit, inputs, (AbstractKeys<?>)matching, fold, p1, p2, initialValue, terminate);
            case Range: return Helper.foldl(AbstractKeys::findNextIntersection, Helper::findLimit, inputs, (AbstractRanges)matching, fold, p1, p2, initialValue, terminate);
        }
    }

    @Inline
    static <P1, P2, Input extends RoutableKey, T> T foldl(AbstractKeys<Input> inputs, AbstractRanges matching, IndexedTriFold<P1, P2, ? super Input, T> fold, P1 p1, P2 p2, T initialValue, Predicate<T> terminate)
    {
        return Helper.foldl(Routables::findNextIntersection, Helper::findLimit, inputs, matching, fold, p1, p2, initialValue, terminate);
    }

    @Inline
    static <P1, P2, Input extends Routable, T> T foldl(Routables<Input> inputs, AbstractRanges matching, IndexedTriFold<P1, P2, ? super Input, T> fold, P1 p1, P2 p2, T initialValue, Predicate<T> terminate)
    {
        return Helper.foldl(Routables::findNextIntersection, Helper::findLimit, inputs, matching, fold, p1, p2, initialValue, terminate);
    }

    @Inline
    static <Input extends RoutableKey> long foldl(AbstractKeys<Input> inputs, AbstractRanges matching, IndexedFoldToLong<? super Input> fold, long param, long initialValue, long terminalValue)
    {
        return Helper.foldl(AbstractKeys::findNextIntersection, Helper::findLimit, inputs, matching, fold, param, initialValue, terminalValue);
    }

    /**
     * Fold-left over the {@code inputs} that intersect with {@code matching} in ascending order.
     * Terminate once we hit {@code terminalValue}.
     */
    @Inline
    static <Input extends Routable> long foldl(Routables<Input> inputs, AbstractRanges matching, IndexedFoldToLong<? super Input> fold, long param, long initialValue, long terminalValue)
    {
        return Helper.foldl(Routables::findNextIntersection, Helper::findLimit, inputs, matching, fold, param, initialValue, terminalValue);
    }

    /**
     * Fold-left over the {@code inputs} that intersect with {@code matching} in ascending order.
     * Terminate once we hit {@code terminalValue}.
     */
    @Inline
    static <Input extends Routable> long foldl(Routables<Input> inputs, AbstractKeys<?> matching, IndexedFoldToLong<? super Input> fold, long param, long initialValue, long terminalValue)
    {
        return Helper.foldl(Routables::findNextIntersection, (ls, li, rs, ri) -> li + 1,
                inputs, matching, fold, param, initialValue, terminalValue);
    }

    /**
     * Fold-left over the {@code inputs} that intersect with {@code matching} in ascending order.
     * Terminate once we hit {@code terminalValue}.
     */
    @Inline
    static <Input extends RoutingKey, Matching extends Routable> long foldl(AbstractKeys<Input> inputs, Routables<Matching> matching, IndexedFoldToLong<? super Input> fold, long param, long initialValue, long terminalValue)
    {
        return Helper.foldl((ls, li, rs, ri) -> SortedArrays.swapHighLow32b(rs.findNextIntersection(ri, ls, li)), (ls, li, rs, ri) -> li + 1,
                inputs, matching, fold, param, initialValue, terminalValue);
    }

    /**
     * Fold-left over the {@code inputs} that intersect with {@code matching} in ascending order, passing the contiguous ranges that intersect to the IndexedRangeFold function.
     * Terminate once we hit {@code terminalValue}.
     */
    @Inline
    static <Input extends Routable> long rangeFoldl(Routables<Input> inputs, AbstractRanges matching, IndexedRangeFoldToLong fold, long param, long initialValue, long terminalValue)
    {
        return Helper.rangeFoldl(Routables::findNextIntersection, (ls, li, rs, ri) -> li + 1,
                inputs, matching, fold, param, initialValue, terminalValue);
    }

    /**
     * Fold-left over the {@code inputs} that intersect with {@code matching} in ascending order, passing the contiguous ranges that intersect to the IndexedRangeFold function.
     * Terminate once we hit {@code terminalValue}.
     */
    @Inline
    static <Input extends Routable> long rangeFoldl(Routables<Input> inputs, AbstractKeys<?> matching, IndexedRangeFoldToLong fold, long param, long initialValue, long terminalValue)
    {
        return Helper.rangeFoldl(Routables::findNextIntersection, (ls, li, rs, ri) -> li + 1,
                inputs, matching, fold, param, initialValue, terminalValue);
    }

    class Helper
    {
        interface SetIntersections<L extends Routables<?>, R extends Routables<?>>
        {
            long findNext(L left, int li, R right, int ri);
        }

        interface ValueIntersections<L extends Routables<?>, R extends Routables<?>>
        {
            int findLimit(L left, int li, R right, int ri);
        }

        @Inline
        static <T> T foldlMinimal(Seekables<?, ?> is, AbstractRanges ms, IndexedFold<? super Seekable, T> fold, T accumulator)
        {
            int i = 0, m = 0;
            while (true)
            {
                long im = is.findNextIntersection(i, ms, m);
                if (im < 0)
                    break;

                i = (int)(im);
                m = (int)(im >>> 32);

                Range mv = ms.get(m);
                int nexti = Helper.findLimit(is, i, ms, m) - 1;
                while (true)
                {
                    accumulator = fold.apply(is.get(i).slice(mv), accumulator, i);
                    if (i < nexti) ++i;
                    else break;
                }
                if (!(is.get(i) instanceof Range) || ((Range)is.get(i)).end().compareTo(mv.end()) <= 0) ++i;
                else ++m;
            }

            return accumulator;
        }

        @Inline
        static <Input extends Routable, Inputs extends Routables<Input>, Matches extends Routables<?>, T>
        T foldl(SetIntersections<Inputs, Matches> setIntersections, ValueIntersections<Inputs, Matches> valueIntersections,
                Inputs is, Matches ms, IndexedFold<? super Input, T> fold, T accumulator)
        {
            int i = 0, m = 0;
            while (true)
            {
                long im = setIntersections.findNext(is, i, ms, m);
                if (im < 0)
                    break;

                i = (int)(im);
                m = (int)(im >>> 32);

                int nexti = valueIntersections.findLimit(is, i, ms, m);
                while (i < nexti)
                {
                    accumulator = fold.apply(is.get(i), accumulator, i);
                    ++i;
                }
            }

            return accumulator;
        }

        @Inline
        static <P1, P2, Input extends Routable, Inputs extends Routables<Input>, Matches extends Routables<?>, T>
        T foldl(SetIntersections<Inputs, Matches> setIntersections, ValueIntersections<Inputs, Matches> valueIntersections,
                Inputs is, Matches ms, IndexedTriFold<P1, P2, ? super Input, T> fold, P1 p1, P2 p2, T accumulator, Predicate<T> terminate)
        {
            int i = 0, m = 0;
            while (true)
            {
                long im = setIntersections.findNext(is, i, ms, m);
                if (im < 0)
                    break;

                i = (int)(im);
                m = (int)(im >>> 32);

                int nexti = valueIntersections.findLimit(is, i, ms, m);
                while (i < nexti)
                {
                    accumulator = fold.apply(p1, p2, is.get(i), accumulator, i);
                    if (terminate.test(accumulator))
                        return accumulator;
                    ++i;
                }
            }

            return accumulator;
        }

        @Inline
        static <Input extends Routable, Inputs extends Routables<Input>, Matches extends Routables<?>>
        long foldl(SetIntersections<Inputs, Matches> setIntersections, ValueIntersections<Inputs, Matches> valueIntersections,
                   Inputs is, Matches ms, IndexedFoldToLong<? super Input> fold, long param, long accumulator, long terminalValue)
        {
            int i = 0, m = 0;
            done: while (true)
            {
                long im = setIntersections.findNext(is, i, ms, m);
                if (im < 0)
                    break;

                i = (int)(im);
                m = (int)(im >>> 32);

                int nexti = valueIntersections.findLimit(is, i, ms, m);
                while (i < nexti)
                {
                    accumulator = fold.apply(is.get(i), param, accumulator, i);
                    if (accumulator == terminalValue)
                        break done;
                    ++i;
                }
            }

            return accumulator;
        }

        static <Input extends Routable, Inputs extends Routables<Input>, Matches extends Routables<?>>
        long rangeFoldl(SetIntersections<Inputs, Matches> setIntersections, ValueIntersections<Inputs, Matches> valueIntersections,
                        Inputs is, Matches ms, IndexedRangeFoldToLong fold, long param, long accumulator, long terminalValue)
        {
            int i = 0, m = 0;
            while (true)
            {
                long kri = setIntersections.findNext(is, i, ms, m);
                if (kri < 0)
                    break;

                i = (int)(kri);
                m = (int)(kri >>> 32);

                int nexti = valueIntersections.findLimit(is, i, ms, m);
                accumulator = fold.apply(param, accumulator, i, nexti);
                if (accumulator == terminalValue)
                    break;
                i = nexti;
            }

            return accumulator;
        }

        static <L extends Routable> int findLimit(Routables<L> ls, int li, AbstractRanges rs, int ri)
        {
            Range range = rs.get(ri);

            int nextl = ls.findNext(li + 1, range, FLOOR);
            if (nextl < 0) nextl = -1 - nextl;
            else nextl++;
            return nextl;
        }

        static int findLimit(AbstractRanges ls, int li, AbstractKeys<?> rs, int ri)
        {
            RoutableKey r = rs.get(ri);

            int nextl = ls.findNext(li + 1, r, FLOOR);
            if (nextl < 0) nextl = -1 - nextl;
            else nextl++;
            return nextl;
        }

        static int findLimit(AbstractKeys<?> ls, int li, AbstractKeys<?> rs, int ri)
        {
            RoutableKey r = rs.get(ri);

            int nextl = ls.findNext(li + 1, r, FLOOR);
            if (nextl < 0) nextl = -1 - nextl;
            else nextl++;
            return nextl;
        }
    }
}
