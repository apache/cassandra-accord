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

package accord.utils.btree;

import java.util.ArrayDeque;
import java.util.Iterator;

import accord.api.RoutingKey;
import accord.primitives.AbstractKeys;
import accord.primitives.AbstractRanges;
import accord.primitives.Range;
import accord.primitives.RoutableKey;
import accord.utils.Invariants;
import accord.utils.QuadFunction;
import accord.utils.SortedArrays;

import static accord.utils.SortedArrays.Search.CEIL;
import static accord.utils.SortedArrays.Search.FAST;
import static accord.utils.btree.BTree.Dir.ASC;
import static accord.utils.btree.BTree.isLeaf;

public class ReducingBTree
{
    public static abstract class Entry<E extends Entry<E>> extends Range
    {
        public Entry(RoutingKey start, RoutingKey end)
        {
            super(start, end);
        }

        protected Entry(RoutingKey start, RoutingKey end, UnsafeMarker unsafeMarker)
        {
            super(start, end, unsafeMarker);
        }

        abstract public boolean equalsIgnoreRange(E that);
        abstract public E with(RoutingKey start, RoutingKey end);

        public Range toPlainRange()
        {
            return Range.of(start(), end());
        }

        public boolean equalsRange(Range that)
        {
            return equalsRange(that.start(), that.end());
        }

        public boolean equalsRange(RoutingKey start, RoutingKey end)
        {
            return this.start().equals(start) && this.end().equals(end);
        }

        public final int compareTo(Entry<?> that)
        {
            return this.compareIntersecting(that);
        }
        public final int compareTo(Range range)
        {
            return this.compareIntersecting(range);
        }
        public static int compare(RoutableKey key, Entry<?> e)
        {
            return -e.compareTo(key);
        }
        public static int compare(Range range, Entry<?> e)
        {
            return range.compareIntersecting(e);
        }

        public static int compareWithStart(RoutingKey start, Entry<?> e)
        {
            if (start.compareTo(e.start()) < 0)
                return -1;
            if (start.compareTo(e.end()) >= 0)
                return 1;
            return 0;
        }
    }

    /**
     * Implement set subtraction/difference using a modified version of the Transformer logic
     *
     * TODO (desired): merge with Transformer
     */
    static class Merge<E extends Entry<E>> extends BTree.AbstractSeekingTransformer<E, E> implements AutoCloseable
    {
        static final ThreadLocal<Merge> SHARED = new ThreadLocal<>();

        static <E extends Entry<E>> Merge<E> get(QuadFunction<RoutingKey, RoutingKey, E, E, E> reduce)
        {
            Merge<E> merge = SHARED.get();
            if (merge == null)
                SHARED.set(merge = new Merge<>());
            merge.reduce = reduce;
            return merge;
        }

        Iterator<E> merge;
        QuadFunction<RoutingKey, RoutingKey, E, E, E> reduce;
        E pendingMerge;
        ArrayDeque<E> pendingInputs = new ArrayDeque<>();

        Object[] merge(Object[] update, Iterator<E> merge)
        {
            this.merge = merge;
            advancePendingMerge();
            Object[] result = apply(update);
            Invariants.require(pendingInputs.isEmpty());
            Invariants.require(!merge.hasNext());
            requireEmpty();
            return result;
        }

        Object[] merge(Object[] update, Object[] merge)
        {
            return merge(update, BTree.<E, E>slice(merge, Entry::compareTo, ASC));
        }

        @Override
        E apply(E in)
        {
            while (true)
            {
                E result = coalesce(mergeInputOrPending(in));
                if (result != null)
                    return result;

                in = pendingInputs.poll();
                if (in == null)
                    return null;
            }
        }

        E applyNoInput()
        {
            while (true)
            {
                E result = pendingMerge;
                if (result == null)
                    return null;

                advancePendingMerge();
                result = coalesce(result);
                if (result != null)
                    return result;
            }
        }

        E coalesce(E out)
        {
            E prev = prev();
            if (prev != null && out.equalsIgnoreRange(prev) && prev.end().compareTo(out.start()) >= 0)
            {
                prev = prev.with(prev.start(), out.end());
                overwritePrev(prev);
                return null;
            }
            return out;
        }

        E prev()
        {
            return (E) super.prev();
        }

        @Override
        Object validateOverwritePrev(Object overwriting, Object with)
        {
            Invariants.require(((Entry)overwriting).start().equals(((Entry)with).start()));
            return with;
        }

        E mergeInputOrPending(E input)
        {
            if (!pendingInputs.isEmpty())
            {
                E tmp = pendingInputs.poll();
                pendingInputs.addLast(input);
                input = tmp;
            }
            return merge(input);
        }

        E merge(E input)
        {
            E prev = prev();
            if (pendingMerge == null)
                return maybeMoveStart(prev, input);

            E merge = pendingMerge;

            RoutingKey pe = prev == null ? null : prev.end();
            RoutingKey ie = input.end();
            RoutingKey ms = merge.start();

            int ces = ie.compareTo(ms);
            if (ces <= 0)
                return maybeMoveStart(prev, input);

            RoutingKey is = input.start();
            // bound starts by previous end
            if (pe != null && ms.compareTo(pe) < 0)
                ms = pe;
            if (pe != null && is.compareTo(pe) < 0)
                is = pe;

            int cs = is.compareTo(ms);
            RoutingKey me = merge.end();
            int ce = ie.compareTo(me);

            if (cs == 0)
            {
                if (ce >= 0)
                    advancePendingMerge();

                if (ce > 0)
                    pendingInputs.addFirst(input);

                RoutingKey start, end;
                if (ce <= 0) { start = is; end = ie; }
                else { start = ms; end = me; }
                return reduce.apply(start, end, input, merge);
            }
            else
            {
                pendingInputs.addFirst(input);
                if (cs < 0)
                {
                    // ces > 0  -> is.end() > ms.start()
                    return input.with(is, ms);
                }
                else
                {
                    int cse = is.compareTo(me);
                    if (cse >= 0)
                    {
                        advancePendingMerge();
                        return merge.with(ms, me);
                    }
                    else
                    {
                        return merge.with(ms, is);
                    }
                }
            }
        }

        private E maybeMoveStart(E prev, E input)
        {
            if (prev != null && prev.end().compareTo(input.start()) > 0)
                return input.with(prev.end(), input.end());
            return input;
        }

        @Override
        int seekInBranch(BTree.BranchBuilder level, Object[] unode, int upos, int usz)
        {
            if (!pendingInputs.isEmpty())
                return -1 - upos;

            if (pendingMerge == null)
                return -1 - (1 + usz);

            int i = SortedArrays.<E, E>exponentialSearchWithCast(unode, upos, usz, pendingMerge, Entry::compareTo, CEIL);
            if (i >= 0)
            {
                RoutingKey start = pendingMerge.start();
                if (start.compareTo(((E) unode[i]).start()) < 0)
                    return -1 - i;
            }
            else if (i == -1 - usz)
            {
                // if we sort after the last key in the branch, we may need to descend into the right-most child
                // but, for all branches besides the root we can try to look at a parent node to decide this.
                Object successor = successorBranchKey();
                if (successor != null && pendingMerge.compareTo((Entry<E>) successor) >= 0)
                {
                    // increase our result index to point to *after* the last child;
                    // (it's an inequality binary search semantic answer, so will be negated)
                    --i;
                }
            }
            return i;
        }

        private void mergeToLeaf(E input)
        {
            E next = coalesce(mergeInputOrPending(input));
            if (next != null)
                leaf().addKey(next);
            drainPendingInputs();
        }

        private void drainPendingInputs()
        {
            while (!pendingInputs.isEmpty())
            {
                E next = coalesce(merge(pendingInputs.poll()));
                if (next != null)
                    leaf().addKey(next);
            }
        }

        @Override
        protected boolean transformLeaf(Object[] unode, int upos, int usz)
        {
            drainPendingInputs();

            int prevUpos = upos;
            while (pendingMerge != null)
            {
                // fast path - buffer is empty and input unconsumed, so may be able to propagate original
                upos = SortedArrays.<E, E>exponentialSearchWithCast(unode, upos, usz, pendingMerge, Entry::compareTo, CEIL);
                if (upos < 0)
                {
                    upos = -1 - upos;
                    if (upos == usz)
                        break;

                    prevUpos = flushPendingMerge((E)unode[upos], unode, prevUpos, upos);
                    continue;
                }

                leaf().copy(unode, prevUpos, upos - prevUpos);
                mergeToLeaf((E) unode[upos]);
                ++upos;
                prevUpos = upos;
            }

            if (pendingMerge == null)
            {
                if (prevUpos == 0 && leaf().isEmpty())
                    // if input is unmodified by transformation, propagate the input node
                    return false;

                if (prevUpos < usz)
                {
                    mergeToLeaf((E)unode[prevUpos++]);
                    leaf().copy(unode, prevUpos, usz - prevUpos);
                }
            }
            else
            {
                E successor = (E) successorBranchKey();
                if (prevUpos == 0 && leaf().isEmpty() && successor != null && pendingMerge.start().compareTo(successor.start()) >= 0)
                {
                    // if input is unmodified by transformation, propagate the input node
                    return false;
                }

                if (prevUpos < usz)
                {
                    mergeToLeaf((E)unode[prevUpos++]);
                    leaf().copy(unode, prevUpos, usz - prevUpos);
                }

                flushPendingMerge(successor, unode, upos, usz);
            }

            return true;
        }

        int flushPendingMerge(E successor, Object[] copy, int copyFrom, int copyTo)
        {
            do
            {
                E next = pendingMerge;
                RoutingKey pendingStart = next.start(), nextStart = pendingStart;
                RoutingKey pendingEnd = next.end(), nextEnd = pendingEnd;

                E prev = copyFrom != copyTo ? (E) copy[copyTo - 1] : prev();
                if (prev != null && prev.end().compareTo(nextStart) > 0)
                    nextStart = prev.end();

                if (successor != null && pendingEnd.compareTo(successor.start()) > 0)
                {
                    if (nextStart.compareTo(successor.start()) >= 0)
                        break;

                    nextEnd = successor.start();
                }

                if (pendingStart != nextStart || pendingEnd != nextEnd)
                    next = next.with(nextStart, nextEnd);

                leaf().copy(copy, copyFrom, copyTo - copyFrom);
                copyFrom = copyTo;

                next = coalesce(next);
                if (next != null)
                    leaf().addKey(next);

                if (pendingEnd != nextEnd)
                    break;

                advancePendingMerge();
            } while (pendingMerge != null);

            return copyFrom;
        }

        private void advancePendingMerge()
        {
            pendingMerge = merge.hasNext() ? merge.next() : null;
        }

        @Override
        public void close()
        {
            reset();
        }

        @Override
        void reset()
        {
            super.reset();
            merge = null;
            reduce = null;
            pendingMerge = null;
            pendingInputs.clear();
        }
    }


    /**
     * Merges {@code update} into {@code with}, combining overlapping entries with the provided reduce function.
     */
    public static <E extends Entry<E>> Object[] merge(Object[] update, Object[] with, QuadFunction<RoutingKey, RoutingKey, E, E, E> reduce)
    {
        try (Merge<E> merge = Merge.get(reduce))
        {
            return merge.merge(update, with);
        }
    }

    /**
     * Merges {@code update} into {@code with}, combining overlapping entries with the provided reduce function.
     */
    public static <E extends Entry<E>> Object[] merge(Object[] update, Iterator<E> with, QuadFunction<RoutingKey, RoutingKey, E, E, E> reduce)
    {
        try (Merge<E> merge = Merge.get(reduce))
        {
            return merge.merge(update, with);
        }
    }

    public static <E extends Entry<E>, V2, P1, P2> V2 foldl(Object[] tree, AbstractKeys<?> keys, int from, int to, QuadFunction<E, V2, P1, P2, V2> fold, V2 accumulator, P1 p1, P2 p2)
    {
        if (isLeaf(tree))
        {
            int size = BTree.sizeOfLeaf(tree);
            int ei = 0;
            for (int k = from; k < to ; ++k)
            {
                ei = SortedArrays.<E, RoutableKey>exponentialSearchWithCast(tree, ei, size, keys.get(k), Entry::compare, FAST);
                if (ei >= 0) accumulator = fold.apply((E)tree[ei], accumulator, p1, p2);
                else ei = -1 - ei;
            }
        }
        else
        {
            int branchSize = BTree.shallowSizeOfBranch(tree);
            int ei = 0, ki = from;
            while (ki < to)
            {
                ei = SortedArrays.<E, RoutableKey>exponentialSearchWithCast(tree, ei, branchSize, keys.get(ki), Entry::compare, FAST);
                if (ei >= 0)
                {
                    accumulator = fold.apply((E)tree[ei], accumulator, p1, p2);
                    ++ki;
                }
                else
                {
                    ei = -1 - ei;
                    if (ei == branchSize)
                        break;

                    int childFrom = ki;
                    int childTo = keys.findNext(ki, to, ((E)tree[ei]), CEIL);
                    if (childTo < 0) childTo = -1 - childTo;
                    if (childTo > ki)
                    {
                        accumulator = foldl((Object[])tree[branchSize + ei], keys, childFrom, childTo, fold, accumulator, p1, p2);
                        ki = childTo;
                    }
                    else ++ki;
                }
            }
            if (ki < to)
                accumulator = foldl((Object[])tree[branchSize * 2], keys, ki, to, fold, accumulator, p1, p2);
        }
        return accumulator;
    }

    public static <E extends Entry<E>, V2, P1, P2> V2 foldl(Object[] tree, AbstractRanges ranges, int from, int to, QuadFunction<E, V2, P1, P2, V2> fold, V2 accumulator, P1 p1, P2 p2)
    {
        int ei = 0, ri = from;
        if (isLeaf(tree))
        {
            int leafSize = BTree.sizeOfLeaf(tree);
            while (ri < to)
            {
                Range rv = ranges.get(ri);

                ei = SortedArrays.<E, RoutingKey>exponentialSearchWithCast(tree, ei, leafSize, rv.start(), Entry::compareWithStart, CEIL);
                if (ei < 0)
                {
                    ei = -1 - ei;
                    if (ei == leafSize)
                        break;
                }

                E ev = (E)tree[ei];
                int ces = rv.end().compareTo(ev.start());
                int cee = -1;
                if (ces > 0)
                {
                    accumulator = fold.apply(ev, accumulator, p1, p2);
                    cee = rv.end().compareTo(ev.end());
                }

                if (cee <= 0) ++ri;
                if (cee >= 0) ++ei;
            }
        }
        else
        {
            int branchSize = BTree.shallowSizeOfBranch(tree);
            while (ri < to)
            {
                Range rv = ranges.get(ri);

                ei = SortedArrays.<E, RoutingKey>exponentialSearchWithCast(tree, ei, branchSize, rv.start(), Entry::compareWithStart, CEIL);
                E ev;
                int ces;
                if (ei >= 0)
                {
                    ces = 1;
                    ev = (E)tree[ei];
                }
                else
                {
                    ei = -1 - ei;
                    if (ei == branchSize)
                    {
                        accumulator = foldl((Object[])tree[branchSize * 2], ranges, ri, to, fold, accumulator, p1, p2);
                        break;
                    }

                    ev = (E)tree[ei];
                    int childFrom = ri, childTo;
                    ces = rv.end().compareTo(ev.start());

                    if (ces >= 0) childTo = ri + 1;
                    else
                    {
                        ri = ranges.findNext(ri, to, ev.start(), ReducingBTree::compareWithEnd, FAST);
                        if (ri < 0) ri = childTo = -1 - ri;
                        else
                        {
                            childTo = ri + 1;

                            // we intersect another range; refresh rv to ensure we advance correctly
                            rv = ranges.get(ri);
                            ces = rv.end().compareTo(ev.start());
                        }
                    }
                    if (childTo > childFrom)
                    {
                        accumulator = foldl((Object[])tree[branchSize + ei], ranges, childFrom, childTo, fold, accumulator, p1, p2);
                    }
                }

                int cee = -1;
                if (ces >= 0)
                {
                    if (ces > 0)
                    {
                        accumulator = fold.apply(ev, accumulator, p1, p2);
                        cee = rv.end().compareTo(ev.end());
                    }
                    if (cee <= 0) ++ri;
                }

                if (cee >= 0)
                    ++ei;
            }
        }
        return accumulator;
    }

    public static <E extends Entry<E>, V2, P1, P2> V2 foldlWithDefault(Object[] tree, AbstractKeys<?> keys, int from, int to, QuadFunction<E, V2, P1, P2, V2> fold, E ifNull, V2 accumulator, P1 p1, P2 p2)
    {
        if (isLeaf(tree))
        {
            int size = BTree.sizeOfLeaf(tree);
            int ei = 0;
            for (int k = from; k < to ; ++k)
            {
                ei = SortedArrays.<E, RoutableKey>exponentialSearchWithCast(tree, ei, size, keys.get(k), Entry::compare, FAST);
                if (ei >= 0) accumulator = fold.apply((E)tree[ei], accumulator, p1, p2);
                else
                {
                    accumulator = fold.apply(ifNull, accumulator, p1, p2);
                    ei = -1 - ei;
                }
            }
        }
        else
        {
            int branchSize = BTree.shallowSizeOfBranch(tree);
            int ei = 0, ki = from;
            while (ki < to)
            {
                ei = SortedArrays.<E, RoutableKey>exponentialSearchWithCast(tree, ei, branchSize, keys.get(ki), Entry::compare, FAST);
                if (ei >= 0)
                {
                    accumulator = fold.apply((E)tree[ei], accumulator, p1, p2);
                    ++ki;
                }
                else
                {
                    ei = -1 - ei;
                    if (ei == branchSize)
                        break;

                    int childFrom = ki;
                    int childTo = keys.findNext(ki, to, ((E)tree[ei]), CEIL);
                    if (childTo < 0) childTo = -1 - childTo;
                    if (childTo > ki)
                    {
                        accumulator = foldlWithDefault((Object[])tree[branchSize + ei], keys, childFrom, childTo, fold, ifNull, accumulator, p1, p2);
                        ki = childTo;
                    }
                    else
                    {
                        accumulator = fold.apply(ifNull, accumulator, p1, p2);
                        ++ki;
                    }
                }
            }
            if (ki < to)
                accumulator = foldlWithDefault((Object[])tree[branchSize * 2], keys, ki, to, fold, ifNull, accumulator, p1, p2);
        }
        return accumulator;
    }

    public static <E extends Entry<E>, V2, P1, P2> V2 foldlWithDefault(Object[] tree, AbstractRanges ranges, int from, int to, QuadFunction<E, V2, P1, P2, V2> fold, E ifNull, V2 accumulator, P1 p1, P2 p2)
    {
        return foldlWithDefault(null, null, tree, ranges, from, to, fold, ifNull, accumulator, p1, p2);
    }

    private static <E extends Entry<E>, V2, P1, P2> V2 foldlWithDefault(E lb, E ub, Object[] tree, AbstractRanges ranges, int from, int to, QuadFunction<E, V2, P1, P2, V2> fold, E ifNull, V2 accumulator, P1 p1, P2 p2)
    {
        RoutingKey min = lb == null ? null : lb.end();
        int ei = 0, ri = from;
        if (isLeaf(tree))
        {
            int leafSize = BTree.sizeOfLeaf(tree);
            while (ri < to)
            {
                Range rv = ranges.get(ri);
                RoutingKey find = rv.start();
                if (min != null && min.compareTo(find) > 0)
                    find = min;

                ei = SortedArrays.<E, RoutingKey>exponentialSearchWithCast(tree, ei, leafSize, find, Entry::compareWithStart, CEIL);
                if (ei < 0)
                {
                    ei = -1 - ei;
                    if (ei == leafSize)
                    {
                        if (ub == null || !find.equals(ub.start()))
                            accumulator = fold.apply(ifNull, accumulator, p1, p2);
                        break;
                    }
                    accumulator = fold.apply(ifNull, accumulator, p1, p2);
                }

                E ev = (E)tree[ei];
                int ces = rv.end().compareTo(ev.start());
                int cee = -1;
                if (ces > 0)
                {
                    accumulator = fold.apply(ev, accumulator, p1, p2);
                    cee = rv.end().compareTo(ev.end());
                }

                if (cee <= 0) ++ri;
                if (cee >= 0)
                {
                    ++ei;
                    min = ev.end();
                }
            }
        }
        else
        {
            int branchSize = BTree.shallowSizeOfBranch(tree);
            while (ri < to)
            {
                Range rv = ranges.get(ri);
                RoutingKey find = rv.start();
                if (min != null && min.compareTo(find) > 0)
                    find = min;

                ei = SortedArrays.<E, RoutingKey>exponentialSearchWithCast(tree, ei, branchSize, find, Entry::compareWithStart, CEIL);
                E ev;
                int ces;
                if (ei >= 0)
                {
                    ces = 1;
                    ev = (E)tree[ei];
                }
                else
                {
                    ei = -1 - ei;
                    if (ei == branchSize)
                    {
                        accumulator = foldlWithDefault((E)tree[branchSize-1], ub, (Object[])tree[branchSize * 2], ranges, ri, to, fold, ifNull, accumulator, p1, p2);
                        break;
                    }

                    ev = (E)tree[ei];
                    int childFrom = ri, childTo;
                    ces = rv.end().compareTo(ev.start());

                    if (ces >= 0) childTo = ri + 1;
                    else
                    {
                        ri = ranges.findNext(ri, to, ev.start(), ReducingBTree::compareWithEnd, FAST);
                        if (ri < 0) ri = childTo = -1 - ri;
                        else
                        {
                            childTo = ri + 1;
                            // we intersect another range but same tree child range; refresh rv to ensure we advance correctly
                            rv = ranges.get(ri);
                            ces = rv.end().compareTo(ev.start());
                        }
                    }
                    if (childTo > childFrom)
                    {
                        accumulator = foldlWithDefault(ei == 0 ? lb : (E)tree[ei-1], ev,
                                                       (Object[])tree[branchSize + ei], ranges, childFrom, childTo, fold, ifNull, accumulator, p1, p2);
                    }
                }

                int cee = -1;
                if (ces >= 0)
                {
                    if (ces > 0)
                    {
                        accumulator = fold.apply(ev, accumulator, p1, p2);
                        cee = rv.end().compareTo(ev.end());
                    }
                    if (cee <= 0) ++ri;
                }

                if (cee >= 0)
                {
                    ++ei;
                    min = ev.end();
                }
            }
        }
        return accumulator;
    }

    private static int compareWithEnd(RoutingKey end, Range range)
    {
        if (end.compareTo(range.end()) > 0) return 1;
        if (end.compareTo(range.start()) <= 0) return -1;
        return 0;
    }

    public static boolean isWellFormed(Object[] btree)
    {
        Entry<?> prev = null;
        for (Entry<?> e : BTree.<Entry<?>>iterable(btree))
        {
            if (prev != null && prev.end().compareTo(e.start()) > 0)
                return false;
            prev = e;
        }
        return true;
    }
}
