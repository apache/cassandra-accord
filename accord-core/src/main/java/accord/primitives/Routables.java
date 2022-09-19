package accord.primitives;

import accord.api.RoutingKey;
import accord.utils.IndexedFold;
import accord.utils.IndexedFoldToLong;
import accord.utils.IndexedRangeFoldToLong;
import accord.utils.SortedArrays;
import net.nicoulaj.compilecommand.annotations.Inline;

import static accord.utils.SortedArrays.Search.FLOOR;

/**
 * A collection of either Seekable or Unseekable
 */
public interface Routables<K extends Routable, U extends Routables<K, ?>> extends Iterable<K>
{
    int indexOf(K item);
    K get(int i);
    int size();

    boolean isEmpty();
    boolean intersects(AbstractRanges<?> ranges);
    boolean contains(RoutableKey key);
    boolean containsAll(Routables<?, ?> keysOrRanges);

    U slice(Ranges ranges);
    Routables<K, U> union(U with);

    /**
     * Search forwards from {code thisIndex} and {@code withIndex} to find the first entries in each collection
     * that intersect with each other. Return their position packed in a long, with low bits representing
     * the resultant {@code thisIndex} and high bits {@code withIndex}.
     */
    long findNextIntersection(int thisIndex, AbstractRanges<?> with, int withIndex);

    /**
     * Search forwards from {code thisIndex} and {@code withIndex} to find the first entries in each collection
     * that intersect with each other. Return their position packed in a long, with low bits representing
     * the resultant {@code thisIndex} and high bits {@code withIndex}.
     */
    long findNextIntersection(int thisIndex, AbstractKeys<?, ?> with, int withIndex);

    /**
     * Search forwards from {code thisIndex} and {@code withIndex} to find the first entries in each collection
     * that intersect with each other. Return their position packed in a long, with low bits representing
     * the resultant {@code thisIndex} and high bits {@code withIndex}.
     */
    long findNextIntersection(int thisIndex, Routables<K, ?> with, int withIndex);

    /**
     * Perform {@link SortedArrays#exponentialSearch} from {@code thisIndex} looking for {@code find} with behaviour of {@code search}
     */
    int findNext(int thisIndex, Range find, SortedArrays.Search search);

    /**
     * Perform {@link SortedArrays#exponentialSearch} from {@code thisIndex} looking for {@code find} with behaviour of {@code search}
     */
    int findNext(int thisIndex, K find, SortedArrays.Search search);

    Routable.Kind kindOfContents();

    @Inline
    static <Input extends Routable, T> T foldl(Routables<Input, ?> inputs, AbstractRanges<?> matching, IndexedFold<? super Input, T> fold, T initialValue)
    {
        return Helper.foldl(Routables::findNextIntersection, Helper::findLimit, inputs, matching, fold, initialValue);
    }

    @Inline
    static <Input extends RoutableKey, T> T foldl(AbstractKeys<Input, ?> inputs, AbstractRanges<?> matching, IndexedFold<? super Input, T> fold, T initialValue)
    {
        return Helper.foldl(AbstractKeys::findNextIntersection, Helper::findLimit, inputs, matching, fold, initialValue);
    }

    @Inline
    static <Input extends RoutableKey> long foldl(AbstractKeys<Input, ?> inputs, AbstractRanges<?> matching, IndexedFoldToLong<? super Input> fold, long param, long initialValue, long terminalValue)
    {
        return Helper.foldl(AbstractKeys::findNextIntersection, Helper::findLimit, inputs, matching, fold, param, initialValue, terminalValue);
    }

    @Inline
    static <Input extends Routable> long foldl(Routables<Input, ?> inputs, AbstractRanges<?> matching, IndexedFoldToLong<? super Input> fold, long param, long initialValue, long terminalValue)
    {
        return Helper.foldl(Routables::findNextIntersection, Helper::findLimit, inputs, matching, fold, param, initialValue, terminalValue);
    }

    @Inline
    static <Input extends Routable> long foldl(Routables<Input, ?> inputs, AbstractKeys<?, ?> matching, IndexedFoldToLong<? super Input> fold, long param, long initialValue, long terminalValue)
    {
        return Helper.foldl(Routables::findNextIntersection, (ls, li, rs, ri) -> li + 1,
                inputs, matching, fold, param, initialValue, terminalValue);
    }

    @Inline
    static <Input extends RoutingKey, Matching extends Routable> long foldl(AbstractKeys<Input, ?> inputs, Routables<Matching, ?> matching, IndexedFoldToLong<? super Input> fold, long param, long initialValue, long terminalValue)
    {
        return Helper.foldl((ls, li, rs, ri) -> SortedArrays.swapHighLow32b(rs.findNextIntersection(ri, ls, li)), (ls, li, rs, ri) -> li + 1,
                inputs, matching, fold, param, initialValue, terminalValue);
    }

    @Inline
    static <Input extends Routable> long rangeFoldl(Routables<Input, ?> inputs, AbstractRanges<?> matching, IndexedRangeFoldToLong fold, long param, long initialValue, long terminalValue)
    {
        return Helper.rangeFoldl(Routables::findNextIntersection, (ls, li, rs, ri) -> li + 1,
                inputs, matching, fold, param, initialValue, terminalValue);
    }

    @Inline
    static <Input extends Routable> long rangeFoldl(Routables<Input, ?> inputs, AbstractKeys<?, ?> matching, IndexedRangeFoldToLong fold, long param, long initialValue, long terminalValue)
    {
        return Helper.rangeFoldl(Routables::findNextIntersection, (ls, li, rs, ri) -> li + 1,
                inputs, matching, fold, param, initialValue, terminalValue);
    }

    @Inline
    static <Input extends Routable> long foldlMissing(Routables<Input, ?> inputs, Routables<Input, ?> notMatching, IndexedFoldToLong<? super Input> fold, long param, long initialValue, long terminalValue)
    {
        return Helper.foldlMissing((ls, li, rs, ri) -> rs.findNextIntersection(ri, ls, li), (ls, li, rs, ri) -> li + 1,
                inputs, notMatching, fold, param, initialValue, terminalValue);
    }

    class Helper
    {
        interface SetIntersections<L extends Routables<?, ?>, R extends Routables<?, ?>>
        {
            long findNext(L left, int li, R right, int ri);
        }

        interface ValueIntersections<L extends Routables<?, ?>, R extends Routables<?, ?>>
        {
            int findLimit(L left, int li, R right, int ri);
        }

        @Inline
        static <Input extends Routable, Inputs extends Routables<Input, ?>, Matches extends Routables<?, ?>, T>
        T foldl(SetIntersections<Inputs, Matches> setIntersections, ValueIntersections<Inputs, Matches> valueIntersections,
                Inputs is, Matches ms, IndexedFold<? super Input, T> fold, T initialValue)
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
                    initialValue = fold.apply(i, is.get(i), initialValue);
                    ++i;
                }
            }

            return initialValue;
        }

        @Inline
        static <Input extends Routable, Inputs extends Routables<Input, ?>, Matches extends Routables<?, ?>>
        long foldl(SetIntersections<Inputs, Matches> setIntersections, ValueIntersections<Inputs, Matches> valueIntersections,
                   Inputs is, Matches ms, IndexedFoldToLong<? super Input> fold, long param, long initialValue, long terminalValue)
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
                    initialValue = fold.apply(i, is.get(i), param, initialValue);
                    if (initialValue == terminalValue)
                        break done;
                    ++i;
                }
            }

            return initialValue;
        }

        @Inline
        static <Input extends Routable, Inputs extends Routables<Input, ?>, Matches extends Routables<?, ?>>
        long foldlMissing(SetIntersections<Inputs, Matches> setIntersections, ValueIntersections<Inputs, Matches> valueIntersections,
                   Inputs is, Matches ms, IndexedFoldToLong<? super Input> fold, long param, long initialValue, long terminalValue)
        {
            int i = 0, m = 0;
            done: while (true)
            {
                long im = setIntersections.findNext(is, i, ms, m);
                if (im < 0)
                    break;

                int nexti = (int)(im);
                while (i < nexti)
                {
                    initialValue = fold.apply(i, is.get(i), param, initialValue);
                    if (initialValue == terminalValue)
                        break done;
                    ++i;
                }

                m = (int)(im >>> 32);
                i = 1 + valueIntersections.findLimit(is, nexti, ms, m);
            }

            return initialValue;
        }

        static <Input extends Routable, Inputs extends Routables<Input, ?>, Matches extends Routables<?, ?>>
        long rangeFoldl(SetIntersections<Inputs, Matches> setIntersections, ValueIntersections<Inputs, Matches> valueIntersections,
                        Inputs is, Matches ms, IndexedRangeFoldToLong fold, long param, long initialValue, long terminalValue)
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
                initialValue = fold.apply(i, nexti, param, initialValue);
                if (initialValue == terminalValue)
                    break;
                i = nexti;
            }

            return initialValue;
        }

        static <L extends Routable> int findLimit(Routables<L, ?> ls, int li, AbstractRanges<?> rs, int ri)
        {
            Range range = rs.get(ri);

            int nextl = ls.findNext(li + 1, range, FLOOR);
            if (nextl < 0) nextl = -1 - nextl;
            else nextl++;
            return nextl;
        }
    }
}
