package accord.utils;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class Functions
{

    public static <T> T reduceNonNull(BiFunction<T, T, T> merge, T a, T b)
    {
        return a == null ? b : b == null ? a : merge.apply(a, b);
    }

    public static <T1, T2> T1 reduceNonNull(BiFunction<T1, T2, T1> merge, @Nonnull T1 a, T2 ... bs)
    {
        for (T2 b : bs)
        {
            if (b != null)
                a = merge.apply(a, b);
        }
        return a;
    }

    public static <I, O> O mapReduceNonNull(Function<I, O> map, BiFunction<O, O, O> reduce, List<I> input)
    {
        O result = null;
        for (I i : input)
        {
            if (i == null) continue;

            O o = map.apply(i);
            if (o == null) continue;

            if (result == null) result = o;
            else result = reduce.apply(result, o);
        }
        return result;
    }

}
