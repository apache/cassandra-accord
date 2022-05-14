package accord.utils;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class Functions
{

    public static <T> T reduceNonNull(BiFunction<T, T, T> merge, T a, T b)
    {
        return a == null ? b : b == null ? a : merge.apply(a, b);
    }

    public static <T> T reduceNonNull(BiFunction<T, T, T> merge, T a, T ... bs)
    {
        for (T b : bs)
        {
            if (b != null)
            {
                if (a == null) a = b;
                else a = merge.apply(a, b);
            }
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
