package accord.utils;

import net.nicoulaj.compilecommand.annotations.Inline;

import java.util.function.Predicate;

public class Invariants
{
    public static <T1, T2 extends T1> T2 checkType(T1 cast)
    {
        return (T2)cast;
    }

    public static <T1, T2 extends T1> T2 checkType(Class<T2> to, T1 cast)
    {
        if (cast != null && !to.isInstance(cast))
            throw new IllegalStateException();
        return (T2)cast;
    }

    public static <T1, T2 extends T1> T2 checkType(Class<T2> to, T1 cast, String msg)
    {
        if (cast != null && !to.isInstance(cast))
            throw new IllegalStateException(msg);
        return (T2)cast;
    }

    public static void checkState(boolean condition)
    {
        if (!condition)
            throw new IllegalStateException();
    }

    public static void checkState(boolean condition, String msg)
    {
        if (!condition)
            throw new IllegalStateException(msg);
    }

    public static <T> T nonNull(T param)
    {
        if (param == null)
            throw new NullPointerException();
        return param;
    }

    public static void checkArgument(boolean condition)
    {
        if (!condition)
            throw new IllegalArgumentException();
    }

    public static void checkArgument(boolean condition, String msg)
    {
        if (!condition)
            throw new IllegalArgumentException(msg);
    }

    public static <T> T checkArgument(T param, boolean condition)
    {
        if (!condition)
            throw new IllegalArgumentException();
        return param;
    }

    public static <T> T checkArgument(T param, boolean condition, String msg)
    {
        if (!condition)
            throw new IllegalArgumentException(msg);
        return param;
    }

    @Inline
    public static <T> T checkArgument(T param, Predicate<T> condition)
    {
        if (!condition.test(param))
            throw new IllegalArgumentException();
        return param;
    }

    @Inline
    public static <T> T checkArgument(T param, Predicate<T> condition, String msg)
    {
        if (!condition.test(param))
            throw new IllegalArgumentException(msg);
        return param;
    }
}
