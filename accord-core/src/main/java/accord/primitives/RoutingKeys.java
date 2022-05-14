package accord.primitives;

import java.util.Arrays;
import java.util.function.IntFunction;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import accord.api.RoutingKey;
import accord.utils.SortedArrays;

@SuppressWarnings("rawtypes")
public class RoutingKeys extends AbstractKeys<RoutingKey, RoutingKeys>
{
    public static final RoutingKeys EMPTY = new RoutingKeys(new RoutingKey[0]);

    public RoutingKeys(RoutingKey[] keys)
    {
        super(keys);
    }

    public static RoutingKeys of(RoutingKey ... keys)
    {
        return new RoutingKeys(sort(keys));
    }

    public RoutingKeys union(RoutingKeys that)
    {
        return wrap(SortedArrays.linearUnion(keys, that.keys, factory()), that);
    }

    public RoutingKeys slice(KeyRanges ranges)
    {
        return wrap(slice(ranges, factory()));
    }

    public RoutingKeys with(RoutingKey addKey)
    {
        return wrap(SortedArrays.insert(keys, addKey, RoutingKey[]::new));
    }

    private RoutingKeys wrap(RoutingKey[] wrap, RoutingKeys that)
    {
        return wrap == keys ? this : wrap == that.keys ? that : new RoutingKeys(wrap);
    }

    private RoutingKeys wrap(RoutingKey[] wrap)
    {
        return wrap == keys ? this : new RoutingKeys(wrap);
    }

    public Route toRoute(RoutingKey homeKey)
    {
        Preconditions.checkNotNull(homeKey);
        return new Route(homeKey, keys);
    }

    private static IntFunction<RoutingKey[]> factory()
    {
        return RoutingKey[]::new;
    }

    private static RoutingKey[] sort(RoutingKey[] keys)
    {
        Arrays.sort(keys);
        return keys;
    }
}
