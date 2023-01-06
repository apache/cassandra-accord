package accord.primitives;

import accord.api.RoutingKey;

import java.util.Arrays;

public abstract class AbstractUnseekableKeys<KS extends Unseekables<RoutingKey, ?>> extends AbstractKeys<RoutingKey, KS> implements Iterable<RoutingKey>, Unseekables<RoutingKey, KS>
{
    AbstractUnseekableKeys(RoutingKey[] keys)
    {
        super(keys);
    }

    @Override
    public final int indexOf(RoutingKey key)
    {
        return Arrays.binarySearch(keys, key);
    }
}
