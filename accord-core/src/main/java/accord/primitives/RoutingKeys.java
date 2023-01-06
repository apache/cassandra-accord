package accord.primitives;

import accord.api.RoutingKey;
import accord.utils.SortedArrays;

import static accord.utils.ArrayBuffers.cachedRoutingKeys;

public class RoutingKeys extends AbstractUnseekableKeys<AbstractUnseekableKeys<?>> implements Unseekables<RoutingKey, AbstractUnseekableKeys<?>>
{
    public static class SerializationSupport
    {
        public static RoutingKeys create(RoutingKey[] keys)
        {
            return new RoutingKeys(keys);
        }
    }

    public static final RoutingKeys EMPTY = new RoutingKeys(new RoutingKey[0]);

    RoutingKeys(RoutingKey[] keys)
    {
        super(keys);
    }

    public static RoutingKeys of(RoutingKey ... keys)
    {
        return new RoutingKeys(sort(keys));
    }

    @Override
    public Unseekables<RoutingKey, ?> with(Unseekables<RoutingKey, ?> with)
    {
        AbstractKeys<RoutingKey, ?> that = (AbstractKeys<RoutingKey, ?>) with;
        return wrap(SortedArrays.linearUnion(keys, that.keys, cachedRoutingKeys()), that);
    }

    public RoutingKeys with(RoutingKey with)
    {
        if (contains(with))
            return this;

        return wrap(toRoutingKeysArray(with));
    }

    @Override
    public UnseekablesKind kind()
    {
        return UnseekablesKind.RoutingKeys;
    }

    @Override
    public RoutingKeys slice(Ranges ranges)
    {
        return wrap(slice(ranges, RoutingKey[]::new));
    }

    public RoutingKeys slice(Ranges ranges, Slice slice)
    {
        return slice(ranges);
    }

    private RoutingKeys wrap(RoutingKey[] wrap, AbstractKeys<RoutingKey, ?> that)
    {
        return wrap == keys ? this : wrap == that.keys && that instanceof RoutingKeys ? (RoutingKeys)that : new RoutingKeys(wrap);
    }

    private RoutingKeys wrap(RoutingKey[] wrap)
    {
        return wrap == keys ? this : new RoutingKeys(wrap);
    }
}
