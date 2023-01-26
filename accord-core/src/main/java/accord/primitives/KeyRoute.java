package accord.primitives;

import accord.utils.Invariants;

import accord.api.RoutingKey;
import accord.utils.SortedArrays;

import javax.annotation.Nonnull;

public abstract class KeyRoute extends AbstractUnseekableKeys<Route<RoutingKey>> implements Route<RoutingKey>
{
    public final RoutingKey homeKey;

    KeyRoute(@Nonnull RoutingKey homeKey, RoutingKey[] keys)
    {
        super(keys);
        this.homeKey = Invariants.nonNull(homeKey);
    }

    @Override
    public Unseekables<RoutingKey, ?> toMaximalUnseekables()
    {
        return new RoutingKeys(SortedArrays.insert(keys, homeKey, RoutingKey[]::new));
    }

    @Override
    public RoutingKey homeKey()
    {
        return homeKey;
    }

    @Override
    public abstract PartialKeyRoute slice(Ranges ranges);
}
