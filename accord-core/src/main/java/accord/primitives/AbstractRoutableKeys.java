package accord.primitives;

import accord.api.RoutingKey;

public abstract class AbstractRoutableKeys<KS extends Unseekables<RoutingKey, ?>> extends AbstractKeys<RoutingKey, KS> implements Iterable<RoutingKey>, Unseekables<RoutingKey, KS>
{
    AbstractRoutableKeys(RoutingKey[] keys)
    {
        super(keys);
    }
}
