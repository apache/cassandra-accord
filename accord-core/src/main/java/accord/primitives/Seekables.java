package accord.primitives;

import accord.api.RoutingKey;

/**
 * Either a Route or a collection of Routable
 */
public interface Seekables<K extends Seekable, U extends Seekables<K, ?>> extends Routables<K, U>
{
    @Override
    U slice(Ranges ranges);
    @Override
    Seekables<K, U> union(U with);
    Unseekables<?, ?> toUnseekables();

    FullRoute<?> toRoute(RoutingKey homeKey);
}
