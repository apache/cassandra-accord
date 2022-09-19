package accord.primitives;

import accord.api.RoutingKey;

/**
 * Either a Route or a collection of Routable
 */
public interface Seekables<K extends Seekable, U extends Seekables<K, ?>> extends Routables<K, U>
{
    U slice(Ranges ranges);
    Seekables<K, U> union(U with);
    Unseekables<?, ?> toUnseekables();

    FullRoute<?> toRoute(RoutingKey homeKey);
}
