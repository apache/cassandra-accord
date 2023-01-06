package accord.primitives;

import accord.api.RoutingKey;

import static accord.primitives.Routables.Slice.Overlapping;

/**
 * Either a Route or a collection of Routable
 */
public interface Seekables<K extends Seekable, U extends Seekables<K, ?>> extends Routables<K, U>
{
    @Override
    default U slice(Ranges ranges) { return slice(ranges, Overlapping); }

    @Override
    U slice(Ranges ranges, Slice slice);
    Seekables<K, U> with(U with);

    Unseekables<?, ?> toUnseekables();

    FullRoute<?> toRoute(RoutingKey homeKey);
}
