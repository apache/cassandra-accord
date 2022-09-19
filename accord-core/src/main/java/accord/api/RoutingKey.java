package accord.api;

import accord.primitives.RoutableKey;
import accord.primitives.Unseekable;

public interface RoutingKey extends Unseekable, RoutableKey
{
    @Override default RoutingKey toUnseekable() { return this; }
}
