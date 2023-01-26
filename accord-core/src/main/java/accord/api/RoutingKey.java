package accord.api;

import accord.primitives.Range;
import accord.primitives.RoutableKey;
import accord.primitives.Unseekable;
import accord.utils.ArrayBuffers;

import java.util.Arrays;

import static accord.utils.ArrayBuffers.cachedRoutingKeys;

public interface RoutingKey extends Unseekable, RoutableKey
{
    @Override default RoutingKey toUnseekable() { return this; }
    Range asRange();
}
