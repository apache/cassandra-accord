package accord.primitives;

import accord.api.RoutingKey;

/**
 * Something that can be found in the cluster, and MAYBE found on disk (if Seekable)
 */
public interface Routable
{
    enum Kind { Key, Range }
    Kind kind();
    Unseekable toUnseekable();
    RoutingKey someIntersectingRoutingKey();
}
