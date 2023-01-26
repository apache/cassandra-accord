package accord.primitives;

import accord.api.Key;

/**
 * Something that can be found within the cluster AND found on disk, queried and returned
 */
public interface Seekable extends Routable
{
    Key asKey();
    Range asRange();
}
