package accord.local;

import accord.primitives.Timestamp;

public interface NodeTimeService
{
    Node.Id id();
    long epoch();
    Timestamp uniqueNow(Timestamp atLeast);
}
