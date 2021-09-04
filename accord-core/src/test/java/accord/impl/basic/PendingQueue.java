package accord.impl.basic;

import java.util.concurrent.TimeUnit;

public interface PendingQueue
{
    void add(Pending item);
    void add(Pending item, long delay, TimeUnit units);
    Pending poll();
    int size();
}
