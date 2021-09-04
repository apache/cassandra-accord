package accord.api;

import java.util.concurrent.TimeUnit;

/**
 * A simple task execution interface
 */
public interface Scheduler
{
    interface Scheduled
    {
        void cancel();
    }

    Scheduled recurring(Runnable run, long delay, TimeUnit units);
    Scheduled once(Runnable run, long delay, TimeUnit units);
    void now(Runnable run);
}
