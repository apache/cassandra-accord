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

    default Scheduled recurring(Runnable run) { return recurring(run, 1L, TimeUnit.SECONDS); }
    Scheduled recurring(Runnable run, long delay, TimeUnit units);

    Scheduled once(Runnable run, long delay, TimeUnit units);
    void now(Runnable run);
}
