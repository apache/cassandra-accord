package accord.impl.basic;

import java.util.concurrent.TimeUnit;

import accord.api.Scheduler.Scheduled;

class RecurringPendingRunnable implements PendingRunnable, Scheduled
{
    final PendingQueue requeue;
    final long delay;
    final TimeUnit units;
    Runnable run;
    Runnable onCancellation;

    RecurringPendingRunnable(PendingQueue requeue, Runnable run, long delay, TimeUnit units)
    {
        this.requeue = requeue;
        this.run = run;
        this.delay = delay;
        this.units = units;
    }

    @Override
    public void run()
    {
        if (run != null)
        {
            run.run();
            if (requeue != null)
                requeue.add(this, delay, units);
        }
    }

    @Override
    public void cancel()
    {
        run = null;
        if (onCancellation != null)
        {
            onCancellation.run();
            onCancellation = null;
        }
    }

    public void onCancellation(Runnable run)
    {
        this.onCancellation = run;
    }

    @Override
    public String toString()
    {
        return run + " with " + delay + " " + units + " delay";
    }
}
