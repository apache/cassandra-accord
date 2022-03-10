package accord.utils;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import accord.api.Scheduler;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadPoolScheduler implements Scheduler
{
    private static final Logger logger = LoggerFactory.getLogger(ThreadPoolScheduler.class);
    final ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1);
    public ThreadPoolScheduler()
    {
        exec.setMaximumPoolSize(1);
    }

    static class FutureAsScheduled implements Scheduled
    {
        final ScheduledFuture<?> f;

        FutureAsScheduled(ScheduledFuture<?> f)
        {
            this.f = f;
        }

        @Override
        public void cancel()
        {
            f.cancel(true);
        }
    }

    static Runnable wrap(Runnable runnable)
    {
        return () ->
        {
            try
            {
                runnable.run();
            }
            catch (Throwable t)
            {
                logger.error("Unhandled Exception", t);
                throw t;
            }
        };
    }

    @Override
    public Scheduled recurring(Runnable run, long delay, TimeUnit units)
    {
        return new FutureAsScheduled(exec.scheduleWithFixedDelay(wrap(run), delay, delay, units));
    }

    @Override
    public Scheduled once(Runnable run, long delay, TimeUnit units)
    {
        return new FutureAsScheduled(exec.schedule(wrap(run), delay, units));
    }

    @Override
    public void now(Runnable run)
    {
        exec.execute(wrap(run));
    }

    public void stop()
    {
        exec.shutdown();
        try
        {
            if (!exec.awaitTermination(1L, TimeUnit.MINUTES))
                throw new IllegalStateException("did not terminate");
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
    }
}
