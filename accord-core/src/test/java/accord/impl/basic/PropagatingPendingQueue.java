package accord.impl.basic;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class PropagatingPendingQueue implements PendingQueue
{
    final List<Throwable> failures;
    final PendingQueue wrapped;

    public PropagatingPendingQueue(List<Throwable> failures, PendingQueue wrapped)
    {
        this.failures = failures;
        this.wrapped = wrapped;
    }

    @Override
    public void add(Pending item)
    {
        wrapped.add(item);
    }

    @Override
    public void add(Pending item, long delay, TimeUnit units)
    {
        wrapped.add(item, delay, units);
    }

    @Override
    public Pending poll()
    {
        if (!failures.isEmpty())
        {
            AssertionError assertion = null;
            for (Throwable t : failures)
            {
                if (t instanceof AssertionError)
                {
                    assertion = (AssertionError) t;
                    break;
                }
            }
            if (assertion == null)
                assertion = new AssertionError("Unexpected exception encountered");

            for (Throwable t : failures)
            {
                if (t != assertion)
                    assertion.addSuppressed(t);
            }
            throw assertion;
        }

        return wrapped.poll();
    }

    @Override
    public int size()
    {
        return wrapped.size();
    }
}
