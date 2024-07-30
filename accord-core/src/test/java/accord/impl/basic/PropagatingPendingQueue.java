/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package accord.impl.basic;

import java.util.Iterator;
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
    public void addNoDelay(Pending item)
    {
        wrapped.addNoDelay(item);
    }

    @Override
    public void add(Pending item, long delay, TimeUnit units)
    {
        wrapped.add(item, delay, units);
    }

    @Override
    public boolean remove(Pending item)
    {
        return wrapped.remove(item);
    }

    @Override
    public Pending poll()
    {
        checkFailures();
        return wrapped.poll();
    }

    public void checkFailures()
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
    }

    @Override
    public int size()
    {
        return wrapped.size();
    }

    @Override
    public long nowInMillis()
    {
        return wrapped.nowInMillis();
    }

    @Override
    public boolean hasNonRecurring()
    {
        return wrapped.hasNonRecurring();
    }

    @Override
    public Iterator<Pending> iterator()
    {
        return wrapped.iterator();
    }
}
