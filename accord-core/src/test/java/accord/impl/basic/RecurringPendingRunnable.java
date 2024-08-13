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
            if (requeue != null) requeue.add(this, delay, units);
            else run = null;
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

    @Override
    public boolean isDone()
    {
        return run == null;
    }

    public void onCancellation(Runnable run)
    {
        this.onCancellation = run;
    }

    @Override
    public String toString()
    {
        if (run == null)
            return "Done/Cancelled";

        return run + " with " + delay + " " + units + " delay";
    }
}
