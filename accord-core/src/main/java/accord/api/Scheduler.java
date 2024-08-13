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

package accord.api;

import java.util.concurrent.TimeUnit;

/**
 * A simple task execution interface
 */
public interface Scheduler
{
    Scheduled CANCELLED = new Scheduled()
    {
        @Override
        public void cancel()
        {
        }

        @Override
        public boolean isDone()
        {
            return true;
        }
    };

    Scheduler NEVER_RUN_SCHEDULED = new Scheduler()
    {
        @Override
        public Scheduled recurring(Runnable run, long delay, TimeUnit units)
        {
            return CANCELLED;
        }

        @Override
        public Scheduled once(Runnable run, long delay, TimeUnit units)
        {
            return CANCELLED;
        }

        @Override
        public void now(Runnable run)
        {
            run.run();
        }
    };

    interface Scheduled
    {
        void cancel();
        boolean isDone();
    }

    default Scheduled recurring(Runnable run) { return recurring(run, 1L, TimeUnit.SECONDS); }
    Scheduled recurring(Runnable run, long delay, TimeUnit units);

    Scheduled once(Runnable run, long delay, TimeUnit units);
    void now(Runnable run);
}
