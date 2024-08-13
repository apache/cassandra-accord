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

import java.time.Duration;

public interface LocalConfig
{
    LocalConfig DEFAULT = new LocalConfig() {};

    // How long before we start notifying waiters on an epoch of timeout,
    default Duration epochFetchInitialTimeout()
    {
        return Duration.ofSeconds(10);
    }

    // How often to check for timeout, and once an epoch has timed out, how often we timeout new waiters
    default Duration epochFetchWatchdogInterval()
    {
        return Duration.ofSeconds(10);
    }
}
