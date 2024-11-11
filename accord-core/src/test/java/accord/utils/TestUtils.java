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

package accord.utils;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.awaitility.core.ThrowingRunnable;

public class TestUtils
{
    public static void spinUntilSuccess(ThrowingRunnable runnable)
    {
        spinUntilSuccess(runnable, 10);
    }

    public static void spinUntilSuccess(ThrowingRunnable runnable, int timeoutInSeconds)
    {
        Awaitility.await()
                  .pollInterval(Duration.ofMillis(100))
                  .pollDelay(0, TimeUnit.MILLISECONDS)
                  .atMost(timeoutInSeconds, TimeUnit.SECONDS)
                  .ignoreExceptions()
                  .untilAsserted(runnable);
    }
}
