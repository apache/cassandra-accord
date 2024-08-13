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

package accord.impl.progresslog;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import accord.api.ProgressLog;
import accord.local.CommandStore;
import accord.local.Node;

// TODO (desired, consider): consider propagating invalidations in the same way as we do applied
// TODO (expected): report transactions not making progress
// TODO (required): evict to disk
public class DefaultProgressLogs implements ProgressLog.Factory
{
    static ConcurrentHashMap<DefaultProgressLog, Runnable> PAUSE_FOR_TEST;
    public static synchronized void unsafePauseForTesting(boolean pause)
    {
        if (pause)
        {
            PAUSE_FOR_TEST = new ConcurrentHashMap<>();
        }
        else
        {
            ConcurrentHashMap<DefaultProgressLog, Runnable> reschedule = PAUSE_FOR_TEST;
            PAUSE_FOR_TEST = null;
            if (reschedule != null)
                reschedule.values().forEach(Runnable::run);
        }
    }

    static boolean pauseForTest(DefaultProgressLog progressLog)
    {
        ConcurrentHashMap<DefaultProgressLog, Runnable> paused = PAUSE_FOR_TEST;
        if (paused == null)
            return false;

        if (!paused.containsKey(progressLog))
            paused.putIfAbsent(progressLog, () -> progressLog.commandStore.execute(progressLog));
        return true;
    }

    final Node node;
    final List<DefaultProgressLog> instances = new CopyOnWriteArrayList<>();

    public DefaultProgressLogs(Node node)
    {
        this.node = node;
    }

    @Override
    public DefaultProgressLog create(CommandStore commandStore)
    {
        DefaultProgressLog instance = new DefaultProgressLog(node, commandStore);
        instances.add(instance);
        return instance;
    }
}
