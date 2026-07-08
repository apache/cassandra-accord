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

package accord.impl;

import accord.api.RoutingKey;
import accord.impl.InMemoryCommandStore.GlobalCommandsForKey;
import accord.local.cfk.NotifySink;
import accord.local.cfk.SafeCommandsForKey;

public class InMemorySafeCommandsForKey extends SafeCommandsForKey
{
    private final GlobalCommandsForKey global;
    private boolean touched;

    public InMemorySafeCommandsForKey(RoutingKey key, GlobalCommandsForKey global)
    {
        super(key);
        this.global = global;
    }

    @Override
    public void overrideSink(NotifySink overrideSink)
    {
        global.overrideSink = overrideSink;
    }

    @Override
    public NotifySink overrideSink()
    {
        return global.overrideSink;
    }

    public final void preExecute()
    {
        requireUninitialised();
        current = global.value();
        if (current == null)
            initialize();
        global.lock(this);
        setSafe();
    }

    protected void postExecute(InMemoryCommandStore commandStore)
    {
        if (isModified())
            global.value(current);
        else if (global.isEmpty())
            commandStore.commandsForKey.remove(key);
        global.unlock(this);
        setReleased();
    }

    protected boolean touch()
    {
        if (touched)
            return false;
        return touched = true;
    }
}
