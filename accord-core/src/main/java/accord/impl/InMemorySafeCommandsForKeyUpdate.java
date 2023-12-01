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

import accord.api.Key;
import accord.impl.CommandTimeseries.CommandLoader;
import accord.impl.InMemoryCommandStore.CFKEntry;
import accord.utils.Invariants;

public class InMemorySafeCommandsForKeyUpdate extends SafeCommandsForKey.Update<CommandsForKey.Update, CFKEntry>
{
    private static final CommandsForKey.Update VALUE = new CommandsForKey.Update() {};

    private boolean invalidated = false;

    public InMemorySafeCommandsForKeyUpdate(Key key, CommandLoader<CFKEntry> loader)
    {
        super(key, loader);
    }

    @Override
    public void initialize()
    {
        Invariants.checkState(isEmpty());
    }

    @Override
    public CommandsForKey.Update current()
    {
        return VALUE;
    }

    @Override
    public void invalidate()
    {
        invalidated = true;
    }

    @Override
    public boolean invalidated()
    {
        return invalidated;
    }
}
