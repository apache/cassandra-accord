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
import accord.impl.InMemoryCommandStore.GlobalCommandsForKey;
import accord.local.CommandsForKey;
import accord.local.SafeCommandsForKey;

public class InMemorySafeCommandsForKey extends SafeCommandsForKey
{
    private boolean invalidated = false;
    private final GlobalCommandsForKey global;

    public InMemorySafeCommandsForKey(Key key, GlobalCommandsForKey global)
    {
        super(key);
        this.global = global;
    }

    @Override
    public accord.local.CommandsForKey current()
    {
        return global.value();
    }

    @Override
    protected void set(CommandsForKey update)
    {
        global.value(update);
    }

    public void invalidate()
    {
        invalidated = true;
    }

    public boolean invalidated()
    {
        return invalidated;
    }
}
