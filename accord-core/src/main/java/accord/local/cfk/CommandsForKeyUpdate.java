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

package accord.local.cfk;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import accord.local.Command;
import accord.local.SafeCommandStore;

/**
 * Encapsulates an updated {@code CommandsForKey} and any follow-up notification work that cannot be performed as part
 * of the {@code CommandsForKey} update because it can trigger updates that may lead to additional updates to the
 * {@code CommandsForKey}, which would still be being constructed. We therefore first update the {@code CommandsForKey},
 * then notify any work that might itself need to update the {@code CommandsForKey}.
 */
public abstract class CommandsForKeyUpdate
{
    @VisibleForTesting
    public abstract CommandsForKey cfk();
    abstract PostProcess postProcess();
    abstract void postProcess(SafeCommandStore safeStore, @Nullable CommandsForKey prevCfk, @Nullable Command command, NotifySink notifySink);

    static class CommandsForKeyUpdateWithPostProcess extends CommandsForKeyUpdate
    {
        final CommandsForKey cfk;
        final PostProcess postProcess;

        CommandsForKeyUpdateWithPostProcess(CommandsForKey cfk, PostProcess postProcess)
        {
            this.cfk = cfk;
            this.postProcess = postProcess;
        }

        @Override
        public CommandsForKey cfk()
        {
            return cfk;
        }

        @Override
        PostProcess postProcess()
        {
            return postProcess;
        }

        @Override
        void postProcess(SafeCommandStore safeStore, @Nullable CommandsForKey prevCfk, @Nullable Command command, NotifySink notifySink)
        {
            cfk.postProcess(safeStore, prevCfk, command, notifySink);
            postProcess.postProcess(safeStore, cfk.key(), notifySink);
        }
    }
}

