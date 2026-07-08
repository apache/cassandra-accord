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

package accord.local;

import accord.utils.UnhandledEnum;

/**
 * For operations that need information associated with keys, this indicates the
 * amount of data needed, and whether it is needed immediately or asynchronously.
 */
public enum LoadKeys
{
    NONE,

    /**
     * This command will likely need to use the provided keys, but can be processed before they are loaded;
     * any work touching keys not loaded will be submitted as a follow-up INCR or SYNC command.
     */
    ASYNC,

    /**
     * Load and process the requested keys incrementally; the operation will be invoked multiples times
     * as keys are loaded, until all the keys have been processed. If submitted by an already running execution
     * this task must declare a subset of the keys and txnIds declared by the originating task.
     * It is not permitted to chain INCR tasks together; INCR may only be submitted by an ASYNC or SYNC task.
     */
    INCR,

    /**
     * Load all keys into memory before processing the command.
     */
    SYNC;

    public boolean satisfiesIfPresent(LoadKeys that)
    {
        return satisfies(that, ASYNC);
    }

    public boolean satisfies(LoadKeys that)
    {
        return satisfies(that, SYNC);
    }

    private boolean satisfies(LoadKeys that, LoadKeys ifSyncRequireAtLeast)
    {
        switch (that)
        {
            default: throw new UnhandledEnum(that);
            case NONE:
                return true;
            case ASYNC:
            case INCR:
            case SYNC:
                return this.compareTo(ifSyncRequireAtLeast) >= 0;
        }
    }

}
