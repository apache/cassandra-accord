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

enum Progress
{
    /**
     * We do not expect any progress for this state machine at present
     */
    NoneExpected,

    /**
     * We expect progress for this state machine, and have queued the TxnState to be processed
     */
    Queued,

    /**
     * The TxnState has recently been processed and sent queries to some replicas to attempt to progress out state machine.
     * These are messages that expect a synchronous response, and we rely on the outcome of this action to proceed.
     * We do not register any local timeouts to retry, as we expect the coordination of this query to have its own timeouts.
     */
    Querying,

    /**
     * The TxnState has recently been processed, and a synchronous remote query has completed without finding the
     * relevant remote state to progress out local state machine, but a remote listener has been registered that
     * will notify us when a remote replica reaches a point where we can advance our state machine.
     *
     * We also register a local timeout to retry, in case this remote reply or the remote listener itself is lost.
     */
    Awaiting;

    private static final Progress[] lookup = values();

    public static Progress forOrdinal(int ordinal)
    {
        if (ordinal < 0 || ordinal > lookup.length)
            throw new IndexOutOfBoundsException(ordinal);
        return lookup[ordinal];
    }
}
