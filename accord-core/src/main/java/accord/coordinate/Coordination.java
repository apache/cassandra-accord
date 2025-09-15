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

package accord.coordinate;

import javax.annotation.Nullable;

import accord.coordinate.tracking.AbstractTracker;
import accord.local.Node.Id;
import accord.local.SequentialAsyncExecutor;
import accord.primitives.Ballot;
import accord.primitives.Participants;
import accord.primitives.TxnId;
import accord.utils.SortedList;
import accord.utils.SortedListMap;
import accord.utils.TinyEnumSet;

public interface Coordination
{
    // approximately ordered, that is we should ordinarily move forwards in the state machine
    enum CoordinationKind
    {
        HomeProgress, WaitProgress,  // not strictly coordinations, but for tracing simplicity we treat them as such here (maybe make this a more general enum)
        Fetch, FetchRoute,
        BeginInvalidate, MaybeRecover, PrepareRecovery, BeginRecovery, RecoverAwait, CollectLatestDeps,
        PreAccept, Propose, ProposeInvalidate, Stabilise, Execute, ExecuteSyncPoint, Persist,
        AsyncAwait, SyncAwait, Bootstrap, Other;

        private static final CoordinationKind[] LOOKUP = values();
        public static final TinyEnumSet<CoordinationKind> COORDINATES_STATE_MACHINE = TinyEnumSet.of(
            PreAccept, Propose, ProposeInvalidate, Stabilise, Execute, ExecuteSyncPoint, Persist,
            MaybeRecover, PrepareRecovery, BeginRecovery, RecoverAwait, BeginInvalidate
        );
        public static final TinyEnumSet<CoordinationKind> ALL = TinyEnumSet.of(LOOKUP);

        public static CoordinationKind forOrdinal(int ordinal)
        {
            return LOOKUP[ordinal];
        }
    }

    long coordinationId();

    TxnId txnId();
    CoordinationKind kind();
    Participants<?> scope();

    default @Nullable Ballot ballot() { return null; }

    default SortedList<Id> nodes()
    {
        AbstractTracker<?> tracker = tracker();
        return tracker == null ? null : tracker.nodes();
    }
    default @Nullable AbstractTracker<?> tracker() { return null; }

    default String describe() { return ""; }

    default @Nullable SortedList<Id> inflight() { return null; }
    default @Nullable SortedList<Id> contacted() { return null; }

    default @Nullable SortedListMap<Id, ?> replies() { return null; }

    SequentialAsyncExecutor executor();

    /**
     * Try to abort the coordination; must be invoked by {@link #executor}
      * @return true if the coordination was aborted
     */
    default boolean abort() { return false; }
}
