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

import accord.local.SafeCommandStore;
import accord.local.Status;
import accord.primitives.*;

import java.util.*;
import java.util.function.Consumer;

public class DepsBuilder
{
    private static boolean CAN_PRUNE = Boolean.getBoolean("accord.can_prune_deps");
    private final TxnId subject;
    private final Map<Seekable, Builder> builders = new HashMap<>();

    public static void setCanPruneUnsafe(boolean value)
    {
        CAN_PRUNE = value;
    }

    public static boolean getCanPrune()
    {
        return CAN_PRUNE;
    }

    public DepsBuilder(TxnId subject)
    {
        this.subject = subject;
    }

    private static class TxnInfo
    {
        private final TxnId txnId;
        private final Status status;
        private final Timestamp executeAt;
        private final List<TxnId> depsIds;
        private boolean required = false;

        public TxnInfo(TxnId txnId, Status status, Timestamp executeAt, List<TxnId> depsIds)
        {
            this.txnId = txnId;
            this.status = status;
            this.executeAt = executeAt;
            this.depsIds = depsIds;
        }
    }

    /**
     * To remove transitive dependencies, we filter out commands that exist in the deps list
     * We need commands that are not superseded by other commands with later execution times
     */
    public static class Builder
    {
        private final Map<TxnId, TxnInfo> commands = new HashMap<>();
        private final Map<TxnId, Set<TxnId>> supportingDeps = new HashMap<>();

        public void add(TxnId txnId, Status status, Timestamp executeAt, List<TxnId> depsIds)
        {
            commands.put(txnId, new TxnInfo(txnId, status, executeAt, depsIds));
            if (depsIds != null)
                depsIds.forEach(id -> supportingDeps.computeIfAbsent(id, i -> new HashSet<>()).add(txnId));
        }

        private void markRequired(TxnInfo info)
        {
            if (info.required)
                return;

            info.required = true;

            Set<TxnId> supportingIds = supportingDeps.get(info.txnId);
            if (supportingIds == null || supportingIds.isEmpty())
                return;

            for (TxnId supportingId : supportingIds)
            {
                TxnInfo supporting = commands.get(supportingId);
                if (supporting == null)
                    continue;

                markRequired(supporting);
            }
        }

        private boolean isRequired(TxnInfo info)
        {
            if (!CAN_PRUNE || info.required)
                return true;

            if (!info.status.hasBeen(Status.Applied))
                return true;

            Set<TxnId> supportingIds = supportingDeps.get(info.txnId);
            if (supportingIds == null || supportingIds.isEmpty())
                return true;

            for (TxnId supportingId : supportingIds)
            {
                TxnInfo supporting = commands.get(supportingId);

                // TODO: check for eviction of txnId and skip if it's evicted

                if (!supporting.status.hasBeen(Status.Applied))
                    return true;

                if (supporting.executeAt.compareTo(info.executeAt) < 0)
                    return true;

                // FIXME: is this actually needed?
                // let execution order strongly connected components
                if (info.depsIds.contains(supportingId))
                    return true;
            }

            return false;
        }

        private void preprocess(TxnInfo info)
        {
            if (isRequired(info))
                markRequired(info);
        }

        private void preprocess()
        {
            commands.values().forEach(this::preprocess);
        }

        public <T> void build(SafeCommandStore safeStore, Seekable seekable, Consumer<TxnId> consumer)
        {
            preprocess();
            commands.forEach(((txnId, info) -> {
                if (info.required)
                    consumer.accept(txnId);
                else
                    safeStore.removeCommandFromSeekableDeps(seekable, txnId, info.executeAt, info.status);
            }));
        }
    }

    public void add(TxnId txnId, Seekable seekable, Status status, Timestamp executeAt, List<TxnId> depsIds)
    {
        // don't add self to deps
        if (txnId.equals(subject))
            return;

        builders.computeIfAbsent(seekable, s -> new Builder()).add(txnId, status, executeAt, depsIds);
    }

    public PartialDeps buildPartialDeps(SafeCommandStore safeStore, Ranges ranges)
    {
        try (PartialDeps.Builder result = new PartialDeps.Builder(ranges);)
        {
            builders.forEach((seekable, builder) -> builder.build(safeStore, seekable, txnId -> result.add(seekable, txnId)));
            return result.build();
        }
    }
}
