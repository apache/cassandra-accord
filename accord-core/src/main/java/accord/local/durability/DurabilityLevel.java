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

package accord.local.durability;

import java.util.Objects;
import javax.annotation.Nullable;

import accord.local.Node;
import accord.local.durability.DurabilityService.SyncLocal;
import accord.local.durability.DurabilityService.SyncRemote;
import accord.local.durability.DurabilityService.SyncReadable;
import accord.utils.SortedArrays.SortedArrayList;

import static accord.local.durability.DurabilityService.SyncLocal.NoLocal;
import static accord.local.durability.DurabilityService.SyncReadable.UnknownReadable;
import static accord.local.durability.DurabilityService.SyncRemote.NoRemote;
import static accord.utils.Invariants.nonNull;
import static accord.utils.SortedArrays.SortedArrayList.intersection;
import static accord.utils.SortedArrays.SortedArrayList.union;

public class DurabilityLevel
{
    public static final DurabilityLevel NONE = new DurabilityLevel(NoLocal, NoRemote, UnknownReadable, null);

    public final SyncLocal local;
    public final SyncRemote remote;
    public final SyncReadable readable;
    public final @Nullable SortedArrayList<Node.Id> including;
    public final @Nullable SortedArrayList<Node.Id> excluding;
    public final @Nullable SortedArrayList<Node.Id> ineligible;

    public DurabilityLevel(SyncLocal local, SyncRemote remote, SyncReadable readable, @Nullable SortedArrayList<Node.Id> including)
    {
        this(local, remote, readable, including, null, null);
    }

    public DurabilityLevel(SyncLocal local, SyncRemote remote, SyncReadable readable, @Nullable SortedArrayList<Node.Id> including, @Nullable SortedArrayList<Node.Id> excluding, @Nullable SortedArrayList<Node.Id> ineligible)
    {
        this.local = nonNull(local);
        this.remote = nonNull(remote);
        this.readable = nonNull(readable);
        this.including = including;
        this.excluding = excluding;
        this.ineligible = ineligible;
    }

    public boolean equals(Object that)
    {
        return that instanceof DurabilityLevel && equals((DurabilityLevel) that);
    }

    private boolean equals(DurabilityLevel that)
    {
        return this.local == that.local
               && this.remote == that.remote
               && Objects.equals(this.including, that.including)
               && Objects.equals(this.excluding, that.excluding);
    }

    @Override
    public String toString()
    {
        return '{' +
               "local=" + local +
               ", remote=" + remote +
               ", readable=" + readable +
               ", including=" + including +
               ", excluding=" + excluding +
               ", ineligible=" + ineligible +
               '}';
    }

    public static DurabilityLevel min(DurabilityLevel a, DurabilityLevel b)
    {
        SyncLocal local = min(a.local, b.local);
        SyncRemote remote = min(a.remote, b.remote);
        SyncReadable readable = min(a.readable, b.readable);
        SortedArrayList<Node.Id> including = union(a.including, b.including);
        SortedArrayList<Node.Id> excluding = union(a.excluding, b.excluding);
        if (including != null && excluding != null)
            including = including.without(excluding);
        SortedArrayList<Node.Id> ineligible = intersection(a.ineligible, b.ineligible);
        return new DurabilityLevel(local, remote, readable, including, excluding, ineligible);
    }

    public static DurabilityLevel max(DurabilityLevel a, DurabilityLevel b)
    {
        SyncLocal local = max(a.local, b.local);
        SyncRemote remote = max(a.remote, b.remote);
        SyncReadable readable = max(a.readable, b.readable);
        SortedArrayList<Node.Id> including = union(a.including, b.including);
        SortedArrayList<Node.Id> excluding = union(a.excluding, b.excluding);
        if (including != null && excluding != null)
            excluding = excluding.without(including);
        // for ineligibility we always take the weakest answer on merge, even for max,
        // since e.g. stale nodes may be marked unstale in a later epoch and no longer be ineligible for the new bound
        SortedArrayList<Node.Id> ineligible = intersection(a.ineligible, b.ineligible);
        return new DurabilityLevel(local, remote, readable, including, excluding, ineligible);
    }

    private static <E extends Enum<E>> E min(E a, E b)
    {
        return a.compareTo(b) <= 0 ? a : b;
    }

    private static <E extends Enum<E>> E max(E a, E b)
    {
        return a.compareTo(b) >= 0 ? a : b;
    }

    public boolean isSatisfiedBy(DurabilityLevel test)
    {
        if (test.local.compareTo(local) < 0 || test.readable.compareTo(readable) < 0)
            return false;

        // if we have included all eligible nodes then we satisfy remote criteria even if quorum calculation is insufficient
        // this is to handle cases of bootstrapping a new node during a period of availability loss
        // (where the new node is ineligible to participate in the quorum)
        if (test.remote.compareTo(remote) < 0
            && (ineligible == null || test.excluding == null || !union(ineligible, test.ineligible).containsAll(test.excluding)))
                return false;

        return including == null || (test.including != null && test.including.containsAll(including));
    }
}
