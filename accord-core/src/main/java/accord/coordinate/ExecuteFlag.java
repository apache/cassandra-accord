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

import accord.local.Node;
import accord.utils.SortedArrays;
import accord.utils.SortedList;
import accord.utils.SortedListSet;
import accord.utils.TinyEnumSet;

public enum ExecuteFlag
{
    READY_TO_EXECUTE, HAS_UNIQUE_HLC;

    private static final ExecuteFlag[] LOOKUP = values();
    public static ExecuteFlag forOrdinal(int ordinal)
    {
        return LOOKUP[ordinal];
    }

    public static final class ExecuteFlags extends TinyEnumSet<ExecuteFlag>
    {
        private static final ExecuteFlags[] LOOKUP = new ExecuteFlags[1 << ExecuteFlag.values().length];
        static
        {
            for (int i = 0 ; i < LOOKUP.length ; ++i)
                LOOKUP[i] = new ExecuteFlags(i);
        }
        public static ExecuteFlags none() { return LOOKUP[0]; }
        public static ExecuteFlags all() { return LOOKUP[LOOKUP.length - 1]; }
        public static ExecuteFlags get(int bits) { return LOOKUP[bits]; }
        public static ExecuteFlags get(ExecuteFlag a) { return LOOKUP[encode(a)]; }
        public static ExecuteFlags get(ExecuteFlag a, ExecuteFlag b) { return LOOKUP[encode(a) | encode(b)]; }
        public ExecuteFlags with(ExecuteFlag a) { return LOOKUP[bitset | encode(a)]; }
        public ExecuteFlags without(ExecuteFlag a) { return LOOKUP[bitset & ~encode(a)]; }
        public ExecuteFlags or(ExecuteFlags that) { return LOOKUP[this.bitset | that.bitset]; }
        public ExecuteFlags and(ExecuteFlags that) { return LOOKUP[this.bitset & that.bitset]; }
        public int bits() { return bitset; }
        private ExecuteFlags(int bits) { super(bits); }

        @Override
        public String toString()
        {
            return toString(ExecuteFlag::forOrdinal);
        }

        public static void collect(CoordinationFlags into, Node.Id id, ExecuteFlags add, Object expectIfReadyToExecute, Object actualReadyToExecute)
        {
            // TODO (expected): this is overly restrictive, disabling the optimisation in multi shard cases; should only expect the parts the shard owns to be equal
            if (add != none() && !expectIfReadyToExecute.equals(actualReadyToExecute))
                add = none(); // HAS_UNIQUE_HLC only accurate if READY_TO_EXECUTE was also accurate
            into.add(id, add);
        }
    }

    public interface CoordinationFlags
    {
        boolean isReadyToExecute(Node.Id node);
        boolean hasUniqueHlc();
        void add(Node.Id node, ExecuteFlags flags);

        default ExecuteFlags get(Node.Id node)
        {
            ExecuteFlags result = ExecuteFlags.none();
            if (hasUniqueHlc()) result = result.with(HAS_UNIQUE_HLC);
            if (isReadyToExecute(node)) result = result.with(READY_TO_EXECUTE);
            return result;
        }

        static CoordinationFlags none()
        {
            return ALWAYS_EMPTY;
        }

        static CoordinationFlags empty(SortedList<Node.Id> list)
        {
            return list.size() <= 64 ? new SmallCoordinationFlags(list) : new LargeCoordinationFlags(list);
        }
    }

    private static final SmallCoordinationFlags ALWAYS_EMPTY = new SmallCoordinationFlags(new SortedArrays.SortedArrayList<>(new Node.Id[0]));
    static
    {
        ALWAYS_EMPTY.hasUniqueHlc = false;
    }

    static class SmallCoordinationFlags extends SortedListSet.SmallSortedListSet<Node.Id> implements CoordinationFlags
    {
        boolean hasUniqueHlc = true;
        private SmallCoordinationFlags(SortedList<Node.Id> list)
        {
            super(list);
        }

        @Override
        public boolean isReadyToExecute(Node.Id node)
        {
            return contains(node);
        }

        @Override
        public boolean hasUniqueHlc()
        {
            return hasUniqueHlc;
        }

        @Override
        public void add(Node.Id node, ExecuteFlags flags)
        {
            hasUniqueHlc &= flags.contains(HAS_UNIQUE_HLC);
            if (flags.contains(READY_TO_EXECUTE))
                add(node);
        }
    }

    static class LargeCoordinationFlags extends SortedListSet.LargeSortedListSet<Node.Id> implements CoordinationFlags
    {
        boolean hasUniqueHlc = true;
        private LargeCoordinationFlags(SortedList<Node.Id> list)
        {
            super(list);
        }

        @Override
        public boolean isReadyToExecute(Node.Id node)
        {
            return contains(node);
        }

        @Override
        public boolean hasUniqueHlc()
        {
            return hasUniqueHlc;
        }

        @Override
        public void add(Node.Id node, ExecuteFlags flags)
        {
            hasUniqueHlc &= flags.contains(HAS_UNIQUE_HLC);
            if (flags.contains(READY_TO_EXECUTE))
                add(node);
        }
    }
}
