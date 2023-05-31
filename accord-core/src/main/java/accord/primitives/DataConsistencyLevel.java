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

package accord.primitives;

/**
 * The consistency level Accord should use when reading/writing non-Accord data
 * For example when reading non-Accord data it must be read at this CL
 * When read repairing non-Accord data it must be persisted at this CL
 *
 * If a CL other than UNSPECIFIED is requested for write then persistence must also be synchronous
 * so that it is globally visible outside of Accord.
 *
 * A read consistency level other than UNSPECIFIED means that merging of read data needs to perform a real merge
 * and not a per key overwrite that assumes the result for each key is the same.
 *
 * INVALID indicates this is a read of Accord metadata which is mostly for clarity as the correct behavior
 * is inferred in other ways at the moment rather than from this enum value.
 */
public enum DataConsistencyLevel
{
    // TODO ONE is silently upgraded to QUORUM
    // The concept of a CL doesn't apply because it's not a data read
    // Used when reading Accord metadata
    INVALID(false, false, 0),
    // A majority must be contacted and digest requests may be sent
    // Blocking read repair should also be performed if replicas disagree to provide monotonic reads
    // If the transaction is a write and this is the write consistency level then synchronous apply should be performed
    // Synchronous apply will always be performed if read repair is necessary
    // It is not required for Accord to honor a specific consistency level such as when
    // only Accord is reading/writing this data
    UNSPECIFIED(false, false, 1),
    QUORUM(true, true, 2),
    ALL(true, true, 3);


    public final boolean requiresDigestReads;
    public final boolean requiresSynchronousCommit;

    public final int index;

    DataConsistencyLevel(boolean requiresDigestReads, boolean requiresSynchronousCommit, int index)
    {
        this.requiresDigestReads = requiresDigestReads;
        this.requiresSynchronousCommit = requiresSynchronousCommit;
        this.index = index;
    }

    public static DataConsistencyLevel max(DataConsistencyLevel a, DataConsistencyLevel b)
    {
        if (a.index < b.index)
            return b;
        else
            return a;
    }
}
