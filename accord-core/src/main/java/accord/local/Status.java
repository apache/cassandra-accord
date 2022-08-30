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

public enum Status
{
    NotWitnessed(0),
    PreAccepted(10),
    Accepted(20),
    AcceptedInvalidate(20),
    Committed(30),
    ReadyToExecute(40),
    Executed(50),
    Applied(60),
    Invalidated(60);

    final int logicalOrdinal;

    Status(int logicalOrdinal)
    {
        this.logicalOrdinal = logicalOrdinal;
    }

    // equivalent to compareTo except Accepted and AcceptedInvalidate sort equal
    public int logicalCompareTo(Status that)
    {
        return this.logicalOrdinal - that.logicalOrdinal;
    }

    public static Status max(Status a, Status b)
    {
        return a.compareTo(b) >= 0 ? a : b;
    }

    public boolean hasBeen(Status equalOrGreaterThan)
    {
        return compareTo(equalOrGreaterThan) >= 0;
    }
}
