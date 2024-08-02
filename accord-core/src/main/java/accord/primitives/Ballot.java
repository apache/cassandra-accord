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

import accord.local.Node.Id;

public class Ballot extends Timestamp
{
    public static Ballot fromBits(long msb, long lsb, Id node)
    {
        if (msb == 0 && lsb == 0 && node.equals(Id.NONE))
            return Ballot.ZERO;

        return new Ballot(msb, lsb, node);
    }

    public static Ballot fromValues(long epoch, long hlc, Id node)
    {
        return fromValues(epoch, hlc, 0, node);
    }

    public static Ballot fromValues(long epoch, long hlc, int flags, Id node)
    {
        if (epoch == 0 && hlc == 0 && flags == 0 && node.equals(Id.NONE))
            return ZERO;

        return new Ballot(epoch, hlc, flags, node);
    }

    public static final Ballot ZERO = new Ballot(Timestamp.NONE);
    public static final Ballot MAX = new Ballot(Timestamp.MAX);

    public Ballot(Timestamp from)
    {
        super(from);
    }

    Ballot(long epoch, long hlc, int flags, Id node)
    {
        super(epoch, hlc, flags, node);
    }

    Ballot(long msb, long lsb, Id node)
    {
        super(msb, lsb, node);
    }

    @Override
    public Ballot merge(Timestamp that)
    {
        return merge(this, that, Ballot::fromBits);
    }
}
