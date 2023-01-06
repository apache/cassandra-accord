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
import accord.primitives.Routable.Domain;
import accord.primitives.Txn.Kind;

import static accord.primitives.Txn.Kind.Read;
import static accord.primitives.Txn.Kind.Write;

public class TxnId extends Timestamp
{
    public static TxnId fromBits(long msb, long lsb, Id node)
    {
        return new TxnId(msb, lsb, node);
    }

    public static TxnId fromValues(long epoch, long hlc, Id node)
    {
        return new TxnId(epoch, hlc, 0, node);
    }

    public static TxnId fromValues(long epoch, long hlc, int flags, Id node)
    {
        return new TxnId(epoch, hlc, flags, node);
    }

    public TxnId(Timestamp timestamp, Kind rw, Domain domain)
    {
        super(timestamp, flags(rw, domain));
    }

    public TxnId(long epoch, long hlc, Kind rw, Domain domain, Id node)
    {
        this(epoch, hlc, flags(rw, domain), node);
    }

    private TxnId(long epoch, long hlc, int flags, Id node)
    {
        super(epoch, hlc, flags, node);
    }

    private TxnId(long msb, long lsb, Id node)
    {
        super(msb, lsb, node);
    }

    public boolean isWrite()
    {
        return rwOrdinal(flags()) == Write.ordinal();
    }

    public boolean isRead()
    {
        return rwOrdinal(flags()) == Read.ordinal();
    }

    public Kind rw()
    {
        return rw(flags());
    }

    public Domain domain()
    {
        return domain(flags());
    }

    @Override
    public TxnId merge(Timestamp that)
    {
        return merge(this, that, TxnId::fromBits);
    }

    private static int flags(Kind rw, Domain domain)
    {
        return flags(rw) | flags(domain);
    }

    private static int flags(Kind rw)
    {
        return rw.ordinal() << 1;
    }

    private static int flags(Domain domain)
    {
        return domain.ordinal();
    }

    private static Kind rw(int flags)
    {
        return Kind.ofOrdinal(rwOrdinal(flags));
    }

    private static Domain domain(int flags)
    {
        return Domain.ofOrdinal(domainOrdinal(flags));
    }

    private static int rwOrdinal(int flags)
    {
        return (flags >> 1) & 1;
    }

    private static int domainOrdinal(int flags)
    {
        return flags & 1;
    }
}
