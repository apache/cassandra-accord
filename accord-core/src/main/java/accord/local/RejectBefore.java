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

import java.util.Objects;

import accord.api.RoutingKey;
import accord.primitives.AbstractRanges;
import accord.primitives.Routables;
import accord.primitives.TxnId;
import accord.utils.ReducingIntervalMap;
import accord.utils.ReducingRangeMap;

public class RejectBefore extends ReducingRangeMap<TxnId>
{
    public static class SerializerSupport
    {
        public static RejectBefore create(boolean inclusiveEnds, RoutingKey[] ends, TxnId[] values)
        {
            return new RejectBefore(inclusiveEnds, ends, values);
        }
    }

    public RejectBefore()
    {
        super();
    }

    protected RejectBefore(boolean inclusiveEnds, RoutingKey[] starts, TxnId[] values)
    {
        super(inclusiveEnds, starts, values);
    }

    public static RejectBefore add(RejectBefore existing, AbstractRanges ranges, TxnId value)
    {
        RejectBefore add = create(ranges, value);
        return merge(existing, add);
    }

    public boolean rejects(TxnId txnId, Routables<?> participants)
    {
        return null == foldl(participants, (rejectIfBefore, test) -> rejects(rejectIfBefore, test) ? null : test, txnId, Objects::isNull);
    }

    private static boolean rejects(TxnId rejectIfBefore, TxnId test)
    {
        return rejectIfBefore.compareTo(test) > 0;
    }

    public static RejectBefore merge(RejectBefore historyLeft, RejectBefore historyRight)
    {
        return ReducingIntervalMap.merge(historyLeft, historyRight, TxnId::max, Builder::new);
    }

    public static RejectBefore create(AbstractRanges ranges, TxnId value)
    {
        if (value == null)
            throw new IllegalArgumentException("value is null");

        if (ranges.isEmpty())
            return new RejectBefore();

        return create(ranges, value, Builder::new);
    }

    static class Builder extends AbstractBoundariesBuilder<RoutingKey, TxnId, RejectBefore>
    {
        protected Builder(boolean inclusiveEnds, int capacity)
        {
            super(inclusiveEnds, capacity);
        }

        @Override
        protected RejectBefore buildInternal()
        {
            return new RejectBefore(inclusiveEnds, starts.toArray(new RoutingKey[0]), values.toArray(new TxnId[0]));
        }
    }
}
