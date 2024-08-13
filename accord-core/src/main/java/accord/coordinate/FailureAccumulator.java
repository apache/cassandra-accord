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

import java.util.function.Predicate;
import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.primitives.Ranges;
import accord.primitives.TxnId;

public class FailureAccumulator
{
    private FailureAccumulator() {}

    public static Throwable append(@Nullable Throwable current, Throwable next)
    {
        return append(current, next, FailureAccumulator::isTimeout);
    }

    public static Throwable append(@Nullable Throwable current, Throwable next, Predicate<Throwable> isTimeout)
    {
        if (current == null) return next;
        // when a non-timeout is seen make sure it shows up in current rather than timeout
        // this is so checking if the cause is a timeout is able to do a single check rather
        // than walk the whole chain
        if (isTimeout.test(current) && !(isTimeout.test(next)))
        {
            Throwable tmp = current;
            current = next;
            next = tmp;
        }
        current.addSuppressed(next);
        return current;
    }

    public static boolean isTimeout(@Nullable Throwable current)
    {
        return current instanceof Timeout;
    }

    public static CoordinationFailed createFailure(@Nullable Throwable current, TxnId txnId, @Nullable RoutingKey homeKey)
    {
        return createFailure(current, txnId, homeKey, null);
    }

    public static CoordinationFailed createFailure(@Nullable Throwable current, TxnId txnId, @Nullable RoutingKey homeKey, @Nullable Ranges unavailable)
    {
        if (current == null) return new Exhausted(txnId, homeKey, unavailable);
        if (isTimeout(current)) return new Timeout(txnId, homeKey);
        Exhausted e = new Exhausted(txnId, homeKey, unavailable);
        e.initCause(current);
        return e;
    }
}
