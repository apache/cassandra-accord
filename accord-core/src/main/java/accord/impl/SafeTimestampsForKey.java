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

import accord.api.Key;
import accord.api.VisibleForImplementation;
import accord.primitives.Timestamp;
import accord.utils.Invariants;
import com.google.common.annotations.VisibleForTesting;

public abstract class SafeTimestampsForKey implements SafeState<TimestampsForKey>
{
    private final Key key;

    public SafeTimestampsForKey(Key key)
    {
        this.key = key;
    }

    protected abstract void set(TimestampsForKey update);

    public Key key()
    {
        return key;
    }

    private TimestampsForKey update(TimestampsForKey update)
    {
        set(update);
        return update;
    }

    public TimestampsForKey initialize()
    {
        return update(new TimestampsForKey(key));
    }

    @VisibleForTesting
    @VisibleForImplementation
    public static Timestamp updateMax(TimestampsForKey tfk, Timestamp timestamp)
    {
        Invariants.checkArgument(tfk != null || timestamp != null);
        if (tfk == null)
            return timestamp;
        if (timestamp == null)
            return tfk.max();
        return Timestamp.max(tfk.max(), timestamp);
    }

    @VisibleForTesting
    @VisibleForImplementation
    public <D> TimestampsForKey updateMax(Timestamp timestamp)
    {
        TimestampsForKey current = current();
        return update(new TimestampsForKey(current.key(),
                                           updateMax(current, timestamp),
                                           current.lastExecutedTimestamp(),
                                           current.rawLastExecutedHlc(),
                                           current.lastWriteTimestamp()));
    }

    <D> TimestampsForKey updateLastExecutionTimestamps(Timestamp lastExecutedTimestamp, long lastExecutedHlc, Timestamp lastWriteTimestamp)
    {
        TimestampsForKey current = current();
        return update(new TimestampsForKey(current.key(),
                                           current.max(),
                                           lastExecutedTimestamp,
                                           lastExecutedHlc,
                                           lastWriteTimestamp));
    }
}
