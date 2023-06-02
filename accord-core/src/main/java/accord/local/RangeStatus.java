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

public enum RangeStatus
{
    NOT_OWNED(4),

    /**
     * This command is owned by the associated store, and
     */
    LIVE(3),

    REDUNDANT(3),
    /**
     * The outcome for this command is durably recorded to all non-faulty replicas
     */
    DURABLE(2),

    /**
     * The local state for this command has been truncated,
     * i.e. we only retain the fact that we have processed the command.
     *
     * At present, this state can only be adopted once a transaction's keys have been
     * completely covered by TRUNCATED entries. These TRUNCATED entries
     *
     * TODO (desired): permit partial truncation
     * TODO (desired): permit truncation without globally durable status
     */
    TRUNCATED(1),
    ;

    /**
     * This is just values().length-ordinal(), but we reify it to ensure future refactors consider when refactoring
     */
    final int testerIndex;

    RangeStatus(int testerIndex)
    {
        this.testerIndex = testerIndex;
    }

    public static RangeStatus min(RangeStatus a, RangeStatus b)
    {
        return a.compareTo(b) <= 0 ? a : b;
    }

    public static RangeStatus max(RangeStatus a, RangeStatus b)
    {
        return a.compareTo(b) >= 0 ? a : b;
    }

    public static RangeStatus nonNullOrMin(RangeStatus a, RangeStatus b)
    {
        return a == null ? b : b == null ? a : a.compareTo(b) <= 0 ? a : b;
    }

    public static RangeStatus nonNullOrMax(RangeStatus a, RangeStatus b)
    {
        return a == null ? b : b == null ? a : a.compareTo(b) >= 0 ? a : b;
    }
}
