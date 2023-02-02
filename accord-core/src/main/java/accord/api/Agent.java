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

package accord.api;

import accord.local.Command;
import accord.local.Node;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

/**
 * Facility for augmenting node behaviour at specific points
 */
public interface Agent extends UncaughtExceptionListener
{
    /**
     * For use by implementations to decide what to do about successfully recovered transactions.
     * Specifically intended to define if and how they should inform clients of the result.
     * e.g. in Maelstrom we send the full result directly, in other impls we may simply acknowledge success via the coordinator
     *
     * Note: may be invoked multiple times in different places
     */
    void onRecover(Node node, Result success, Throwable fail);

    /**
     * For use by implementations to decide what to do about timestamp inconsistency, i.e. two different timestamps
     * committed for the same transaction. This is a protocol consistency violation, potentially leading to non-linearizable
     * histories. In test cases this is used to fail the transaction, whereas in real systems this likely will be used for
     * reporting the violation, as it is no more correct at this point to refuse the operation than it is to complete it.
     *
     * Should throw an exception if the inconsistent timestamp should not be applied
     */
    void onInconsistentTimestamp(Command command, Timestamp prev, Timestamp next);

    @Override
    void onUncaughtException(Throwable t);

    void onHandledException(Throwable t);

    boolean isExpired(TxnId initiated, long now);
}
