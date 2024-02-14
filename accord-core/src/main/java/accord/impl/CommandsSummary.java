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

import accord.local.SafeCommandStore.CommandFunction;
import accord.local.SafeCommandStore.TestDep;
import accord.local.SafeCommandStore.TestStartedAt;
import accord.local.SafeCommandStore.TestStatus;
import accord.primitives.Timestamp;
import accord.primitives.Txn.Kind.Kinds;
import accord.primitives.TxnId;

public interface CommandsSummary
{
    <P1, T> T mapReduceFull(TxnId testTxnId,
                            Kinds testKind,
                            TestStartedAt testStartedAt,
                            TestDep testDep,
                            TestStatus testStatus,
                            CommandFunction<P1, T, T> map, P1 p1, T initialValue);

    <P1, T> T mapReduceActive(Timestamp startedBefore,
                              Kinds testKind,
                              CommandFunction<P1, T, T> map, P1 p1, T initialValue);
}
