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

import accord.local.RedundantStatus.SomeStatus;

import static accord.local.RedundantStatus.SomeStatus.LOG_INCOMPLETE_ONLY;
import static accord.local.RedundantStatus.SomeStatus.LOG_UNAVAILABLE_ONLY;
import static accord.local.RedundantStatus.SomeStatus.UNREADY_ONLY;

public enum BootstrapReason
{
    GAIN_OWNERSHIP(UNREADY_ONLY),

    /**
     * Our local log is intact, but we cannot catchup with peers; no need to mark local records unsafe
     * for consensus decisions, but we must mark the ranges as potentially not executable
     */
    CATCHUP(UNREADY_ONLY),

    /**
     * Marks pre-rebootstrap transactions with LOG_INCOMPLETE, which means the node cannot
     * safely participate in pre-rebootstrap transactions for which the decision is not known
     * _even_ if they arrive after the node is done bootstrapping.
     */
    LOG_INCOMPLETE(LOG_INCOMPLETE_ONLY),

    /**
     * Marks pre-rebootstrap transactions with LOG_UNAVAILABLE, which means the node cannot
     * safely participate in any pre-rebootstrap transactions.
     */
    LOG_CORRUPTED(LOG_UNAVAILABLE_ONLY);

    final SomeStatus redundantStatus;

    BootstrapReason(SomeStatus redundantStatus)
    {
        this.redundantStatus = redundantStatus;
    }
}
