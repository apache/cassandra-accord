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

package accord.verify;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

class ElleVerifierTest
{
    @Test
    void simple()
    {
        Assumptions.assumeTrue(ElleVerifier.Support.allowed(), "Elle doesn't support JDK 8");

        ElleVerifier verifier = new ElleVerifier();
        try (Verifier.Checker checker = verifier.witness(0, 1))
        {
            checker.write(0, 1);
        }

        try (Verifier.Checker checker = verifier.witness(2, 3))
        {
            checker.read(0, new int[] {1});
        }
        verifier.close();
    }
}