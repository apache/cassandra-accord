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

    @Test
    void largerHistory()
    {
        Assumptions.assumeTrue(ElleVerifier.Support.allowed(), "Elle doesn't support JDK 8");

        ElleVerifier verifier = new ElleVerifier();

        int id = 0;
        try (Verifier.Checker checker = verifier.witness(id++, 63))
        {
            checker.read(5, new int[] {});
            checker.write(5, 1);
            checker.read(6, new int[] {});
            checker.write(6, 2);
            checker.read(8, new int[] {});
        }
        try (Verifier.Checker checker = verifier.witness(id++, 75))
        {
            checker.read(6, new int[]{2});
        }
        try (Verifier.Checker checker = verifier.witness(id++, 79))
        {
            checker.read(5, new int[]{1});
        }
        try (Verifier.Checker checker = verifier.witness(id++, 83))
        {
            checker.read(6, new int[]{2});
            checker.read(7, new int[]{});
        }
        try (Verifier.Checker checker = verifier.witness(id++, 92))
        {
            checker.read(4, new int[]{});
            checker.write(4, 2);
        }
        try (Verifier.Checker checker = verifier.witness(id++, 94))
        {
            checker.read(5, new int[]{1});
            checker.read(4, new int[] {2});
            checker.write(4, 3);
            checker.read(8, new int[] {});
            checker.write(8, 2);
        }
        try (Verifier.Checker checker = verifier.witness(id++, 94))
        {
            checker.read(4, new int[]{2, 3});
            checker.read(7, new int[] {});
            checker.write(7, 2);
            checker.read(8, new int[] {2});
        }
        verifier.close();
    }

    @Test
    void readOnly()
    {
        Assumptions.assumeTrue(ElleVerifier.Support.allowed(), "Elle doesn't support JDK 8");
        ElleVerifier verifier = new ElleVerifier();
        try (Verifier.Checker checker = verifier.witness(3, 63))
        {
            checker.read(6, new int[] {});
        }
        try (Verifier.Checker checker = verifier.witness(60, 64))
        {
            checker.read(4, new int[] {});
        }
        verifier.close();
    }
}