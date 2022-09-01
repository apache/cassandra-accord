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

import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import static accord.verify.SerializabilityVerifierTest.Observation.r;
import static accord.verify.SerializabilityVerifierTest.Observation.rw;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SerializabilityVerifierTest
{
    static class Observation
    {
        final int write;
        final int[] reads;

        Observation(int write, int[] reads)
        {
            this.reads = reads;
            this.write = write;
        }

        static Observation w(int write)
        {
            return new Observation(write, null);
        }

        static Observation rw(int write, int ... reads)
        {
            return new Observation(write, reads);
        }

        static Observation r(int ... reads)
        {
            return new Observation(-1, reads);
        }
    }

    @Test
    public void noCycle()
    {
        assertNoViolation(new Observation[] { r( ), r( ) },
                          new Observation[] { r( ), r(1) },
                          new Observation[] { r(1), r(1) });
        assertNoViolation(new Observation[] { r(   ), r(   ) },
                          new Observation[] { r(   ), r(1,2) },
                          new Observation[] { r(1,2), r(1,2) });
        assertNoViolation(new Observation[] { r(   ), r(   ) },
                          new Observation[] { r(   ), r(1  ) },
                          new Observation[] { r(1  ), r(1  ) },
                          new Observation[] { r(1,2), r(1  ) },
                          new Observation[] { r(1,2), r(1,2) });
    }

    @Test
    public void directCycle()
    {
        assertViolation(new Observation[] { r(0), r( ) },
                        new Observation[] { r( ), r(1) });
        assertViolation(new Observation[] { r(0), r( ), r( ) },
                        new Observation[] { r( ), r(1), r( ) });
    }

    @Test
    public void indirectCycle()
    {
        assertViolation(new Observation[] { r(0), r( ), r( ) },
                        new Observation[] { r( ), r(1), r( ) },
                        new Observation[] { r( ), r( ), r(2) });
        assertViolation(new Observation[] { r( ), r(1), null },
                        new Observation[] { null, r( ), r(2) },
                        new Observation[] { r(0), null, r( ) });
        assertViolation(new Observation[] { r(   ), r(1,2), r(   ) },
                        new Observation[] { r(   ), r(   ), r(2,3) },
                        new Observation[] { r(0,1), r(   ), r(   ) });
        assertViolation(new Observation[] { r(0), r( ), r( ), r( ), r( ) },
                        new Observation[] { r( ), r(1), r( ), r( ), r( ) },
                        new Observation[] { r( ), r( ), r(2), r( ), r( ) },
                        new Observation[] { r( ), r( ), r( ), r(3), r( ) },
                        new Observation[] { r( ), r( ), r( ), r( ), r(4) });
    }

    @Test
    public void writeCycle()
    {
        assertViolation(new Observation[] { r(), rw(1) },
                        new Observation[] { r(0), r(1)});
        assertViolation(new Observation[] { r(),     rw(1) },
                        new Observation[] { r(0, 1), r(1)});
        assertViolation(new Observation[] { r(),     rw(1) },
                        new Observation[] { r(0, 1), r(1, 2)});
    }

    private static void assertViolation(int[][] ... setOfObservations)
    {
        assertThrows(HistoryViolation.class, () -> {
            SerializabilityVerifier verifier = new SerializabilityVerifier(setOfObservations[0].length);
            for (int[][] observations : setOfObservations)
            {
                verifier.begin();
                for (int i = 0 ; i < observations.length ; ++i)
                {
                    int[] observation = observations[i];
                    if (observation != null)
                        verifier.witnessRead(i, observation);
                }
                verifier.apply();
            }
        });
    }

    private static void run(Observation[][] setOfObservations)
    {
        SerializabilityVerifier verifier = new SerializabilityVerifier(setOfObservations[0].length);
        for (Observation[] observations : setOfObservations)
        {
            verifier.begin();
            for (int key = 0; key < observations.length; ++key)
            {
                Observation observation = observations[key];
                if (observation != null && observation.reads != null)
                    verifier.witnessRead(key, observation.reads);
                if (observation != null && observation.write >= 0)
                    verifier.witnessWrite(key, observation.write);
            }
            verifier.apply();
        }
    }

    private static void forEach(Observation[][] permute, Consumer<Observation[][]> forEach)
    {
        Observation[][] permuted = new Observation[permute.length][];
        for (int offset = 0 ; offset < permute.length ; ++offset)
        {
            // TODO: more permutations
            for (int i = 0 ; i < permute.length ; ++i)
                permuted[i] = permute[(offset + i) % permute.length];

            forEach.accept(permuted);
        }
    }

    private static void assertNoViolation(Observation[] ... setOfObservations)
    {
        forEach(setOfObservations, SerializabilityVerifierTest::run);
    }

    private static void assertViolation(Observation[] ... setOfObservations)
    {
        forEach(setOfObservations, permuted -> assertThrows(HistoryViolation.class, () -> run(permuted)));
    }

}
