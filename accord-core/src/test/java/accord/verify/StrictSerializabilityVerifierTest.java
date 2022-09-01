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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class StrictSerializabilityVerifierTest
{
    private static final int[][] SEQUENCES = new int[][] { null, new int[]{ }, new int[] { 1 }, new int[] { 1, 2 }, new int[] { 1, 2, 3 } };

    static class History
    {
        final int keys;
        int overlap, nextAt;
        List<Observation> observations = new ArrayList<>();

        History(int keys)
        {
            this.keys = keys;
        }

        History w(int writeKey, int writeSeq)
        {
            return rw(writeKey, writeSeq, null);
        }

        History rw(int writeKey, int writeSeq, int ... reads)
        {
            int start = overlap > 0 ? this.overlap : ++nextAt;
            int end = ++nextAt;
            observations.add(new Observation(writeKey, writeSeq, reads, start, end));
            return this;
        }

        History overlap()
        {
            overlap = ++nextAt;
            return this;
        }

        History deoverlap()
        {
            overlap = 0;
            return this;
        }

        History r(int ... reads)
        {
            return rw(-1, -1, reads);
        }

        void assertNoViolation()
        {
            run(Runnable::run);
        }

        void assertViolation()
        {
            run(runnable -> assertThrows(HistoryViolation.class, runnable::run));
        }

        private void run(Consumer<Runnable> onLast)
        {
            StrictSerializabilityVerifier verifier = new StrictSerializabilityVerifier(keys);
            for (int i = 0 ; i < observations.size() ; ++i)
            {
                Observation observation = observations.get(i);
                verifier.begin();
                if (observation.reads != null)
                {
                    for (int key = 0 ; key < observation.reads.length ; ++key)
                    {
                        if (observation.reads[key] >= 0)
                            verifier.witnessRead(key, SEQUENCES[observation.reads[key] + 1]);
                    }
                }
                if (observation.writeKey >= 0)
                    verifier.witnessWrite(observation.writeKey, observation.writeSeq);

                if (i == observations.size() - 1) onLast.accept(() -> verifier.apply(observation.start, observation.end));
                else verifier.apply(observation.start, observation.end);
            }
        }
    }

    static class Observation
    {
        final int writeKey;
        final int writeSeq;
        final int[] reads;
        final int start, end;

        Observation(int writeKey, int writeSeq, int[] reads, int start, int end)
        {
            this.writeKey = writeKey;
            this.writeSeq = writeSeq;
            this.reads = reads;
            this.start = start;
            this.end = end;
        }
    }

    @Test
    public void lostRead()
    {
        new History(1).r(1).r(0).assertViolation();
        new History(1).r(2).r(1).assertViolation();
        new History(2).r(1, 2).r(2, 1).assertViolation();
    }

    @Test
    public void lostWrite()
    {
        new History(1).w(0, 1).r(0).assertViolation();
        new History(1).rw(0, 1, 0).r(0).assertViolation();
    }

    @Test
    public void readBeforeWrite()
    {
        new History(1).r(1).rw(0, 1, 0).assertViolation();
        new History(1).r(1).w(0, 1).assertViolation();
    }

    @Test
    public void readCycle()
    {
        new History(2).overlap().r(0, 1).r(1, 0).assertViolation();
        new History(3).overlap().r(1, 0, -1).r(-1, 1, 0).r(0, -1, 1).assertViolation();
        new History(4).overlap().r(1, 0, -1, -1).r(-1, 1, 0, -1).r(-1, -1, 1, 0).r(0, -1, -1, 1).assertViolation();
    }

    @Test
    public void readWriteCycle()
    {
        new History(2).overlap().rw(0, 1, -1, 0).rw(1, 1, 1, -1).assertNoViolation();
        new History(2).overlap().rw(0, 1, -1, 1).rw(1, 1, 0, -1).assertNoViolation();
        new History(2).overlap().rw(0, 1, -1, 0).rw(1, 1, 0, -1).assertViolation();
        new History(2).overlap().rw(0, 1, -1, 0).w(1, 1).r(0, 1).assertViolation();
    }

    @Test
    public void simpleWrite()
    {
        new History(1).r(0).w(0, 1).r(1).assertNoViolation();
        new History(1).rw(0, 1, 0).r(1).assertNoViolation();
    }
}
