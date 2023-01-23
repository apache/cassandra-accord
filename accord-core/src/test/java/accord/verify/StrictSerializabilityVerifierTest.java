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
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
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
            return rw(writeKey, writeSeq, (int[])null);
        }

        History ws(int ... writes)
        {
            return rw(writes, (int[])null);
        }

        History rw(int writeKey, int writeSeq, int ... reads)
        {
            int[] writes;
            if (writeKey < 0)
            {
                writes = new int[0];
            }
            else
            {
                writes = new int[writeKey + 1];
                Arrays.fill(writes, -1);
                writes[writeKey] = writeSeq;
            }
            return rw(writes, reads);
        }

        History rw(int[] writes, int ... reads)
        {
            int start = overlap > 0 ? this.overlap : ++nextAt;
            int end = ++nextAt;
            observations.add(new Observation(writes, reads, start, end));
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
                for (int j = 0 ; j < observation.writes.length ; ++j)
                {
                    if (observation.writes[j] >= 0)
                        verifier.witnessWrite(j, observation.writes[j]);
                }

                if (i == observations.size() - 1) onLast.accept(() -> verifier.apply(observation.start, observation.end));
                else verifier.apply(observation.start, observation.end);
            }
        }
    }

    static class Observation
    {
        final int[] writes;
        final int[] reads;
        final int start, end;

        Observation(int[] writes, int[] reads, int start, int end)
        {
            this.writes = writes;
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

    @Test
    public void blindWrites()
    {
        new History(2).ws(1, 1).r(1, -1).ws(-1, 2).assertNoViolation();
        new History(2).ws(1, 1).ws(-1, 2).assertNoViolation();
        new History(2).ws(1, 1).ws(2, 2).r(2).assertNoViolation();
        new History(2).ws(1, 1).ws(2, 2).r(2).r(1).assertViolation();
        new History(2).ws(1, 1).overlap().ws(2, 2).r(2).r(1).deoverlap().r(2).assertNoViolation();
        new History(2).ws(1, 1).ws(2, 2).r(1).assertViolation();
    }

    private void fromLog(String log)
    {
        IntSet pks = new IntHashSet();
        class Read
        {
            final int pk, id, count;
            final int[] seq;

            Read(int pk, int id, int count, int[] seq)
            {
                this.pk = pk;
                this.id = id;
                this.count = count;
                this.seq = seq;
            }
        }
        class Write
        {
            final int pk, id;
            final boolean success;

            Write(int pk, int id, boolean success)
            {
                this.pk = pk;
                this.id = id;
                this.success = success;
            }
        }
        class Witness
        {
            final int start, end;
            final List<Object> actions = new ArrayList<>();

            Witness(int start, int end)
            {
                this.start = start;
                this.end = end;
            }

            public void read(int pk, int id, int count, int[] seq)
            {
                actions.add(new Read(pk, id, count, seq));
            }

            public void write(int pk, int id, boolean success)
            {
                actions.add(new Write(pk, id, success));
            }

            public void process(StrictSerializabilityVerifier verifier)
            {
                verifier.begin();
                for (Object a : actions)
                {
                    if (a instanceof Read)
                    {
                        Read read = (Read) a;
                        verifier.witnessRead(read.pk, read.seq);
                    }
                    else
                    {
                        Write write = (Write) a;
                        if (write.success)
                            verifier.witnessWrite(write.pk, write.id);
                    }
                }
                verifier.apply(start, end);
            }
        }
        List<Witness> witnesses = new ArrayList<>();
        Witness current = null;
        for (String line : log.split("\n"))
        {
            if (line.startsWith("Witness"))
            {
                if (current != null)
                {
                    witnesses.add(current);
                    current = null;
                }
                Matcher matcher = Pattern.compile("Witness\\(start=(.+), end=(.+)\\)").matcher(line);
                if (!matcher.find()) throw new AssertionError("Unable to match start/end of " + line);
                current = new Witness(Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2)));
            }
            else if (line.startsWith("\tread"))
            {
                Matcher matcher = Pattern.compile("\tread\\(pk=(.+), id=(.+), count=(.+), seq=\\[(.*)\\]\\)").matcher(line);
                if (!matcher.find()) throw new AssertionError("Unable to match read of " + line);
                int pk = Integer.parseInt(matcher.group(1));
                pks.add(pk);
                int id = Integer.parseInt(matcher.group(2));
                int count = Integer.parseInt(matcher.group(3));
                String seqStr = matcher.group(4);
                int[] seq = seqStr.isEmpty() ? new int[0] : Stream.of(seqStr.split(",")).map(String::trim).mapToInt(Integer::parseInt).toArray();
                current.read(pk, id, count, seq);
            }
            else if (line.startsWith("\twrite"))
            {
                Matcher matcher = Pattern.compile("\twrite\\(pk=(.+), id=(.+), success=(.+)\\)").matcher(line);
                if (!matcher.find()) throw new AssertionError("Unable to match write of " + line);
                int pk = Integer.parseInt(matcher.group(1));
                pks.add(pk);
                int id = Integer.parseInt(matcher.group(2));
                boolean success = Boolean.parseBoolean(matcher.group(3));
                current.write(pk, id, success);
            }
            else
            {
                throw new IllegalArgumentException("Unknow line: " + line);
            }
        }
        if (current != null)
        {
            witnesses.add(current);
            current = null;
        }
        int[] keys = pks.toArray();
        Arrays.sort(keys);
        StrictSerializabilityVerifier validator = new StrictSerializabilityVerifier(3);
        for (Witness w : witnesses)
        {
            w.process(validator);
        }
    }

    @Test
    public void seenBehavior()
    {
        fromLog("Witness(start=4, end=7)\n" +
                "\tread(pk=0, id=2, count=0, seq=[])\n" +
                "\twrite(pk=0, id=2, success=true)\n" +
                "Witness(start=3, end=8)\n" +
                "\tread(pk=2, id=0, count=0, seq=[])\n" +
                "\twrite(pk=2, id=0, success=true)\n" +
                "\twrite(pk=1, id=0, success=true)\n" +
                "Witness(start=5, end=9)\n" +
                "\tread(pk=0, id=3, count=1, seq=[2])\n" +
                "\twrite(pk=0, id=3, success=true)\n" +
                "Witness(start=2, end=10)\n" +
                "\twrite(pk=2, id=1, success=true)\n" +
                "\twrite(pk=1, id=1, success=true)\n" +
                "Witness(start=6, end=11)\n" +
                "\tread(pk=0, id=4, count=2, seq=[2, 3])\n" +
                "\twrite(pk=0, id=4, success=true)\n" +
                "Witness(start=12, end=14)\n" +
                "\twrite(pk=0, id=5, success=true)\n" +
                "Witness(start=13, end=16)\n" +
                "\tread(pk=1, id=6, count=2, seq=[0, 1])\n" +
                "\twrite(pk=1, id=6, success=true)\n" +
                "\twrite(pk=2, id=6, success=true)\n" +
                "Witness(start=15, end=18)\n" +
                "\tread(pk=0, id=7, count=4, seq=[2, 3, 4, 5])\n" +
                "\twrite(pk=0, id=7, success=true)\n" +
                "Witness(start=17, end=20)\n" +
                "\tread(pk=1, id=8, count=3, seq=[0, 1, 6])\n" +
                "\twrite(pk=1, id=8, success=true)\n" +
                "\twrite(pk=2, id=8, success=true)\n");
    }
}
