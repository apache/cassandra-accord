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

package accord.burn;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import accord.utils.DefaultRandom;

import static accord.utils.Invariants.illegalArgument;

public class BurnTest extends BurnTestBase
{
    @Test
    public void testOne()
    {
//        run(System.nanoTime());
        run(396484948997916l);
    }

    public static void main(String[] args)
    {
        int count = 1;
        int operations = 1000;
        boolean reconcile = false;
        LongStream seeds = LongStream.generate(ThreadLocalRandom.current()::nextLong);
        boolean hasSetCount = false;
        for (int i = 0 ; i < args.length ; i += 2)
        {
            switch (args[i])
            {
                default: throw illegalArgument("Invalid option: " + args[i]);
                case "-c":
                    count = Integer.parseInt(args[i + 1]);
                    if (count < 0)
                        throw illegalArgument("Cannot override both seed (-s) and number of seeds to run (-c)");
                    hasSetCount = true;
                    break;
                case "-s":
                    seeds = Stream.of(args[i + 1].split(",")).mapToLong(Long::parseLong);
                    if (hasSetCount)
                        throw illegalArgument("Cannot override both seed (-s) and number of seeds to run (-c)");
                    count = -1;
                    break;
                case "-o":
                    operations = Integer.parseInt(args[i + 1]);
                    break;
                case "-r":
                    reconcile = true;
                    --i;
                    break;
                case "--loop-seed":
                    seeds = LongStream.generate(new DefaultRandom(Long.parseLong(args[i + 1]))::nextLong);
            }
        }

        if (count > 0)
            seeds = seeds.limit(count);

        int opCount = operations;
        seeds.forEach(reconcile ? seed -> reconcile(seed, opCount) : seed -> run(seed, opCount));
    }
}
