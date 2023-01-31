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

package accord.utils;

import java.util.Random;

public interface JDKRandomAPI
{
    void nextBytes(byte[] bytes);

    boolean nextBoolean();

    int nextInt();
    int nextInt(int bound);
    long nextLong();
    float nextFloat();
    double nextDouble();
    double nextGaussian();

    void setSeed(long seed);
    
    default Random asJdkRandom()
    {
        return new Random() {
            @Override
            public void setSeed(long seed) {
                JDKRandomAPI.this.setSeed(seed);
            }

            @Override
            public void nextBytes(byte[] bytes) {
                JDKRandomAPI.this.nextBytes(bytes);
            }

            @Override
            public int nextInt() {
                return JDKRandomAPI.this.nextInt();
            }

            @Override
            public int nextInt(int bound) {
                return JDKRandomAPI.this.nextInt(bound);
            }

            @Override
            public long nextLong() {
                return JDKRandomAPI.this.nextLong();
            }

            @Override
            public boolean nextBoolean() {
                return JDKRandomAPI.this.nextBoolean();
            }

            @Override
            public float nextFloat() {
                return JDKRandomAPI.this.nextFloat();
            }

            @Override
            public double nextDouble() {
                return JDKRandomAPI.this.nextDouble();
            }

            @Override
            public double nextGaussian() {
                return JDKRandomAPI.this.nextGaussian();
            }
        };
    }
}
