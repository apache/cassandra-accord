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

import java.util.List;
import java.util.stream.IntStream;

import com.google.common.primitives.Ints;
import org.junit.jupiter.api.Test;

import static accord.utils.Property.qt;
import static accord.utils.Utils.reduce;
import static org.assertj.core.api.Assertions.assertThat;

class UtilsTest
{
    @Test
    public void simpleReduce()
    {
        qt().forAll(Gens.arrays(Gens.ints().all()).ofSizeBetween(0, 10)).check(array -> {
            List<Integer> list = Ints.asList(array);

            // filter doesn't return non matches
            assertThat(reduce(true, list, v -> v % 2 == 0, (acc, v) -> acc & (v % 2 == 0))).isTrue();
            // count
            assertThat(reduce(0, list, i -> true, (acc, v) -> acc + 1)).isEqualTo(array.length);
            // sum
            assertThat(reduce(0, list, i -> true, (acc, v) -> acc + v)).isEqualTo(IntStream.of(array).sum());
        });
    }
}