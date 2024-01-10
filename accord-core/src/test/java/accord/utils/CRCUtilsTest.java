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

import org.junit.jupiter.api.Test;

import org.assertj.core.api.Assertions;

import static accord.utils.CRCUtils.crc32LittleEnding;
import static accord.utils.CRCUtils.reverseCRC32LittleEnding;
import static accord.utils.Property.qt;

class CRCUtilsTest
{
    @Test
    void backAndForth()
    {
        qt().withExamples(100_000).forAll(Gens.ints().all()).check(key -> {
            int hash = crc32LittleEnding(key);
            int unhash = reverseCRC32LittleEnding(hash);
            Assertions.assertThat(unhash).isEqualTo(key);
        });
    }
}