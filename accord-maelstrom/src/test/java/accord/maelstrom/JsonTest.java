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

package accord.maelstrom;

import org.junit.jupiter.api.Test;

import accord.api.Key;
import accord.primitives.Deps;
import accord.primitives.Range;
import accord.primitives.RangeDeps;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;

class JsonTest
{
    @Test
    void serdeDeps()
    {
        qt().forAll(depsGen()).check(JsonTest::serde);
    }

    private static <T> void serde(T expected)
    {
        String json = Json.GSON.toJson(expected);
        T parsed;
        try
        {
            parsed = Json.GSON.<T>fromJson(json, expected.getClass());
        }
        catch (Throwable t)
        {
            throw new AssertionError("Unable to parse json: " + json, t);
        }
        Assertions.assertThat(parsed)
                  .isEqualTo(expected);
    }

    static Gen<Key> keyGen()
    {
        Gen<Datum.Kind> kindGen = Gens.enums().all(Datum.Kind.class);
        Gen<String> strings = Gens.strings().all().ofLengthBetween(0, 10);
        return rs -> {
            Datum.Kind next = kindGen.next(rs);
            switch (next)
            {
                case STRING: return new MaelstromKey.Key(Datum.Kind.STRING, strings.next(rs));
                case LONG: return new MaelstromKey.Key(Datum.Kind.LONG, rs.nextLong());
                case HASH: return new MaelstromKey.Key(Datum.Kind.HASH, new Datum.Hash(rs.nextInt()));
                case DOUBLE: return new MaelstromKey.Key(Datum.Kind.DOUBLE, rs.nextDouble());
                default: throw new AssertionError("Unknown kind: " + next);
            }
        };
    }

    static Gen<Range> rangeGen()
    {
        return AccordGens.ranges(keyGen().map(Key::toUnseekable), MaelstromKey.Range::new);
    }

    static Gen<RangeDeps> rangeDepsGen()
    {
        return AccordGens.rangeDeps(rangeGen());
    }

    static Gen<Deps> depsGen()
    {
        return AccordGens.deps(AccordGens.keyDeps(keyGen()), rangeDepsGen(), AccordGens.directKeyDeps(keyGen()));
    }

}