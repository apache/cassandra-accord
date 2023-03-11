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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import org.junit.jupiter.api.Test;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.local.Node;
import accord.primitives.Deps;
import accord.primitives.KeyDeps;
import accord.primitives.Range;
import accord.primitives.RangeDeps;
import accord.primitives.Routable;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
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

    public static Gen.LongGen epochs()
    {
        return Gens.longs().between(0, Timestamp.MAX_EPOCH);
    }

    public static Gen<TxnId> ids()
    {
        return ids(epochs()::nextLong, RandomSource::nextLong, RandomSource::nextInt);
    }

    public static Gen<TxnId> ids(ToLongFunction<RandomSource> epochs, ToLongFunction<RandomSource> hlcs, ToIntFunction<RandomSource> nodes)
    {
        Gen<Txn.Kind> kinds = Gens.enums().all(Txn.Kind.class);
        Gen<Routable.Domain> domains = Gens.enums().all(Routable.Domain.class);
        return rs -> new TxnId(epochs.applyAsLong(rs), hlcs.applyAsLong(rs), kinds.next(rs), domains.next(rs), new Node.Id(nodes.applyAsInt(rs)));
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
//        return rs -> new IntKey.Raw(rs.nextInt());
    }

    static Gen<KeyDeps> keyDepsGen()
    {
        Gen<TxnId> idGen = ids();
        Gen<Key> keyGen = keyGen();
        double emptyProb = .2D;
        return rs -> {
            if (rs.decide(emptyProb)) return KeyDeps.NONE;
            Set<Key> seenKeys = new HashSet<>();
            Set<TxnId> seenTxn = new HashSet<>();
            Gen<Key> uniqKeyGen = keyGen.filter(seenKeys::add);
            Gen<TxnId> uniqIdGen = idGen.filter(seenTxn::add);
            try (KeyDeps.Builder builder = KeyDeps.builder())
            {
                for (int i = 0, numKeys = rs.nextInt(1, 10); i < numKeys; i++)
                {
                    builder.nextKey(uniqKeyGen.next(rs));
                    seenTxn.clear();
                    for (int j = 0, numTxn = rs.nextInt(1, 10); j < numTxn; j++)
                        builder.add(uniqIdGen.next(rs));
                }
                return builder.build();
            }
        };
    }

//    static Gen<Range> rangeGen()
//    {
//        Gen<RoutingKey> keyGen = keyGen().map(Key::toUnseekable);
//        RoutingKey[] keys = new RoutingKey[2];
//        return rs -> {
//            keys[0] = keyGen.next(rs);
//            // range doesn't allow a=b
//            do keys[1] = keyGen.next(rs);
//            while (Objects.equals(keys[0], keys[1]));
//            Arrays.sort(keys);
//            boolean left = rs.nextBoolean();
//            return Range.range(keys[0], keys[1], left, !left);
//        };
//    }
    static Gen<Range> rangeGen()
    {
        Gen<RoutingKey> keyGen = keyGen().map(Key::toUnseekable);
        RoutingKey[] keys = new RoutingKey[2];
        return rs -> {
            keys[0] = keyGen.next(rs);
            // range doesn't allow a=b
            do
            {
                keys[1] = keyGen.next(rs);
            }
            while (Objects.equals(keys[0], keys[1]));
            Arrays.sort(keys);
            return new MaelstromKey.Range(keys[0], keys[1]);
        };
    }

    static Gen<RangeDeps> rangeDepsGen()
    {
        return Gens.constant(RangeDeps.NONE);
//        Gen<TxnId> idGen = ids();
//        Gen<Range> rangeGen = rangeGen();
//        double emptyProb = .2D;
//        return rs -> {
//            if (rs.decide(emptyProb)) return RangeDeps.NONE;
//            RangeDeps.Builder builder = RangeDeps.builder();
//            for (int i = 0, numKeys = rs.nextInt(1, 10); i < numKeys; i++)
//            {
//                builder.nextKey(rangeGen.next(rs));
//                for (int j = 0, numTxn = rs.nextInt(1, 10); j < numTxn; j++)
//                    builder.add(idGen.next(rs));
//            }
//            return builder.build();
//        };
    }

    static Gen<Deps> depsGen()
    {
        Gen<KeyDeps> keyDepsGen = keyDepsGen();
        Gen<RangeDeps> rangeDepsGen = rangeDepsGen();
        return rs -> new Deps(keyDepsGen.next(rs), rangeDepsGen.next(rs));
    }
}