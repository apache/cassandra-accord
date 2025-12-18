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


import java.util.BitSet;
import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import org.assertj.core.api.Assertions;

import static accord.utils.Property.commands;
import static accord.utils.Property.stateful;

class SimpleBitSetTest
{
    private static Property.Command<State, Void, ?> get(RandomSource rs, State state)
    {
        int idx = rs.nextInt(0, state.size);
        return new Property.SimpleCommand<>("Get(" + idx + ')', s2 -> s2.get(idx));
    }

    private static Property.Command<State, Void, ?> set(RandomSource rs, State state)
    {
        int idx = rs.nextInt(0, state.size);
        return new Property.SimpleCommand<>("Set(" + idx + ')', s2 -> s2.set(idx));
    }

    private static Property.Command<State, Void, ?> setRange(RandomSource rs, State state)
    {
        int from = rs.nextInt(0, state.size);
        int to = rs.nextInt(0, state.size + 1); // do not filter from == to as this "must" no-op
        if (from > to)
        {
            int tmp = from;
            from = to;
            to = tmp;
        }
        int finalFrom = from;
        int finalTo = to;
        return new Property.SimpleCommand<>("setRange(" + from + ", " + to + ')', s2 -> s2.setRange(finalFrom, finalTo));
    }

    private static Property.Command<State, Void, ?> unset(RandomSource rs, State state)
    {
        int idx = rs.nextInt(0, state.size);
        return new Property.SimpleCommand<>("Unset(" + idx + ')', s2 -> s2.unset(idx));
    }

    private static Property.Command<State, Void, ?> clear(RandomSource rs, State state)
    {
        return new Property.SimpleCommand<>("Clear", State::clear);
    }

    private static Property.Command<State, Void, ?> isEmpty(RandomSource rs, State state)
    {
        return new Property.SimpleCommand<>("Is Empty", State::isEmpty);
    }

    private static Property.Command<State, Void, ?> nextSetBit(RandomSource rs, State state)
    {
        int idx = rs.nextInt(0, state.size);
        return new Property.SimpleCommand<>("NextSetBit(" + idx + ')', s2 -> s2.nextSetBit(idx));
    }

    private static Property.Command<State, Void, ?> getSetBitCount(RandomSource rs, State state)
    {
        return new Property.SimpleCommand<>("getSetBitCount()", s2 -> s2.getSetBitCount());
    }

    @Test
    public void test()
    {
        stateful().withExamples(1000).check(commands(() -> State::new)
                                            .add(SimpleBitSetTest::get)
                                            .add(SimpleBitSetTest::set)
                                            .add(SimpleBitSetTest::setRange)
                                            .add(SimpleBitSetTest::unset)
                                            .add(SimpleBitSetTest::clear)
                                            .add(SimpleBitSetTest::isEmpty)
                                            .add(SimpleBitSetTest::nextSetBit)
                                            .add(SimpleBitSetTest::getSetBitCount)
                                            .build());
    }

    @Test
    public void outOfRange()
    {
        SmallBitSet model = new SmallBitSet();
        LargeBitSet target = new LargeBitSet(64);

        for (var set : List.of(model, target))
            set.setRange(0, 64);

        // get methods do not reject, but return false or -1
        Assertions.assertThat(target.getSetBitCount()).isEqualTo(model.getSetBitCount());
        Assertions.assertThat(target.isEmpty()).isEqualTo(model.isEmpty());

        Assertions.assertThat(target.nextSetBit(64))
                .describedAs("nextSetBit(%s)", 64)
                .isEqualTo(model.nextSetBit(64))
                .isEqualTo(-1);

        // set methods reject
        for (int index : new int[] {64, 65, Integer.MAX_VALUE})
        {
            asertThrownSameWay(model, target, bs -> bs.get(index));
            asertThrownSameWay(model, target, bs -> bs.set(index));
            asertThrownSameWay(model, target, bs -> bs.setRange(index, 100)); // from/index gets rejected
            asertThrownSameWay(model, target, bs -> bs.setRange(0, 100));     // to gets rejected
            asertThrownSameWay(model, target, bs -> bs.unset(index));

            if (index > 64)
                asertThrownSameWay(model, target, bs -> bs.nextSetBit(index));
        }

        asertThrownSameWay(model, target, bs -> bs.setRange(1, 0));
    }

    private static void asertThrownSameWay(SimpleBitSet model, SimpleBitSet target, Consumer<SimpleBitSet> fn)
    {
        Throwable expected = assertThrown(() -> fn.accept(target));
        Throwable actual = assertThrown(() -> fn.accept(model));

        Assertions.assertThat(actual).hasSameClassAs(expected).hasMessage(expected.getMessage());
    }

    private static Throwable assertThrown(Runnable fn)
    {
        try
        {
            fn.run();
        }
        catch (Throwable t)
        {
            return t;
        }
        throw new AssertionError("Expected logic to throw but did not");
    }

    private static class State implements SimpleBitSet
    {
        private final BitSet model;
        private final SimpleBitSet sut;
        private final int size;

        private State(RandomSource rs)
        {
            this.size = rs.nextInt(1, (1 << 7) + 1); // small vs large have roughly equal probability
            this.model = new BitSet(size);
            this.sut = SimpleBitSet.allocate(size);
        }

        @Override
        public boolean get(int i)
        {
            boolean expected = model.get(i);
            boolean actual = sut.get(i);
            Assertions.assertThat(actual).isEqualTo(expected);
            return actual;
        }

        @Override
        public boolean set(int i)
        {
            boolean expected = !model.get(i);
            model.set(i);
            boolean actual = sut.set(i);

            Assertions.assertThat(actual).isEqualTo(expected);

            return actual;
        }

        @Override
        public void setRange(int from, int to)
        {
            model.set(from, to);
            sut.setRange(from, to);

            getSetBitCount();
        }

        @Override
        public boolean unset(int i)
        {
            boolean expected = model.get(i);
            model.clear(i);
            boolean actual = sut.unset(i);

            Assertions.assertThat(actual).isEqualTo(expected);

            return actual;
        }

        @Override
        public void clear()
        {
            model.clear();
            sut.clear();
        }

        @Override
        public boolean isEmpty()
        {
            boolean expected = model.isEmpty();
            boolean actual = sut.isEmpty();

            Assertions.assertThat(actual).isEqualTo(expected);

            return actual;
        }

        @Override
        public int nextSetBit(int fromIndex)
        {
            int expected;
            for (expected = fromIndex; expected < size; expected++)
            {
                if (model.get(expected))
                    break;
            }
            if (expected == size)
                expected = -1;
            int actual = sut.nextSetBit(fromIndex);

            Assertions.assertThat(actual).isEqualTo(expected);
            return actual;
        }

        @Override
        public int getSetBitCount()
        {
            Assertions.assertThat(sut.getSetBitCount())
                      .describedAs(toBinaryString())
                      .isEqualTo(model.cardinality());
            return sut.getSetBitCount();
        }

        public String toBinaryString()
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < size; i++)
            {
                boolean m = model.get(i);
                boolean s = sut.get(i);
                if (m == s)
                    sb.append(m ? 1 : 0);
                else
                    sb.append("(expected=").append(m ? 1 : 0).append(", actual=").append(s ? 1 : 0).append(')');
            }
            return sb.toString();
        }

        @Override
        public String toString()
        {
            return sut.getClass().getSimpleName();
        }
    }
}