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

import accord.utils.Gen.Random;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class Property
{
    public static abstract class Common<T extends Common<T>>
    {
        protected long seed = ThreadLocalRandom.current().nextLong();
        protected int examples = 1000;

        protected boolean pure = true;

        protected Common() {
        }

        protected Common(Common<?> other) {
            this.seed = other.seed;
            this.examples = other.examples;
            this.pure = other.pure;
        }

        public T withSeed(long seed)
        {
            this.seed = seed;
            return (T) this;
        }

        public T withExamples(int examples)
        {
            if (examples <= 0)
                throw new IllegalArgumentException("Examples must be positive");
            this.examples = examples;
            return (T) this;
        }

        public T withPure(boolean pure)
        {
            this.pure = pure;
            return (T) this;
        }
    }

    public static class ForBuilder extends Common<ForBuilder>
    {
        public <T> SingleBuilder<T> forAll(Gen<T> gen)
        {
            return new SingleBuilder<>(gen, this);
        }

        public <A, B> DoubleBuilder<A, B> forAll(Gen<A> a, Gen<B> b)
        {
            return new DoubleBuilder<>(a, b, this);
        }
    }

    private static Object normalizeValue(Object value)
    {
        if (value == null)
            return null;
        // one day java arrays will have a useful toString... one day...
        if (value.getClass().isArray())
        {
            Class<?> subType = value.getClass().getComponentType();
            if (!subType.isPrimitive())
                return Arrays.asList((Object[]) value);
            if (Byte.TYPE == subType)
                return Arrays.toString((byte[]) value);
            if (Character.TYPE == subType)
                return Arrays.toString((char[]) value);
            if (Short.TYPE == subType)
                return Arrays.toString((short[]) value);
            if (Integer.TYPE == subType)
                return Arrays.toString((int[]) value);
            if (Long.TYPE == subType)
                return Arrays.toString((long[]) value);
            if (Float.TYPE == subType)
                return Arrays.toString((float[]) value);
            if (Double.TYPE == subType)
                return Arrays.toString((double[]) value);
        }
        return value;
    }

    private static String propertyError(Common<?> input, Throwable cause, Object... values)
    {
        StringBuilder sb = new StringBuilder();
        // return "Seed=" + seed + "\nExamples=" + examples;
        sb.append("Property error detected:\nSeed = ").append(input.seed).append('\n');
        sb.append("Examples = ").append(input.examples).append('\n');
        sb.append("Pure = ").append(input.pure).append('\n');
        if (cause != null)
        {
            String msg = cause.getMessage();
            sb.append("Error: ");
            // to improve readability, if a newline is detected move the error msg to the next line
            if (msg != null && msg.contains("\n"))
                msg = "\n\t" + msg.replace("\n", "\n\t");
            if (msg == null)
                msg = cause.getClass().getCanonicalName();
            sb.append(msg).append('\n');
        }
        if (values != null)
        {
            sb.append("Values:\n");
            for (int i = 0; i < values.length; i++)
                sb.append('\t').append(i).append(" = ").append(normalizeValue(values[i])).append('\n');
        }
        return sb.toString();
    }

    public interface FailingConsumer<A>
    {
        void accept(A value) throws Exception;
    }

    public static class SingleBuilder<T> extends Common<SingleBuilder<T>>
    {
        private final Gen<T> gen;

        private SingleBuilder(Gen<T> gen, Common<?> other) {
            super(other);
            this.gen = Objects.requireNonNull(gen);
        }

        public void check(FailingConsumer<T> fn)
        {
            Random random = new Random(seed);
            for (int i = 0; i < examples; i++)
            {
                T value = null;
                try
                {
                    checkInterrupted();
                    fn.accept(value = gen.next(random));
                }
                catch (Throwable t)
                {
                    throw new PropertyError(propertyError(this, t, value), t);
                }
                if (pure)
                {
                    seed = random.nextLong();
                    random.setSeed(seed);
                }
            }
        }
    }

    public interface FailingBiConsumer<A, B>
    {
        void accept(A a, B b) throws Exception;
    }

    public static class DoubleBuilder<A, B> extends Common<DoubleBuilder<A, B>>
    {
        private final Gen<A> aGen;
        private final Gen<B> bGen;

        private DoubleBuilder(Gen<A> aGen, Gen<B> bGen, Common<?> other) {
            super(other);
            this.aGen = Objects.requireNonNull(aGen);
            this.bGen = Objects.requireNonNull(bGen);
        }

        public void check(FailingBiConsumer<A, B> fn)
        {
            Random random = new Random(seed);
            for (int i = 0; i < examples; i++)
            {
                A a = null;
                B b = null;
                try
                {
                    checkInterrupted();
                    fn.accept(a = aGen.next(random), b = bGen.next(random));
                }
                catch (Throwable t)
                {
                    throw new PropertyError(propertyError(this, t, a, b), t);
                }
                if (pure)
                {
                    seed = random.nextLong();
                    random.setSeed(seed);
                }
            }
        }
    }

    private static void checkInterrupted() throws InterruptedException {
        if (Thread.currentThread().isInterrupted())
            throw new InterruptedException();
    }

    public static class PropertyError extends AssertionError
    {
        public PropertyError(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static ForBuilder qt()
    {
        return new ForBuilder();
    }
}
