/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package accord.utils.btree;

import java.util.function.BiFunction;

import static accord.utils.Invariants.illegalState;

/**
 * An interface defining the method to be applied to the existing and replacing object in a BTree. The objects returned
 * by the methods will be the object that need to be stored in the BTree.
 */
public interface UpdateFunction<K, V>
{
    /**
     * Computes the value that should be inserted in the BTree.
     *
     * @param insert the update value
     * @return the value that should be inserted in the BTree
     */
    V insert(K insert);

    /**
     * Computes the result of merging the existing value with the one from the update.
     *
     * @param replacing the value in the original tree we have matched
     * @param update the value in the updating collection that matched
     * @return the value to insert into the new tree
     */
    V merge(V replacing, K update);

    class Simple<V> implements UpdateFunction<V, V>
    {
        private final BiFunction<V, V, V> wrapped;
        public Simple(BiFunction<V, V, V> wrapped)
        {
            this.wrapped = wrapped;
        }

        @Override
        public final V insert(V v)
        {
            return v;
        }

        @Override
        public final V merge(V replacing, V update)
        {
            return wrapped.apply(replacing, update);
        }

        public static <V> Simple<V> of(BiFunction<V, V, V> f)
        {
            return new Simple<>(f);
        }

        Simple<V> flip()
        {
            return of((a, b) -> wrapped.apply(b, a));
        }
    }

    final class NoOp<V> extends Simple<V>
    {
        public NoOp(BiFunction<V, V, V> wrapped)
        {
            super(wrapped);
        }

        NoOp<V> flip()
        {
            return noOp == this ? noOpReplace() : noOp();
        }
    }

    NoOp<Object> noOp = new NoOp<>((a, b) -> a);
    NoOp<Object> replace = new NoOp<>((a, b) -> b);
    NoOp<Object> reject = new NoOp<>((a, b) -> { throw illegalState("%s = %s; but updates are rejected", a, b); });

    static <K> NoOp<K> noOp()
    {
        return (NoOp<K>) noOp;
    }

    static <K> NoOp<K> noOpReplace()
    {
        return (NoOp<K>) replace;
    }

    static <K> NoOp<K> rejectUpdates()
    {
        return (NoOp<K>) reject;
    }
}
