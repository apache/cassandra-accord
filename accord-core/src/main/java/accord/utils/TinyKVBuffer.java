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

import static accord.utils.Invariants.checkState;
import static accord.utils.Invariants.illegalState;

/**
 * A 3-element buffer of key-value pairs
 */
class TinyKVBuffer<K, V>
{
    private K k0, k1, k2;
    private V v0, v1, v2;

    void append(K key, V value)
    {
             if (k0 == null) { k0 = key; v0 = value; }
        else if (k1 == null) { k1 = key; v1 = value; }
        else if (k2 == null) { k2 = key; v2 = value; }
        else illegalState();
    }

    void dropFirst()
    {
        checkState(k0 != null);
        k0 = k1;   v0 = v1;
        k1 = k2;   v1 = v2;
        k2 = null; v2 = null;
    }

    void dropLast()
    {
             if (k2 != null) { k2 = null; v2 = null; }
        else if (k1 != null) { k1 = null; v1 = null; }
        else if (k0 != null) { k0 = null; v0 = null; }
        else illegalState();
    }

    int size()
    {
        if (k0 == null) return 0;
        if (k1 == null) return 1;
        if (k2 == null) return 2;
        else            return 3;
    }

    boolean isFull()
    {
        return k2 != null;
    }

    boolean isEmpty()
    {
        return k0 == null;
    }

    K firstKey()
    {
        if (k0 != null) return k0;
        throw illegalState();
    }

    V firstValue()
    {
        if (k0 != null) return v0;
        throw illegalState();
    }

    V penultimateValue()
    {
        if (k2 != null) return v1;
        if (k1 != null) return v0;
        throw illegalState();
    }

    K lastKey()
    {
        if (k2 != null) return k2;
        if (k1 != null) return k1;
        if (k0 != null) return k0;
        throw illegalState();
    }

    V lastValue()
    {
        if (k2 != null) return v2;
        if (k1 != null) return v1;
        if (k0 != null) return v0;
        throw illegalState();
    }

    void lastValue(V value)
    {
             if (k2 != null) v2 = value;
        else if (k1 != null) v1 = value;
        else if (k0 != null) v0 = value;
        else throw illegalState();
    }
}
