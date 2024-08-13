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

package accord.primitives;

/**
 * A marker interface for cases where we do not include any non-participating home key in the collection.
 * Note that this interface is implemented by classes that may or may not contain only participants; it is expected
 * that these classes will only be used via interfaces such as Route that do not extend this interface,
 * so that the implementation may return itself when suitable, and a converted copy otherwise.
 */
// TODO (desired): so we need this abstraction anymore, now that we have removed the concept of non-participating home keys?
public interface Participants<K extends Unseekable> extends Unseekables<K>
{
    @Override
    Participants<K> intersecting(Unseekables<?> intersecting);
    @Override
    Participants<K> intersecting(Unseekables<?> intersecting, Slice slice);

    @Override
    Participants<K> slice(int from, int to);
    @Override
    Participants<K> slice(Ranges ranges);
    @Override
    Participants<K> slice(Ranges ranges, Slice slice);

    Participants<K> with(Participants<K> with);
    @Override
    Participants<K> without(Ranges ranges);
    @Override
    Participants<K> without(Unseekables<?> without);

    Ranges toRanges();

    /**
     * If both left and right are a Route, invoke {@link Route#with} on them. Otherwise invoke {@link #with}.
     */
    static <K extends Unseekable> Participants<K> merge(Participants<K> left, Participants<K> right)
    {
        if (left == null) return right;
        if (right == null) return left;
        return left.with(right);
    }
}
