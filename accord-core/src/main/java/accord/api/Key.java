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

package accord.api;

import javax.annotation.Nonnull;

import accord.primitives.Range;
import accord.primitives.RoutableKey;
import accord.primitives.Seekable;

/**
 * A key we can find in both the cluster and on disk
 */
public interface Key extends Seekable, RoutableKey
{
    @Override
    default Key asKey() { return this; }

    @Override
    default Key slice(Range range) { return this; }

    @Override
    default Range asRange() { throw new UnsupportedOperationException(); }

    default int compareAsRoutingKey(@Nonnull RoutingKey that) { return toUnseekable().compareTo(that); }
}
