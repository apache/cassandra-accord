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

import javax.annotation.Nonnull;

public class UnhandledEnum extends AssertionError
{
    public UnhandledEnum(@Nonnull Enum<?> value)
    {
        this("Unhandled ", value);
    }

    private UnhandledEnum(String prefix, @Nonnull Enum<?> value)
    {
        super(prefix + value.getClass().getSimpleName() + ": " + value);
    }

    private UnhandledEnum(String prefix, @Nonnull Enum<?> value, String explain)
    {
        super(prefix + value.getClass().getSimpleName() + ": " + value + ". " + explain + '.');
    }

    public static UnhandledEnum invalid(@Nonnull Enum<?> value)
    {
        return new UnhandledEnum("Invalid ", value);
    }

    public static UnhandledEnum invalid(@Nonnull Enum<?> value, String explain)
    {
        return new UnhandledEnum("Invalid ", value, explain);
    }

    public static UnhandledEnum unknown(@Nonnull Enum<?> value)
    {
        return new UnhandledEnum("Unknown ", value);
    }
}
