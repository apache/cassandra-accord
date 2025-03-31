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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.function.Consumer;

import accord.local.SafeCommandStore;

/**
 * Async annotations for IntelliJ debugging.
 *
 * These annotations were tested with 2024.3.5, but should in principle work with any version that supports async debugging:
 * https://www.jetbrains.com/help/idea/debug-asynchronous-code.html
 *
 * Add @Schedule annotation to capture a runnable or an object that identifies required stack trace to capture it, and
 * add @Execute annotation to the call site where you would like the async stack trace injected.
 *
 * If you are seeing java.io.FileNotFoundException (The system cannot find the file specified) during startup,
 * just make sure you do not use fork mode. Looks like debugger was not picking props for this mode for a while:
 * https://youtrack.jetbrains.com/issue/IDEA-192356
 */
public final class Async
{
    private Async()
    {
        throw new AssertionError("Async should not be instantiated");
    }

    @Retention(RetentionPolicy.CLASS)
    @Target({ ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.PARAMETER })
    public @interface Execute {}

    @Retention(RetentionPolicy.CLASS)
    @Target({ ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.PARAMETER })
    public @interface Schedule {}

    /**
     * A helper method to capture a consumer. Unfortunately, just using <T> does not work because of erasures and
     * wildcard types.
     */
    public static Consumer<? super SafeCommandStore> capture(@Schedule Consumer<? super SafeCommandStore> capture)
    {
        return new Consumer<>()
        {
            @Override
            public void accept(SafeCommandStore safeCommandStore)
            {
                execute(safeCommandStore, capture);
            }

            private void execute(SafeCommandStore safeCommandStore, @Execute Consumer<? super SafeCommandStore> capture)
            {
                capture.accept(safeCommandStore);
            }
        };
    }

    /**
     * An identity method that, when used in conjunction with IntelliJ async debugging, hints the agent to capture
     * stack trace, identifying it with a key provided to this method.
     */
    public static <T> T captureAny(@Schedule T capture)
    {
        return capture;
    }
}
