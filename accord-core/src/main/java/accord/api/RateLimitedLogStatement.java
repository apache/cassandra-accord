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

import org.slf4j.Logger;

public interface RateLimitedLogStatement
{
    interface Factory
    {
        RateLimitedLogStatement info(String message, Logger logger);
        RateLimitedLogStatement warn(String message, Logger logger);
        RateLimitedLogStatement error(String message, Logger logger);
    }

    class NoLimitFactory implements Factory
    {
        @Override
        public RateLimitedLogStatement info(String message, Logger logger)
        {
            return params -> logger.info(message, params);
        }

        @Override
        public RateLimitedLogStatement warn(String message, Logger logger)
        {
            return params -> logger.warn(message, params);
        }

        @Override
        public RateLimitedLogStatement error(String message, Logger logger)
        {
            return params -> logger.error(message, params);
        }
    }

    void log(Object ... params);
}
