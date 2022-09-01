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

package accord.burn;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReconcilingLoggerTest
{
    private static final Logger logger = LoggerFactory.getLogger(ReconcilingLoggerTest.class);

    @Test
    void testMatches()
    {
        ReconcilingLogger reconcilingLogger = new ReconcilingLogger(logger);
        logger.info("pre-reconcile");
        try (ReconcilingLogger.Session session = reconcilingLogger.nextSession())
        {
            logger.info("message {}", 1);
            logger.info("message 2");
        }
        try (ReconcilingLogger.Session session = reconcilingLogger.nextSession())
        {
            logger.info("message 1");
            logger.info("message {}", 2);
        }
        logger.info("post-reconcile");
        Assertions.assertTrue(reconcilingLogger.reconcile());
    }

    @Test
    void testMismatches()
    {
        ReconcilingLogger reconcilingLogger = new ReconcilingLogger(logger);
        logger.info("pre-reconcile");
        try (ReconcilingLogger.Session session = reconcilingLogger.nextSession())
        {
            logger.info("message {}", 1);
            logger.info("message 2");
        }
        try (ReconcilingLogger.Session session = reconcilingLogger.nextSession())
        {
            logger.info("message {}", 2);
            logger.info("message 1");
            logger.info("message 3");
        }
        logger.info("post-reconcile");
        Assertions.assertFalse(reconcilingLogger.reconcile());
    }
}
