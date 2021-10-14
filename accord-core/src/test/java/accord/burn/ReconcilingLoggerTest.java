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
