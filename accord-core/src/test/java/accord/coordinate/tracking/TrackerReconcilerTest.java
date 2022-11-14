package accord.coordinate.tracking;

import accord.topology.Topologies;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.function.BiFunction;

public class TrackerReconcilerTest
{
    @Test
    public void testReadTracker()
    {
        test(10000, ReadTrackerReconciler::new);
    }

    @Test
    public void testQuorumTracker()
    {
        test(10000, QuorumTrackerReconciler::new);
    }
    
    @Test
    public void testFastPathTracker()
    {
        test(10000, FastPathTrackerReconciler::new);
    }

    @Test
    public void testRecoveryTracker()
    {
        test(10000, RecoveryTrackerReconciler::new);
    }

    @Test
    public void testInvalidationTracker()
    {
        test(10000, InvalidationTrackerReconciler::new);
    }

    static <ST extends ShardTracker, T extends AbstractTracker<ST, ?>, E extends Enum<E>>
    void test(int count, BiFunction<Random, Topologies, ? extends TrackerReconciler<ST, T, E>> constructor)
    {
        long seed = System.currentTimeMillis();
        while (--count >= 0)
            test(seed++, constructor);
    }

    static <ST extends ShardTracker, T extends AbstractTracker<ST, ?>, E extends Enum<E>>
    void test(long seed, BiFunction<Random, Topologies, ? extends TrackerReconciler<ST, T, E>> constructor)
    {
        for (TrackerReconciler<?, ?, ?> test : TrackerReconciler.generate(seed, constructor))
            test.test();
    }
}
