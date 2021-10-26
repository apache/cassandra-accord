package accord.impl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static accord.Utils.ranges;
import static accord.impl.IntKey.range;

public class TopologyUtilsTest
{
    @Test
    void initialRangesTest()
    {
        Assertions.assertEquals(ranges(range(0, 100), range(100, 200), range(200, 300)),
                                TopologyUtils.initialRanges(3, 300));
    }
}
