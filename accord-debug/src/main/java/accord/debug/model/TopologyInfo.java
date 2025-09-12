package accord.debug.model;

import java.util.List;

public class TopologyInfo
{
    public final EpochInfo epoch;
    public final List<TableEpoch> tableEpochs;

    public TopologyInfo(EpochInfo epoch, List<TableEpoch> tableEpochs)
    {
        this.epoch = epoch;
        this.tableEpochs = tableEpochs;
    }
}
