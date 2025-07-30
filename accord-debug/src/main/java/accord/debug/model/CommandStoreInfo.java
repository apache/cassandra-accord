package accord.debug.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CommandStoreInfo
{
    public final int commandStoreId;
    public final Map<String, List<String>> safeToRead;
    public final Map<String, List<String>> rangesForEpoch;

    public CommandStoreInfo(int commandStoreId,
                            Map<String, List<String>> safeToRead,
                            Map<String, List<String>> rangesForEpoch)
    {
        this.commandStoreId = commandStoreId;
        this.safeToRead = safeToRead != null ? safeToRead : Collections.emptyMap();
        this.rangesForEpoch = rangesForEpoch != null ? rangesForEpoch : Collections.emptyMap();
    }
}
