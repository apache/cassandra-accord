package accord.debug.model;

import java.util.ArrayList;
import java.util.List;

public class TableEpoch
{
    public final long epoch;
    public final String keyspaceName;
    public final String tableName;
    public final List<String> added;
    public final List<String> removed;
    public final List<String> synced;
    public final List<String> closed;
    public final List<String> retired;

    public TableEpoch(long epoch, String keyspaceName, String tableName,
                      List<String> added, List<String> removed, List<String> synced,
                      List<String> closed, List<String> retired)
    {
        this.epoch = epoch;
        this.keyspaceName = keyspaceName;
        this.tableName = tableName;
        this.added = added != null ? added : new ArrayList<>();
        this.removed = removed != null ? removed : new ArrayList<>();
        this.synced = synced != null ? synced : new ArrayList<>();
        this.closed = closed != null ? closed : new ArrayList<>();
        this.retired = retired != null ? retired : new ArrayList<>();
    }
}
