package accord.debug.model;

public class EpochInfo
{
    public final long epoch;
    public final String readyMetadata;
    public final String readyCoordinate;
    public final String readyData;
    public final String readyReads;
    public final boolean ready;

    public EpochInfo(long epoch, String readyMetadata, String readyCoordinate,
                     String readyData, String readyReads, boolean ready)
    {
        this.epoch = epoch;
        this.readyMetadata = readyMetadata;
        this.readyCoordinate = readyCoordinate;
        this.readyData = readyData;
        this.readyReads = readyReads;
        this.ready = ready;
    }
}
