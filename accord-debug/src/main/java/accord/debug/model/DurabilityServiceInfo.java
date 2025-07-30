package accord.debug.model;

public class DurabilityServiceInfo
{
    public final String keyspaceName;
    public final String tableName;
    public final String tokenStart;
    public final String tokenEnd;
    public final long lastStartedAt;
    public final long cycleStartedAt;
    public final int retries;
    public final String min;
    public final String requestedBy;
    public final String active;
    public final String waiting;
    public final int nodeOffset;
    public final int cycleOffset;
    public final int activeIndex;
    public final int nextIndex;
    public final int nextToIndex;
    public final int endIndex;
    public final int currentSplits;
    public final boolean stopping;
    public final boolean stopped;

    public DurabilityServiceInfo(String keyspaceName, String tableName, String tokenStart, String tokenEnd,
                                 long lastStartedAt, long cycleStartedAt, int retries, String min, String requestedBy,
                                 String active, String waiting, int nodeOffset, int cycleOffset, int activeIndex,
                                 int nextIndex, int nextToIndex, int endIndex, int currentSplits, boolean stopping,
                                 boolean stopped)
    {
        this.keyspaceName = keyspaceName;
        this.tableName = tableName;
        this.tokenStart = tokenStart;
        this.tokenEnd = tokenEnd;
        this.lastStartedAt = lastStartedAt;
        this.cycleStartedAt = cycleStartedAt;
        this.retries = retries;
        this.min = min;
        this.requestedBy = requestedBy;
        this.active = active;
        this.waiting = waiting;
        this.nodeOffset = nodeOffset;
        this.cycleOffset = cycleOffset;
        this.activeIndex = activeIndex;
        this.nextIndex = nextIndex;
        this.nextToIndex = nextToIndex;
        this.endIndex = endIndex;
        this.currentSplits = currentSplits;
        this.stopping = stopping;
        this.stopped = stopped;
    }
}
