package accord.debug.model;

public class DurableBeforeInfo
{
    public final String keyspaceName;
    public final String tableName;
    public final String tokenStart;
    public final String tokenEnd;
    public final String quorum;
    public final String universal;

    public DurableBeforeInfo(String keyspaceName, String tableName, String tokenStart, String tokenEnd,
                             String quorum, String universal)
    {
        this.keyspaceName = keyspaceName;
        this.tableName = tableName;
        this.tokenStart = tokenStart;
        this.tokenEnd = tokenEnd;
        this.quorum = quorum;
        this.universal = universal;
    }
}
