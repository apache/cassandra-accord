package accord.verify;

public class HistoryViolation extends AssertionError
{
    final int primaryKey;

    public HistoryViolation(int primaryKey, Object detailMessage)
    {
        super(detailMessage);
        this.primaryKey = primaryKey;
    }
}
