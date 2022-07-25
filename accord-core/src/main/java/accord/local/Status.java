package accord.local;

public enum Status
{
    NotWitnessed(0),
    PreAccepted(10),
    Accepted(20),
    AcceptedInvalidate(20),
    Committed(30),
    ReadyToExecute(40),
    Executed(50),
    Applied(60),
    Invalidated(60);

    final int logicalOrdinal;

    Status(int logicalOrdinal)
    {
        this.logicalOrdinal = logicalOrdinal;
    }

    // equivalent to compareTo except Accepted and AcceptedInvalidate sort equal
    public int logicalCompareTo(Status that)
    {
        return this.logicalOrdinal - that.logicalOrdinal;
    }

    public static Status max(Status a, Status b)
    {
        return a.compareTo(b) >= 0 ? a : b;
    }

    public boolean hasBeen(Status equalOrGreaterThan)
    {
        return compareTo(equalOrGreaterThan) >= 0;
    }
}
