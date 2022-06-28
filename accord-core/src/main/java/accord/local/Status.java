package accord.local;

public enum Status
{
    NotWitnessed, PreAccepted, Accepted, Committed, ReadyToExecute, Executed, Applied;

    public static Status max(Status a, Status b)
    {
        return a.compareTo(b) >= 0 ? a : b;
    }

    public boolean hasBeen(Status equalOrGreaterThan)
    {
        return compareTo(equalOrGreaterThan) >= 0;
    }
}
