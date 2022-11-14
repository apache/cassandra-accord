package accord.coordinate.tracking;

public enum RequestStatus
{
    Failed,
    Success,
    NoChange;

    public static RequestStatus min(RequestStatus a, RequestStatus b)
    {
        return a.compareTo(b) <= 0 ? a : b;
    }
}
