package accord.messages;

public interface EpochRequest extends Request
{
    long waitForEpoch();
}
