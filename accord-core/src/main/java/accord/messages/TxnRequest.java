package accord.messages;

public abstract class TxnRequest implements Request
{
    private final TxnRequestScope scope;

    public TxnRequest(TxnRequestScope scope)
    {
        this.scope = scope;
    }

    public TxnRequestScope scope()
    {
        return scope;
    }
}
