package accord.messages;

public abstract class TxnRequest implements Request
{
    private final RequestScope scope;

    public TxnRequest(RequestScope scope)
    {
        this.scope = scope;
    }

    public RequestScope scope()
    {
        return scope;
    }
}
