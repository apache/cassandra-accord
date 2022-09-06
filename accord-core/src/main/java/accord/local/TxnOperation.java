package accord.local;

import accord.api.Key;
import accord.primitives.TxnId;

import java.util.Collections;

/**
 * An operation that is executed in the context of a command store. The methods communicate to the implementation which
 * commands and commandsPerKey items will be needed to run the operation
 */
public interface TxnOperation
{
    Iterable<TxnId> txnIds();
    Iterable<Key> keys();

    static TxnOperation scopeFor(Iterable<TxnId> txnIds, Iterable<Key> keys)
    {
        return new TxnOperation()
        {
            @Override
            public Iterable<TxnId> txnIds() { return txnIds; }

            @Override
            public Iterable<Key> keys() { return keys; }
        };
    }

    static TxnOperation scopeFor(TxnId txnId, Iterable<Key> keys)
    {
        return scopeFor(Collections.singleton(txnId), keys);
    }

    static TxnOperation scopeFor(TxnId txnId)
    {
        return scopeFor(Collections.singleton(txnId), Collections.emptyList());
    }
}
