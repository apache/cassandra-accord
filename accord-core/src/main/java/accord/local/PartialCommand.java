package accord.local;

import accord.primitives.Deps;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.txn.Txn;

/**
 * A minimal, read only view of a full command. Provided so C* can denormalize command data onto
 * CommandsForKey and Command implementations. Implementer must guarantee that PartialCommand
 * implementations behave as an immutable view and are always in sync with the 'real' command;
 */
public interface PartialCommand
{
    interface WithDeps extends PartialCommand
    {
        Deps savedDeps();
    }

    TxnId txnId();
    Txn txn();

    default Keys someKeys()
    {
        if (txn() != null)
            return txn().keys();

        return null;
    }

    Timestamp executeAt();

    Status status();

    default boolean hasBeen(Status status)
    {
        return status().hasBeen(status);
    }

    default boolean is(Status status)
    {
        return status() == status;
    }

    void removeListener(Listener listener);
}
