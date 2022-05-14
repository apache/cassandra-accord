package accord.local;

import accord.primitives.*;

/**
 * A minimal, read only view of a full command. Provided so C* can denormalize command data onto
 * CommandsForKey and Command implementations. Implementer must guarantee that PartialCommand
 * implementations behave as an immutable view and are always in sync with the 'real' command;
 */
public interface PartialCommand
{
    TxnId txnId();
    Timestamp executeAt();

    // TODO: pack this into TxnId
    Txn.Kind kind();
    Status status();

    default boolean hasBeen(Status status)
    {
        return status().hasBeen(status);
    }
    default boolean is(Status status)
    {
        return status() == status;
    }
}
