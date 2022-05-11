package accord.api;

import accord.local.Node;
import accord.local.Command;
import accord.primitives.Timestamp;
import accord.txn.Txn;

/**
 * Facility for augmenting node behaviour at specific points
 */
public interface Agent
{
    /**
     * For use by implementations to decide what to do about successfully recovered transactions.
     * Specifically intended to define if and how they should inform clients of the result.
     * e.g. in Maelstrom we send the full result directly, in other impls we may simply acknowledge success via the coordinator
     *
     * Note: may be invoked multiple times in different places
     */
    void onRecover(Node node, Result success, Throwable fail);

    /**
     * For use by implementations to decide what to do about timestamp inconsistency, i.e. two different timestamps
     * committed for the same transaction. This is a protocol consistency violation, potentially leading to non-linearizable
     * histories. In test cases this is used to fail the transaction, whereas in real systems this likely will be used for
     * reporting the violation, as it is no more correct at this point to refuse the operation than it is to complete it.
     */
    void onInconsistentTimestamp(Command command, Timestamp prev, Timestamp next);

    void onUncaughtException(Throwable t);

}
