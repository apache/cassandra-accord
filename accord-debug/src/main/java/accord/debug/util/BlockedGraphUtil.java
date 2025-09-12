package accord.debug.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import accord.api.RoutingKey;
import accord.impl.InMemoryCommandStore;
import accord.impl.InMemorySafeCommand;
import accord.local.Command;
import accord.local.CommandStores;
import accord.local.Node;
import accord.local.cfk.CommandsForKey;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;

public class BlockedGraphUtil
{
    public List<CommandStoreTxnBlockedGraph> loadDebug(Node node, TxnId original)
    {
        CommandStores commandStores = node.commandStores();
        if (commandStores.count() == 0)
            return Collections.emptyList();
        int[] ids = commandStores.ids();
        List<CommandStoreTxnBlockedGraph> res = new ArrayList<>(ids.length);
        for (int id : ids)
            res.add(loadDebug(original, (InMemoryCommandStore) commandStores.forId(id)));
        return res;
    }

    private CommandStoreTxnBlockedGraph loadDebug(TxnId txnId, InMemoryCommandStore store)
    {
        CommandStoreTxnBlockedGraph.Builder state = new CommandStoreTxnBlockedGraph.Builder(store.id());
        populateSync(state, store, txnId);
        return state.build();
    }

    private static void populate(CommandStoreTxnBlockedGraph.Builder state, InMemoryCommandStore safeStore, TxnId blockedBy)
    {
        populateSync(state, safeStore, blockedBy);
    }

    private static void populateSync(CommandStoreTxnBlockedGraph.Builder state, InMemoryCommandStore store, TxnId txnId)
    {
        try
        {
            if (state.txns.containsKey(txnId))
                return; // could plausibly request same txn twice

            InMemorySafeCommand command = store.command(txnId).createSafeReference();
            Invariants.nonNull(command, "Txn %s is not in the cache", txnId);
            if (command.current() == null || command.current().saveStatus() == SaveStatus.Uninitialised)
                return;

            CommandStoreTxnBlockedGraph.TxnState cmdTxnState = populateSync(state, command.current());
            if (cmdTxnState.notBlocked())
                return;

            for (TxnId blockedBy : cmdTxnState.blockedBy)
            {
                if (!state.knows(blockedBy))
                    populate(state, store, blockedBy);
            }
            for (RoutingKey blockedBy : cmdTxnState.blockedByKey)
            {
                if (!state.keys.containsKey(blockedBy))
                    populate(state, store, blockedBy, txnId, command.current().executeAt());
            }
        }
        catch (Throwable t)
        {
            state.tryFailure(t);
        }
    }

    private static void populate(CommandStoreTxnBlockedGraph.Builder state, InMemoryCommandStore safeStore, RoutingKey blockedBy, TxnId txnId, Timestamp executeAt)
    {
        populateSync(state, safeStore, blockedBy, txnId, executeAt);
    }

    private static void populateSync(CommandStoreTxnBlockedGraph.Builder state, InMemoryCommandStore store, RoutingKey pk, TxnId txnId, Timestamp executeAt)
    {
        try
        {
            InMemoryCommandStore.GlobalCommandsForKey commandsForKey = store.commandsForKey(pk);
            TxnId blocking = commandsForKey.value().blockedOnTxnId(txnId, executeAt);
            if (blocking instanceof CommandsForKey.TxnInfo)
                blocking = ((CommandsForKey.TxnInfo) blocking).plainTxnId();
            state.keys.put(pk, blocking);
            if (state.txns.containsKey(blocking))
                return;
            populate(state, store, blocking);
        }
        catch (Throwable t)
        {
            state.tryFailure(t);
        }
    }

    private static CommandStoreTxnBlockedGraph.TxnState populateSync(CommandStoreTxnBlockedGraph.Builder state, Command cmd)
    {
        CommandStoreTxnBlockedGraph.Builder.TxnBuilder cmdTxnState = state.txn(cmd.txnId(), cmd.executeAt(), cmd.saveStatus());
        if (!cmd.hasBeen(Status.Applied) && cmd.hasBeen(Status.Stable))
        {
            // check blocking state
            Command.WaitingOn waitingOn = cmd.asCommitted().waitingOn();
            waitingOn.waitingOn.reverseForEach(null, null, null, null, (i1, i2, i3, i4, i) -> {
                if (i < waitingOn.txnIdCount())
                {
                    // blocked on txn
                    cmdTxnState.blockedBy.add(waitingOn.txnId(i));
                }
                else
                {
                    // blocked on key
                    cmdTxnState.blockedByKey.add(waitingOn.keys.get(i - waitingOn.txnIdCount()));
                }
            });
        }
        return cmdTxnState.build();
    }

}
