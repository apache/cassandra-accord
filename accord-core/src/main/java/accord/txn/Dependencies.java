package accord.txn;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

import accord.api.Key;
import accord.local.Command;
import accord.local.CommandStore;
import accord.topology.KeyRanges;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

// TODO: do not send Txn
// TODO: implementation efficiency
public class Dependencies implements Iterable<Entry<TxnId, Txn>>
{
    // TEMPORARY: this is used ONLY for SimpleProgressLog.blockingState which may not know the homeKey
    // or be able to invalidate a transaction it cannot witness anywhere
    public static class TxnAndHomeKey
    {
        public final Txn txn;
        public final Key homeKey;

        TxnAndHomeKey(Txn txn, Key homeKey)
        {
            this.txn = txn;
            this.homeKey = homeKey;
        }
    }

    // TODO: encapsulate
    public final NavigableMap<TxnId, TxnAndHomeKey> deps;

    public Dependencies()
    {
        this.deps = new TreeMap<>();
    }

    public Dependencies(NavigableMap<TxnId, TxnAndHomeKey> deps)
    {
        this.deps = deps;
    }

    public void add(Command command)
    {
        add(command.txnId(), command.txn(), command.homeKey());
    }

    @VisibleForTesting
    public Dependencies add(TxnId txnId, Txn txn, Key homeKey)
    {
        Preconditions.checkState(homeKey != null);
        deps.put(txnId, new TxnAndHomeKey(txn, homeKey));
        return this;
    }

    public void addAll(Dependencies add)
    {
        this.deps.putAll(add.deps);
    }

    public void removeAll(Dependencies add)
    {
        this.deps.keySet().removeAll(add.deps.keySet());
    }

    public boolean contains(TxnId txnId)
    {
        return deps.containsKey(txnId);
    }

    public boolean isEmpty()
    {
        return deps.isEmpty();
    }

    public Txn get(TxnId txnId)
    {
        return deps.get(txnId).txn;
    }

    public Iterable<TxnId> on(CommandStore commandStore, Timestamp executeAt)
    {
        KeyRanges ranges = commandStore.ranges().since(executeAt.epoch);
        if (ranges == null)
            return Collections.emptyList();

        return deps.entrySet()
                .stream()
                .filter(e -> commandStore.intersects(e.getValue().txn.keys(), ranges))
                .map(Entry::getKey)::iterator;
    }

    public Key homeKey(TxnId txnId)
    {
        return deps.get(txnId).homeKey;
    }

    @Override
    public Iterator<Entry<TxnId, Txn>> iterator()
    {
        return deps.entrySet().stream().map(e -> Map.entry(e.getKey(), e.getValue().txn)).iterator();
    }

    public int size()
    {
        return deps.size();
    }

    @Override
    public String toString()
    {
        return deps.keySet().toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Dependencies entries = (Dependencies) o;
        return deps.equals(entries.deps);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(deps);
    }
}
