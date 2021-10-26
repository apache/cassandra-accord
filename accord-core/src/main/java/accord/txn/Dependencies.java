package accord.txn;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

import accord.local.Command;
import accord.local.CommandStore;
import com.google.common.annotations.VisibleForTesting;

// TODO: do not send Txn
// TODO: implementation efficiency
public class Dependencies implements Iterable<Entry<TxnId, Txn>>
{
    // TODO: encapsulate
    public final NavigableMap<TxnId, Txn> deps;

    public Dependencies()
    {
        this.deps = new TreeMap<>();
    }

    public Dependencies(NavigableMap<TxnId, Txn> deps)
    {
        this.deps = deps;
    }

    public void add(Command command)
    {
        add(command.txnId(), command.txn());
    }

    @VisibleForTesting
    public Dependencies add(TxnId txnId, Txn txn)
    {
        deps.put(txnId, txn);
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
        return deps.get(txnId);
    }

    public Iterable<TxnId> on(CommandStore commandStore)
    {
        return deps.entrySet()
                .stream()
                .filter(e -> commandStore.intersects(e.getValue().keys()))
                .map(Entry::getKey)::iterator;
    }

    @Override
    public Iterator<Entry<TxnId, Txn>> iterator()
    {
        return deps.entrySet().iterator();
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
