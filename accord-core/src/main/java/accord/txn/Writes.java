package accord.txn;

import accord.api.Write;
import accord.local.CommandStore;

public class Writes
{
    public final Timestamp executeAt;
    public final Keys keys;
    public final Write write;

    public Writes(Timestamp executeAt, Keys keys, Write write)
    {
        this.executeAt = executeAt;
        this.keys = keys;
        this.write = write;
    }

    public void apply(CommandStore commandStore)
    {
        if (write == null)
            return;

        keys.accumulate(commandStore.ranges(), (key, accumulate) -> {
            if (commandStore.hashIntersects(key))
                write.apply(key, executeAt, commandStore.store());
            return accumulate;
        });
    }

    @Override
    public String toString()
    {
        return "TxnWrites{" +
               "executeAt:" + executeAt +
               ", keys:" + keys +
               ", write:" + write +
               '}';
    }
}
