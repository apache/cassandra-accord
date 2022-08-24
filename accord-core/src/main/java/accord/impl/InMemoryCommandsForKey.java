package accord.impl;

import accord.api.Key;
import accord.local.Command;
import accord.local.CommandsForKey;
import accord.primitives.Timestamp;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Stream;

public class InMemoryCommandsForKey extends CommandsForKey
{
    static class InMemoryCommandTimeseries implements CommandTimeseries
    {
        private final NavigableMap<Timestamp, Command> commands = new TreeMap<>();

        @Override
        public Command get(Timestamp timestamp)
        {
            return commands.get(timestamp);
        }

        @Override
        public void add(Timestamp timestamp, Command command)
        {
            commands.put(timestamp, command);
        }

        @Override
        public void remove(Timestamp timestamp)
        {
            commands.remove(timestamp);
        }

        @Override
        public boolean isEmpty()
        {
            return commands.isEmpty();
        }

        @Override
        public Stream<Command> before(Timestamp timestamp)
        {
            return commands.headMap(timestamp, false).values().stream();
        }

        @Override
        public Stream<Command> after(Timestamp timestamp)
        {
            return commands.tailMap(timestamp, false).values().stream();
        }

        @Override
        public Stream<Command> between(Timestamp min, Timestamp max)
        {
            return commands.subMap(min, true, max, true).values().stream();
        }

        @Override
        public Stream<Command> all()
        {
            return commands.values().stream();
        }
    }

    private final Key key;
    private final InMemoryCommandTimeseries uncommitted = new InMemoryCommandTimeseries();
    private final InMemoryCommandTimeseries committedById = new InMemoryCommandTimeseries();
    private final InMemoryCommandTimeseries committedByExecuteAt = new InMemoryCommandTimeseries();

    private Timestamp max = Timestamp.NONE;

    public InMemoryCommandsForKey(Key key)
    {
        this.key = key;
    }

    @Override
    public Key key()
    {
        return key;
    }

    @Override
    public Timestamp max()
    {
        return max;
    }

    @Override
    public void updateMax(Timestamp timestamp)
    {
        max = Timestamp.max(max, timestamp);
    }

    @Override
    public CommandTimeseries uncommitted()
    {
        return uncommitted;
    }

    @Override
    public CommandTimeseries committedById()
    {
        return committedById;
    }

    @Override
    public CommandTimeseries committedByExecuteAt()
    {
        return committedByExecuteAt;
    }
}
