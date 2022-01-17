package accord.topology;

import accord.local.Node;
import accord.utils.IndexedConsumer;
import com.google.common.base.Preconditions;

import java.util.*;
import java.util.function.Consumer;

public interface Topologies
{
    Topology current();

    int findEarliestSinceEpoch(long epoch);

    long oldestEpoch();

    default long currentEpoch()
    {
        return current().epoch;
    }

    boolean fastPathPermitted();

    // topologies are stored in reverse epoch order, with the highest epoch at idx 0
    Topology get(int i);

    int size();

    int totalShards();

    Set<Node.Id> nodes();

    Topologies removeEpochsBefore(long epoch);

    default void forEach(IndexedConsumer<Topology> consumer)
    {
        for (int i=0, mi=size(); i<mi; i++)
            consumer.accept(i, get(i));
    }

    default void forEachShard(Consumer<Shard> consumer)
    {
        for (int i=0, mi=size(); i<mi; i++)
        {
            Topology topology = get(i);
            for (int j=0, mj=topology.size(); j<mj; j++)
            {
                consumer.accept(topology.get(j));
            }
        }
    }

    private static boolean equals(Topologies t, Object o)
    {
        if (o == t)
            return true;

        if (!(o instanceof Topologies))
            return false;

        Topologies that = (Topologies) o;
        if (t.size() != that.size())
            return false;

        for (int i=0, mi=t.size(); i<mi; i++)
        {
            if (!t.get(i).equals(that.get(i)))
                return false;
        }
        return true;
    }

    private static int hashCode(Topologies t)
    {
        int hashCode = 1;
        for (int i=0, mi=t.size(); i<mi; i++) {
            hashCode = 31 * hashCode + t.get(i).hashCode();
        }
        return hashCode;
    }

    private static String toString(Topologies t)
    {
        StringBuilder sb = new StringBuilder("[");
        for (int i=0, mi=t.size(); i<mi; i++)
        {
            if (i < 0)
                sb.append(", ");

            sb.append(t.get(i).toString());
        }
        sb.append("]");
        return sb.toString();
    }

    class Single implements Topologies
    {
        private final Topology topology;
        private final boolean fastPathPermitted;

        public Single(Topology topology, boolean fastPathPermitted)
        {
            this.topology = topology;
            this.fastPathPermitted = fastPathPermitted;
        }

        @Override
        public Topology current()
        {
            return topology;
        }

        @Override
        public long oldestEpoch()
        {
            return currentEpoch();
        }

        @Override
        public int findEarliestSinceEpoch(long epoch)
        {
            return 0;
        }

        @Override
        public boolean fastPathPermitted()
        {
            return fastPathPermitted;
        }

        @Override
        public Topology get(int i)
        {
            if (i != 0)
                throw new IndexOutOfBoundsException(i);
            return topology;
        }

        @Override
        public int size()
        {
            return 1;
        }

        @Override
        public int totalShards()
        {
            return topology.size();
        }

        @Override
        public Set<Node.Id> nodes()
        {
            return topology.nodes();
        }

        @Override
        public Topologies removeEpochsBefore(long epoch)
        {
            if (epoch > topology.epoch())
                throw new IndexOutOfBoundsException(epoch + " is greater than current epoch " + topology.epoch());
            return this;
        }

        @Override
        public boolean equals(Object obj)
        {
            return Topologies.equals(this, obj);
        }

        @Override
        public int hashCode()
        {
            return Topologies.hashCode(this);
        }

        @Override
        public String toString()
        {
            return Topologies.toString(this);
        }
    }

    class Multi implements Topologies
    {
        private final List<Topology> topologies;

        public Multi(int initialCapacity)
        {
            this.topologies = new ArrayList<>(initialCapacity);
        }

        public Multi(Topology... topologies)
        {
            this(topologies.length);
            for (Topology topology : topologies)
                add(topology);
        }

        @Override
        public Topology current()
        {
            return get(0);
        }

        @Override
        public long oldestEpoch()
        {
            return get(size() - 1).epoch;
        }

        @Override
        public int findEarliestSinceEpoch(long epoch)
        {
            long current = current().epoch;
            if (current < epoch)
                return 0;

            long index = current - epoch;
            if (index > topologies.size())
                return topologies.size() - 1;

            return (int) index;
        }

        @Override
        public boolean fastPathPermitted()
        {
            return false;
        }

        @Override
        public Topology get(int i)
        {
            return topologies.get(i);
        }

        @Override
        public int size()
        {
            return topologies.size();
        }

        @Override
        public int totalShards()
        {
            int count = 0;
            for (int i=0, mi= topologies.size(); i<mi; i++)
                count += topologies.get(i).size();
            return count;
        }

        @Override
        public Set<Node.Id> nodes()
        {
            Set<Node.Id> result = new HashSet<>();
            for (int i=0,mi=size(); i<mi; i++)
                result.addAll(get(i).nodes());
            return result;
        }

        @Override
        public Topologies removeEpochsBefore(long epoch)
        {
            long current = currentEpoch();
            if (epoch > current)
                throw new IndexOutOfBoundsException(epoch + " is greater than current epoch " + current);
            if (epoch <= topologies.get(0).epoch())
                return this;
            if (epoch == current)
                return new Single(current(), fastPathPermitted());

            int numEpochs = (int) (current - epoch + 1);
            Topology[] result = new Topology[numEpochs];
            int startIdx = topologies.size() - numEpochs;
            for (int i=0; i<result.length; i++)
                result[i] = topologies.get(startIdx + 1);
            Preconditions.checkState(result[0].epoch() >= epoch);
            Preconditions.checkState(current == result[result.length - 1].epoch());
            return new Multi(result);
        }

        public void add(Topology topology)
        {
            Preconditions.checkArgument(topologies.isEmpty() || topology.epoch == topologies.get(topologies.size() - 1).epoch - 1);
            topologies.add(topology);
        }

        @Override
        public boolean equals(Object obj)
        {
            return Topologies.equals(this, obj);
        }

        @Override
        public int hashCode()
        {
            return Topologies.hashCode(this);
        }

        @Override
        public String toString()
        {
            return Topologies.toString(this);
        }
    }
}
