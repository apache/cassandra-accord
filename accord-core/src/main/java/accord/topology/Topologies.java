package accord.topology;

import accord.local.Node;
import accord.utils.IndexedConsumer;
import com.google.common.base.Preconditions;

import java.util.*;

public interface Topologies
{
    Topology current();

    default long currentEpoch()
    {
        return current().epoch;
    }

    Topology get(int i);
    int size();

    Set<Node.Id> nodes();

    default void forEach(IndexedConsumer<Topology> consumer)
    {
        for (int i=0, mi=size(); i<mi; i++)
            consumer.accept(i, get(i));
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

    class Singleton implements Topologies
    {
        private final Topology topology;

        public Singleton(Topology topology)
        {
            this.topology = topology;
        }

        @Override
        public Topology current()
        {
            return topology;
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
        public Set<Node.Id> nodes()
        {
            return topology.nodes();
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

        public void add(Topology topology)
        {
            Preconditions.checkArgument(topologies.isEmpty() || topology.epoch == topologies.get(topology.size() - 1).epoch - 1);
            topologies.add(topology);
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
        public Topology current()
        {
            return get(0);
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
