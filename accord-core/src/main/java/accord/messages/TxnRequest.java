package accord.messages;

import accord.local.Node;
import accord.topology.KeyRanges;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.txn.Keys;
import accord.txn.Txn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public abstract class TxnRequest implements Request
{
    private final Scope scope;

    public TxnRequest(Scope scope)
    {
        this.scope = scope;
    }

    public Scope scope()
    {
        return scope;
    }

    /**
     * Indicates the keys the coordinator expects the recipient to service for a request, and
     * the minimum epochs the recipient will need to be aware of for each set of keys
     */
    public static class Scope
    {
        public static class KeysForEpoch
        {
            public final long epoch;
            public final Keys keys;

            public KeysForEpoch(long epoch, Keys keys)
            {
                this.epoch = epoch;
                this.keys = keys;
            }

            static KeysForEpoch forTopology(Topology topology, Node.Id node, Keys keys)
            {
                KeyRanges topologyRanges = topology.rangesForNode(node);
                if (topologyRanges == null)
                    return null;
                topologyRanges = topologyRanges.intersection(keys);
                Keys scopeKeys = keys.intersection(topologyRanges);
                return !topologyRanges.isEmpty() ? new KeysForEpoch(topology.epoch(), scopeKeys) : null;
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                KeysForEpoch that = (KeysForEpoch) o;
                return epoch == that.epoch && keys.equals(that.keys);
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(epoch, keys);
            }

            @Override
            public String toString()
            {
                return "EpochRanges{" +
                        "epoch=" + epoch +
                        ", keys=" + keys +
                        '}';
            }
        }

        private final long maxEpoch;
        private final KeysForEpoch[] epochs;

        public Scope(long maxEpoch, KeysForEpoch... epochKeys)
        {
            this.maxEpoch = maxEpoch;
            this.epochs = epochKeys;
        }

        public int size()
        {
            return epochs.length;
        }

        public KeysForEpoch get(int i)
        {
            return epochs[i];
        }

        public static Scope forTopologies(Node.Id node, Topologies topologies, Keys keys)
        {
            List<KeysForEpoch> ranges = new ArrayList<>(topologies.size());
            for (int i=topologies.size() - 1; i>=0; i--)
            {
                Topology topology = topologies.get(i);
                KeysForEpoch keysForEpoch = KeysForEpoch.forTopology(topology, node, keys);
                if (keysForEpoch != null)
                {
                    ranges.add(keysForEpoch);
                }
            }

            return new Scope(topologies.currentEpoch(), ranges.toArray(KeysForEpoch[]::new));
        }

        public static Scope forTopologies(Node.Id node, Topologies topologies, Txn txn)
        {
            return forTopologies(node, topologies, txn.keys());
        }

        public long maxEpoch()
        {
            return maxEpoch;
        }

        public boolean intersects(KeyRanges ranges)
        {
            for (KeysForEpoch keysForEpoch : this.epochs)
            {
                if (ranges.intersects(keysForEpoch.keys))
                    return true;
            }

            return false;
        }

        public Keys keys()
        {
            Keys keys = epochs[0].keys;
            for (int i = 1; i< epochs.length; i++)
                keys = keys.merge(epochs[i].keys);
            return keys;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Scope that = (Scope) o;
            return maxEpoch == that.maxEpoch && Arrays.equals(epochs, that.epochs);
        }

        @Override
        public int hashCode()
        {
            int result = Objects.hash(maxEpoch);
            result = 31 * result + Arrays.hashCode(epochs);
            return result;
        }

        @Override
        public String toString()
        {
            return "TxnRequestScope{" +
                    "epochs=" + Arrays.toString(epochs) +
                    '}';
        }
    }
}
