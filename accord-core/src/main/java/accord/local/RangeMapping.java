package accord.local;

import accord.topology.KeyRanges;
import accord.topology.Topology;

/**
 * maps ranges handled by this command store to their current shards by index
 */
class RangeMapping
{
    static final RangeMapping EMPTY = new RangeMapping(KeyRanges.EMPTY, Topology.EMPTY);
    final KeyRanges ranges;
    final Topology topology;

    public RangeMapping(KeyRanges ranges, Topology topology)
    {
        this.ranges = ranges;
        this.topology = topology;
    }

    static class Multi
    {
        final Topology cluster;
        final Topology local;
        final RangeMapping[] mappings;

        public Multi(Topology cluster, Topology local, RangeMapping[] mappings)
        {
            this.cluster = cluster;
            this.local = local;
            this.mappings = mappings;
        }

        static Multi empty(int size)
        {
            RangeMapping[] mappings = new RangeMapping[size];
            for (int i=0; i<size; i++)
                mappings[i] = RangeMapping.EMPTY;

            return new Multi(Topology.EMPTY, Topology.EMPTY, mappings);
        }
    }
}
