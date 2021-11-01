package accord.local;

import accord.api.KeyRange;
import accord.topology.KeyRanges;
import accord.topology.Shard;
import accord.topology.Topology;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * maps ranges handled by this command store to their current shards by index
 */
class RangeMapping
{
    static final RangeMapping EMPTY = new RangeMapping(KeyRanges.EMPTY, new Shard[0], Topology.EMPTY);
    final KeyRanges ranges;
    final Shard[] shards;
    final Topology topology;

    public RangeMapping(KeyRanges ranges, Shard[] shards, Topology topology)
    {
        Preconditions.checkArgument(ranges.size() == shards.length);
        this.ranges = ranges;
        this.shards = shards;
        this.topology = topology;
    }

    private static class Builder
    {
        private final Topology localTopology;
        private final List<KeyRange> ranges;
        private final List<Shard> shards;

        public Builder(int minSize, Topology localTopology)
        {
            this.localTopology = localTopology;
            this.ranges = new ArrayList<>(minSize);
            this.shards = new ArrayList<>(minSize);
        }

        public void addMapping(KeyRange range, Shard shard)
        {
            Preconditions.checkArgument(shard.range.fullyContains(range));
            ranges.add(range);
            shards.add(shard);
        }

        public RangeMapping build()
        {
            return new RangeMapping(new KeyRanges(ranges), shards.toArray(Shard[]::new), localTopology);
        }
    }

    static RangeMapping mapRanges(KeyRanges mergedRanges, Topology localTopology)
    {
        RangeMapping.Builder builder = new RangeMapping.Builder(mergedRanges.size(), localTopology);
        int shardIdx = 0;
        for (int rangeIdx=0; rangeIdx<mergedRanges.size(); rangeIdx++)
        {
            KeyRange mergedRange = mergedRanges.get(rangeIdx);
            while (shardIdx < localTopology.size())
            {
                Shard shard = localTopology.get(shardIdx);

                int cmp = shard.range.compareIntersecting(mergedRange);
                if (cmp > 0)
                    throw new IllegalStateException("mapped shards should always be intersecting or greater than the current shard");

                if (cmp < 0)
                {
                    shardIdx++;
                    continue;
                }

                if (shard.range.fullyContains(mergedRange))
                {
                    builder.addMapping(mergedRange, shard);
                    break;
                }
                else
                {
                    KeyRange intersection = mergedRange.intersection(shard.range);
                    Preconditions.checkState(intersection.start().equals(mergedRange.start()));
                    builder.addMapping(intersection, shard);
                    mergedRange = mergedRange.subRange(intersection.end(), mergedRange.end());
                    shardIdx++;
                }
            }
        }
        return builder.build();
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

            return new Multi(RangeMapping.EMPTY.topology, RangeMapping.EMPTY.topology, mappings);
        }
    }
}
