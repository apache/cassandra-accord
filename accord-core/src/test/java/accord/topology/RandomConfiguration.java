package accord.topology;

import accord.api.KeyRange;
import accord.burn.RandomConfigurationService;
import accord.impl.IntHashKey;
import accord.local.Node;
import accord.messages.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class RandomConfiguration
{
    private static final Logger logger = LoggerFactory.getLogger(RandomConfiguration.class);
    private final Supplier<Random> randomSupplier;
    private final Function<Node.Id, Node> lookup;
    private final List<Topology> epochs = new ArrayList<>();

    public RandomConfiguration(Supplier<Random> randomSupplier, Topology initialTopology, Function<Node.Id, Node> lookup)
    {
        this.randomSupplier = randomSupplier;
        this.lookup = lookup;
        this.epochs.add(Topology.EMPTY);
        this.epochs.add(initialTopology);
    }

    private enum UpdateType
    {
        BOUNDARY(RandomConfiguration::updateBoundary),
        MEMBERSHIP(RandomConfiguration::updateMembership),
        FASTPATH(RandomConfiguration::updateFastPath);



        private final BiFunction<Shard[], Random, Shard[]> function;

        UpdateType(BiFunction<Shard[], Random, Shard[]> function)
        {
            this.function = function;
        }

        public Shard[] apply(Shard[] shards, Random random)
        {
            return function.apply(shards, random);
        }

        static UpdateType kind(Random random)
        {
            int idx = random.nextInt(values().length);
            return values()[idx];
        }
    }

    private static Shard[] updateBoundary(Shard[] shards, Random random)
    {
        int idx = random.nextInt(shards.length - 1);
        Shard left = shards[idx];
        KeyRange<IntHashKey> leftRange = left.range;
        Shard right = shards[idx + 1];
        KeyRange<IntHashKey> rightRange = right.range;
        IntHashKey minBound = (IntHashKey) leftRange.split(2).get(0).end();
        IntHashKey maxBound = (IntHashKey) rightRange.split(2).get(0).start();
        IntHashKey newBound = IntHashKey.forHash(minBound.hash + random.nextInt(maxBound.hash - minBound.hash));

        shards[idx] = new Shard(IntHashKey.range(leftRange.start(), newBound), left.nodes, left.fastPathElectorate, left.pending);
        shards[idx+1] = new Shard(IntHashKey.range(newBound, rightRange.end()), right.nodes, right.fastPathElectorate, right.pending);
        logger.info("Updated boundary on {} & {}\n{} {}\n{} {}", idx, idx + 1, left, right, shards[idx], shards[idx + 1]);

        return shards;
    }

    private static Shard[] updateMembership(Shard[] shards, Random random)
    {
        if (shards.length <= 1)
            return shards;

        int idxLeft = random.nextInt(shards.length);
        Shard shardLeft = shards[idxLeft];

        int idxRight;
        Shard shardRight;
        do {
            idxRight = random.nextInt(shards.length);
            shardRight = shards[idxRight];
        } while (idxRight == idxLeft && shardLeft.nodeSet.equals(shardRight.nodeSet));

        List<Node.Id> nodesLeft;
        Node.Id toRight;
        for (;;)
        {
            nodesLeft = new ArrayList<>(shardLeft.nodes);
            toRight = nodesLeft.remove(random.nextInt(nodesLeft.size()));
            if (!shardRight.nodes.contains(toRight))
                break;
        }

        List<Node.Id> nodesRight;
        Node.Id toLeft;
        for (;;)
        {
            nodesRight = new ArrayList<>(shardRight.nodes);
            toLeft = nodesRight.remove(random.nextInt(nodesRight.size()));
            if (!nodesLeft.contains(toLeft))
                break;
        }

        nodesLeft.add(toLeft);
        nodesRight.add(toRight);

        shards[idxLeft] = new Shard(shardLeft.range, nodesLeft, newFastPath(nodesLeft, random), shardLeft.pending);
        shards[idxRight] = new Shard(shardRight.range, nodesRight, newFastPath(nodesRight, random), shardLeft.pending);
        logger.info("updated membership on {} & {}\n{} {}\n{} {}",
                    idxLeft, idxRight,
                    shardLeft.toString(true), shardRight.toString(true),
                    shards[idxLeft].toString(true), shards[idxRight].toString(true));

        return shards;
    }

    private static Set<Node.Id> newFastPath(List<Node.Id> nodes, Random random)
    {
        List<Node.Id> available = new ArrayList<>(nodes);
        int rf = available.size();
        int f = Shard.maxToleratedFailures(rf);
        int minSize = rf - f;
        int newSize = minSize + random.nextInt(f + 1);

        Set<Node.Id> fastPath = new HashSet<>();
        for (int i=0; i<newSize; i++)
        {
            int idx = random.nextInt(available.size());
            fastPath.add(available.remove(idx));
        }

        return fastPath;
    }

    private static Shard[] updateFastPath(Shard[] shards, Random random)
    {
        int idx = random.nextInt(shards.length);
        Shard shard = shards[idx];
        shards[idx] = new Shard(shard.range, shard.nodes, newFastPath(shard.nodes, random), shard.pending);
        logger.info("Updated fast path on {}\n{}\n{}", idx, shard.toString(true), shards[idx].toString(true));
        return shards;
    }

    private static class AdvertiseTopology implements Request
    {
        private final Topology topology;

        public AdvertiseTopology(Topology topology)
        {
            this.topology = topology;
        }

        @Override
        public void process(Node on, Node.Id from, long messageId)
        {
            RandomConfigurationService configService = (RandomConfigurationService) on.configService();
            configService.reportTopology(topology);
        }

        @Override
        public String toString()
        {
            return "AdvertiseTopology{" + topology.epoch() + '}';
        }
    }

    public synchronized void maybeUpdateTopology()
    {
        if (randomSupplier.get().nextInt(25) != 0)
            return;

        Random random = randomSupplier.get();
        Topology current = epochs.get(epochs.size() - 1);
        Shard[] shards = current.shards();
        int mutations = randomSupplier.get().nextInt(current.size());
        logger.info("Updating topology with {} mutations", mutations);
        for (int i=0; i<mutations; i++)
        {
            shards = UpdateType.kind(random).apply(shards, random);
        }

        Topology nextTopology = new Topology(current.epoch + 1, shards);

        logger.info("topology update to: \n{}\nfrom: \n{}", nextTopology, current);
        epochs.add(nextTopology);

        List<Node.Id> nodes = new ArrayList<>(nextTopology.nodes());
        AdvertiseTopology message = new AdvertiseTopology(nextTopology);

        int originatorIdx = randomSupplier.get().nextInt(nodes.size());
        Node originator = lookup.apply(nodes.remove(originatorIdx));
        message.process(originator, null, -1);
        originator.send(nodes, message);
    }
}