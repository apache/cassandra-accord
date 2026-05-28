/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package accord.local.durability;

import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import accord.Utils;
import accord.api.MessageSink;
import accord.impl.IntKey;
import accord.impl.mock.MockCluster;
import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.Range;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.SortedArrays.SortedArrayList;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import static accord.Utils.id;
import static accord.Utils.idList;

/**
 * Tests for {@link ShardDurability}, specifically verifying that topology updates
 * do not produce redundant {@code markDefunct()} calls on already-defunct schedulers.
 */
public class ShardDurabilityTest
{
    private ListAppender<ILoggingEvent> logAppender;
    private Logger shardDurabilityLogger;
    private Node node;

    @BeforeEach
    public void setUp()
    {
        // Capture log events from ShardDurability
        shardDurabilityLogger = (Logger) LoggerFactory.getLogger(ShardDurability.class);
        logAppender = new ListAppender<>();
        logAppender.start();
        shardDurabilityLogger.addAppender(logAppender);
    }

    @AfterEach
    public void tearDown()
    {
        shardDurabilityLogger.detachAppender(logAppender);
        logAppender.stop();
        if (node != null)
            node.shutdown();
    }

    /**
     * Verify that when a shard range is replaced by a new range through successive topology updates,
     * the old scheduler is marked defunct exactly once — not re-marked on each subsequent topology change.
     *
     * Before the fix in {@code markDefunct()}, the method would log and set {@code stopping = true}
     * on every call. Since {@code updateTopology()} puts defunct schedulers back into the
     * {@code shardSchedulers} map (line 664: {@code shardSchedulers.putAll(prev)}), each subsequent
     * topology update would re-encounter the already-defunct schedulers in the {@code prev} copy
     * and call {@code markDefunct()} again, producing O(N²) log messages across N topology updates.
     */
    @Test
    public void markDefunctCalledOncePerScheduler()
    {
        Id nodeId = id(1);
        SortedArrayList<Id> nodes = idList(1, 2, 3);
        Set<Id> fastPath = new TreeSet<>(nodes);
        MockCluster.Clock clock = new MockCluster.Clock(100);

        // Initial topology: node 1 owns range [0, 100)
        Range range1 = IntKey.range(0, 100);
        Shard shard1 = Shard.create(range1, nodes, fastPath);
        Topology topology1 = new Topology(1, shard1);

        node = Utils.createNode(nodeId, topology1, new MessageSink.NoOpSink(), clock);
        ShardDurability durability = new ShardDurability(node);

        // First topology update: establishes scheduler for range [0, 100)
        durability.updateTopology(topology1);
        long startCount = countDefunctMessages();
        Assertions.assertEquals(0, startCount, "No defunct messages expected after first topology");

        // Now simulate N topology changes, each replacing the shard range entirely.
        // This mimics what happens in a test like ShortReadProtectionTest that creates
        // many tables, each causing a topology update with new Accord shard ranges.
        int topologyChanges = 20;
        for (int i = 0; i < topologyChanges; i++)
        {
            // Each epoch introduces a new range, removing the previous one.
            // The old range's scheduler should be marked defunct exactly once.
            Range newRange = IntKey.range(100 * (i + 1), 100 * (i + 2));
            Shard newShard = Shard.create(newRange, nodes, fastPath);
            Topology newTopology = new Topology(i + 2, newShard);
            durability.updateTopology(newTopology);
        }

        long defunctCount = countDefunctMessages();

        // With N topology changes (each replacing one range), we expect:
        //  - Topology 2 marks range1's scheduler defunct (1 message)
        //  - Topology 3 marks topology2's scheduler defunct (1 message) 
        //  - ... and so on
        // Total: exactly N defunct messages (one per replaced scheduler).
        //
        // WITHOUT the fix, each topology change re-marks ALL previously-defunct schedulers,
        // producing 1 + 2 + 3 + ... + N = N*(N+1)/2 defunct messages.
        Assertions.assertEquals(topologyChanges, defunctCount,
                                "Each scheduler should be marked defunct exactly once. " +
                                "Got " + defunctCount + " defunct messages for " + topologyChanges +
                                " topology changes (expected " + topologyChanges + ", " +
                                "would be " + (topologyChanges * (topologyChanges + 1) / 2) +
                                " without the fix)");
    }

    private long countDefunctMessages()
    {
        return logAppender.list.stream()
                               .filter(e -> e.getFormattedMessage().contains("defunct"))
                               .count();
    }
}
