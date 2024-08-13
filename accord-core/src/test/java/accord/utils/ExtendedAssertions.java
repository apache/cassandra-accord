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

package accord.utils;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.messages.PreAccept;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.TxnRequest;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Unseekables;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.SortedArrays.SortedArrayList;
import org.agrona.collections.LongArrayList;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ObjectAssert;
import org.assertj.core.error.BasicErrorMessageFactory;
import org.assertj.core.error.ShouldBeEmpty;
import org.assertj.core.error.ShouldHaveSize;
import org.assertj.core.error.ShouldNotBeEmpty;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class ExtendedAssertions
{
    public static class ShardAssert extends AbstractAssert<ShardAssert, Shard>
    {
        protected ShardAssert(Shard shard)
        {
            super(shard, ShardAssert.class);
        }

        public ShardAssert contains(RoutingKey key)
        {
            isNotNull();
            if (!actual.contains(key))
                throwAssertionError(new BasicErrorMessageFactory("%nExpected shard to contain key %s, but did not; range was %s", key, actual.range));
            return myself;
        }
    }

    public static class TopologyAssert extends AbstractAssert<TopologyAssert, Topology>
    {
        protected TopologyAssert(Topology topology)
        {
            super(topology, TopologyAssert.class);
        }

        public TopologyAssert isSubset()
        {
            isNotNull();
            if (!actual.isSubset())
                throwAssertionError(new BasicErrorMessageFactory("%nExpected to be a subset but was not"));
            return myself;
        }

        public TopologyAssert isNotSubset()
        {
            isNotNull();
            if (actual.isSubset())
                throwAssertionError(new BasicErrorMessageFactory("%nExpected not to be a subset but was"));
            return myself;
        }

        public TopologyAssert isEmpty()
        {
            isNotNull();
            if (actual.size() != 0 || !actual.shards().isEmpty() || !actual.nodes().isEmpty())
                throwAssertionError(ShouldBeEmpty.shouldBeEmpty(actual));
            return myself;
        }

        public TopologyAssert isNotEmpty()
        {
            isNotNull();
            if (actual.size() == 0 || actual.shards().isEmpty() || actual.nodes().isEmpty())
                throwAssertionError(ShouldNotBeEmpty.shouldNotBeEmpty());
            return myself;
        }

        public TopologyAssert hasSize(int size)
        {
            isNotNull();
            if (actual.size() != size)
                throwAssertionError(ShouldHaveSize.shouldHaveSize(actual, actual.size(), size));
            return myself;
        }

        public TopologyAssert isShardsEqualTo(Shard... shards)
        {
            return isShardsEqualTo(Arrays.asList(shards));
        }

        public TopologyAssert isShardsEqualTo(List<Shard> shards)
        {
            isNotNull();
            objects.assertEqual(info, actual.shards(), shards);
            return myself;
        }

        public TopologyAssert isHostsEqualTo(SortedArrayList<Node.Id> nodes)
        {
            isNotNull();
//            Collection<Node.Id> actualNodes = actual.nodes();
//            if (!(actualNodes instanceof SortedArrayList)) actualNodes = SortedArrayList.copyUnsorted(nodes, Node.Id[]::new);
            objects.assertEqual(info, actual.nodes(), nodes);
            return myself;
        }

        public TopologyAssert isRangesEqualTo(Range... range)
        {
            return isRangesEqualTo(Ranges.of(range));
        }

        public TopologyAssert isRangesEqualTo(Ranges ranges)
        {
            isNotNull();
            objects.assertEqual(info, actual.ranges(), ranges);
            return myself;
        }
    }

    public static class TopologiesAssert extends AbstractAssert<TopologiesAssert, Topologies>
    {
        protected TopologiesAssert(Topologies topologies)
        {
            super(topologies, TopologiesAssert.class);
        }

        public TopologiesAssert isEmpty()
        {
            isNotNull();
            if (!actual.isEmpty())
                throwAssertionError(ShouldBeEmpty.shouldBeEmpty(actual));
            return myself;
        }

        public TopologiesAssert isNotEmpty()
        {
            isNotNull();
            if (actual.isEmpty())
                throwAssertionError(ShouldNotBeEmpty.shouldNotBeEmpty());
            return myself;
        }

        public TopologiesAssert epochsBetween(long min, long max)
        {
            return epochsBetween(min, max, true);
        }

        public TopologiesAssert epochsBetween(long min, long max, boolean checkOldest)
        {
            isNotNull();
            LongArrayList missing = new LongArrayList();
            for (long epoch = min; epoch <= max; epoch++)
            {
                try
                {
                    assertThat(actual.forEpoch(epoch)).isNotNull();
                }
                catch (Throwable t)
                {
                    missing.add(epoch);
                }
            }
            if (!missing.isEmpty())
                throwAssertionError(new BasicErrorMessageFactory("%nExpected all epochs in [%s, %s], but %s were missing", min, max, missing));
            if (checkOldest && actual.oldestEpoch() < min)
                throwAssertionError(new BasicErrorMessageFactory("%nExpected oldest epoch to be %s, but was %s", min, actual.oldestEpoch()));
            if (actual.currentEpoch() > max)
                throwAssertionError(new BasicErrorMessageFactory("%nExpected latest epoch to be %s, but was %s", max, actual.currentEpoch()));
            return myself;
        }

        public TopologiesAssert hasEpoch(long epoch)
        {
            isNotNull();
            try
            {
                assertThat(actual.forEpoch(epoch)).isNotNull();
            }
            catch (Throwable t)
            {
                throwAssertionError(new BasicErrorMessageFactory("%nExpected epoch %s, but was missing; epochs [%s, %s]", epoch, actual.oldestEpoch(), actual.currentEpoch()));
            }
            return myself;
        }

        public TopologiesAssert containsAll(Unseekables<?> select)
        {
            isNotNull();
            for (int i = 0; i < actual.size(); i++)
            {
                Topology topology = actual.get(i);
                select = select.without(topology.ranges());
                if (select.isEmpty()) return myself;
            }
            throwAssertionError(new BasicErrorMessageFactory("%nMissing ranges detected: %s", select));
            return myself;
        }

        public TopologiesAssert topology(long epoch, Consumer<TopologyAssert> fn)
        {
            hasEpoch(epoch);
            fn.accept(assertThat(actual.forEpoch(epoch)));
            return myself;
        }
    }

    public static class PreAcceptReplyAssert extends AbstractAssert<PreAcceptReplyAssert, PreAccept.PreAcceptReply>
    {
        public PreAcceptReplyAssert(PreAccept.PreAcceptReply preAcceptReply)
        {
            super(preAcceptReply, PreAcceptReplyAssert.class);
        }

        public ObjectAssert<PreAccept.PreAcceptReply> isOk()
        {
            isNotNull();
            if (!actual.isOk())
                throwAssertionError(new BasicErrorMessageFactory("Expected Ok but was not: given %s", actual));
            return Assertions.assertThat(actual);
        }
    }

    public static <T extends Reply> ObjectAssert<T> process(TxnRequest<?> request, Node on, Node.Id replyTo, Class<T> replyType)
    {
        ReplyContext replyContext = Mockito.mock(ReplyContext.class);
        request.process(on, replyTo, replyContext);
        ArgumentCaptor<T> reply = ArgumentCaptor.forClass(replyType);
        Mockito.verify(on.messageSink()).reply(Mockito.eq(replyTo), Mockito.eq(replyContext), reply.capture());
        return Assertions.assertThat(reply.getValue());
    }

    public static PreAcceptReplyAssert assertThat(PreAccept.PreAcceptReply reply)
    {
        return new PreAcceptReplyAssert(reply);
    }

    public static ShardAssert assertThat(Shard shard)
    {
        return new ShardAssert(shard);
    }

    public static TopologyAssert assertThat(Topology topology)
    {
        return new TopologyAssert(topology);
    }

    public static TopologiesAssert assertThat(Topologies topologies)
    {
        return new TopologiesAssert(topologies);
    }
}
