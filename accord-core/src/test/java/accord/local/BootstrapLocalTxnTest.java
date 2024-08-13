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

package accord.local;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import accord.impl.PrefixedIntHashKey;
import accord.impl.basic.Cluster;
import accord.impl.basic.DelayedCommandStores.DelayedCommandStore;
import accord.messages.MessageType;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.SyncPoint;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.topology.TopologyUtils;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Invariants;
import org.assertj.core.api.Assertions;

import static accord.local.PreLoadContext.contextFor;
import static accord.utils.Property.qt;

class BootstrapLocalTxnTest
{
    private static final Gen<Gen<Cleanup>> CLEANUP_DISTRIBUTION = Gens.mixedDistribution(Cleanup.NO, Cleanup.TRUNCATE, Cleanup.TRUNCATE_WITH_OUTCOME, Cleanup.ERASE);

    @Test
    public void localOnlyTxnLifeCycle()
    {
        Ranges ranges = Ranges.ofSortedAndDeoverlapped(PrefixedIntHashKey.ranges(0, 1));
        List<Node.Id> nodes = Collections.singletonList(new Node.Id(42));
        Topology t = TopologyUtils.topology(1, nodes, ranges, 2);
        qt().check(rs -> Cluster.run(rs::fork, nodes, t, nodeMap -> new Request()
        {
            @Override
            public void process(Node on, Node.Id from, ReplyContext replyContext)
            {
                Gen<Cleanup> cleanupGen = CLEANUP_DISTRIBUTION.next(rs);
                for (int storeId : on.commandStores().ids())
                {
                    DelayedCommandStore store = (DelayedCommandStore) on.commandStores().forId(storeId);
                    // this is a bit redudent but here to make the test easier to maintain.  Pre/Post execute we validate each command to make sure everything is fine
                    // but that logic could be changed and this test has a dependency on validation the command... so to make this dependency explicit
                    // the test will call the validation logic within the test even though it will be called again in the background...
                    Consumer<Command> validate = store::validateRead;
                    TxnId globalSyncId = on.nextTxnId(Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range);
                    TxnId localSyncId = globalSyncId.as(Txn.Kind.LocalOnly);
                    TxnId nextGlobalSyncId = on.nextTxnId(Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range).withEpoch(globalSyncId.epoch() + 1);
                    Ranges ranges = AccordGens.rangesInsideRanges(store.updateRangesForEpoch().currentRanges(), (rs2, r) -> rs2.nextInt(1, 4)).next(rs);

                    FullRoute<?> route = on.computeRoute(globalSyncId, ranges);
                    SyncPoint<Ranges> syncPoint = new SyncPoint<>(globalSyncId, Deps.NONE, ranges, route);
                    Ranges valid = AccordGens.rangesInsideRanges(ranges, (rs2, r) -> rs2.nextInt(1, 4)).next(rs);
                    Invariants.checkArgument(syncPoint.keysOrRanges.containsAll(valid));
                    store.execute(contextFor(localSyncId, syncPoint.waitFor.keyDeps.keys(), KeyHistory.COMMANDS), safe -> Commands.createBootstrapCompleteMarkerTransaction(safe, localSyncId, valid))
                         .flatMap(ignore -> store.execute(contextFor(localSyncId), safe -> validate.accept(safe.get(localSyncId, route.homeKey()).current())))
                         .flatMap(ignore -> store.execute(contextFor(localSyncId), safe -> Commands.markBootstrapComplete(safe, localSyncId, ranges)))
                         .flatMap(ignore -> store.execute(contextFor(localSyncId), safe -> validate.accept(safe.get(localSyncId, route.homeKey()).current())))
                         // cleanup txn
                         .flatMap(ignore -> store.submit(PreLoadContext.empty(), safe -> {
                             Cleanup target = cleanupGen.next(rs);
                             if (target == Cleanup.NO)
                                 return Cleanup.NO;
                             safe.commandStore().setRedundantBefore(RedundantBefore.create(ranges, Long.MIN_VALUE, Long.MAX_VALUE, nextGlobalSyncId, nextGlobalSyncId, TxnId.NONE));
                             switch (target)
                             {
                                 case ERASE:
                                     safe.commandStore().setDurableBefore(DurableBefore.create(ranges, nextGlobalSyncId, nextGlobalSyncId));
                                     break;
                                 case TRUNCATE:
                                     safe.commandStore().setDurableBefore(DurableBefore.create(ranges, nextGlobalSyncId, globalSyncId));
                                     break;
                                 case TRUNCATE_WITH_OUTCOME:
                                 case INVALIDATE:
                                     // no update to DurableBefore = TRUNCATE_WITH_OUTCOME
                                     break;
                                 default:
                                     throw new UnsupportedOperationException(target.name());
                             }
                             return target;
                         }))
                         // validateRead is called implicitly _on command completion_
                         .flatMap(target -> store.execute(contextFor(localSyncId), safe -> {
                             SafeCommand cmd = safe.get(localSyncId, route.homeKey());
                             Command current = cmd.current();
                             Assertions.assertThat(current.saveStatus()).isEqualTo(target == Cleanup.NO ? SaveStatus.Applied : target.appliesIfNot);
                         }))
                         .begin(on.agent());
                }
            }

            @Override
            public MessageType type()
            {
                return null;
            }
        }));
    }
}