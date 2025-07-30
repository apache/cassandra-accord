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

package accord.debug.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import accord.coordinate.Coordination;
import accord.debug.model.*;
import accord.debug.util.BlockedGraphUtil;
import accord.debug.util.CommandStoreTxnBlockedGraph;
import accord.impl.InMemoryCommandStore;
import accord.impl.progresslog.DefaultProgressLog;
import accord.impl.progresslog.TxnStateKind;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.local.RedundantBefore;
import accord.local.RedundantStatus;
import accord.local.StoreParticipants;
import accord.local.cfk.CommandsForKey;
import accord.local.durability.ShardDurability;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.TxnId;
import accord.topology.TopologyManager;
import accord.utils.Invariants;

import static accord.local.RedundantStatus.Property.GC_BEFORE;
import static accord.local.RedundantStatus.Property.LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_COMMAND_STORE;
import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_DATA_STORE;
import static accord.local.RedundantStatus.Property.LOCALLY_REDUNDANT;
import static accord.local.RedundantStatus.Property.LOCALLY_SYNCED;
import static accord.local.RedundantStatus.Property.LOCALLY_WITNESSED;
import static accord.local.RedundantStatus.Property.PRE_BOOTSTRAP;
import static accord.local.RedundantStatus.Property.QUORUM_APPLIED;
import static accord.local.RedundantStatus.Property.SHARD_APPLIED;

public class BurnTestController implements Controller
{
    private final Map<Integer, Node> nodes;

    public BurnTestController(Map<Integer, Node> nodes)
    {
        this.nodes = nodes;
    }

    public void registerNode(Node node)
    {
        Invariants.require(nodes.put(node.id().id, node) == null);
    }

    @Override
    public List<NodeInfo> getNodes()
    {
        List<NodeInfo> nodes = new ArrayList<>();
        this.nodes.forEach((i, n) -> {
            List<StoreInfo> stores = new ArrayList<>();
            for (CommandStore store : n.commandStores().all())
                stores.add(new StoreInfo(store.id(), store.unsafeGetRangesForEpoch().currentRanges().toRanges()));

            nodes.add(new NodeInfo(Integer.toString(n.id().id), stores));
        });

        return nodes;
    }

    @Override
    public List<RedundantBeforeInfo> getRedundantBefore(String nodeId)
    {
        Node node = nodes.get(Integer.parseInt(nodeId));
        Invariants.nonNull(node);
        List<RedundantBeforeInfo> res = new ArrayList<>();
        for (CommandStore store : node.commandStores().all())
        {
            // Use foldl to iterate through all ranges and bounds
            store.unsafeGetRedundantBefore().foldl((entry, acc) -> {
                acc.add(new RedundantBeforeInfo("n/a", "n/a", "n/a",
                                                entry.range.start().toString(),
                                                entry.range.end().toString(),
                                                store.id(),
                                                entry.startEpoch,
                                                entry.endEpoch,
                                                entry.maxBound(GC_BEFORE).toString(),
                                                entry.maxBound(SHARD_APPLIED).toString(),
                                                entry.maxBound(QUORUM_APPLIED).toString(),
                                                entry.maxBound(LOCALLY_APPLIED).toString(),
                                                entry.maxBound(LOCALLY_DURABLE_TO_COMMAND_STORE).toString(),
                                                entry.maxBound(LOCALLY_DURABLE_TO_DATA_STORE).toString(),
                                                entry.maxBound(LOCALLY_REDUNDANT).toString(),
                                                entry.maxBound(LOCALLY_SYNCED).toString(),
                                                entry.maxBound(LOCALLY_WITNESSED).toString(),
                                                entry.maxBound(PRE_BOOTSTRAP).toString(),
                                                entry.staleUntilAtLeast != null ? entry.staleUntilAtLeast.toString() : null));
                return acc;
            }, res, ignore -> false);
        }
        return res;
    }

    @Override
    public List<CoordinationInfo> getCoordinations(String nodeId)
    {
        Node node = nodes.get(Integer.parseInt(nodeId));
        Invariants.nonNull(node);

        List<CoordinationInfo> coordinations = new ArrayList<>();
        
        for (Coordination c : node.coordinations())
        {
            coordinations.add(new CoordinationInfo(
                toStringOrNull(c.txnId()),
                c.kind().toString(),
                c.coordinationId(),
                c.describe(),
                toStringOrNull(c.nodes()),
                toStringOrNull(c.inflight()),
                toStringOrNull(c.contacted()),
                toStringOrNull(c.scope()),
                summarise(c.replies()),
                summarise(c.tracker())
            ));
        }

        return coordinations;
    }

    @Override
    public List<TxnInfo> getTxn(String nodeId, String txnId)
    {
        Node node = nodes.get(Integer.parseInt(nodeId));
        Invariants.nonNull(node);
        
        List<TxnInfo> results = new ArrayList<>();
        TxnId parsedTxnId = TxnId.parse(txnId);

        // Use the same pattern as AccordDebugKeyspace.TxnTable
        for (CommandStore s : node.commandStores().all())
        {
            InMemoryCommandStore store = (InMemoryCommandStore) s;
            InMemoryCommandStore.GlobalCommand command = store.command(parsedTxnId);
            if (command.value() != null)
                results.add(createTxnInfo(store.id(), txnId, command.value()));
        }
        
        return results;
    }
    
    private TxnInfo createTxnInfo(int commandStoreId, String txnIdStr, Command command)
    {
        System.out.println(command.waitingOn());
        return new TxnInfo(commandStoreId,
                           txnIdStr,
                           toStringOrNull(command.saveStatus()),
                           toStringOrNull(command.route()),
                           toStringOrNull(command.durability()),
                           toStringOrNull(command.executeAt()),
                           toStringOrNull(command.executesAtLeast()),
                           toStringOrNull(command.partialTxn()),
                           toStringOrNull(command.partialDeps()),
                           command.waitingOn() == null ? null : command.waitingOn().keys.stream().map(Object::toString).collect(Collectors.toList()),
                           command.waitingOn() == null ? null : command.waitingOn().asListUnsafe().stream().map(Object::toString).collect(Collectors.toList()),
                           toStringOrNull(command.writes()),
                           toStringOrNull(command.result()),
                           toStr(command.participants(), StoreParticipants::owns),
                           toStr(command.participants(), StoreParticipants::touches),
                           toStringOrNull(command.participants().hasTouched()),
                           toStr(command.participants(), StoreParticipants::executes),
                           toStr(command.participants(), StoreParticipants::waitsOn)
        );
    }

    public List<TxnBlockedByInfo> getTxnBlockedBy(String nodeId, String txnId)
    {
        Node node = nodes.get(Integer.parseInt(nodeId));
        Invariants.nonNull(node);
        
        List<TxnBlockedByInfo> results = new ArrayList<>();
        TxnId parsedTxnId = TxnId.parse(txnId);
        
        BlockedGraphUtil util = new BlockedGraphUtil();
        List<CommandStoreTxnBlockedGraph> shards = util.loadDebug(node, parsedTxnId);
        
        for (CommandStoreTxnBlockedGraph shard : shards)
        {
            Set<TxnId> processed = new HashSet<>();
            process(results, shard, processed, parsedTxnId, 0, Integer.MAX_VALUE, parsedTxnId, "Self", null);
            
            // Verify everything was processed
            if (!shard.txns.isEmpty() && !shard.txns.keySet().containsAll(processed))
            {
                Set<TxnId> skipped = new HashSet<>(shard.txns.keySet());
                skipped.removeAll(processed);
                // Log skipped transactions but don't fail - this is debug information
            }
        }
        
        return results;
    }
    
    private void process(List<TxnBlockedByInfo> results, CommandStoreTxnBlockedGraph shard, 
                        Set<TxnId> processed, TxnId userTxn, int depth, int maxDepth, 
                        TxnId txnId, String reason, Runnable onDone)
    {
        if (!processed.add(txnId))
            return; // Already processed
            
        CommandStoreTxnBlockedGraph.TxnState txn = shard.txns.get(txnId);
        if (txn == null)
        {
            if (!"Self".equals(reason))
                return; // Unknown transaction
        }
        
        // Skip applied transactions unless it's the root transaction
        if (!"Self".equals(reason) && txn != null && txn.saveStatus.hasBeen(accord.primitives.Status.Applied))
            return;
            
        // Create TxnBlockedByInfo entry
        results.add(new TxnBlockedByInfo(
            userTxn.toString(), // txn_id
            "n/a", // keyspace_name - would need table metadata
            "n/a", // table_name - would need table metadata  
            shard.commandStoreId, // command_store_id
            depth, // depth
            "Self".equals(reason) ? "" : txn.txnId.toString(), // blocked_by
            reason, // reason
            txn != null ? txn.saveStatus.name() : null, // save_status
            txn != null && txn.executeAt != null ? txn.executeAt.toString() : null, // execute_at
            null // key (set by onDone for key-based blocking)
        ));
        
        if (onDone != null)
            onDone.run();
            
        if (txn != null && txn.isBlocked() && depth < maxDepth)
        {
            // Process transactions this one is blocked by
            for (TxnId blockedBy : txn.blockedBy)
            {
                if (!processed.contains(blockedBy))
                    process(results, shard, processed, userTxn, depth + 1, maxDepth, blockedBy, "Txn", null);
            }
            
            // Process keys this one is blocked by  
            for (accord.api.RoutingKey blockedBy : txn.blockedByKey)
            {
                TxnId blocking = shard.keys.get(blockedBy);
                if (blocking != null && !processed.contains(blocking))
                {
                    process(results, shard, processed, userTxn, depth + 1, maxDepth, blocking, "Key", 
                           () -> {
                               // Update the key field for the last added result
                               if (!results.isEmpty())
                               {
                                   TxnBlockedByInfo last = results.get(results.size() - 1);
                                   // Create a new TxnBlockedByInfo with the key field set
                                   TxnBlockedByInfo updated = new TxnBlockedByInfo(
                                       last.txnId, last.keyspaceName, last.tableName, 
                                       last.commandStoreId, last.depth, last.blockedBy, 
                                       last.reason, last.saveStatus, last.executeAt,
                                       blockedBy.toString() // key
                                   );
                                   results.set(results.size() - 1, updated);
                               }
                           });
                }
            }
        }
    }

    @Override
    public List<ProgressLogInfo> getProgressLog(String nodeId)
    {
        Node node = nodes.get(Integer.parseInt(nodeId));
        Invariants.nonNull(node);
        
        List<ProgressLogInfo> results = new ArrayList<>();
        
        // Access progress log from command stores
        node.commandStores().forEach((store, rangesForEpoch) -> {
            try 
            {
                DefaultProgressLog.ImmutableView view = ((DefaultProgressLog) store.unsafeProgressLog()).immutableView();
                while (view.advance())
                {
                    results.add(new ProgressLogInfo(
                        "n/a", // keyspace_name - would need table metadata access
                        "n/a", // table_name - would need table metadata access  
                        "n/a", // table_id - would need table metadata access
                        view.commandStoreId(), // command_store_id
                        view.txnId().toString(), // txn_id
                        view.contactEveryone(), // contact_everyone
                        view.isWaitingUninitialised(), // waiting_is_uninitialised
                        view.waitingIsBlockedUntil().name(), // waiting_blocked_until
                        view.waitingHomeSatisfies().name(), // waiting_home_satisfies
                        view.waitingProgress().name(), // waiting_progress
                        view.waitingRetryCounter(), // waiting_retry_counter
                        Long.toBinaryString(view.waitingPackedKeyTrackerBits()), // waiting_packed_key_tracker_bits
                        toTimestamp(view.timerScheduledAt(TxnStateKind.Waiting)), // waiting_scheduled_at
                        view.homePhase().name(), // home_phase
                        view.homeProgress().name(), // home_progress
                        view.homeRetryCounter(), // home_retry_counter
                        toTimestamp(view.timerScheduledAt(TxnStateKind.Home)) // home_scheduled_at
                    ));
                }
            } 
            catch (Exception e) 
            {
                // Progress log not accessible, continue
            }
        });
        
        return results;
    }
    
    private static long toTimestamp(Long deadline)
    {
        if (deadline == null)
            return 0;
        // Convert from microseconds to milliseconds (similar to AccordDebugKeyspace pattern)
        return deadline / 1000L;
    }

    @Override
    public List<DurabilityServiceInfo> getDurabilityService(String nodeId)
    {
        Node node = nodes.get(Integer.parseInt(nodeId));
        Invariants.nonNull(node);
        
        List<DurabilityServiceInfo> results = new ArrayList<>();
        
        ShardDurability.ImmutableView view = node.durability().shards().immutableView();
        while (view.advance())
        {
            results.add(new DurabilityServiceInfo(
                "n/a", // keyspace_name - would need table metadata access
                "n/a", // table_name - would need table metadata access  
                view.shard().range.start().toString(), // token_start
                view.shard().range.end().toString(), // token_end
                view.lastStartedAtMicros() * 1000, // last_started_at (convert to millis)
                view.cycleStartedAtMicros() * 1000, // cycle_started_at (convert to millis)
                view.retries(), // retries
                toStringOrNull(view.min()), // min
                toStringOrNull(view.requestedBy()), // requested_by
                toStringOrNull(view.active()), // active
                toStringOrNull(view.waiting()), // waiting
                view.nodeOffset(), // node_offset
                view.cycleOffset(), // cycle_offset
                view.activeIndex(), // active_index
                view.nextIndex(), // next_index
                view.toIndex(), // next_to_index
                view.cycleLength(), // end_index
                view.currentSplits(), // current_splits
                view.stopping(), // stopping
                view.stopped() // stopped
            ));
        }
        
        return results;
    }

    @Override
    public List<CommandStoreInfo> getCommandStores(String nodeId)
    {
        Node node = nodes.get(Integer.parseInt(nodeId));
        Invariants.nonNull(node);
        List<CommandStoreInfo> res = new ArrayList<>();
        node.commandStores().forEach((store, rangesForEpoch) -> {
            Map<String, List<String>> safeToReadMap = new LinkedHashMap<>();
            Map<String, List<String>> rangesForEpochMap = new LinkedHashMap<>();
            rangesForEpoch.forEach((epoch, ranges) -> {
                rangesForEpochMap.put(Long.toString(epoch),
                                      toStrings(ranges));
            });

            store.unsafeGetSafeToRead().forEach((timestamp, ranges) -> {
                safeToReadMap.put(timestamp.toStandardString(),
                                  toStrings(ranges));
            });
            res.add(new CommandStoreInfo(store.id(),
                                         safeToReadMap,
                                         rangesForEpochMap));
        });
        return res;
    }

    @Override
    public List<DurableBeforeInfo> getDurableBefore(String nodeId)
    {
        Node node = nodes.get(Integer.parseInt(nodeId));
        Invariants.nonNull(node);
        
        List<DurableBeforeInfo> results = new ArrayList<>();
        
        DurableBefore durableBefore = node.durableBefore();
        durableBefore.foldlWithBounds(
            (entry, acc, start, end) -> {
                acc.add(new DurableBeforeInfo("n/a",
                                              "n/a",
                                              start.toString(),
                                              end.toString(),
                                              entry.quorumBefore.toString(),
                                              entry.universalBefore.toString()
                ));
                return acc;
            },
            results,
            ignore -> false
        );
        
        return results;
    }

    @Override
    public List<TopologyInfo> getTopologies(String nodeId)
    {
        Node node = nodes.get(Integer.parseInt(nodeId));
        Invariants.nonNull(node);
        
        List<TopologyInfo> results = new ArrayList<>();
        
        TopologyManager.EpochsSnapshot snapshot = node.topology().epochsSnapshot();
        for (TopologyManager.EpochsSnapshot.Epoch epoch : snapshot)
        {
            // Create EpochInfo from the epoch data
            EpochInfo epochInfo = new EpochInfo(
                epoch.epoch,
                epoch.ready.metadata.value,
                epoch.ready.coordinate.value, 
                epoch.ready.data.value,
                epoch.ready.reads.value,
                epoch.ready.reads == TopologyManager.EpochsSnapshot.ResultStatus.SUCCESS
            );
            
            // Create a single TableEpoch entry with all range types
            List<TableEpoch> tableEpochs = List.of(new TableEpoch(
                epoch.epoch,
                "n/a", // keyspace_name
                "n/a", // table_name  
                rangesToStrings(epoch.addedRanges), // added
                rangesToStrings(epoch.removedRanges), // removed
                rangesToStrings(epoch.synced), // synced
                rangesToStrings(epoch.closed), // closed
                rangesToStrings(epoch.retired)  // retired
            ));
            
            results.add(new TopologyInfo(epochInfo, tableEpochs));
        }
        
        return results;
    }
    
    public List<CommandsForKeyInfo> getCommandsForKey(String nodeId, String key)
    {
        Node node = nodes.get(Integer.parseInt(nodeId));
        Invariants.nonNull(node);

        List<CommandsForKeyInfo> res = new ArrayList<>();
        // In burn test environment, we need to work with available RoutingKey implementations
        // The AccordDebugKeyspace uses TokenKey.parse() but we don't have that here
        // For now, we'll create a framework for when proper key parsing becomes available

        for (CommandStore s : node.commandStores().all())
        {
            InMemoryCommandStore store = (InMemoryCommandStore) s;
            InMemoryCommandStore.GlobalCommandsForKey ref = store.commandsForKey(key);
            if (ref.value() != null)
            {
                CommandsForKey cfk = ref.value();
                for (int i = 0; i < cfk.size(); ++i)
                {
                    CommandsForKey.TxnInfo txn = cfk.get(i);
                    res.add(new CommandsForKeyInfo(
                    key,
                    store.id(),
                    toStringOrNull(txn.plainTxnId()),
                    toStringOrNull(txn.ballot()),
                    toStringOrNull(txn.depsKnownUntilExecuteAt()),
                    toStringOrNull(txn.plainExecuteAt()),
                    buildFlags(txn),
                    Arrays.toString(txn.missing()),
                    toStringOrNull(txn.status()),
                    txn.statusOverrides() == 0 ? null : ("0x" + Integer.toHexString(txn.statusOverrides()))
                    ));
                }
            }
        }

        return res;
    }

    @Override
    public List<TxnInfo> getTransactions(String nodeId, int storeId, String propertyFilter)
    {
        Node node = nodes.get(Integer.parseInt(nodeId));
        Invariants.nonNull(node);

        // For now only for BurnTest
        InMemoryCommandStore store = (InMemoryCommandStore) node.commandStores().all()[storeId];

        // Get redundant before data for property filtering
        RedundantBefore redundantBefore = null;
        RedundantStatus.Property property = null;
        if (propertyFilter != null && !propertyFilter.trim().isEmpty())
        {
            try
            {
                property = RedundantStatus.Property.valueOf(propertyFilter.trim());
                redundantBefore = store.unsafeGetRedundantBefore();
            }
            catch (IllegalArgumentException e)
            {
                throw new IllegalArgumentException(String.format("Invalid property: %s. Valid properties: %s", propertyFilter, Arrays.toString(RedundantStatus.Property.values())));
            }
        }

        List<TxnInfo> transactions = new ArrayList<>();
        NavigableMap<TxnId, InMemoryCommandStore.GlobalCommand> commands = store.unsafeCommands();

        for (InMemoryCommandStore.GlobalCommand globalCommand : commands.values())
        {
            Command command = globalCommand.value();
            if (command == null)
                continue;

            // Apply property filter if specified
            boolean satisfiesProperty = true;
            if (property != null && redundantBefore != null)
                satisfiesProperty = RedundantBefore.satisfies(redundantBefore, command.txnId(), command.route().homeKey(), property);

            if (satisfiesProperty)
            {
                TxnInfo txnInfo = createTxnInfo(store.id(), command.txnId().toString(), command);
                transactions.add(txnInfo);
            }
        }
        return transactions;
    }

    @Override
    public TxnInfo getTransaction(String nodeId, int storeId, String txnId)
    {
        Node node = nodes.get(Integer.parseInt(nodeId));
        Invariants.nonNull(node);

        // For now only for BurnTest
        InMemoryCommandStore store = (InMemoryCommandStore) node.commandStores().all()[storeId];
        TxnId parsedTxnId = TxnId.parse(txnId);
        InMemoryCommandStore.GlobalCommand command = store.unsafeCommands().get(parsedTxnId);
        if (command.value() == null)
            return null;

        return createTxnInfo(store.id(), txnId, command.value());
    }

    private static String buildFlags(CommandsForKey.TxnInfo txn)
    {
        StringBuilder sb = new StringBuilder();
        if (!txn.mayExecute())
        {
            sb.append("NO EXECUTE");
        }
        if (txn.hasNotifiedReady())
        {
            if (sb.length() > 0) sb.append(", ");
            sb.append("NOTIFIED READY");
        }
        if (txn.hasNotifiedWaiting())
        {
            if (sb.length() > 0) sb.append(", ");
            sb.append("NOTIFIED WAITING");
        }
        return sb.toString();
    }

    private static String safeToString(Object obj)
    {
        if (obj == null) return "null";
        try
        {
            return obj.toString();
        }
        catch (Exception e)
        {
            return "Error: " + e.getMessage();
        }
    }

    private static List<String> rangesToStrings(Ranges ranges)
    {
        List<String> result = new ArrayList<>();
        for (Range range : ranges)
        {
            result.add(range.toString());
        }
        return result;
    }

    private static List<String> toStrings(Ranges ranges)
    {
        List<String> rangeStrings = new ArrayList<>();
        for (Range range : ranges)
            rangeStrings.add(range.toString());
        return rangeStrings;
    }

    private static String toStringOrNull(Object obj)
    {
        return obj == null ? null : obj.toString();
    }

    private static String summarise(Object obj)
    {
        // TODO: Implement proper summarization logic based on the object type
        // This should provide a concise summary of complex objects like replies and trackers
        return obj == null ? null : obj.toString();
    }

    private static String toStr(StoreParticipants participants, Function<StoreParticipants, ?> extractor)
    {
        if (participants == null) return null;
        try 
        {
            Object result = extractor.apply(participants);
            return toStringOrNull(result);
        } 
        catch (Exception e) 
        {
            return null;
        }
    }
}
