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

package accord.debug;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import accord.impl.InMemoryCommandStore;
import accord.impl.InMemoryCommandStore.GlobalCommand;
import accord.impl.InMemoryCommandStore.GlobalCommandsForKey;
import accord.local.Command;
import accord.local.cfk.CommandsForKey;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Node;
import accord.local.RedundantBefore;
import accord.local.RedundantStatus;
import accord.api.RoutingKey;
import accord.primitives.Range;
import accord.primitives.TxnId;

public class DeprecatedController
{
    public static Response<List<Model.NodeInfo>> getNodesWithStores(Map<Integer, Node> nodes)
    {
        try
        {
            List<Model.NodeInfo> nodesWithStores = new ArrayList<>();
            
            for (Map.Entry<Integer, Node> entry : nodes.entrySet())
            {
                int nodeId = entry.getKey();
                Node node = entry.getValue();
                
                List<Model.StoreInfo> stores = new ArrayList<>();
                
                try
                {
                    CommandStores commandStores = node.commandStores();
                    CommandStore[] allStores = commandStores.all();
                    
                    for (int i = 0; i < allStores.length; i++)
                    {
                        CommandStore store = allStores[i];
                        if (store != null)
                        {
                            List<Range> ranges = new ArrayList<>();
                            for (Range range : store.unsafeGetRangesForEpoch().all())
                                ranges.add(range);
                            stores.add(new Model.StoreInfo(store.id(), ranges));
                        }
                    }
                }
                catch (Exception e)
                {
                    // Log warning but continue with empty store list for this node
                }
                
                nodesWithStores.add(new Model.NodeInfo(nodeId, stores));
            }
            
            nodesWithStores.sort((a, b) -> Integer.compare(a.id, b.id));
            return Response.success(nodesWithStores);
        }
        catch (Exception e)
        {
            return Response.failure("Error getting nodes with stores: " + e.getMessage());
        }
    }

    public static Response<List<Model.NodeInfo>> getNodesByRange(Map<Integer, Node> nodes, String rangeString)
    {
        try
        {
            Range range = parseRange(rangeString);
            List<Model.NodeInfo> nodesWithStores = new ArrayList<>();
            
            for (Map.Entry<Integer, Node> entry : nodes.entrySet())
            {
                int nodeId = entry.getKey();
                Node node = entry.getValue();
                
                List<Model.StoreInfo> filteredStores = new ArrayList<>();
                
                for (CommandStore store : node.commandStores().all())
                {
                    if (store != null)
                    {
                        List<Range> matchingRanges = new ArrayList<>();

                        // Find ranges that intersect with the parsed range
                        for (Range storeRange : store.unsafeGetRangesForEpoch().all())
                        {
                            if (range.compareIntersecting(storeRange) == 0)
                                matchingRanges.add(storeRange);
                        }
                        
                        // Only include store if it has matching ranges
                        if (!matchingRanges.isEmpty())
                        {
                            filteredStores.add(new Model.StoreInfo(store.id(), matchingRanges));
                        }
                    }
                }
                
                // Only include node if it has stores with matching ranges
                if (!filteredStores.isEmpty())
                {
                    nodesWithStores.add(new Model.NodeInfo(nodeId, filteredStores));
                }
            }
            
            nodesWithStores.sort((a, b) -> Integer.compare(a.id, b.id));
            return Response.success(nodesWithStores);
        }
        catch (Exception e)
        {
            return Response.failure("Error filtering by range string '" + rangeString + "': " + e.getMessage());
        }
    }

    public static Range parseRange(String fromString)
    {
        try
        {
            // Parse range format: "prefix:(start,end]" or "prefix:[start,end)"
            int colonIndex = fromString.indexOf(':');
            if (colonIndex < 0)
            {
                throw new IllegalArgumentException("Invalid range format, expected 'prefix:(start,end]': " + fromString);
            }
            
            String prefixStr = fromString.substring(0, colonIndex);
            String rangeStr = fromString.substring(colonIndex + 1);
            
            // Parse the bracket notation
            boolean startInclusive = rangeStr.startsWith("[");
            boolean endInclusive = rangeStr.endsWith("]");
            
            if (!startInclusive && !rangeStr.startsWith("("))
                throw new IllegalArgumentException("Range must start with [ or (");
            if (!endInclusive && !rangeStr.endsWith(")"))
                throw new IllegalArgumentException("Range must end with ] or )");
            
            // Extract the content between brackets
            String content = rangeStr.substring(1, rangeStr.length() - 1);
            String[] parts = content.split(",", 2);
            if (parts.length != 2)
                throw new IllegalArgumentException("Range must contain exactly one comma");
            
            int prefix = Integer.parseInt(prefixStr);
            int start = Integer.parseInt(parts[0].trim());
            int end = Integer.parseInt(parts[1].trim());
            
            // Use reflection to call the PrefixedIntHashKey.range(prefix, start, end) method
            try
            {
                Class<?> prefixedIntHashKeyClass = Class.forName("accord.impl.PrefixedIntHashKey");
                java.lang.reflect.Method rangeMethod = prefixedIntHashKeyClass.getMethod("range", int.class, int.class, int.class);
                return (Range) rangeMethod.invoke(null, prefix, start, end);
            }
            catch (Exception e)
            {
                throw new IllegalArgumentException("Failed to create PrefixedIntHashKey range", e);
            }
        }
        catch (NumberFormatException e)
        {
            throw new IllegalArgumentException("Invalid number format in range: " + fromString, e);
        }
    }

    public static Response<List<Model.StoreInfo>> getStoresList(Node node)
    {
        List<Model.StoreInfo> stores = new ArrayList<>();
        
        try
        {
            CommandStores commandStores = node.commandStores();
            CommandStore[] allStores = commandStores.all();
            
            for (int i = 0; i < allStores.length; i++)
            {
                CommandStore store = allStores[i];
                if (store != null)
                {
                    List<Range> ranges = new ArrayList<>();
                    for (Range range : store.unsafeGetRangesForEpoch().all())
                        ranges.add(range);
                    stores.add(new Model.StoreInfo(store.id(), ranges));
                }
            }
        }
        catch (Exception e)
        {
            return Response.failure("Error accessing stores: " + e.getMessage());
        }
        
        return Response.success(stores);
    }

    public static Response<List<Model.TxnInfo>> getTransactionsList(Node node, int storeId, String propertyFilter)
    {
        try
        {
            CommandStores commandStores = node.commandStores();
            CommandStore[] allStores = commandStores.all();
            
            if (storeId < 0 || storeId >= allStores.length || allStores[storeId] == null)
            {
                return Response.failure("Store " + storeId + " not found");
            }
            
            // For now only for BurnTest
            InMemoryCommandStore store = (InMemoryCommandStore) allStores[storeId];
            CommandStore commandStore = allStores[storeId];
            
            // Get redundant before data for property filtering
            RedundantBefore redundantBefore = null;
            RedundantStatus.Property property = null;
            if (propertyFilter != null && !propertyFilter.trim().isEmpty())
            {
                try
                {
                    property = RedundantStatus.Property.valueOf(propertyFilter.trim());
                    redundantBefore = commandStore.unsafeGetRedundantBefore();
                }
                catch (IllegalArgumentException e)
                {
                    return Response.failure("Invalid property: " + propertyFilter + ". Valid properties: " + 
                                            java.util.Arrays.toString(RedundantStatus.Property.values()));
                }
            }
            
            List<Model.TxnInfo> transactions = new ArrayList<>();
            NavigableMap<TxnId, GlobalCommand> commands = store.unsafeCommands();

            try
            {
                for (GlobalCommand globalCommand : commands.values())
                {
                    Command command = globalCommand.value();
                    if (command == null)
                        continue;

                    // Apply property filter if specified
                    boolean satisfiesProperty = false;
                    if (property != null && redundantBefore != null)
                        satisfiesProperty = RedundantBefore.satisfies(redundantBefore, command.txnId(), command.route().homeKey(), property);

                    Model.TxnInfo txnInfo = new Model.TxnInfo(command.txnId(),
                                                              command.route() == null ? null : command.route().homeKey(),
                                                              safeToString(command.participants()),
                                                              command.saveStatus(),
                                                              command.durability(),
                                                              command.executeAt(),
                                                              command.promised(),
                                                              command.acceptedOrCommitted(),
                                                              command.partialDeps() == null ? null : command.partialDeps().asListUnsafe(),
                                                              command.waitingOn() == null? null : command.waitingOn().asListUnsafe(),
                                                              satisfiesProperty);
                    transactions.add(txnInfo);
                }
            }
            catch (Exception e)
            {
                return Response.failure(e.getMessage());
            }
            return Response.success(transactions);
        }
        catch (Exception e)
        {
            return Response.failure("Failed to access command store: " + e.getMessage());
        }
    }
    
    public static Response<Model.TxnInfo> getSingleTransaction(Node node, int storeId, String txnIdStr)
    {
        try
        {
            CommandStores commandStores = node.commandStores();
            CommandStore[] allStores = commandStores.all();
            
            if (storeId < 0 || storeId >= allStores.length || allStores[storeId] == null)
            {
                return Response.failure("Store " + storeId + " not found");
            }
            
            // For now only for BurnTest
            InMemoryCommandStore store = (InMemoryCommandStore) allStores[storeId];
            
            NavigableMap<TxnId, GlobalCommand> commands = store.unsafeCommands();

            // TODO: use journal instead!

            // Find the specific transaction
            for (GlobalCommand globalCommand : commands.values())
            {
                try
                {
                    Command command = globalCommand.value();
                    if (command == null) continue;
                    
                    // Check if this is the transaction we're looking for
                    if (txnIdStr.equals(command.txnId().toString()))
                    {
                        Model.TxnInfo txnInfo = new Model.TxnInfo(command.txnId(),
                                                                  command.route().homeKey(),
                                                                  // TODO: turn into something more digestable
                                                                  safeToString(command.participants()),
                                                                  command.saveStatus(),
                                                                  command.durability(),
                                                                  command.executeAt(),
                                                                  command.promised(),
                                                                  command.acceptedOrCommitted(),
                                                                  command.partialDeps() == null ? null : command.partialDeps().asListUnsafe(),
                                                                  command.waitingOn() == null ? null : command.waitingOn().asListUnsafe(),
                                                                  true); // Single transaction doesn't use property filtering
                        return Response.success(txnInfo);
                    }
                }
                catch (Exception e)
                {
                    // Skip commands that can't be processed
                    continue;
                }
            }
            
            return Response.failure("Transaction " + txnIdStr + " not found in store " + storeId);
        }
        catch (Exception e)
        {
            return Response.failure("Failed to access transaction: " + e.getMessage());
        }
    }
    
    public static Response<Model.RedundantBeforeInfo> getRedundantBefore(Node node, int storeId)
    {
        try
        {
            CommandStores commandStores = node.commandStores();
            CommandStore[] allStores = commandStores.all();
            
            if (storeId < 0 || storeId >= allStores.length || allStores[storeId] == null)
            {
                return Response.failure("Store " + storeId + " not found");
            }
            
            CommandStore store = allStores[storeId];
            RedundantBefore redundantBefore = store.unsafeGetRedundantBefore();
            
            return Response.success(Model.RedundantBeforeInfo.asJson(redundantBefore));
        }
        catch (Exception e)
        {
            return Response.failure("Failed to access redundant before data: " + e.getMessage());
        }
    }

    public static Response<Model.CommandsForKeyInfo> getCommandsForKey(Node node, int storeId, String txnIdStr)
    {
        try
        {
            CommandStores commandStores = node.commandStores();
            CommandStore[] allStores = commandStores.all();
            
            if (storeId < 0 || storeId >= allStores.length || allStores[storeId] == null)
            {
                return Response.failure("Store " + storeId + " not found");
            }
            
            InMemoryCommandStore store = (InMemoryCommandStore) allStores[storeId];
            NavigableMap<TxnId, GlobalCommand> commands = store.unsafeCommands();

            // First, find the transaction to get its routing key
            RoutingKey routingKey = null;
            for (GlobalCommand globalCommand : commands.values())
            {
                try
                {
                    Command command = globalCommand.value();
                    if (command == null) continue;
                    
                    if (txnIdStr.equals(command.txnId().toString()))
                    {
                        routingKey = command.route().homeKey();
                        break;
                    }
                }
                catch (Exception e)
                {
                    continue;
                }
            }
            
            if (routingKey == null)
            {
                return Response.failure("Transaction " + txnIdStr + " not found in store " + storeId);
            }

            // Now get CommandsForKey for this routing key
            GlobalCommandsForKey globalCfk = store.commandsForKey(routingKey);
            if (globalCfk == null)
            {
                return Response.failure("CommandsForKey not found for routing key " + routingKey);
            }

            CommandsForKey cfk = globalCfk.value();
            if (cfk == null)
            {
                return Response.failure("CommandsForKey value is null for routing key " + routingKey);
            }

            // Build the transaction info list
            List<Model.CommandsForKeyTxnInfo> txnInfos = new ArrayList<>();
            for (int i = 0; i < cfk.size(); ++i)
            {
                CommandsForKey.TxnInfo txn = cfk.get(i);
                String plainTxnId = toStringOrNull(txn.plainTxnId());
                String ballot = toStringOrNull(txn.ballot());
                String depsKnownUntilExecuteAt = toStringOrNull(txn.depsKnownUntilExecuteAt());
                String flags = flags(txn);
                String plainExecuteAt = toStringOrNull(txn.plainExecuteAt());
                String missing = java.util.Arrays.toString(txn.missing());
                String status = toStringOrNull(txn.status());
                String statusOverrides = txn.statusOverrides() == 0 ? null : ("0x" + Integer.toHexString(txn.statusOverrides()));

                txnInfos.add(new Model.CommandsForKeyTxnInfo(plainTxnId, ballot, depsKnownUntilExecuteAt,
                                                             flags, plainExecuteAt, missing, status, statusOverrides));
            }

            return Response.success(new Model.CommandsForKeyInfo(routingKey, txnInfos));
        }
        catch (Exception e)
        {
            return Response.failure("Failed to access CommandsForKey: " + e.getMessage());
        }
    }

    private static String toStringOrNull(Object obj)
    {
        return obj == null ? null : obj.toString();
    }

    private static String flags(CommandsForKey.TxnInfo txn)
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
}