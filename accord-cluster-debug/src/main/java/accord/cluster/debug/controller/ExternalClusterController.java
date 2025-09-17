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

package accord.cluster.debug.controller;

import accord.cluster.debug.server.ExclusiveConnection;
import accord.debug.Response;
import accord.debug.controller.Controller;
import accord.debug.model.*;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ExternalClusterController implements Controller
{
    private static final Logger logger = LoggerFactory.getLogger(ExternalClusterController.class);

    private final DebugServerConfig config;
    private final Map<String, Cluster> exclusiveConnections = new ConcurrentHashMap<>();

    public ExternalClusterController(DebugServerConfig config)
    {
        this.config = config;
        initializeExclusiveConnections();
    }

    private void initializeExclusiveConnections()
    {
        if (config == null || config.getHosts() == null)
        {
            logger.warn("No host configuration found, skipping exclusive connections setup");
            return;
        }

        for (DebugServerConfig.HostConfig hostConfig : config.getHosts())
        {
            try
            {
                Cluster exclusiveCluster = ExclusiveConnection.session(builder -> builder.withPort(hostConfig.port), hostConfig.host);
                exclusiveConnections.put(hostConfig.toString(), exclusiveCluster);
                logger.info("Created exclusive connection to {} ({}:{})",
                            hostConfig, hostConfig.host, hostConfig.port);
            }
            catch (Exception e)
            {
                logger.error("Failed to create exclusive connection to {} ({}:{}): {}",
                             hostConfig.toString(), hostConfig.host, hostConfig.port, e.getMessage());
            }
        }
    }

    @Override
    public List<NodeInfo> getNodes()
    {
        for (Map.Entry<String, Cluster> e : exclusiveConnections.entrySet())
        {

            new NodeInfo(e.getKey(),
                         new StoreInfo())
        }
        return List.of();
    }

    private static final String REDUNDANT_BEFORE_QUERY =
        "SELECT keyspace_name, table_name, table_id, token_start, token_end, " +
        "command_store_id, start_epoch, end_epoch, gc_before, shard_applied, " +
        "quorum_applied, locally_applied, locally_durable_to_command_store, " +
        "locally_durable_to_data_store, locally_redundant, locally_synced, " +
        "locally_witnessed, pre_bootstrap, stale_until_at_least " +
        "FROM system_accord_debug.redundant_before";

    private static final String COORDINATIONS_QUERY =
        "SELECT txn_id, kind, coordination_id, description, nodes, " +
        "nodes_inflight, nodes_contacted, participants, replies, tracker " +
        "FROM system_accord_debug.coordinations";

    private static final String TRANSACTION_SEARCH_QUERY =
        "SELECT command_store_id, txn_id, save_status, route, durability, " +
        "execute_at, executes_at_least, txn, deps, waiting_on, writes, result, " +
        "participants_owns, participants_touches, participants_has_touched, " +
        "participants_executes, participants_waits_on " +
        "FROM system_accord_debug.txn WHERE txn_id = ?";

    private static final String TXN_BLOCKED_BY_QUERY =
        "SELECT txn_id, keyspace_name, table_name, command_store_id, depth, " +
        "blocked_by, reason, save_status, execute_at, key " +
        "FROM system_accord_debug.txn_blocked_by WHERE txn_id = ?";

    private static final String PROGRESS_LOG_QUERY =
        "SELECT keyspace_name, table_name, table_id, command_store_id, txn_id, " +
        "contact_everyone, waiting_is_uninitialised, waiting_blocked_until, " +
        "waiting_home_satisfies, waiting_progress, waiting_retry_counter, " +
        "waiting_packed_key_tracker_bits, waiting_scheduled_at, home_phase, " +
        "home_progress, home_retry_counter, home_scheduled_at " +
        "FROM system_accord_debug.progress_log";

    private static final String DURABILITY_SERVICE_QUERY =
        "SELECT keyspace_name, table_name, token_start, token_end, " +
        "last_started_at, cycle_started_at, retries, min, requested_by, " +
        "active, waiting, node_offset, cycle_offset, active_index, " +
        "next_index, next_to_index, end_index, current_splits, stopping, stopped " +
        "FROM system_accord_debug.durability_service";

    private static final String COMMAND_STORE_QUERY =
        "SELECT command_store_id, ranges " +
        "FROM system_accord_debug.command_store";

    private static final String DURABLE_BEFORE_QUERY =
        "SELECT keyspace_name, table_name, token_start, token_end, quorum, universal " +
        "FROM system_accord_debug.durable_before";

    private static final String EPOCHS_QUERY =
        "SELECT epoch, ready_metadata, ready_coordinate, ready_data, ready_reads, ready " +
        "FROM system_views.accord_epochs";

    private static final String TABLE_EPOCHS_QUERY =
        "SELECT epoch, keyspace_name, table_name, added, removed, synced, closed, retired " +
        "FROM system_views.accord_table_epochs";

    public static Response<List<RedundantBeforeInfo>> getRedundantBefore(Session session)
    {
        try
        {
            ResultSet resultSet = session.execute(REDUNDANT_BEFORE_QUERY);
            List<RedundantBeforeInfo> results = new ArrayList<>();

            for (Row row : resultSet)
            {
                RedundantBeforeInfo rb = new RedundantBeforeInfo(
                    row.getString("keyspace_name"),
                    row.getString("table_name"),
                    row.getString("table_id"),
                    row.getString("token_start"),
                    row.getString("token_end"),
                    row.getInt("command_store_id"),
                    row.getLong("start_epoch"),
                    row.getLong("end_epoch"),
                    row.getString("gc_before"),
                    row.getString("shard_applied"),
                    row.getString("quorum_applied"),
                    row.getString("locally_applied"),
                    row.getString("locally_durable_to_command_store"),
                    row.getString("locally_durable_to_data_store"),
                    row.getString("locally_redundant"),
                    row.getString("locally_synced"),
                    row.getString("locally_witnessed"),
                    row.getString("pre_bootstrap"),
                    row.getString("stale_until_at_least")
                );
                results.add(rb);
            }

            return Response.success(results);
        }
        catch (Exception e)
        {
            logger.error("Caught an exception in controller", e);
            return Response.failure("Failed to query redundant_before table: " + e.getMessage());
        }
    }

    public static Response<List<CoordinationInfo>> getCoordinations(Session session)
    {
        try
        {
            ResultSet resultSet = session.execute(COORDINATIONS_QUERY);
            List<CoordinationInfo> results = new ArrayList<>();

            for (Row row : resultSet)
            {
                CoordinationInfo coordination = new CoordinationInfo(
                    row.getString("txn_id"),
                    row.getString("kind"),
                    row.getLong("coordination_id"),
                    row.getString("description"),
                    row.getString("nodes"),
                    row.getString("nodes_inflight"),
                    row.getString("nodes_contacted"),
                    row.getString("participants"),
                    row.getString("replies"),
                    row.getString("tracker")
                );
                results.add(coordination);
            }

            return Response.success(results);
        }
        catch (Exception e)
        {
            logger.error("Caught an exception in controller", e);
            return Response.failure("Failed to query coordinations table: " + e.getMessage());
        }
    }

    public static Response<List<TxnInfo>> searchTransactions(Session session, String txnId)
    {
        try
        {
            ResultSet resultSet = session.execute(TRANSACTION_SEARCH_QUERY, txnId);
            List<TxnInfo> results = new ArrayList<>();

            for (Row row : resultSet)
            {
                TxnInfo transaction = new TxnInfo(
                    row.getInt("command_store_id"),
                    row.getString("txn_id"),
                    row.getString("save_status"),
                    row.getString("route"),
                    row.getString("durability"),
                    row.getString("execute_at"),
                    row.getString("executes_at_least"),
                    row.getString("txn"),
                    row.getString("deps"),
                    null, null,
// TODO:
//                    row.getString("waiting_on"),
//                    row.getString("waiting_on"),
                    row.getString("writes"),
                    row.getString("result"),
                    row.getString("participants_owns"),
                    row.getString("participants_touches"),
                    row.getString("participants_has_touched"),
                    row.getString("participants_executes"),
                    row.getString("participants_waits_on")
                );
                results.add(transaction);
            }

            return Response.success(results);
        }
        catch (Exception e)
        {
            logger.error("Caught an exception in controller", e);
            return Response.failure("Failed to search transactions: " + e.getMessage());
        }
    }

    public static Response<List<TxnBlockedByInfo>> getTxnBlockedBy(Session session, String txnId)
    {
        try
        {
            ResultSet resultSet = session.execute(TXN_BLOCKED_BY_QUERY, txnId);
            List<TxnBlockedByInfo> results = new ArrayList<>();

            for (Row row : resultSet)
            {
                TxnBlockedByInfo blockedBy = new TxnBlockedByInfo(
                    row.getString("txn_id"),
                    row.getString("keyspace_name"),
                    row.getString("table_name"),
                    row.getInt("command_store_id"),
                    row.getInt("depth"),
                    row.getString("blocked_by"),
                    row.getString("reason"),
                    row.getString("save_status"),
                    row.getString("execute_at"),
                    row.getString("key")
                );
                results.add(blockedBy);
            }

            return Response.success(results);
        }
        catch (Exception e)
        {
            logger.error("Caught an exception in controller", e);
            return Response.failure("Failed to query txn_blocked_by table: " + e.getMessage());
        }
    }

    /**
     *    keyspace_name text,
     *    table_name text,
     *    table_id text,
     *    command_store_id int,
     *    txn_id 'TxnIdUtf8Type',
     *
     *    // Timer + BaseTxnState
     *    contact_everyone boolean,
     *
     *    // WaitingState
     *    waiting_is_uninitialised boolean,
     *    waiting_blocked_until text,
     *    waiting_home_satisfies text,
     *    waiting_progress text,
     *    waiting_retry_counter int,
     *    waiting_packed_key_tracker_bits text,
     *    waiting_scheduled_at timestamp,
     *
     *    //HomeState/TxnState
     *    home_phase text,
     *    home_progress text,
     *    home_retry_counter int,
     *    home_scheduled_at timestamp,
     *    PRIMARY KEY (keyspace_name, table_name, table_id, command_store_id, txn_id)" +
     */
    public static Response<List<ProgressLogInfo>> getProgressLog(Session session)
    {
        try
        {
            ResultSet resultSet = session.execute(PROGRESS_LOG_QUERY);
            List<ProgressLogInfo> results = new ArrayList<>();

            for (Row row : resultSet)
            {
                ProgressLogInfo progressLog = new ProgressLogInfo(
                    row.getString("keyspace_name"),
                    row.getString("table_name"),
                    row.getString("table_id"),
                    row.getInt("command_store_id"),
                    row.getString("txn_id"),
                    row.getBool("contact_everyone"),
                    row.getBool("waiting_is_uninitialised"),
                    row.getString("waiting_blocked_until"),
                    row.getString("waiting_home_satisfies"),
                    row.getString("waiting_progress"),
                    row.getInt("waiting_retry_counter"),
                    row.getString("waiting_packed_key_tracker_bits"),
                    row.getTimestamp("waiting_scheduled_at") != null ? row.getTimestamp("waiting_scheduled_at").getTime() : 0,
                    row.getString("home_phase"),
                    row.getString("home_progress"),
                    row.getInt("home_retry_counter"),
                    row.getTimestamp("home_scheduled_at") != null ? row.getTimestamp("home_scheduled_at").getTime() : 0
                );
                results.add(progressLog);
            }

            return Response.success(results);
        }
        catch (Exception e)
        {
            logger.error("Caught an exception in controller", e);
            return Response.failure("Failed to query progress_log table: " + e.getMessage());
        }
    }

    public static Response<List<DurabilityServiceInfo>> getDurabilityService(Session session)
    {
        try
        {
            ResultSet resultSet = session.execute(DURABILITY_SERVICE_QUERY);
            List<DurabilityServiceInfo> results = new ArrayList<>();

            for (Row row : resultSet)
            {
                DurabilityServiceInfo durabilityService = new DurabilityServiceInfo(
                    row.getString("keyspace_name"),
                    row.getString("table_name"),
                    row.getString("token_start"),
                    row.getString("token_end"),
                    row.getLong("last_started_at"),
                    row.getLong("cycle_started_at"),
                    row.getInt("retries"),
                    row.getString("min"),
                    row.getString("requested_by"),
                    row.getString("active"),
                    row.getString("waiting"),
                    row.getInt("node_offset"),
                    row.getInt("cycle_offset"),
                    row.getInt("active_index"),
                    row.getInt("next_index"),
                    row.getInt("next_to_index"),
                    row.getInt("end_index"),
                    row.getInt("current_splits"),
                    row.getBool("stopping"),
                    row.getBool("stopped")
                );
                results.add(durabilityService);
            }

            return Response.success(results);
        }
        catch (Exception e)
        {
            logger.error("Caught an exception in controller", e);
            return Response.failure("Failed to query durability_service table: " + e.getMessage());
        }
    }

    public static Response<List<CommandStoreInfo>> getCommandStore(Session session)
    {
        try
        {
            ResultSet resultSet = session.execute(COMMAND_STORE_QUERY);
            List<CommandStoreInfo> results = new ArrayList<>();

            for (Row row : resultSet)
            {
                CommandStoreInfo commandStore = new CommandStoreInfo(
                    row.getInt("command_store_id"),
                    getList(row, "ranges")
                );
                results.add(commandStore);
            }

            return Response.success(results);
        }
        catch (Exception e)
        {
            logger.error("Caught an exception in controller", e);
            return Response.failure("Failed to query command_store table: " + e.getMessage());
        }
    }

    private static List<String> getList(Row row, String column)
    {
        Object safeToReadObj = row.getObject(column);
        if (safeToReadObj instanceof List)
            return (List<String>) safeToReadObj;

        throw new IllegalStateException();
    }

    private static Map<String, List<String>> getMap(Row row, String column)
    {
        // Linked to preserve iteration order
        Map<String, List<String>> res = new LinkedHashMap<>();

        // Try to get as generic object first, then convert
        Object safeToReadObj = row.getObject(column);
        if (safeToReadObj instanceof Map)
        {
            @SuppressWarnings("unchecked")
            Map<String, Object> rawMap = (Map<String, Object>) safeToReadObj;

            for (Map.Entry<String, Object> entry : rawMap.entrySet())
            {
                String key = entry.getKey();
                Object value = entry.getValue();

                if (value instanceof List)
                {
                    @SuppressWarnings("unchecked")
                    List<String> stringList = (List<String>) value;
                    res.put(key, stringList);
                }
                else if (value != null)
                {
                    // Convert single values to single-item lists
                    res.put(key, Arrays.asList(value.toString()));
                }
            }
        }
        return res;
    }

    public static Response<List<DurableBeforeInfo>> getDurableBefore(Session session)
    {
        try
        {
            ResultSet resultSet = session.execute(DURABLE_BEFORE_QUERY);
            List<DurableBeforeInfo> results = new ArrayList<>();

            for (Row row : resultSet)
            {
                DurableBeforeInfo durableBefore = new DurableBeforeInfo(
                    row.getString("keyspace_name"),
                    row.getString("table_name"),
                    row.getString("token_start"),
                    row.getString("token_end"),
                    row.getString("quorum"),
                    row.getString("universal")
                );
                results.add(durableBefore);
            }

            return Response.success(results);
        }
        catch (Exception e)
        {
            logger.error("Caught an exception in controller", e);
            return Response.failure("Failed to query durable_before table: " + e.getMessage());
        }
    }

    public static Response<List<TopologyInfo>> getTopologies(Session session)
    {
        try
        {
            // Get epochs data
            ResultSet epochsResultSet = session.execute(EPOCHS_QUERY);
            Map<Long, EpochInfo> epochsMap = new HashMap<>();

            for (Row row : epochsResultSet)
            {
                long epochNum = row.getLong("epoch");
                EpochInfo epoch = new EpochInfo(
                    epochNum,
                    row.getString("ready_metadata"),
                    row.getString("ready_coordinate"),
                    row.getString("ready_data"),
                    row.getString("ready_reads"),
                    row.getBool("ready")
                );
                epochsMap.put(epochNum, epoch);
            }

            // Get table epochs data
            ResultSet tableEpochsResultSet = session.execute(TABLE_EPOCHS_QUERY);
            Map<Long, List<TableEpoch>> tableEpochsMap = new HashMap<>();

            for (Row row : tableEpochsResultSet)
            {
                long epochNum = row.getLong("epoch");
                TableEpoch tableEpoch = new TableEpoch(
                    epochNum,
                    row.getString("keyspace_name"),
                    row.getString("table_name"),
                    row.getList("added", String.class),
                    row.getList("removed", String.class),
                    row.getList("synced", String.class),
                    row.getList("closed", String.class),
                    row.getList("retired", String.class)
                );

                tableEpochsMap.computeIfAbsent(epochNum, k -> new ArrayList<>()).add(tableEpoch);
            }

            // Combine data into Topology objects
            List<TopologyInfo> results = new ArrayList<>();
            Set<Long> allEpochs = new HashSet<>();
            allEpochs.addAll(epochsMap.keySet());
            allEpochs.addAll(tableEpochsMap.keySet());

            for (Long epochNum : allEpochs)
            {
                EpochInfo epoch = epochsMap.get(epochNum);
                List<TableEpoch> tableEpochs = tableEpochsMap.getOrDefault(epochNum, new ArrayList<>());

                TopologyInfo topology = new TopologyInfo(epoch, tableEpochs);
                results.add(topology);
            }

            // Sort by epoch number
            results.sort(Comparator.comparingLong(a -> a.epoch != null ? a.epoch.epoch : -1L));

            return Response.success(results);
        }
        catch (Exception e)
        {
            logger.error("Caught an exception in controller", e);
            return Response.failure("Failed to query topology tables: " + e.getMessage());
        }
    }
}