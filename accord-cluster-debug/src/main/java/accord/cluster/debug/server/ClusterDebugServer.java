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

package accord.cluster.debug.server;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.cluster.debug.controller.ExternalClusterController;
import accord.debug.model.DebugServerConfig;
import accord.debug.Response;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.staticfiles.Location;
import io.javalin.json.JavalinGson;

public class ClusterDebugServer
{
    private static final Logger logger = LoggerFactory.getLogger(ClusterDebugServer.class);

    private static final AtomicReference<ClusterDebugServer> instance = new AtomicReference<>();
    
    private final Javalin app;
    private final int port;
    private final DebugServerConfig config;
    private final Map<String, Cluster> exclusiveConnections = new ConcurrentHashMap<>();
    
    public ClusterDebugServer(int port, DebugServerConfig config)
    {
        this.port = port;
        this.config = config;
        this.app = Javalin.create(appConfig -> {
            appConfig.staticFiles.add("/web", Location.CLASSPATH);
            appConfig.jsonMapper(createCustomJsonMapper());
        });

        setupRoutes();
        initializeExclusiveConnections();
        
        instance.compareAndSet(null, this);
    }
    
    private static JavalinGson createCustomJsonMapper()
    {
        Gson gson = new GsonBuilder()
            .setPrettyPrinting()
            .create();
        
        return new JavalinGson(gson);
    }

    private void setupRoutes()
    {
        app.get("/hosts/{hostname}/redundant_before", this::handleRedundantBefore);
        app.get("/hosts/{hostname}/transactions/{txnId}", this::handleTransactionSearch);     
        app.get("/hosts/{hostname}/coordinations", this::handleCoordinations);
        app.get("/hosts/{hostname}/blocked_by/{txnId}", this::handleTxnBlockedBy);
        app.get("/hosts/{hostname}/progress_log", this::handleProgressLog);
        app.get("/hosts/{hostname}/durability_service", this::handleDurabilityService);
        app.get("/hosts/{hostname}/command_store", this::handleCommandStore); // TODO: rename!
        app.get("/hosts/{hostname}/durable_before", this::handleDurableBefore);
        app.get("/hosts/{hostname}/topologies", this::handleTopologies);
        app.get("/hosts", this::handleGetHosts);
    }
    
    public void start()
    {
        app.start(port);
        logger.info("Cluster debug server started on port {}", app.port());
    }
    
    public void stop()
    {
        exclusiveConnections.values().forEach(conn -> {
            if (!conn.isClosed()) {
                conn.close();
            }
        });
        exclusiveConnections.clear();
        
        app.stop();
        logger.info("Cluster debug server stopped");
    }
    
    
    private void handleRedundantBefore(Context ctx)
    {
        try
        {
            String hostname = ctx.pathParam("hostname");
            
            Cluster exclusiveCluster = exclusiveConnections.get(hostname);
            if (exclusiveCluster == null)
            {
                Response.sendResponse(ctx, Response.failure("Host not found in configuration: " + hostname), 404);
                return;
            }
            
            if (exclusiveCluster.isClosed())
            {
                Response.sendResponse(ctx, Response.failure("Connection to host is closed: " + hostname), 503);
                return;
            }
            
            try (Session hostSession = exclusiveCluster.connect())
            {
                var response = ExternalClusterController.getRedundantBefore(hostSession);
                Response.sendResponse(ctx, response);
            }
        }
        catch (Exception e)
        {
            logger.error("Error getting redundant_before data for host", e);
            Response.sendResponse(ctx, Response.failure("Internal server error: " + e.getMessage()));
        }
    }
    
    private void handleTransactionSearch(Context ctx)
    {
        try
        {
            String hostname = ctx.pathParam("hostname");
            String txnId = ctx.pathParam("txnId");
            
            if (txnId == null || txnId.trim().isEmpty())
            {
                Response.sendResponse(ctx, Response.failure("Transaction ID is required"), 400);
                return;
            }
            
            Cluster exclusiveCluster = exclusiveConnections.get(hostname);
            if (exclusiveCluster == null)
            {
                Response.sendResponse(ctx, Response.failure("Host not found in configuration: " + hostname), 404);
                return;
            }
            
            if (exclusiveCluster.isClosed())
            {
                Response.sendResponse(ctx, Response.failure("Connection to host is closed: " + hostname), 503);
                return;
            }
            
            try (Session hostSession = exclusiveCluster.connect())
            {
                var response = ExternalClusterController.searchTransactions(hostSession, txnId);
                Response.sendResponse(ctx, response);
            }
        }
        catch (Exception e)
        {
            logger.error("Error searching transactions for host", e);
            Response.sendResponse(ctx, Response.failure("Internal server error: " + e.getMessage()));
        }
    }
    
    private void handleCoordinations(Context ctx)
    {
        try
        {
            String hostname = ctx.pathParam("hostname");
            
            Cluster exclusiveCluster = exclusiveConnections.get(hostname);
            if (exclusiveCluster == null)
            {
                Response.sendResponse(ctx, Response.failure("Host not found in configuration: " + hostname), 404);
                return;
            }
            
            if (exclusiveCluster.isClosed())
            {
                Response.sendResponse(ctx, Response.failure("Connection to host is closed: " + hostname), 503);
                return;
            }
            
            try (Session hostSession = exclusiveCluster.connect())
            {
                var response = ExternalClusterController.getCoordinations(hostSession);
                Response.sendResponse(ctx, response);
            }
        }
        catch (Exception e)
        {
            logger.error("Error getting coordinations data for host", e);
            Response.sendResponse(ctx, Response.failure("Internal server error: " + e.getMessage()));
        }
    }
    
    private void handleTxnBlockedBy(Context ctx)
    {
        try
        {
            String hostname = ctx.pathParam("hostname");
            String txnId = ctx.pathParam("txnId");
            
            if (txnId == null || txnId.trim().isEmpty())
            {
                Response.sendResponse(ctx, Response.failure("Transaction ID is required"), 400);
                return;
            }
            
            Cluster exclusiveCluster = exclusiveConnections.get(hostname);
            if (exclusiveCluster == null)
            {
                Response.sendResponse(ctx, Response.failure("Host not found in configuration: " + hostname), 404);
                return;
            }
            
            if (exclusiveCluster.isClosed())
            {
                Response.sendResponse(ctx, Response.failure("Connection to host is closed: " + hostname), 503);
                return;
            }
            
            try (Session hostSession = exclusiveCluster.connect())
            {
                var response = ExternalClusterController.getTxnBlockedBy(hostSession, txnId);
                Response.sendResponse(ctx, response);
            }
        }
        catch (Exception e)
        {
            logger.error("Error getting blocked by data for host", e);
            Response.sendResponse(ctx, Response.failure("Internal server error: " + e.getMessage()));
        }
    }
    
    private void handleProgressLog(Context ctx)
    {
        try
        {
            String hostname = ctx.pathParam("hostname");
            
            Cluster exclusiveCluster = exclusiveConnections.get(hostname);
            if (exclusiveCluster == null)
            {
                Response.sendResponse(ctx, Response.failure("Host not found in configuration: " + hostname), 404);
                return;
            }
            
            if (exclusiveCluster.isClosed())
            {
                Response.sendResponse(ctx, Response.failure("Connection to host is closed: " + hostname), 503);
                return;
            }
            
            try (Session hostSession = exclusiveCluster.connect())
            {
                var response = ExternalClusterController.getProgressLog(hostSession);
                Response.sendResponse(ctx, response);
            }
        }
        catch (Exception e)
        {
            logger.error("Error getting progress log data for host", e);
            Response.sendResponse(ctx, Response.failure("Internal server error: " + e.getMessage()));
        }
    }
    
    private void handleDurabilityService(Context ctx)
    {
        try
        {
            String hostname = ctx.pathParam("hostname");
            
            Cluster exclusiveCluster = exclusiveConnections.get(hostname);
            if (exclusiveCluster == null)
            {
                Response.sendResponse(ctx, Response.failure("Host not found in configuration: " + hostname), 404);
                return;
            }
            
            if (exclusiveCluster.isClosed())
            {
                Response.sendResponse(ctx, Response.failure("Connection to host is closed: " + hostname), 503);
                return;
            }
            
            try (Session hostSession = exclusiveCluster.connect())
            {
                var response = ExternalClusterController.getDurabilityService(hostSession);
                Response.sendResponse(ctx, response);
            }
        }
        catch (Exception e)
        {
            logger.error("Error getting durability service data for host", e);
            Response.sendResponse(ctx, Response.failure("Internal server error: " + e.getMessage()));
        }
    }
    
    private void handleCommandStore(Context ctx)
    {
        try
        {
            String hostname = ctx.pathParam("hostname");
            
            Cluster exclusiveCluster = exclusiveConnections.get(hostname);
            if (exclusiveCluster == null)
            {
                Response.sendResponse(ctx, Response.failure("Host not found in configuration: " + hostname), 404);
                return;
            }
            
            if (exclusiveCluster.isClosed())
            {
                Response.sendResponse(ctx, Response.failure("Connection to host is closed: " + hostname), 503);
                return;
            }
            
            try (Session hostSession = exclusiveCluster.connect())
            {
                var response = ExternalClusterController.getCommandStore(hostSession);
                Response.sendResponse(ctx, response);
            }
        }
        catch (Exception e)
        {
            logger.error("Error getting command store data for host", e);
            Response.sendResponse(ctx, Response.failure("Internal server error: " + e.getMessage()));
        }
    }
    
    private void handleDurableBefore(Context ctx)
    {
        try
        {
            String hostname = ctx.pathParam("hostname");
            
            Cluster exclusiveCluster = exclusiveConnections.get(hostname);
            if (exclusiveCluster == null)
            {
                Response.sendResponse(ctx, Response.failure("Host not found in configuration: " + hostname), 404);
                return;
            }
            
            if (exclusiveCluster.isClosed())
            {
                Response.sendResponse(ctx, Response.failure("Connection to host is closed: " + hostname), 503);
                return;
            }
            
            try (Session hostSession = exclusiveCluster.connect())
            {
                var response = ExternalClusterController.getDurableBefore(hostSession);
                Response.sendResponse(ctx, response);
            }
        }
        catch (Exception e)
        {
            logger.error("Error getting durable before data for host", e);
            Response.sendResponse(ctx, Response.failure("Internal server error: " + e.getMessage()));
        }
    }
    
    private void handleTopologies(Context ctx)
    {
        try
        {
            String hostname = ctx.pathParam("hostname");
            
            Cluster exclusiveCluster = exclusiveConnections.get(hostname);
            if (exclusiveCluster == null)
            {
                Response.sendResponse(ctx, Response.failure("Host not found in configuration: " + hostname), 404);
                return;
            }
            
            if (exclusiveCluster.isClosed())
            {
                Response.sendResponse(ctx, Response.failure("Connection to host is closed: " + hostname), 503);
                return;
            }
            
            try (Session hostSession = exclusiveCluster.connect())
            {
                var response = ExternalClusterController.getTopologies(hostSession);
                Response.sendResponse(ctx, response);
            }
        }
        catch (Exception e)
        {
            logger.error("Error getting topologies data for host", e);
            Response.sendResponse(ctx, Response.failure("Internal server error: " + e.getMessage()));
        }
    }
    
    private void handleGetHosts(Context ctx)
    {
        try
        {
            if (config == null || config.getHosts() == null)
            {
                Response.sendResponse(ctx, Response.success(java.util.Collections.emptyList()));
                return;
            }
            
            Response.sendResponse(ctx, Response.success(config.getHosts()));
        }
        catch (Exception e)
        {
            logger.error("Error getting hosts list", e);
            Response.sendResponse(ctx, Response.failure("Internal server error: " + e.getMessage()));
        }
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
                Cluster exclusiveCluster = ExclusiveConnection.session(
                    builder -> builder.withPort(hostConfig.port),
                    hostConfig.host
                );
                exclusiveConnections.put(hostConfig.toString(), exclusiveCluster);
                logger.info("Created exclusive connection to {} ({}:{})", 
                    hostConfig.toString(), hostConfig.host, hostConfig.port);
            }
            catch (Exception e)
            {
                logger.error("Failed to create exclusive connection to {} ({}:{}): {}", 
                    hostConfig.toString(), hostConfig.host, hostConfig.port, e.getMessage());
            }
        }
    }
    
    public Map<String, Cluster> getExclusiveConnections()
    {
        return exclusiveConnections;
    }

    public static void main(String[] args) throws IOException
    {
        String configPath = args.length > 0 ? args[0] : "debug-config.json";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 8081;
        
        DebugServerConfig config = ConfigLoader.loadConfig(configPath);
        if (config.getServer() != null && config.getServer().port > 0)
        {
            port = config.getServer().port;
        }
        
        ClusterDebugServer server = new ClusterDebugServer(port, config);
        
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        
        server.start();
        logger.info("Cluster debug server running on http://localhost:{}", port);
        logger.info("Configuration loaded from: {}", configPath);
        logger.info("Exclusive connections established: {}", server.getExclusiveConnections().size());
        logger.info("Available endpoints:");
        logger.info("  GET /hosts/<hostname>/redundant_before - Get redundant_before data for specific host");
        logger.info("  GET /hosts/<hostname>/transactions/<txnId> - Search for transaction by ID on specific host");
        logger.info("  GET /hosts/<hostname>/coordinations - Get coordination data for specific host");
        logger.info("  GET /hosts/<hostname>/blocked_by/<txnId> - Get blocked by data for transaction on specific host");
        logger.info("  GET /hosts/<hostname>/progress_log - Get progress log data for specific host");
        logger.info("  GET /hosts/<hostname>/durability_service - Get durability service data for specific host");
        logger.info("  GET /hosts/<hostname>/command_store - Get command store data for specific host");
        logger.info("  GET /hosts/<hostname>/durable_before - Get durable before data for specific host");
        logger.info("  GET /hosts/<hostname>/topologies - Get topology information for specific host");
        logger.info("Web Interface:");
        logger.info("  /redundant_before.html?host=<hostname> - Redundant Before interface");
        logger.info("  /coordinations.html?host=<hostname> - Coordinations interface");
        logger.info("  /progress_log.html?host=<hostname> - Progress Log interface");
        logger.info("  /durability_service.html?host=<hostname> - Durability Service interface");
        logger.info("  /command_store_tmp.html?host=<hostname> - Command Store interface");
        logger.info("  /durable_before.html?host=<hostname> - Durable Before interface");
        logger.info("  /topologies.html?host=<hostname> - Topologies interface");
    }
}