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

import accord.local.Node;
import accord.local.RedundantStatus;
import accord.primitives.Range;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.staticfiles.Location;
import io.javalin.json.JavalinGson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class OldServer
{
    private static final Logger logger = LoggerFactory.getLogger(OldServer.class);

    private static final AtomicReference<OldServer> instance = new AtomicReference<>();
    
    private final Javalin app;
    private final Map<Integer, Node> nodes = new ConcurrentHashMap<>();
    private final AtomicReference<CompletableFuture<Void>> debugFuture = new AtomicReference<>();

    private final int port;
    
    public OldServer(int port)
    {
        this.port = port;
        this.app = Javalin.create(config -> {
//            config.staticFiles.add("/web", Location.CLASSPATH);
            config.staticFiles.add("/Users/ifesdjeen/p/java/cassandra-accord-protocol/accord-debug/src/main/resources/web", Location.EXTERNAL);
            config.jsonMapper(createCustomJsonMapper());
        });

        setupRoutes();
        
        // Register as singleton instance
        instance.compareAndSet(null, this);
    }
    
    private static JavalinGson createCustomJsonMapper()
    {
        Gson gson = new GsonBuilder()
            .registerTypeAdapter(Range.class, new ToStringSerializer<Range>())
            .registerTypeAdapter(TxnId.class, new ToStringSerializer<TxnId>())
            .registerTypeAdapter(Timestamp.class, new ToStringSerializer<Timestamp>())
            .registerTypeAdapter(RedundantStatus.Property.class, new ToStringSerializer<RedundantStatus.Property>())
            .create();
        
        return new JavalinGson(gson);
    }
    
    private static class ToStringSerializer<T> implements JsonSerializer<T>
    {
        @Override
        public JsonElement serialize(T src, java.lang.reflect.Type typeOfSrc, JsonSerializationContext context)
        {
            if (src == null)
            {
                return null;
            }
            return new JsonPrimitive(src.toString());
        }
    }
    
    public static OldServer getInstance()
    {
        return instance.get();
    }
    
    private void setupRoutes()
    {
        app.get("/nodes", this::handleNodes);
        app.get("/node/{nodeId}/stores", this::handleStores);
        app.get("/node/{nodeId}/stores/{storeId}/txns", this::handleTransactions);
        app.get("/node/{nodeId}/stores/{storeId}/txn/{txnId}", this::handleSingleTransaction);
        app.get("/node/{nodeId}/stores/{storeId}/txn/{txnId}/commands-for-key", this::handleCommandsForKey);
        app.get("/node/{nodeId}/stores/{storeId}/redundant-before", this::handleRedundantBefore);
        app.get("/unpause", this::handleUnpause);
    }
    
    public void start()
    {
        app.start(port);
        logger.info("Debug server started on port {}", app.port());
    }
    
    public void stop()
    {
        app.stop();
        logger.info("Debug server stopped");
    }
    
    public void registerNode(int nodeId, Node node)
    {
        nodes.put(nodeId, node);
        logger.info("Registered node {} with debug server", nodeId);
    }

    public void pause()
    {
        try
        {
            pauseInternal().get();
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
    }

    private CompletableFuture<Void> pauseInternal()
    {
        while (true)
        {
            CompletableFuture<Void> future = debugFuture.get();
            if (future == null)
            {
                future = new CompletableFuture<>();
                if (debugFuture.compareAndSet(null, future))
                    return future;

                future.cancel(true);
            }
            else
                return future;
        }
    }
    
    private void handleStores(Context ctx)
    {
        try
        {
            int nodeId = Integer.parseInt(ctx.pathParam("nodeId"));
            Node node = nodes.get(nodeId);
            
            if (node == null)
            {
                Response.sendResponse(ctx, Response.failure("Node " + nodeId + " not found"), 404);
                return;
            }
            
            Response<List<Model.StoreInfo>> response = DeprecatedController.getStoresList(node);
            Response.sendResponse(ctx, response);
        }
        catch (Exception e)
        {
            logger.error("Error handling stores request", e);
            Response.sendResponse(ctx, Response.failure("Internal server error: " + e.getMessage()));
        }
    }
    
    private void handleTransactions(Context ctx)
    {
        try
        {
            int nodeId = Integer.parseInt(ctx.pathParam("nodeId"));
            int storeId = Integer.parseInt(ctx.pathParam("storeId"));
            String property = ctx.queryParam("property");
            
            Node node = nodes.get(nodeId);
            if (node == null)
            {
                Response.sendResponse(ctx, Response.failure("Node " + nodeId + " not found"), 404);
                return;
            }
            
            Response<List<Model.TxnInfo>> response = DeprecatedController.getTransactionsList(node, storeId, property);
            Response.sendResponse(ctx, response, 404);
        }
        catch (Exception e)
        {
            logger.error("Error handling transactions request", e);
            Response.sendResponse(ctx, Response.failure("Internal server error: " + e.getMessage()));
        }
    }
    
    private void handleSingleTransaction(Context ctx)
    {
        try
        {
            int nodeId = Integer.parseInt(ctx.pathParam("nodeId"));
            int storeId = Integer.parseInt(ctx.pathParam("storeId"));
            String txnIdStr = ctx.pathParam("txnId");
            
            Node node = nodes.get(nodeId);
            if (node == null)
            {
                Response.sendResponse(ctx, Response.failure("Node " + nodeId + " not found"), 404);
                return;
            }
            
            Response<Model.TxnInfo> response = DeprecatedController.getSingleTransaction(node, storeId, txnIdStr);
            Response.sendResponse(ctx, response, 404);
        }
        catch (Exception e)
        {
            logger.error("Error handling single transaction request", e);
            Response.sendResponse(ctx, Response.failure("Internal server error: " + e.getMessage()));
        }
    }

    private void handleCommandsForKey(Context ctx)
    {
        try
        {
            int nodeId = Integer.parseInt(ctx.pathParam("nodeId"));
            int storeId = Integer.parseInt(ctx.pathParam("storeId"));
            String txnIdStr = ctx.pathParam("txnId");
            
            Node node = nodes.get(nodeId);
            if (node == null)
            {
                Response.sendResponse(ctx, Response.failure("Node " + nodeId + " not found"), 404);
                return;
            }
            
            Response<Model.CommandsForKeyInfo> response = DeprecatedController.getCommandsForKey(node, storeId, txnIdStr);
            Response.sendResponse(ctx, response, 404);
        }
        catch (Exception e)
        {
            logger.error("Error handling commands for key request", e);
            Response.sendResponse(ctx, Response.failure("Internal server error: " + e.getMessage()));
        }
    }

    private void handleRedundantBefore(Context ctx)
    {
        try
        {
            int nodeId = Integer.parseInt(ctx.pathParam("nodeId"));
            int storeId = Integer.parseInt(ctx.pathParam("storeId"));
            
            Node node = nodes.get(nodeId);
            if (node == null)
            {
                Response.sendResponse(ctx, Response.failure("Node " + nodeId + " not found"), 404);
                return;
            }
            
            Response<Model.RedundantBeforeInfo> response = DeprecatedController.getRedundantBefore(node, storeId);
            Response.sendResponse(ctx, response, 404);
        }
        catch (Exception e)
        {
            logger.error("Error handling redundant before request", e);
            Response.sendResponse(ctx, Response.failure("Internal server error: " + e.getMessage()));
        }
    }

    private void handleNodes(Context ctx)
    {
        try
        {
            String rangeFilter = ctx.queryParam("range");
            
            if (rangeFilter != null && !rangeFilter.trim().isEmpty())
            {
                Response<List<Model.NodeInfo>> response = DeprecatedController.getNodesByRange(nodes, rangeFilter.trim());
                Response.sendResponse(ctx, response);
            }
            else
            {
                Response<List<Model.NodeInfo>> response = DeprecatedController.getNodesWithStores(nodes);
                Response.sendResponse(ctx, response);
            }
        }
        catch (Exception e)
        {
            logger.error("Error getting nodes", e);
            Response.sendResponse(ctx, Response.failure("Internal server error: " + e.getMessage()));
        }
    }

    private void handleUnpause(Context ctx)
    {
        try
        {
            CompletableFuture<Void> future = debugFuture.getAndSet(null);
            if (future != null) future.complete(null);
            Response.sendResponse(ctx, Response.success("Debug session ended"));
            logger.info("Debug session ended via /unpause endpoint");
        }
        catch (Exception e)
        {
            logger.error("Error ending debug session", e);
            Response.sendResponse(ctx, Response.failure("Internal server error: " + e.getMessage()));
        }
    }

    public static void main(String[] args) throws IOException
    {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        OldServer server = new OldServer(port);
        
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        
        server.start();
        logger.info("Debug server running on http://localhost:{}", port);
        logger.info("Web Interface: http://localhost:{}/", port);
        logger.info("Available endpoints:");
        logger.info("  GET / - Web interface");
        logger.info("  GET /nodes - List all registered node IDs");
        logger.info("  GET /health - Server health status");
        logger.info("  GET /node/{id}/stores - List command stores for node");
        logger.info("  GET /node/{id}/stores/{storeId}/txns - List transactions for store");
        logger.info("  GET /unpause - End debug session (completes debug future)");
    }
}