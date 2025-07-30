package accord.debug;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.debug.controller.Controller;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.staticfiles.Location;
import io.javalin.json.JavalinGson;

public abstract class AbstractServer
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractServer.class);

    private final AtomicReference<CompletableFuture<Void>> debugFuture = new AtomicReference<>();

    private final Javalin app;
    private final int port;
    private final Controller controller;
    
    public AbstractServer(int port, Controller controller)
    {
        this.controller = controller;
        this.port = port;
        this.app = Javalin.create(config -> {
//            appConfig.staticFiles.add("/web", Location.CLASSPATH);
            config.staticFiles.add("/Users/ifesdjeen/p/java/cassandra-accord-protocol/accord-debug/src/main/resources/web", Location.EXTERNAL);
            config.jsonMapper(createCustomJsonMapper());
        });

        setupRoutes();
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
        app.get("/hosts/{hostname}/commands_for_keys/{key}", this::handleCommandsForKeys);
        app.get("/hosts/{hostname}/transactions/{txnId}", this::handleGetTxn);
        app.get("/hosts/{hostname}/stores/{storeId}/transactions", this::handleTransactions);
        app.get("/hosts/{hostname}/stores/{storeId}/transactions/{txnId}", this::handleTransaction);
        app.get("/hosts/{hostname}/coordinations", this::handleCoordinations);
        app.get("/hosts/{hostname}/blocked_by/{txnId}", this::handleTxnBlockedBy);
        app.get("/hosts/{hostname}/progress_log", this::handleProgressLog);
        app.get("/hosts/{hostname}/durability_service", this::handleDurabilityService);
        app.get("/hosts/{hostname}/command_store", this::handleCommandStores);
        app.get("/hosts/{hostname}/durable_before", this::handleDurableBefore);
        app.get("/hosts/{hostname}/topologies", this::handleTopologies);
        app.get("/hosts", this::handleGetHosts);
        app.get("/unpause", this::handleUnpause);
    }

    public void start()
    {
        app.start(port);
        logger.info("Cluster debug server started on port {}", app.port());
    }

    public void stop()
    {
        app.stop();
        logger.info("Cluster debug server stopped");
    }


    private void handleRedundantBefore(Context ctx)
    {
            String hostname = ctx.pathParam("hostname");
            Response.sendResponse(ctx, Response.compute(() -> controller.getRedundantBefore(hostname)));
    }

    private void handleCommandsForKeys(Context ctx)
    {
        String hostname = ctx.pathParam("hostname");
        String key = ctx.pathParam("key");
        Response.sendResponse(ctx, Response.compute(() -> controller.getCommandsForKey(hostname, key)));
    }

    private void handleGetTxn(Context ctx)
    {
        String hostname = ctx.pathParam("hostname");
        String txnId = ctx.pathParam("txnId");

        Response.sendResponse(ctx, Response.compute(() -> controller.getTxn(hostname, txnId)));
    }

    private void handleTransactions(Context ctx)
    {
        String hostname = ctx.pathParam("hostname");
        int storeId = Integer.parseInt(ctx.pathParam("storeId"));
        String property = ctx.queryParam("property");
        Response.sendResponse(ctx, Response.compute(() -> controller.getTransactions(hostname, storeId, property)));
    }

    private void handleTransaction(Context ctx)
    {
        String hostname = ctx.pathParam("hostname");
        int storeId = Integer.parseInt(ctx.pathParam("storeId"));
        String txnId = ctx.pathParam("txnId");
        Response.sendResponse(ctx, Response.compute(() -> controller.getTransaction(hostname, storeId, txnId)));
    }

    private void handleCoordinations(Context ctx)
    {
            String hostname = ctx.pathParam("hostname");
            Response.sendResponse(ctx, Response.compute(() -> controller.getCoordinations(hostname)));
    }

    private void handleTxnBlockedBy(Context ctx)
    {
            String hostname = ctx.pathParam("hostname");
            String txnId = ctx.pathParam("txnId");
            Response.sendResponse(ctx, Response.compute(() -> controller.getTxnBlockedBy(hostname, txnId)));
    }

    private void handleProgressLog(Context ctx)
    {
            String hostname = ctx.pathParam("hostname");
            Response.sendResponse(ctx, Response.compute(() -> controller.getProgressLog(hostname)));
    }

    private void handleDurabilityService(Context ctx)
    {
            String hostname = ctx.pathParam("hostname");
            Response.sendResponse(ctx, Response.compute(() -> controller.getDurabilityService(hostname)));

    }

    private void handleCommandStores(Context ctx)
    {
        String hostname = ctx.pathParam("hostname");
        Response.sendResponse(ctx, Response.compute(() -> controller.getCommandStores(hostname)));
    }

    private void handleDurableBefore(Context ctx)
    {
        String hostname = ctx.pathParam("hostname");
        Response.sendResponse(ctx, Response.compute(() -> controller.getDurableBefore(hostname)));
    }

    private void handleTopologies(Context ctx)
    {
        String hostname = ctx.pathParam("hostname");
        Response.sendResponse(ctx, Response.compute(() -> controller.getTopologies(hostname)));
    }

    private void handleGetHosts(Context ctx)
    {
        Response.sendResponse(ctx, Response.compute(controller::getNodes));
    }

    /**
     * Future Management
     */
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
}