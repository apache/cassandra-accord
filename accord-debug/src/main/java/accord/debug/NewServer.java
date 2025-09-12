package accord.debug;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import accord.debug.controller.BurnTestController;
import accord.local.Node;

public class NewServer extends AbstractServer
{
    private static final AtomicReference<AbstractServer> instance = new AtomicReference<>();

    private BurnTestController controller;

    public NewServer(int port)
    {
        this(port, new BurnTestController(new ConcurrentHashMap<>()));
    }

    private NewServer(int port, BurnTestController controller)
    {
        super(port, controller);
        this.controller = controller;
        instance.compareAndSet(null, this);
    }

    public void registerNode(Node node)
    {
        controller.registerNode(node);
    }

    public static AbstractServer getInstance()
    {
        return instance.get();
    }
}
