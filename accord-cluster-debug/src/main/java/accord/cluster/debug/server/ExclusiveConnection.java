package accord.cluster.debug.server;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import com.google.common.collect.Iterators;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.LoadBalancingPolicy;

public class ExclusiveConnection
{
    public static Cluster session(Consumer<Cluster.Builder> configure, String host)
    {
        try
        {
            Cluster.Builder builder = Cluster.builder()
                                      .withCodecRegistry(new CodecRegistry()
                                                         .register(PseudoUtf8TypeCodec.TOKEN_CODEC)
                                                         .register(PseudoUtf8TypeCodec.TXNID_CODEC));
            configure.accept(builder);
            builder.addContactPoint(host);
            InetAddress addr = InetAddress.getByName(host);
            builder.withLoadBalancingPolicy(new SingleHostLoadBalancingPolicy(addr));
            return builder.build();
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Could not build session", t);
        }
    }

    public static class SingleHostLoadBalancingPolicy implements LoadBalancingPolicy
    {
        private final InetAddress address;
        private Host host;

        public SingleHostLoadBalancingPolicy(InetAddress address)
        {
            this.address = address;
        }

        protected final List<Host> hosts = new CopyOnWriteArrayList<>();

        @Override
        public void init(Cluster cluster, Collection<Host> hosts)
        {
            host = hosts.stream()
                        .filter(h -> h.getBroadcastAddress().equals(address)).findFirst()
                        .orElseThrow(() -> new AssertionError("The host should be a contact point"));
            this.hosts.add(host);
        }

        @Override
        public HostDistance distance(Host host)
        {
            return HostDistance.LOCAL;
        }

        @Override
        public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement)
        {
            return Iterators.singletonIterator(host);
        }

        @Override
        public void onAdd(Host host)
        {
            // no-op
        }

        @Override
        public void onUp(Host host)
        {
            // no-op
        }

        @Override
        public void onDown(Host host)
        {
            // no-op
        }

        @Override
        public void onRemove(Host host)
        {
            // no-op
        }

        @Override
        public void close()
        {
            // no-op
        }
    }
}
