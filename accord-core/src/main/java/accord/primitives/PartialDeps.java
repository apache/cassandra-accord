package accord.primitives;

import com.google.common.base.Preconditions;

public class PartialDeps extends Deps
{
    public static final PartialDeps NONE = new PartialDeps(KeyRanges.EMPTY, Deps.NONE.keys, Deps.NONE.txnIds, Deps.NONE.keyToTxnId);

    public static class SerializerSupport
    {
        private SerializerSupport() {}

        public static PartialDeps create(KeyRanges covering, Keys keys, TxnId[] txnIds, int[] keyToTxnId)
        {
            return new PartialDeps(covering, keys, txnIds, keyToTxnId);
        }
    }

    public static class OrderedBuilder extends AbstractOrderedBuilder<PartialDeps>
    {
        final KeyRanges covering;
        public OrderedBuilder(KeyRanges covering, boolean hasOrderedTxnId)
        {
            super(hasOrderedTxnId);
            this.covering = covering;
        }

        @Override
        PartialDeps build(Keys keys, TxnId[] txnIds, int[] keysToTxnIds)
        {
            return new PartialDeps(covering, keys, txnIds, keysToTxnIds);
        }
    }

    public static OrderedBuilder orderedBuilder(KeyRanges ranges, boolean hasOrderedTxnId)
    {
        return new OrderedBuilder(ranges, hasOrderedTxnId);
    }

    public final KeyRanges covering;

    PartialDeps(KeyRanges covering, Keys keys, TxnId[] txnIds, int[] keyToTxnId)
    {
        super(keys, txnIds, keyToTxnId);
        this.covering = covering;
        Preconditions.checkState(covering.containsAll(keys));
    }

    public boolean covers(KeyRanges ranges)
    {
        return covering.contains(ranges);
    }

    public boolean covers(AbstractKeys<?, ?> keys)
    {
        return covering.containsAll(keys);
    }

    public PartialDeps with(PartialDeps that)
    {
        Deps merged = with((Deps) that);
        return new PartialDeps(covering.union(that.covering), merged.keys, merged.txnIds, merged.keyToTxnId);
    }

    public Deps reconstitute(Route route)
    {
        if (!covers(route))
            throw new IllegalArgumentException();
        return new Deps(keys, txnIds, keyToTxnId);
    }

    // PartialRoute might cover a wider set of ranges, some of which may have no involved keys
    public PartialDeps reconstitutePartial(PartialRoute route)
    {
        if (!covers(route))
            throw new IllegalArgumentException();

        if (covers(route.covering))
            return this;

        return new PartialDeps(route.covering, keys, txnIds, keyToTxnId);
    }

    @Override
    public String toString()
    {
        return covering + ":" + super.toString();
    }

}
