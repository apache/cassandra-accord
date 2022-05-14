package accord.primitives;

public class PartialDeps extends Deps
{
    public static final PartialDeps NONE = new PartialDeps(KeyRanges.EMPTY, Deps.NONE.keys, Deps.NONE.txnIds, Deps.NONE.keyToTxnId);

    public static class Builder extends AbstractBuilder<PartialDeps>
    {
        final KeyRanges covering;
        public Builder(KeyRanges covering, Keys keys)
        {
            super(keys);
            this.covering = covering;
        }

        @Override
        PartialDeps build(Keys keys, TxnId[] txnIds, int[] keysToTxnIds)
        {
            return new PartialDeps(covering, keys, txnIds, keysToTxnIds);
        }
    }

    public static Builder builder(KeyRanges ranges, Keys keys)
    {
        return new Builder(ranges, keys);
    }

    public final KeyRanges covering;

    PartialDeps(KeyRanges covering, Keys keys, TxnId[] txnIds, int[] keyToTxnId)
    {
        super(keys, txnIds, keyToTxnId);
        this.covering = covering;
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
