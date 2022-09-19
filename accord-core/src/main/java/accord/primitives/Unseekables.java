package accord.primitives;

import accord.api.RoutingKey;

/**
 * Either a Route or a simple collection of Unseekable
 */
public interface Unseekables<K extends Unseekable, U extends Unseekables<K, ?>> extends Iterable<K>, Routables<K, U>
{
    enum UnseekablesKind
    {
        RoutingKeys, PartialKeyRoute, FullKeyRoute, RoutingRanges, PartialRangeRoute, FullRangeRoute;

        public boolean isRoute()
        {
            return this != RoutingKeys & this != RoutingRanges;
        }

        public boolean isFullRoute()
        {
            return this == FullKeyRoute | this == FullRangeRoute;
        }
    }

    U slice(Ranges ranges);
    Unseekables<K, U> union(U with);
    Unseekables<K, ?> with(RoutingKey withKey);
    UnseekablesKind kind();

    static <K extends Unseekable> Unseekables<K, ?> merge(Unseekables<K, ?> left, Unseekables<K, ?> right)
    {
        if (left == null) return right;
        if (right == null) return left;

        UnseekablesKind leftKind = left.kind();
        UnseekablesKind rightKind = right.kind();
        if (leftKind.isRoute() || rightKind.isRoute())
        {
            if (leftKind.isRoute() != rightKind.isRoute())
            {
                // one is a route, one is not
                if (leftKind.isRoute() && left.containsAll(right))
                    return left;
                if (rightKind.isRoute() && right.containsAll(left))
                    return right;

                // non-route types can always accept route types as input, so just call its union method on the other
                return leftKind.isRoute() ? ((Unseekables)right).union(left) : ((Unseekables)left).union(right);
            }

            if (leftKind.isFullRoute())
                return left;

            if (rightKind.isFullRoute())
                return right;
        }
        return ((Unseekables)left).union(right);
    }
}
