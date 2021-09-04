package accord;

import accord.local.Node.Id;
import accord.messages.Message;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.function.Predicate;

public class NetworkFilter
{
    private final Logger logger = LoggerFactory.getLogger(NetworkFilter.class);

    private interface DiscardPredicate
    {
        boolean check(Id from, Id to, Message message);
    }

    Set<DiscardPredicate> discardPredicates = Sets.newConcurrentHashSet();

    public boolean shouldDiscard(Id from, Id to, Message message)
    {
        return discardPredicates.stream().anyMatch(p -> p.check(from, to, message));
    }

    public void isolate(Id node)
    {
        logger.info("Isolating node {}", node);
        discardPredicates.add((from, to, msg) -> from.equals(node) || to.equals(node));
    }

    public void isolate(Iterable<Id> ids)
    {
        for (Id id : ids)
            isolate(id);
    }

    public static Predicate<Id> anyId()
    {
        return id -> true;
    }

    public static Predicate<Id> isId(Iterable<Id> ids)
    {
        return ImmutableSet.copyOf(ids)::contains;
    }

    public static Predicate<Message> isMessageType(Class<? extends Message> klass)
    {
        return msg -> klass.isAssignableFrom(msg.getClass());
    }

    public static Predicate<Message> notMessageType(Class<? extends Message> klass)
    {
        return Predicate.not(isMessageType(klass));
    }

    /**
     * message will be discarded if all predicates apply
     */
    public void addFilter(Predicate<Id> fromPredicate, Predicate<Id> toPredicate, Predicate<Message> messagePredicate)
    {
        discardPredicates.add((from, to, msg) -> fromPredicate.test(from) && toPredicate.test(to) && messagePredicate.test(msg));
    }

    public void clear()
    {
        logger.info("Clearing network filters");
        discardPredicates.clear();
    }
}
