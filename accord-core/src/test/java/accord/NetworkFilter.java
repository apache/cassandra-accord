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
