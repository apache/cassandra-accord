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

package accord.verify;

import clojure.java.api.Clojure;
import clojure.lang.ArraySeq;
import clojure.lang.IFn;
import clojure.lang.IMapEntry;
import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.IteratorSeq;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentVector;
import clojure.lang.RT;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ElleVerifier implements Verifier
{
    public static class Support
    {
        public static boolean allowed()
        {
            // Elle only works on JDK 11
            int jdkVersion = Integer.parseInt(StandardSystemProperty.JAVA_VERSION.value().split("\\.")[0]);
            return !(jdkVersion == 1 /* 1.8 */ || jdkVersion == 8);
        }
    }

    // In order to build the jepsen history, we need the full history... so must buffer everything
    private final List<Event> events = new ArrayList<>();

    @Override
    public Checker witness(int start, int end)
    {
        List<Action> invoked = new ArrayList<>();
        List<Action> witnessed = new ArrayList<>();
        return new Checker()
        {
            @Override
            public void read(int index, int[] seq)
            {
                invoked.add(new Read(index, null));
                witnessed.add(new Read(index, seq));
            }

            @Override
            public void write(int index, int value)
            {
                Append e = new Append(index, value);
                invoked.add(e);
                witnessed.add(e);
            }

            @Override
            public void close()
            {
                // When a range read is performed, if the result was no matching keys then history isn't clear.
                // Since StrictSerializabilityVerifier uses indexes and not pk values, it is not possible to find expected keys and putting empty result for them...
                if (witnessed.isEmpty())
                    return;
                events.add(new Event(start, Event.Type.invoke, start, invoked));
                events.add(new Event(start, Event.Type.ok, end, witnessed));
            }
        };
    }

    @Override
    public void close()
    {
        if (events.isEmpty())
            throw new IllegalArgumentException("No events seen");
        // invoke and ok are mixed together in order, but there could be time gaps, so order based off time...
        events.sort(Comparator.comparingLong(a -> a.time));

        Object eventHistory = Clj.history.invoke(Event.toClojure(events));
        events.clear();
        PersistentArrayMap result = (PersistentArrayMap) Clj.check.invoke(Clj.elleListAppendOps, eventHistory);
        Object isValid = result.get(Keys.valid);
        if (isValid == Boolean.TRUE)
            return;
        if (isValid == Keys.unknown)
        {
            // Elle couldn't figure out if the history is bad or not... why?
            Object anomalyTypes = result.get(Keys.anomalyTypes);
            if (anomalyTypes != null)
            {
                ArraySeq seq = (ArraySeq) anomalyTypes;
                if (!seq.isEmpty())
                {
                    for (Object type : seq)
                    {
                        if (type == Keys.emptyTransactionGraph)
                            continue; // nothing to see here
                        throw new AssertionError("Unexpected anomaly type detected: " + type);
                    }
                    return;  // all good
                }
            }
        }
        throw new HistoryViolation(-1, "Violation detected: " + result);
    }

    private static abstract class Action extends java.util.AbstractList<Object> implements RandomAccess
    {
        enum Type
        {
            append, r;

            final Keyword keyword;

            Type()
            {
                keyword = RT.keyword(null, name());
            }
        }
        private final Action.Type type;
        private final int key;
        private final Object value;

        protected Action(Action.Type type, int key, @Nullable Object value)
        {
            this.type = type;
            this.key = key;
            this.value = value;
        }

        @Override
        public Object get(int index)
        {
            switch (index)
            {
                case 0:
                    return type.keyword;
                case 1:
                    return key;
                case 2:
                    if (value != null)
                        return value;
                default:
                    throw new IndexOutOfBoundsException();
            }
        }

        @Override
        public int size()
        {
            return value == null ? 2 : 3;
        }
    }

    private static class Read extends Action
    {
        protected Read(int key, int[] seq)
        {
            // TODO (optimization): rather than vector of boxed int, can we use the interfaces so we can stay primitive array?
            super(Type.r, key, seq == null ? null : PersistentVector.create(IntStream.of(seq).boxed().collect(Collectors.toList())));
        }
    }

    private static class Append extends Action
    {
        protected Append(int key, int value)
        {
            super(Type.append, key, value);
        }
    }

    private static class Event extends ObjectPersistentMap
    {
        enum Type
        {
            invoke, ok, fail; // info is left out as burn test does not have access to the original request, so can't populate an "invoke" event

            final Keyword keyword;

            Type()
            {
                keyword = RT.keyword(null, name());
            }
        }

        private final int process;
        private final Event.Type type;
        private final List<Action> actions;
        private final long time;
        private long index = -1;

        private Event(int process, Type type, long time, List<Action> actions)
        {
            super(Keys.eventKeys);
            this.process = process;
            this.type = type;
            this.actions = actions;
            this.time = time;
        }

        public static Object toClojure(List<Event> events)
        {
            return PersistentVector.create(events);
        }

        @Override
        public boolean containsKey(Object key)
        {
            if (key == Keys.index)
                return index != -1;
            return super.containsKey(key);
        }

        @Override
        public Object valAt(Object key, Object notFound)
        {
            if      (key == Keys.process) return process;
            else if (key == Keys.index)   return index == -1 ? notFound : index;
            else if (key == Keys.time)    return time;
            else if (key == Keys.type)    return type.keyword;
            else if (key == Keys.value)   return actions;
            return notFound;
        }

        @Override
        public IPersistentMap assoc(Object key, Object val)
        {
            if (key == Keys.index)
                index = ((Long) val).longValue();
            else
                throw new UnsupportedOperationException("Unable to update key " + key);
            return this;
        }
    }

    private static class Keys
    {
        // event keys
        private final static Keyword process = RT.keyword(null, "process");
        private final static Keyword index = RT.keyword(null, "index");
        private final static Keyword time = RT.keyword(null, "time");
        private final static Keyword type = RT.keyword(null, "type");
        private final static Keyword value = RT.keyword(null, "value");

        // elle check results
        private final static Keyword valid = RT.keyword(null, "valid?");
        private final static Keyword unknown = RT.keyword(null, "unknown");
        private final static Keyword anomalyTypes = RT.keyword(null, "anomaly-types");
        private final static Keyword emptyTransactionGraph = RT.keyword(null, "empty-transaction-graph");

        private static final Set<Keyword> eventKeys = ImmutableSet.of(Keys.process, Keys.time, Keys.type, Keys.value);
    }

    private static class Clj
    {
        static
        {
            // Needed else elle loads rhizome which then would call java.awt.Toolkit.getDefaultToolkit().getMenuShortcutKeyMask() which fails in CI
            System.setProperty("java.awt.headless", "true");

            IFn require = Clojure.var("clojure.core", "require");
            require.invoke(Clojure.read("elle.list-append"));
            require.invoke(Clojure.read("jepsen.history"));
        }

        private static final IFn check = Clojure.var("elle.list-append", "check");
        private static final IFn history = Clojure.var("jepsen.history", "history");
        private static final Object elleListAppendOps = Clojure.read("{:consistency-models [:strict-serializable]}");
    }

    private static abstract class ObjectPersistentMap implements clojure.lang.IPersistentMap
    {
        private Set<Keyword> keys;

        private ObjectPersistentMap(Set<Keyword> keys)
        {
            this.keys = keys;
        }

        @Override
        public boolean containsKey(Object key)
        {
            if (!(key instanceof Keyword))
                throw new AssertionError(String.format("Unexpected key %s; type %s", key, key == null ? null : key.getClass()));
            return keys.contains(key);
        }

        @Override
        public IMapEntry entryAt(Object key)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public IPersistentMap assoc(Object key, Object val)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public IPersistentMap assocEx(Object key, Object val)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public IPersistentMap without(Object key)
        {
            keys = Sets.filter(keys, k -> !k.equals(key));
            return this;
        }

        @Override
        public Object valAt(Object key)
        {
            return valAt(key, null);
        }

        @Override
        public abstract Object valAt(Object key, Object notFound);

        @Override
        public int count()
        {
            return keys.size();
        }

        @Override
        public IPersistentCollection cons(Object o)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public IPersistentCollection empty()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equiv(Object o)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ISeq seq()
        {
            return IteratorSeq.create(iterator());
        }

        @Override
        public Iterator iterator()
        {
            return keys.iterator();
        }
    }
}
