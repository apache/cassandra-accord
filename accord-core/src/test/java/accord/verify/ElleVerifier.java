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

import java.util.ArrayList;
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
    private final List<Event> events = new ArrayList<>();

    @Override
    public Checker witness(int start, int end)
    {
        List<Action> actions = new ArrayList<>();
        return new Checker()
        {
            @Override
            public void read(int index, int[] seq)
            {
                actions.add(new Read(index, seq));
            }

            @Override
            public void write(int index, int value)
            {
                actions.add(new Append(index, value));
            }

            @Override
            public void close()
            {
                // When a range read is performed, if the result was no matching keys then history isn't clear.
                // Since StrictSerializabilityVerifier uses indexes and not pk values, it is not possible to find expected keys and putting empty result for them...
                if (actions.isEmpty())
                    return;
                events.add(new Event(0, Event.Type.ok, end, actions));
            }
        };
    }

    @Override
    public void close()
    {
        if (events.isEmpty())
            throw new IllegalArgumentException("No events seen");
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("elle.list-append"));
        require.invoke(Clojure.read("jepsen.history"));

        IFn check = Clojure.var("elle.list-append", "check");
        IFn history = Clojure.var("jepsen.history", "history");

        Object eventHistory = history.invoke(Event.toClojure(events));
        events.clear();
        PersistentArrayMap result = (PersistentArrayMap) check.invoke(Clojure.read("{:consistency-models [:strict-serializable]}"), eventHistory);
        Object isValid = result.get(RT.keyword(null, "valid?"));
        if (isValid == Boolean.TRUE)
            return;
        if (isValid == RT.keyword(null, "unknown"))
        {
            // Elle couldn't figure out if the history is bad or not... why?
            Object anomalyTypes = result.get(RT.keyword(null, "anomaly-types"));
            if (anomalyTypes != null)
            {
                ArraySeq seq = (ArraySeq) anomalyTypes;
                if (!seq.isEmpty())
                {
                    boolean empty = false;
                    for (Object type : seq)
                    {
                        if (type == RT.keyword(null, "empty-transaction-graph"))
                        {
                            empty = true;
                            continue; // nothing to see here
                        }
                        throw new AssertionError("Unexpected anomaly type detected: " + type);
                    }
                    if (empty)
                        return; // all good
                }
            }
        }
        throw new HistoryViolation(-1, "Violation detected: " + result);
    }

    private static abstract class Action extends java.util.AbstractList<Object> implements RandomAccess
    {
        enum Type {append, r;

            final Keyword keyword;

            Type()
            {
                keyword = Keyword.intern(null, name());
            }
        }
        private final Action.Type type;
        private final int key;
        private final Object value;

        protected Action(Action.Type type, int key, Object value)
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
                case 0: return type.keyword;
                case 1: return key;
                case 2: return value;
                default:
                    throw new IndexOutOfBoundsException();
            }
        }

        @Override
        public int size()
        {
            return 3;
        }
    }

    private static class Read extends Action
    {
        protected Read(int key, int[] seq)
        {
            super(Type.r, key, PersistentVector.create(IntStream.of(seq).mapToObj(Integer::valueOf).collect(Collectors.toList())));
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
        enum Type {
            ok, fail; // invoke, info

            final Keyword keyword;

            Type()
            {
                keyword = Keyword.intern(null, name());
            }
        }

        private final int process;
        private long index = -1;
        private final Event.Type type;
        private final List<Action> actions;
        // value
        private final long time;

        private Event(int process, Event.Type type, long time, List<Action> actions)
        {
            super(Keys.eventKeys);
            this.process = process;
            this.type = type;
            this.actions = actions;
            this.time = time;
        }

        public static Object toClojure(List<Event> events)
        {
//            events = events.stream().limit(7).collect(Collectors.toList());
            boolean useMemoryOptimized = true;
            if (useMemoryOptimized)
                return PersistentVector.create(events);
            StringBuilder sb = new StringBuilder();
            sb.append('[');
            for (Event e : events)
            {
                sb.append('{');
                toClojure(sb, e);
                sb.append("}\n");
            }
            sb.append(']');
            return Clojure.read(sb.toString());
        }

        private static void toClojure(StringBuilder sb, Event e)
        {
            sb.append(":process ").append(e.process).append(",\n");
            sb.append(":time ").append(e.time).append(",\n");
            sb.append(":type :").append(e.type.name()).append(",\n");
            sb.append(":value [");
            for (Action a : e.actions)
            {
                toClojure(sb, a);
                sb.append(' ');
            }
            sb.append(']');
        }

        private static void toClojure(StringBuilder sb, Action e)
        {
            sb.append("[:").append(e.type.name());
            sb.append(' ').append(e.key).append(' ');
            sb.append(e.value);
            sb.append(']');
        }

        @Override
        protected ObjectPersistentMap create(Set<Keyword> keys)
        {
            return new EventClj(keys);
        }

        @Override
        public Object valAt(Object key, Object notFound)
        {
            if (key == Keys.process) return process;
            else if (key == Keys.index) return index == -1 ? notFound : index;
            else if (key == Keys.time) return time;
            else if (key == Keys.type) return type.keyword;
            else if (key == Keys.value) return actions;
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

        @Override
        public boolean containsKey(Object key)
        {
            if (key == Keys.index && index != -1)
                return true;
            return super.containsKey(key);
        }

        private class EventClj extends ObjectPersistentMap
        {
            private EventClj(Set<Keyword> keys)
            {
                super(keys);
            }

            @Override
            public boolean containsKey(Object key)
            {
                return Event.this.containsKey(key);
            }

            @Override
            protected ObjectPersistentMap create(Set<Keyword> keys)
            {
                return new EventClj(keys);
            }

            @Override
            public Object valAt(Object key, Object notFound)
            {
                return Event.this.valAt(key, notFound);
            }

            @Override
            public IPersistentMap assoc(Object key, Object val)
            {
                return Event.this.assoc(key, val);
            }
        }
    }

    private static class Keys
    {
        private static Keyword process = Keyword.intern(null, "process");
        private static Keyword index = Keyword.intern(null, "index");
        private static Keyword time = Keyword.intern(null, "time");
        private static Keyword type = Keyword.intern(null, "type");
        private static Keyword value = Keyword.intern(null, "value");

        private static final Set<Keyword> eventKeys = ImmutableSet.of(Keys.process, Keys.time, Keys.type, Keys.value);
    }

    private static class Clj
    {
        private static final IFn require = Clojure.var("clojure.core", "require");

        static
        {
            require.invoke(Clojure.read("elle.list-append"));
            require.invoke(Clojure.read("jepsen.history"));
        }

        private static final IFn check = Clojure.var("elle.list-append", "check");
        private static final IFn history = Clojure.var("jepsen.history", "history");
    }

    private static abstract class ObjectPersistentMap implements clojure.lang.IPersistentMap
    {
        private final Set<Keyword> keys;

        private ObjectPersistentMap(Set<Keyword> keys)
        {
            this.keys = keys;
        }

        protected abstract ObjectPersistentMap create(Set<Keyword> filter);

        @Override
        public boolean containsKey(Object key)
        {
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
            return create(Sets.filter(keys, k -> !k.equals(key)));
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
