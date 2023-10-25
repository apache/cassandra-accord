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
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentHashMap;
import clojure.lang.PersistentList;
import clojure.lang.RT;
import com.google.common.base.StandardSystemProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    // TODO (now): this is very expensive, its going to be 1k events on average and will have the whole read key!
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

        Object clj = Event.toClojure(events);
        events.clear();
        PersistentArrayMap result = (PersistentArrayMap) check.invoke(Clojure.read("{:consistency-models [:strict-serializable]}"), history.invoke(clj));
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

    private static abstract class Action
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

        protected Action(Action.Type type, int key)
        {
            this.type = type;
            this.key = key;
        }

        private List<Object> asOp()
        {
            List<Object> values = new ArrayList<>(3);
            values.add(type.keyword);
            values.add(key);
            values.add(valuesClojure());
            return values;
        }

        protected abstract Object valuesClojure();

        final void toClojure(StringBuilder sb)
        {
            sb.append("[:").append(type.name());
            sb.append(' ').append(key).append(' ');
            toClojureWithValues(sb);
            sb.append(']');
        }

        abstract void toClojureWithValues(StringBuilder sb);
    }

    private static class Read extends Action
    {
        private final int[] seq;

        protected Read(int key, int[] seq)
        {
            super(Type.r, key);
            this.seq = seq;
        }

        @Override
        protected Object valuesClojure()
        {
            return PersistentList.create(IntStream.of(seq).mapToObj(Integer::valueOf).collect(Collectors.toList()));
        }

        @Override
        void toClojureWithValues(StringBuilder sb)
        {
            sb.append('[');
            for (int s : seq)
                sb.append(s).append(' ');
            sb.append(']');
        }
    }

    private static class Append extends Action
    {
        private final int value;

        protected Append(int key, int value)
        {
            super(Type.append, key);
            this.value = value;
        }

        @Override
        protected Object valuesClojure()
        {
            return value;
        }

        @Override
        void toClojureWithValues(StringBuilder sb)
        {
            sb.append(value);
        }
    }

    private static class Event
    {
        enum Type {
            ok, fail;

            final Keyword keyword;

            Type()
            {
                keyword = Keyword.intern(null, name());
            }
        }

        private final int process;
        private final Event.Type type;
        private final List<Action> actions;
        // value
        private final long time;

        private Event(int process, Event.Type type, long time, List<Action> actions)
        {
            this.process = process;
            this.type = type;
            this.actions = actions;
            this.time = time;
        }

        static Object toClojure(List<Event> events)
        {
            List<clojure.lang.Associative> ops = new ArrayList<>(events.size());
            for (Event e : events)
                ops.add(e.asOp());
            return ops;
//            StringBuilder sb = new StringBuilder();
//            sb.append('[');
//            for (Event e : events)
//            {
//                sb.append('{');
//                e.toClojure(sb);
//                sb.append("}\n");
//            }
//            sb.append(']');
//            // TODO (now): can we avoid string and make this cheaper?
//            return Clojure.read(sb.toString());
        }

        private clojure.lang.Associative asOp()
        {
            Map<Object, Object> op = new HashMap<>();
            op.put(Keys.process, process);
            op.put(Keys.time, time);
            op.put(Keys.type, type.keyword);
            op.put(Keys.value, actions.stream().map(a -> a.asOp()).collect(Collectors.toList()));
            return PersistentHashMap.create(op);
        }

        final void toClojure(StringBuilder sb)
        {
            sb.append(":process ").append(process).append(",\n");
            sb.append(":time ").append(time).append(",\n");
            sb.append(":type :").append(type.name()).append(",\n");
            sb.append(":value [");
            for (Action a : actions)
            {
                a.toClojure(sb);
                sb.append(' ');
            }
            sb.append(']');
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append('{');
            toClojure(sb);
            sb.append("}\n");
            return sb.toString();
        }
    }

    private static class Keys
    {
        private static Keyword process = Keyword.intern(null, "process");
        private static Keyword time = Keyword.intern(null, "time");
        private static Keyword type = Keyword.intern(null, "type");
        private static Keyword value = Keyword.intern(null, "value");
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
}
