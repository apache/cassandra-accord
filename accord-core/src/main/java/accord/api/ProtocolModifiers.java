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

package accord.api;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import accord.utils.Invariants;

import static accord.api.ProtocolModifiers.Faults.initialiseFaults;
import static accord.api.ProtocolModifiers.QuorumEpochIntersections.ChaseFixedPoint.Chase;
import static accord.api.ProtocolModifiers.QuorumEpochIntersections.ChaseFixedPoint.DoNotChase;
import static accord.api.ProtocolModifiers.QuorumEpochIntersections.Include.Owned;
import static accord.api.ProtocolModifiers.QuorumEpochIntersections.Include.Unsynced;
import static accord.api.ProtocolModifiers.QuorumEpochIntersections.initialiseQuorumEpochIntersections;

public class ProtocolModifiers
{
    public static final ProtocolModifiers.QuorumEpochIntersections QuorumEpochIntersections = initialiseQuorumEpochIntersections();
    public static final Faults Faults = initialiseFaults();

    public static class QuorumEpochIntersections
    {
        private static final Pattern PARSE = Pattern.compile("(preaccept|accept|commit|stable|recover)=([+-]{0,2})");

        static ProtocolModifiers.QuorumEpochIntersections initialiseQuorumEpochIntersections()
        {
            String description = System.getProperty("accord.quorums.epochs", "preaccept=-,accept=-,commit=-,stable=,recover=-");
            Matcher m = PARSE.matcher(description);
            ChaseAndInclude preaccept = null;
            Include accept = null;
            Include commit = null;
            Include stable = null;
            Include recover = null;
            while (m.find())
            {
                ChaseFixedPoint cfp = DoNotChase;
                Include include = Owned;
                String str = m.group(2);
                for (int i = 0 ; i < str.length() ; ++i)
                {
                    switch (str.charAt(i))
                    {
                        default: throw new AssertionError("Unexpected char: '" + str.charAt(i) + "'");
                        case '+': cfp = Chase; break;
                        case '-': include = Unsynced; break;
                    }
                }

                switch (m.group(1))
                {
                    default: throw new AssertionError("Unexpected phase: " + m.group(1));
                    case "preaccept": preaccept = new ChaseAndInclude(cfp, include); break;
                    case "accept": accept = include; Invariants.checkState(cfp == DoNotChase, "Invalid to specify ChaseFixedPoint.Chase for accept"); break;
                    case "commit": commit = include; Invariants.checkState(cfp == DoNotChase, "Invalid to specify ChaseFixedPoint.Chase for commit"); break;
                    case "stable": stable = include; Invariants.checkState(cfp == DoNotChase, "Invalid to specify ChaseFixedPoint.Chase for stable"); break;
                    case "recover": recover = include; Invariants.checkState(cfp == DoNotChase, "Invalid to specify ChaseFixedPoint.Chase for stable"); break;
                }
            }

            Invariants.checkState(preaccept != null, "preaccept not specified for quorum epoch intersections: " + description);
            Invariants.checkState(accept != null, "accept not specified for quorum epoch intersections: " + description);
            Invariants.checkState(commit != null, "commit not specified for quorum epoch intersections: " + description);
            Invariants.checkState(stable != null, "stable not specified for quorum epoch intersections: " + description);
            Invariants.checkState(recover != null, "recover not specified for quorum epoch intersections: " + description);
            return new QuorumEpochIntersections(preaccept, accept, commit, stable, recover);
        }

        public enum ChaseFixedPoint
        {
            Chase, DoNotChase;
        }
        public enum Include
        {
            Unsynced, Owned;
        }

        public static class ChaseAndInclude
        {
            public final ChaseFixedPoint chase;
            public final Include include;

            public ChaseAndInclude(ChaseFixedPoint chase, Include include)
            {
                this.chase = chase;
                this.include = include;
            }
        }

        // TODO (desired): support fixed-point chasing for recovery
        public final ChaseAndInclude preaccept;
        public final Include accept, commit, stable, recover;
        public final Include preacceptOrRecover, preacceptOrCommit;

        public QuorumEpochIntersections(ChaseAndInclude preaccept, Include accept, Include commit, Include stable, Include recover)
        {
            this.preaccept = preaccept;
            this.accept = accept;
            this.commit = commit;
            this.stable = stable;
            this.recover = recover;
            this.preacceptOrRecover = preaccept.include == Owned || recover == Owned ? Owned : Unsynced;
            this.preacceptOrCommit = preaccept.include == Owned || commit == Owned ? Owned : Unsynced;
        }
    }

    public static class Faults
    {
        static Faults initialiseFaults()
        {
            // TODO (expected): configurable
            return new Faults(false, false, false, false);
        }

        public final boolean txnInstability, txnDiscardPreAcceptDeps;
        public final boolean syncPointInstability, syncPointDiscardPreAcceptDeps;

        public Faults(boolean txnInstability, boolean txnDiscardPreAcceptDeps, boolean syncPointInstability, boolean syncPointDiscardPreAcceptDeps)
        {
            this.txnInstability = txnInstability;
            this.txnDiscardPreAcceptDeps = txnDiscardPreAcceptDeps;
            this.syncPointInstability = syncPointInstability;
            this.syncPointDiscardPreAcceptDeps = syncPointDiscardPreAcceptDeps;
        }
    }

}
