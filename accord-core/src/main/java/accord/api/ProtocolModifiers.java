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

import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import accord.local.LoadKeys;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.Routable.Domain;
import accord.primitives.SaveStatus;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.TxnId.FastPath;
import accord.primitives.TxnId.FastPaths;
import accord.primitives.TxnId.MediumPath;
import accord.utils.Invariants;
import accord.utils.TinyEnumSet;
import accord.utils.UnhandledEnum;

import static accord.api.ProtocolModifiers.CleanCfkBefore.SHARD_APPLIED;
import static accord.api.ProtocolModifiers.DependencyElision.IF_DURABLY_COMMITTED;
import static accord.api.ProtocolModifiers.FastExecution.MAY_BYPASS_COMMANDSFORKEY;
import static accord.api.ProtocolModifiers.FastExecution.MAY_BYPASS_SAFESTORE;
import static accord.api.ProtocolModifiers.QuorumEpochIntersections.ChaseFixedPoint.Chase;
import static accord.api.ProtocolModifiers.QuorumEpochIntersections.ChaseFixedPoint.DoNotChase;
import static accord.api.ProtocolModifiers.QuorumEpochIntersections.Include.Owned;
import static accord.api.ProtocolModifiers.QuorumEpochIntersections.Include.Unsynced;
import static accord.api.ProtocolModifiers.SendStableMessages.FOR_READS;
import static accord.api.ProtocolModifiers.SendStableMessages.FOR_READS_OR_NONE_IF_FASTEXEC;
import static accord.api.ProtocolModifiers.SendStableMessages.TO_ALL_REPLICA_EXECUTABLE_ELSE_FOR_READS;
import static accord.local.cfk.CommandsForKey.managesExecution;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.primitives.Txn.Kind.VisibilitySyncPoint;
import static accord.primitives.TxnId.Cardinality.Any;
import static accord.primitives.TxnId.Cardinality.SingleKey;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithDeps;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithoutDeps;
import static accord.utils.Invariants.illegalState;

/**
 * Configure various protocol behaviours. Many of these switches are correctness impacting, and should not be touched.
 */
public class ProtocolModifiers
{
    public enum SendStableMessages
    {
        /**
         * All participating replicas will receive a Stable message once the transaction begins the execution phase.
         * This is potentially expensive and may harm throughput by increasing the number of messages and transaction
         * state updates we need to perform, but it guarantees the fastest forward progress for any particular transaction.
         */
        TO_ALL,

        /**
         * If the transaction is able to be direct executed, send Stable messages to all replicas so that they may do so.
         * Otherwise, behaves as {@link #FOR_READS}.
         */
        TO_ALL_REPLICA_EXECUTABLE_ELSE_FOR_READS,

        /**
         * Thrifty execution. Send a Stable message only to replicas we want to read from, so that they may execute the read.
         * The coordinator may then send the Apply message to all replicas, saving O(n) messages.
         */
        FOR_READS,

        /**
         * Even thriftier: if the read is permitted to fast execute, there is no need to send a Stable message;
         * we can simply perform the read and permit the coordinator to send the Apply message. Otherwise
         * identical to {@link #FOR_READS}
         */
        FOR_READS_OR_NONE_IF_FASTEXEC
    }

    public enum CleanCfkBefore { SHARD_APPLIED, GC }

    public enum DependencyElision { OFF, ALWAYS, IF_DURABLY_COMMITTED, IF_DURABLY_PREAPPLIED }

    public enum AbandonFastPath { IF_ANY_DELAYED_OR_REJECTED, IF_FAST_QUORUM_DELAYED_OR_REJECTED }

    public enum FastExecution { DISABLED, MAY_BYPASS_COMMANDSFORKEY, MAY_BYPASS_SAFESTORE }

    public enum CoordinatorBacklogExecution { DISABLED, ON_RECOVERY, ALWAYS }

    public enum InformOfDurability { NONE, HOME, ALL }

    public enum ReplicaExecution { NONE, BLIND_WRITE, ALL }
    public enum UniqueTimestampOnConflict { STALE, NOW }

    public static class Configure
    {
        private static boolean isConfigured;
        private static synchronized void setConfigured() { isConfigured = true; }
        private static void pre() { if (isConfigured) throw illegalState("Cannot set ProtocolModifiers after the configuration has been first used"); }

        private static boolean rangeEndInclusive = true;
        public static synchronized void setRangeEndInclusive(boolean newRangeEndInclusive) { pre(); rangeEndInclusive = newRangeEndInclusive; }

        private static FastPaths permittedFastPaths = new FastPaths(FastPath.values());
        public static synchronized void setPermittedFastPaths(FastPaths newPermittedFastPaths) { pre(); permittedFastPaths = newPermittedFastPaths; }

        private static MediumPath defaultMediumPath = MediumPath.TrackStable;
        public static synchronized void setDefaultMediumPath(MediumPath newDefaultMediumPath) { pre(); defaultMediumPath = newDefaultMediumPath; }

        private static boolean permitFastQuorumMediumPath = true;
        public static synchronized void setPermitFastQuorumMediumPath(boolean newPermitFastQuorumMediumPath) { pre(); permitFastQuorumMediumPath = newPermitFastQuorumMediumPath; }

        private static AbandonFastPath abandonFastPath = AbandonFastPath.IF_ANY_DELAYED_OR_REJECTED;
        public static synchronized void setAbandonFastPath(AbandonFastPath newAbandonFastPath) { pre(); abandonFastPath = newAbandonFastPath; }

        private static UniqueTimestampOnConflict uniqueTimestampOnConflict = UniqueTimestampOnConflict.STALE;
        public static synchronized void setUniqueTimestampOnConflict(UniqueTimestampOnConflict newUniqueTimestampOnConflict) { pre();uniqueTimestampOnConflict = newUniqueTimestampOnConflict; }

        private static boolean recoveryAwaitsSupersedingSyncPoints = true;
        public static synchronized void setRecoveryAwaitsSupersedingSyncPoints(boolean newRecoveryAwaitsSupersedingSyncPoints) { pre(); recoveryAwaitsSupersedingSyncPoints = newRecoveryAwaitsSupersedingSyncPoints; }

        private static boolean syncPointsTrackUnstableMediumPathDependencies = false;
        public static synchronized void setSyncPointsTrackUnstableMediumPathDependencies(boolean newSyncPointsTrackUnstableMediumPathDependencies) { pre(); syncPointsTrackUnstableMediumPathDependencies = newSyncPointsTrackUnstableMediumPathDependencies; }

        private static boolean recoverPartialAcceptPhaseIfNoFastPath = false;
        public static synchronized void setRecoverPartialAcceptPhaseIfNoFastPath(boolean newSyncPointsRecoverPartialAcceptPhase) { pre(); recoverPartialAcceptPhaseIfNoFastPath = newSyncPointsRecoverPartialAcceptPhase; }

        private static boolean recoverReads = false;
        public static synchronized void setRecoverReads(boolean newRecoverReads) { pre(); recoverReads = newRecoverReads; }

        private static boolean filterDuplicateDependenciesFromAcceptReply = true;
        public static synchronized void setFilterDuplicateDependenciesFromAcceptReply(boolean newFilterDuplicateDependenciesFromAcceptReply) { pre(); filterDuplicateDependenciesFromAcceptReply = newFilterDuplicateDependenciesFromAcceptReply; }

        private static SendStableMessages sendStableMessages = FOR_READS_OR_NONE_IF_FASTEXEC;
        public static synchronized void setSendStableMessages(SendStableMessages newSendStableMessages) { pre(); sendStableMessages = newSendStableMessages; }

        private static boolean sendMinimal = true;
        public static synchronized void setSendMinimal(boolean newSendMinimal) { pre(); sendMinimal = newSendMinimal; }

        private static boolean permitCoordinatorLocalExecution = true;
        public static synchronized void setPermitCoordinatorLocalExecution(boolean newPermitCoordinatorLocalExecution) { pre(); permitCoordinatorLocalExecution = newPermitCoordinatorLocalExecution; }

        private static boolean permitLocalDelivery = true;
        public static synchronized void setPermitLocalDelivery(boolean newPermitLocalDelivery) { pre(); permitLocalDelivery = newPermitLocalDelivery; }

        private static boolean permitAsyncTasks = true;
        public static synchronized void setPermitAsyncTasks(boolean newPermitAsyncTasks) { pre(); permitAsyncTasks = newPermitAsyncTasks; }

        private static boolean permitAtomicIncrementalTasks = true;
        public static synchronized void setPermitAtomicIncrementalTasks(boolean newPermitAtomicIncrementalTasks) { pre(); permitAtomicIncrementalTasks = newPermitAtomicIncrementalTasks; }

        private static boolean dataStoreRequiresUniqueHlcs = true;
        public static synchronized void setDataStoreRequiresUniqueHlcs(boolean newDataStoreRequiresUniqueHlcs) { pre();dataStoreRequiresUniqueHlcs = newDataStoreRequiresUniqueHlcs; }

        private static int markStaleIfCannotExecute = TinyEnumSet.encode(Txn.Kind.Write);
        public static synchronized void setMarkStaleIfCannotExecute(Txn.Kind ... kinds)
        {
            pre();
            int newMarkStaleIfCannotExecute = TinyEnumSet.encode(kinds);
            Invariants.require(TinyEnumSet.contains(newMarkStaleIfCannotExecute, Txn.Kind.Write));
            markStaleIfCannotExecute = newMarkStaleIfCannotExecute;
        }

        private static int transitiveDependenciesAreVisible = TinyEnumSet.encode(VisibilitySyncPoint);
        public static synchronized void setTransitiveDependenciesAreVisible(Txn.Kind ... kinds)
        {
            pre();
            int newTransitiveDependenciesAreVisible = TinyEnumSet.encode(kinds);
            Invariants.require(TinyEnumSet.contains(newTransitiveDependenciesAreVisible, Txn.Kind.VisibilitySyncPoint));
            transitiveDependenciesAreVisible = newTransitiveDependenciesAreVisible;
        }

        private static CleanCfkBefore cleanCfkBefore = SHARD_APPLIED;
        public static synchronized void setCleanCfkBefore(CleanCfkBefore newCleanCfkBefore) { pre(); cleanCfkBefore = newCleanCfkBefore; }

        private static DependencyElision dependencyElision = IF_DURABLY_COMMITTED;
        public static synchronized void setDependencyElision(DependencyElision newDependencyElision) { pre(); dependencyElision = newDependencyElision; }

        private static boolean visitMaybePruned = true;
        public static synchronized void setVisitMaybePruned(boolean newVisitMaybePruned) { pre(); visitMaybePruned = newVisitMaybePruned; }

        private static InformOfDurability informOfDurability = InformOfDurability.ALL;
        private static int informOfSingleKeyDurabilityIfDepsSizeAtLeast = 16;
        public static synchronized void setInformOfDurability(InformOfDurability newInformOfDurability) { pre(); informOfDurability = newInformOfDurability; }
        public static synchronized void setInformOfSingleKeyDurabilityIfDepsSizeAtLeast(int newInformOfSingleKeyDurabilityIfDepsSizeAtLeast) { pre();informOfSingleKeyDurabilityIfDepsSizeAtLeast = newInformOfSingleKeyDurabilityIfDepsSizeAtLeast; }

        private static FastExecution fastReadExecution = MAY_BYPASS_COMMANDSFORKEY;
        public static synchronized void setFastReadExecution(FastExecution newFastReads) { pre();fastReadExecution = newFastReads; }

        private static boolean fastReadExecMayResendTxn = true;
        public static synchronized void setFastReadExecMayResendTxn(boolean newFastReadExecMayResendTxn) { pre(); fastReadExecMayResendTxn = newFastReadExecMayResendTxn; }

        private static FastExecution fastWriteExecution = MAY_BYPASS_COMMANDSFORKEY;
        public static synchronized void setFastWriteExecution(FastExecution newFastWrites) { pre();fastWriteExecution = newFastWrites; }

        private static boolean dataStoreDetectsFutureReads = false;
        public static synchronized void setDataStoreDetectsFutureReads(boolean newDataStoreDetectsFutureReads) { pre(); dataStoreDetectsFutureReads = newDataStoreDetectsFutureReads; }

        private static CoordinatorBacklogExecution coordinatorBacklogExecution = CoordinatorBacklogExecution.ON_RECOVERY;
        public static synchronized void setCoordinatorBacklogExecution(CoordinatorBacklogExecution newCoordinatorExecuteBacklog) { pre(); coordinatorBacklogExecution = newCoordinatorExecuteBacklog; }

        private static ReplicaExecution replicaExecution = ReplicaExecution.NONE;
        public static synchronized void setReplicaExecution(ReplicaExecution newReplicaExecution) { pre(); replicaExecution = newReplicaExecution; }

        private static float replicaExecuteDistributedPersistChance = 1.0f;
        public static synchronized void setReplicaExecuteDistributedPersistChance(float newChance) { pre(); replicaExecuteDistributedPersistChance = newChance; }

        public static void validate()
        {
            Invariants.require(dataStoreDetectsFutureReads || (fastWriteExecution != MAY_BYPASS_SAFESTORE && fastReadExecution != MAY_BYPASS_SAFESTORE), "MAY_BYPASS_SAFESTORE is only permitted when dataStoreDetectsFutureReads");
            Invariants.require(permitCoordinatorLocalExecution || (!permittedFastPaths.contains(PrivilegedCoordinatorWithDeps) && !permittedFastPaths.contains(PrivilegedCoordinatorWithoutDeps)), "Privileged coordinator optimisations require coordinator local execution");
        }
    }

    static
    {
        Configure.validate();
        Configure.setConfigured();
    }

    public static final boolean rangeEndInclusive = Configure.rangeEndInclusive;
    public static boolean isRangeEndInclusive() { return rangeEndInclusive; }
    public static boolean isRangeEndExclusive() { return !rangeEndInclusive; }
    public static boolean isRangeStartInclusive() { return !rangeEndInclusive; }
    public static boolean isRangeStartExclusive() { return rangeEndInclusive; }

    private static final FastPaths permittedFastPaths = Configure.permittedFastPaths;
    public static boolean usePrivilegedCoordinator() { return permittedFastPaths.hasPrivilegedCoordinator(); }
    public static FastPath ensurePermitted(FastPath path) { return path.toPermitted(permittedFastPaths); }

    private static final MediumPath defaultMediumPath = Configure.defaultMediumPath;
    public static MediumPath defaultMediumPath() { return defaultMediumPath; }

    private static final boolean permitFastQuorumMediumPath = Configure.permitFastQuorumMediumPath;
    public static boolean permitFastQuorumMediumPath() { return permitFastQuorumMediumPath; }

    private static final AbandonFastPath abandonFastPath = Configure.abandonFastPath;
    public static AbandonFastPath abandonFastPath() { return abandonFastPath; }

    private static final UniqueTimestampOnConflict UNIQUE_TIMESTAMP_ON_CONFLICT = Configure.uniqueTimestampOnConflict;
    public static UniqueTimestampOnConflict uniqueTimestampOnConflict() { return UNIQUE_TIMESTAMP_ON_CONFLICT; }

    private static final boolean recoveryAwaitsSupersedingSyncPoints = Configure.recoveryAwaitsSupersedingSyncPoints;
    public static boolean recoveryAwaitsSupersedingSyncPoints() { return recoveryAwaitsSupersedingSyncPoints; }

    private static final boolean syncPointsTrackUnstableMediumPathDependencies = Configure.syncPointsTrackUnstableMediumPathDependencies;
    public static boolean syncPointsTrackUnstableMediumPathDependencies() { return syncPointsTrackUnstableMediumPathDependencies; }

    private static final boolean recoverPartialAcceptPhaseIfNoFastPath = Configure.recoverPartialAcceptPhaseIfNoFastPath;
    public static boolean recoverPartialAcceptPhaseIfNoFastPath() { return recoverPartialAcceptPhaseIfNoFastPath; }

    // TODO (expected): make final, but requires some solution to burn test mutating these randomly
    private static boolean recoverReads = Configure.recoverReads;
    public static boolean recoverReads() { return recoverReads; }

    private static final boolean filterDuplicateDependenciesFromAcceptReply = Configure.filterDuplicateDependenciesFromAcceptReply;
    public static boolean filterDuplicateDependenciesFromAcceptReply() { return filterDuplicateDependenciesFromAcceptReply; }

    private static final SendStableMessages sendStableMessages = Configure.sendStableMessages;
    public static boolean sendOnlyReadStableMessages(TxnId txnId) { return sendStableMessages.compareTo(FOR_READS) >= 0 || (sendStableMessages == TO_ALL_REPLICA_EXECUTABLE_ELSE_FOR_READS && (!txnId.is(SingleKey) || !txnId.is(Txn.Kind.Write))); }
    public static boolean sendNoStableIfFastExec() { return sendStableMessages == FOR_READS_OR_NONE_IF_FASTEXEC; }

    private static final boolean sendMinimal = Configure.sendMinimal;
    public static boolean sendMinimal() { return sendMinimal; }

    private static final boolean dataStoreRequiresUniqueHlcs = Configure.dataStoreRequiresUniqueHlcs;
    public static boolean dataStoreRequiresUniqueHlcs() { return dataStoreRequiresUniqueHlcs; }

    private static int markStaleIfCannotExecute = Configure.markStaleIfCannotExecute;
    public static boolean markStaleIfCannotExecute(TxnId txnId) { return TinyEnumSet.contains(markStaleIfCannotExecute, txnId.kindOrdinal()); }
    public static boolean markStaleIfCannotExecute(Txn.Kind kind) { return TinyEnumSet.contains(markStaleIfCannotExecute, kind); }

    private static final int transitiveDependenciesAreVisible = Configure.transitiveDependenciesAreVisible;
    public static boolean isTransitiveDependencyVisible(TxnId txnId) { return TinyEnumSet.contains(transitiveDependenciesAreVisible, txnId.kindOrdinal()); }
    public static boolean isTransitiveDependencyVisible(Txn.Kind kind) { return TinyEnumSet.contains(transitiveDependenciesAreVisible, kind); }

    private static final CleanCfkBefore cleanCfkBefore = Configure.cleanCfkBefore;
    public static CleanCfkBefore cleanCfkBefore() { return cleanCfkBefore; }

    private static final DependencyElision dependencyElision = Configure.dependencyElision;
    public static DependencyElision dependencyElision() { return dependencyElision; }

    private static final boolean visitMaybePruned = Configure.visitMaybePruned;
    public static boolean shouldVisitMaybePruned() { return visitMaybePruned; }

    public static boolean isHomeDoneIfNotDurable(SaveStatus saveStatus, TxnId txnId, Timestamp executeAt, @Nullable Txn txn)
    {
        if (txnId.isSystemTxn() || saveStatus.compareTo(SaveStatus.Stable) < 0)
            return false;

        if (executeAt == null || (executeAt != txnId && executeAt.epoch() != txnId.epoch()))
            return false;

        if (saveStatus.compareTo(SaveStatus.PreApplied) >= 0 && informOfSingleKeyDurabilityIfDepsSizeAtLeast > 0 && txnId.is(SingleKey))
            return true;

        // TODO (expected): why do we check txn != null?
        return executeAtReplica(txnId, txn) && txn != null;
    }

    private static final InformOfDurability informOfDurability = Configure.informOfDurability;
    // TODO (expected): improve dependency elision, so this (and replicaExecuteDistributedPersistChance) are no longer needed
    private static final int informOfSingleKeyDurabilityIfDepsSizeAtLeast = Configure.informOfSingleKeyDurabilityIfDepsSizeAtLeast;
    public static InformOfDurability informOfDurability(TxnId txnId, @Nullable Deps deps)
    {
        if (txnId.is(Any) || deps == null || informOfSingleKeyDurabilityIfDepsSizeAtLeast <= deps.txnIdCount())
            return informOfDurability;

        return InformOfDurability.NONE;
    }

    private static FastExecution fastReadExecution = Configure.fastReadExecution;
    public static boolean fastReadsMayBypassSafeStore(TxnId txnId) { return fastReadExecution == MAY_BYPASS_SAFESTORE && (dataStoreDetectsFutureReads() || txnId.is(EphemeralRead)) && txnId.is(Domain.Key); }
    public static boolean fastReadsMayBypassCommandsForKey(TxnId txnId) { return fastReadExecution != FastExecution.DISABLED && !txnId.is(Txn.Kind.Write) && txnId.is(Domain.Key); }

    private static final boolean fastReadExecutionMayResendTxn = Configure.fastReadExecMayResendTxn;
    public static boolean fastReadExecutionMayResendTxn() { return fastReadExecutionMayResendTxn; }

    private static FastExecution fastWriteExecution = Configure.fastWriteExecution;
    public static boolean fastWritesMayBypassSafeStore() { return fastWriteExecution == MAY_BYPASS_SAFESTORE; }
    public static boolean fastWritesMayBypassCommandsForKey() { return fastWriteExecution != FastExecution.DISABLED; }

    private static boolean dataStoreDetectsFutureReads = Configure.dataStoreDetectsFutureReads;
    public static boolean dataStoreDetectsFutureReads() { return dataStoreDetectsFutureReads; }

    private static CoordinatorBacklogExecution coordinatorBacklogExecution = Configure.coordinatorBacklogExecution;
    public static boolean coordinatorBacklogExecution(Ballot ballot)
    {
        switch (coordinatorBacklogExecution)
        {
            default: throw new UnhandledEnum(coordinatorBacklogExecution);
            case DISABLED: return false;
            case ALWAYS: return true;
            case ON_RECOVERY: return !ballot.equals(Ballot.ZERO);
        }
    }

    private static final boolean permitCoordinatorLocalExecution = Configure.permitCoordinatorLocalExecution;
    public static boolean permitCoordinatorLocalExecution() { return permitCoordinatorLocalExecution; }
    
    private static final boolean permitLocalDelivery = Configure.permitLocalDelivery;
    public static boolean permitLocalDelivery() { return permitLocalDelivery; }

    private static final boolean permitAsyncTasks = Configure.permitAsyncTasks;
    private static final boolean permitAtomicIncrementalTasks = Configure.permitAtomicIncrementalTasks;
    public static boolean permitAtomicIncrementalTasks() { return permitAtomicIncrementalTasks; }
    public static LoadKeys loadKeysAsyncIfPermitted(TxnId txnId)
    {
        if (!permitAsyncTasks)
            return LoadKeys.SYNC;

        if (permitAtomicIncrementalTasks())
            return LoadKeys.ASYNC;
        return managesExecution(txnId) ? LoadKeys.SYNC : LoadKeys.ASYNC;
    }

    private static ReplicaExecution replicaExecution = Configure.replicaExecution;
    public static boolean executeAtReplica(TxnId txnId, @Nullable Txn txn)
    {
        if (!txnId.is(SingleKey))
            return false;

        switch (replicaExecution)
        {
            default:          throw new UnhandledEnum(replicaExecution);
            case NONE:        return false;
            case ALL:         return txnId.is(Txn.Kind.Write);
            case BLIND_WRITE: return txnId.is(Txn.Kind.Write) && txn != null && txn.read().keys().isEmpty();
        }
    }

    private static final float replicaExecuteDistributedPersistChance = Configure.replicaExecuteDistributedPersistChance;
    public static boolean replicaExecuteDistributedPersist()
    {
        float chance = replicaExecuteDistributedPersistChance;
        if (chance <= 0f) return false;
        if (chance >= 1f) return true;
        return ThreadLocalRandom.current().nextFloat() < chance;
    }

    public static class QuorumEpochIntersections
    {
        public enum ChaseFixedPoint
        {
            Chase, DoNotChase;
        }

        public enum Include
        {
            // include all nodes and epochs with unsynced ranges
            Unsynced,
            // include only replicas that own the ranges, and only the ownership epochs
            Owned;

            private static Include max(Include a, Include b)
            {
                return a.compareTo(b) >= 0 ? a : b;
            }
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

        static class Spec
        {
            private static final Pattern PARSE = Pattern.compile("(preaccept|accept|commit|stable|recover)=([+-]{0,2})");

            final ChaseAndInclude preaccept;
            final Include accept, commit, stable, recover;
            final Include preacceptOrRecover, preacceptOrCommit;

            Spec(ChaseAndInclude preaccept, Include accept, Include commit, Include stable, Include recover)
            {
                this.preaccept = preaccept;
                Invariants.require(preaccept.chase == DoNotChase, "PreAccept chasing epoch is not implemented as not needed for current formulation and it complicated some aspects. An implementation can be found in git history.");
                this.accept = accept;
                this.commit = commit;
                this.stable = stable;
                this.recover = recover;
                this.preacceptOrRecover = Include.max(preaccept.include, recover);
                this.preacceptOrCommit = Include.max(preaccept.include, commit);
            }

            static Spec parse(String description)
            {
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
                        case "accept": accept = include; Invariants.require(cfp == DoNotChase, "Invalid to specify ChaseFixedPoint.Chase for accept"); break;
                        case "commit": commit = include; Invariants.require(cfp == DoNotChase, "Invalid to specify ChaseFixedPoint.Chase for commit"); break;
                        case "stable": stable = include; Invariants.require(cfp == DoNotChase, "Invalid to specify ChaseFixedPoint.Chase for stable"); break;
                        case "recover": recover = include; Invariants.require(cfp == DoNotChase, "Invalid to specify ChaseFixedPoint.Chase for recover"); break;
                    }
                }

                Invariants.require(preaccept != null, "preaccept not specified for quorum epoch intersections: " + description);
                Invariants.require(accept != null, "accept not specified for quorum epoch intersections: " + description);
                Invariants.require(commit != null, "commit not specified for quorum epoch intersections: " + description);
                Invariants.require(stable != null, "stable not specified for quorum epoch intersections: " + description);
                Invariants.require(recover != null, "recover not specified for quorum epoch intersections: " + description);
                return new Spec(preaccept, accept, commit, stable, recover);
            }
        }

        static
        {
            Spec spec = Spec.parse(System.getProperty("accord.quorums.epochs", "preaccept=-,accept=-,commit=-,stable=-,recover=-"));
            preaccept = spec.preaccept;
            accept = spec.accept;
            commit = spec.commit;
            stable = spec.stable;
            recover = spec.recover;
            preacceptOrRecover = spec.preacceptOrRecover;
            preacceptOrCommit = spec.preacceptOrCommit;
        }

        // TODO (desired): support fixed-point chasing for recovery
        public static final ChaseAndInclude preaccept;
        public static final Include accept, commit, stable, recover;
        public static final Include preacceptOrRecover, preacceptOrCommit;
    }

    public static class Faults
    {
        static class Spec
        {
            // note that {txn,syncPoint}DiscardPreAcceptDeps faults expect filterDuplicateDependenciesFromAcceptReply to be off
            final boolean txnInstability, txnDiscardPreAcceptDeps;
            final boolean syncPointInstability, syncPointDiscardPreAcceptDeps;

            Spec(boolean txnInstability, boolean txnDiscardPreAcceptDeps, boolean syncPointInstability, boolean syncPointDiscardPreAcceptDeps)
            {
                this.txnInstability = txnInstability;
                this.txnDiscardPreAcceptDeps = txnDiscardPreAcceptDeps;
                this.syncPointInstability = syncPointInstability;
                this.syncPointDiscardPreAcceptDeps = syncPointDiscardPreAcceptDeps;
            }
        }

        static
        {
            Spec spec = new Spec(false, false, false, false);
            txnInstability = spec.txnInstability;
            txnDiscardPreAcceptDeps = spec.txnDiscardPreAcceptDeps;
            syncPointInstability = spec.syncPointInstability;
            syncPointDiscardPreAcceptDeps = spec.syncPointDiscardPreAcceptDeps;
        }

        public static final boolean txnInstability, txnDiscardPreAcceptDeps;
        public static final boolean syncPointInstability, syncPointDiscardPreAcceptDeps;

        public static boolean discardPreAcceptDeps(TxnId txnId)
        {
            return (txnDiscardPreAcceptDeps | syncPointDiscardPreAcceptDeps)
                   && (txnId.isSyncPoint() ? syncPointDiscardPreAcceptDeps : txnDiscardPreAcceptDeps);
        }
    }}
