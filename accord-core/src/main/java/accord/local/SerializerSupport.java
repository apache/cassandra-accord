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
package accord.local;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import accord.api.Result;
import accord.api.VisibleForImplementation;
import accord.local.Command.WaitingOn;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.CommonAttributes.Mutable;
import accord.messages.Accept;
import accord.messages.Apply;
import accord.messages.BeginRecovery;
import accord.messages.Commit;
import accord.messages.MessageType;
import accord.messages.PreAccept;
import accord.messages.Propagate;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.Writes;
import accord.utils.Invariants;

import static accord.messages.MessageType.APPLY_MAXIMAL_REQ;
import static accord.messages.MessageType.APPLY_MINIMAL_REQ;
import static accord.messages.MessageType.BEGIN_RECOVER_REQ;
import static accord.messages.MessageType.COMMIT_MAXIMAL_REQ;
import static accord.messages.MessageType.COMMIT_SLOW_PATH_REQ;
import static accord.messages.MessageType.PRE_ACCEPT_REQ;
import static accord.messages.MessageType.PROPAGATE_APPLY_MSG;
import static accord.messages.MessageType.PROPAGATE_STABLE_MSG;
import static accord.messages.MessageType.PROPAGATE_PRE_ACCEPT_MSG;
import static accord.messages.MessageType.STABLE_FAST_PATH_REQ;
import static accord.messages.MessageType.STABLE_MAXIMAL_REQ;
import static accord.messages.MessageType.STABLE_SLOW_PATH_REQ;
import static accord.primitives.PartialTxn.merge;
import static accord.utils.Invariants.checkState;
import static accord.utils.Invariants.illegalState;

@VisibleForImplementation
public class SerializerSupport
{
    /**
     * Reconstructs Command from register values and protocol messages.
     */
    public static Command reconstruct(RangesForEpoch rangesForEpoch, Mutable attrs, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOnProvider waitingOnProvider, MessageProvider messageProvider)
    {
        switch (status.status)
        {
            case NotDefined:
                return Command.NotDefined.notDefined(attrs, promised);
            case PreAccepted:
                return preAccepted(rangesForEpoch, attrs, executeAt, promised, messageProvider);
            case AcceptedInvalidate:
            case Accepted:
            case PreCommitted:
                return accepted(rangesForEpoch, attrs, status, executeAt, promised, accepted, messageProvider);
            case Committed:
            case Stable:
                return committed(rangesForEpoch, attrs, status, executeAt, promised, accepted, waitingOnProvider, messageProvider);
            case PreApplied:
            case Applied:
                return executed(rangesForEpoch, attrs, status, executeAt, promised, accepted, waitingOnProvider, messageProvider);
            case Truncated:
            case Invalidated:
                return truncated(attrs, status, executeAt, messageProvider);
            default:
                throw new IllegalStateException();
        }
    }

    private static final Set<MessageType> PRE_ACCEPT_TYPES =
        ImmutableSet.of(PRE_ACCEPT_REQ, BEGIN_RECOVER_REQ, PROPAGATE_PRE_ACCEPT_MSG);

    private static Command.PreAccepted preAccepted(RangesForEpoch rangesForEpoch, Mutable attrs, Timestamp executeAt, Ballot promised, MessageProvider messageProvider)
    {
        Set<MessageType> witnessed = messageProvider.test(PRE_ACCEPT_TYPES);
        checkState(!witnessed.isEmpty());
        attrs.partialTxn(txnFromPreAcceptOrBeginRecover(rangesForEpoch, witnessed, messageProvider));
        return Command.PreAccepted.preAccepted(attrs, executeAt, promised);
    }

    private static Command.Accepted accepted(RangesForEpoch rangesForEpoch, Mutable attrs, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, MessageProvider messageProvider)
    {
        if (status.known.isDefinitionKnown())
        {
            Set<MessageType> witnessed = messageProvider.test(PRE_ACCEPT_TYPES);
            checkState(!witnessed.isEmpty());
            attrs.partialTxn(txnFromPreAcceptOrBeginRecover(rangesForEpoch, witnessed, messageProvider));
        }

        if (status.known.deps.hasProposedDeps())
        {
            Accept accept = messageProvider.accept(accepted);
            attrs.partialDeps(slicePartialDeps(rangesForEpoch, accept));
        }

        return Command.Accepted.accepted(attrs, status, executeAt, promised, accepted);
    }

    private static final Set<MessageType> PRE_ACCEPT_COMMIT_TYPES =
        ImmutableSet.of(PRE_ACCEPT_REQ, BEGIN_RECOVER_REQ, PROPAGATE_PRE_ACCEPT_MSG, COMMIT_SLOW_PATH_REQ, COMMIT_MAXIMAL_REQ);

    private static final Set<MessageType> PRE_ACCEPT_STABLE_TYPES =
        ImmutableSet.of(PRE_ACCEPT_REQ, BEGIN_RECOVER_REQ, PROPAGATE_PRE_ACCEPT_MSG,
                        COMMIT_SLOW_PATH_REQ, COMMIT_MAXIMAL_REQ, STABLE_FAST_PATH_REQ, STABLE_SLOW_PATH_REQ, STABLE_MAXIMAL_REQ, PROPAGATE_STABLE_MSG);

    private static Command.Committed committed(RangesForEpoch rangesForEpoch, Mutable attrs, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOnProvider waitingOnProvider, MessageProvider messageProvider)
    {
        attrs = extract(rangesForEpoch, status, accepted, messageProvider, (attrs0, txn, deps, i1, i2) -> attrs0.partialTxn(txn).partialDeps(deps), attrs);
        return Command.Committed.committed(attrs, status, executeAt, promised, accepted, waitingOnProvider.provide(attrs.partialDeps()));
    }

    private static final Set<MessageType> PRE_ACCEPT_COMMIT_APPLY_TYPES =
        ImmutableSet.of(PRE_ACCEPT_REQ, BEGIN_RECOVER_REQ, PROPAGATE_PRE_ACCEPT_MSG,
                        COMMIT_SLOW_PATH_REQ, COMMIT_MAXIMAL_REQ, STABLE_MAXIMAL_REQ, STABLE_FAST_PATH_REQ, PROPAGATE_STABLE_MSG,
                        APPLY_MINIMAL_REQ, APPLY_MAXIMAL_REQ, PROPAGATE_APPLY_MSG);

    private static Command.Executed executed(RangesForEpoch rangesForEpoch, Mutable attrs, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOnProvider waitingOnProvider, MessageProvider messageProvider)
    {
        return extract(rangesForEpoch, status, accepted, messageProvider, (attrs0, txn, deps, writes, result) -> {
            attrs0.partialTxn(txn)
                 .partialDeps(deps);

            return Command.Executed.executed(attrs, status, executeAt, promised, accepted, waitingOnProvider.provide(deps), writes, result);
        }, attrs);
    }

    private static final Set<MessageType> APPLY_TYPES =
            ImmutableSet.of(APPLY_MINIMAL_REQ, APPLY_MAXIMAL_REQ, PROPAGATE_APPLY_MSG);

    private static Command.Truncated truncated(Mutable attrs, SaveStatus status, Timestamp executeAt, MessageProvider messageProvider)
    {
        Writes writes = null;
        Result result = null;

        switch (status)
        {
            default:
                throw illegalState("Unhandled SaveStatus: " + status);
            case TruncatedApplyWithOutcome:
            case TruncatedApplyWithDeps:
                Set<MessageType> witnessed = messageProvider.test(APPLY_TYPES);
                checkState(!witnessed.isEmpty());
                if (witnessed.contains(APPLY_MINIMAL_REQ))
                {
                    Apply apply = messageProvider.applyMinimal();
                    writes = apply.writes;
                    result = apply.result;
                }
                if (witnessed.contains(APPLY_MAXIMAL_REQ))
                {
                    Apply apply = messageProvider.applyMaximal();
                    writes = apply.writes;
                    result = apply.result;
                }
                else if (witnessed.contains(PROPAGATE_APPLY_MSG))
                {
                    Propagate propagate = messageProvider.propagateApply();
                    writes = propagate.writes;
                    result = propagate.result;
                }
            case TruncatedApply:
                return Command.Truncated.truncatedApply(attrs, status, executeAt, writes, result);
            case ErasedOrInvalidated:
                return Command.Truncated.erasedOrInvalidated(attrs.txnId(), attrs.durability(), attrs.route());
            case Erased:
                return Command.Truncated.erased(attrs.txnId(), attrs.durability(), attrs.route());
            case Invalidated:
                return Command.Truncated.invalidated(attrs.txnId(), attrs.durableListeners());
        }
    }

    public static class TxnAndDeps
    {
        public static TxnAndDeps EMPTY = new TxnAndDeps(null, null);

        public final PartialTxn txn;
        public final PartialDeps deps;

        TxnAndDeps(PartialTxn txn, PartialDeps deps)
        {
            this.txn = txn;
            this.deps = deps;
        }
    }

    interface WithContents<I, O>
    {
        O apply(I in, PartialTxn txn, PartialDeps deps, Writes writes, Result result);
    }

    public static TxnAndDeps extractTxnAndDeps(RangesForEpoch rangesForEpoch, SaveStatus status, Ballot accepted, MessageProvider messageProvider)
    {
        return extract(rangesForEpoch, status, accepted, messageProvider, (i1, txn, deps, i2, i3) -> new TxnAndDeps(txn, deps), null);
    }

    private static <I, O> O extract(RangesForEpoch rangesForEpoch, SaveStatus status, Ballot accepted, MessageProvider messageProvider, WithContents<I, O> withContents, I param)
    {
        // TODO (expected): first check if we have taken the normal path
        Set<MessageType> witnessed;

        // TODO (required): we must select the deps we would have used for initialiseWaitingOn
        switch (status.status)
        {
            case PreAccepted:
                witnessed = messageProvider.test(PRE_ACCEPT_TYPES);
                checkState(!witnessed.isEmpty());
                return withContents.apply(param, txnFromPreAcceptOrBeginRecover(rangesForEpoch, witnessed, messageProvider), null, null, null);

            case AcceptedInvalidate:
            case Accepted:
            case PreCommitted:
                PartialTxn txn = null;
                PartialDeps deps = null;

                if (status.known.isDefinitionKnown())
                {
                    witnessed = messageProvider.test(PRE_ACCEPT_TYPES);
                    checkState(!witnessed.isEmpty());
                    txn = txnFromPreAcceptOrBeginRecover(rangesForEpoch, witnessed, messageProvider);
                }

                if (status.known.deps.hasProposedDeps())
                {
                    Accept accept = messageProvider.accept(accepted);
                    deps = slicePartialDeps(rangesForEpoch, accept);
                }
                return withContents.apply(param, txn, deps, null, null);

            case Committed:
            {
                witnessed = messageProvider.test(PRE_ACCEPT_COMMIT_TYPES);
                Commit commit;
                if (witnessed.contains(COMMIT_MAXIMAL_REQ))
                {
                    commit = messageProvider.commitMaximal();
                }
                else
                {
                    Invariants.checkState(witnessed.contains(COMMIT_SLOW_PATH_REQ));
                    commit = messageProvider.commitSlowPath();
                }

                return sliceAndApply(rangesForEpoch, messageProvider, witnessed, commit, withContents, param, null, null);
            }

            case Stable:
            {
                // TODO (required): we should piece this back together in the precedence order of arrival
                witnessed = messageProvider.test(PRE_ACCEPT_STABLE_TYPES);
                Commit commit;
                if (witnessed.contains(STABLE_MAXIMAL_REQ))
                {
                    commit = messageProvider.stableMaximal();
                }
                else if (witnessed.contains(PROPAGATE_STABLE_MSG))
                {
                    return sliceAndApply(rangesForEpoch, messageProvider.propagateStable(), withContents, param, null, null);
                }
                else if (witnessed.contains(STABLE_FAST_PATH_REQ))
                {
                    commit = messageProvider.stableFastPath();
                }
                else
                {
                    checkState(witnessed.contains(STABLE_SLOW_PATH_REQ));
                    if (witnessed.contains(COMMIT_SLOW_PATH_REQ))
                    {
                        commit = messageProvider.commitSlowPath();
                    }
                    else
                    {
                        checkState(witnessed.contains(COMMIT_MAXIMAL_REQ));
                        commit = messageProvider.commitMaximal();
                    }
                }

                return sliceAndApply(rangesForEpoch, messageProvider, witnessed, commit, withContents, param, null, null);
            }

            case PreApplied:
            case Applied:
            {
                witnessed = messageProvider.test(PRE_ACCEPT_COMMIT_APPLY_TYPES);
                if (witnessed.contains(APPLY_MAXIMAL_REQ))
                {
                    Apply apply = messageProvider.applyMaximal();
                    Ranges ranges = rangesForEpoch.allBetween(apply.txnId.epoch(), apply.executeAt.epoch());
                    return withContents.apply(param, apply.txn.slice(ranges, true), apply.deps.slice(ranges), apply.writes, apply.result);
                }
                else if (witnessed.contains(PROPAGATE_APPLY_MSG))
                {
                    Propagate propagate = messageProvider.propagateApply();
                    return sliceAndApply(rangesForEpoch, propagate, withContents, param, propagate.writes, propagate.result);
                }
                else
                {
                    checkState(witnessed.contains(APPLY_MINIMAL_REQ));
                    Apply apply = messageProvider.applyMinimal();
                    Commit commit;
                    if (witnessed.contains(STABLE_MAXIMAL_REQ))
                    {
                        commit = messageProvider.stableMaximal();
                    }
                    else if (witnessed.contains(PROPAGATE_STABLE_MSG))
                    {
                        Propagate propagate = messageProvider.propagateStable();
                        return withContents.apply(param, propagate.partialTxn, propagate.stableDeps, apply.writes, apply.result);
                    }
                    else if (witnessed.contains(COMMIT_SLOW_PATH_REQ))
                    {
                        commit = messageProvider.commitSlowPath();
                    }
                    else if (witnessed.contains(STABLE_FAST_PATH_REQ))
                    {
                        commit = messageProvider.stableFastPath();
                    }
                    else
                    {
                        throw illegalState("Invalid state: insufficient stable or commit messages found to reconstruct PreApplied or greater SaveStatus");
                    }

                    return sliceAndApply(rangesForEpoch, messageProvider, witnessed, commit, withContents, param, apply.writes, apply.result);
                }
            }

            case NotDefined:
            case Truncated:
            case Invalidated:
                return withContents.apply(param, null, null, null, null);

            default:
                throw new IllegalStateException();
        }
    }

    private static PartialTxn txnFromPreAcceptOrBeginRecover(RangesForEpoch rangesForEpoch, Set<MessageType> witnessed, MessageProvider messageProvider)
    {
        if (witnessed.contains(PRE_ACCEPT_REQ))
        {
            PreAccept preAccept = messageProvider.preAccept();
            return preAccept.partialTxn.slice(rangesForEpoch.allBetween(preAccept.txnId.epoch(), preAccept.maxEpoch), true);
        }

        if (witnessed.contains(BEGIN_RECOVER_REQ))
        {
            BeginRecovery beginRecovery = messageProvider.beginRecover();
            return beginRecovery.partialTxn.slice(rangesForEpoch.allAt(beginRecovery.txnId.epoch()), true);
        }

        // TODO (expected): do we ever propagate only preaccept anymore?
        if (witnessed.contains(PROPAGATE_PRE_ACCEPT_MSG))
        {
            Propagate propagate = messageProvider.propagatePreAccept();
            return propagate.partialTxn.slice(rangesForEpoch.allBetween(propagate.txnId.epoch(), propagate.toEpoch), true);
        }

        return null;
    }

    private static PartialDeps slicePartialDeps(RangesForEpoch rangesForEpoch, Accept accept)
    {
        return accept.partialDeps.slice(rangesForEpoch.allBetween(accept.txnId.epoch(), accept.executeAt.epoch()));
    }

    private static <I, O> O sliceAndApply(RangesForEpoch rangesForEpoch, Propagate propagate, WithContents<I, O> withContents, I param, Writes writes, Result result)
    {
        Ranges ranges = rangesForEpoch.allBetween(propagate.txnId.epoch(), propagate.committedExecuteAt.epoch());
        PartialDeps partialDeps = propagate.stableDeps.slice(ranges);
        PartialTxn partialTxn = propagate.partialTxn.slice(ranges, true);
        return withContents.apply(param, partialTxn, partialDeps, writes, result);
    }

    private static <I, O> O sliceAndApply(RangesForEpoch rangesForEpoch, MessageProvider messageProvider, Set<MessageType> witnessed, Commit commit, WithContents<I, O> withContents, I param, Writes writes, Result result)
    {
        Ranges ranges = rangesForEpoch.allBetween(commit.txnId.epoch(), commit.executeAt.epoch());
        PartialDeps partialDeps = commit.partialDeps.slice(ranges);
        PartialTxn partialTxn = commit.partialTxn == null ? null : commit.partialTxn.slice(ranges, true);
        switch (commit.kind)
        {
            default: throw new AssertionError("Unhandled Commit.Kind: " + commit.kind);
            case CommitSlowPath:
            case StableFastPath:
            case StableSlowPath:
                PartialTxn preAcceptedPartialTxn = txnFromPreAcceptOrBeginRecover(rangesForEpoch, witnessed, messageProvider);
                if (partialTxn == null || partialTxn.keys().size() == 0) partialTxn = preAcceptedPartialTxn;
                else partialTxn = merge(preAcceptedPartialTxn, partialTxn);
            case StableWithTxnAndDeps:
            case CommitWithTxn:
        }
        return withContents.apply(param, partialTxn, partialDeps, writes, result);
    }


    public interface WaitingOnProvider
    {
        WaitingOn provide(PartialDeps deps);
    }

    // TODO (required): randomised testing that we always restore the exact same state
    public interface MessageProvider
    {
        Set<MessageType> test(Set<MessageType> messages);

        PreAccept preAccept();

        BeginRecovery beginRecover();

        Propagate propagatePreAccept();

        Accept accept(Ballot ballot);

        Commit commitSlowPath();

        Commit commitMaximal();

        Commit stableFastPath();

        Commit stableMaximal();

        Propagate propagateStable();

        Apply applyMinimal();

        Apply applyMaximal();

        Propagate propagateApply();
    }
}
