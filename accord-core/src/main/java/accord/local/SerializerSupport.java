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
import accord.primitives.Timestamp;
import accord.primitives.Writes;

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
    public static Command reconstruct(Mutable attrs, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOnProvider waitingOnProvider, MessageProvider messageProvider)
    {
        switch (status.status)
        {
            case NotDefined:
                return Command.NotDefined.notDefined(attrs, promised);
            case PreAccepted:
                return preAccepted(attrs, executeAt, promised, messageProvider);
            case AcceptedInvalidate:
            case Accepted:
            case PreCommitted:
                return accepted(attrs, status, executeAt, promised, accepted, messageProvider);
            case Committed:
            case ReadyToExecute:
                return committed(attrs, status, executeAt, promised, accepted, waitingOnProvider, messageProvider);
            case PreApplied:
            case Applied:
                return executed(attrs, status, executeAt, promised, accepted, waitingOnProvider, messageProvider);
            case Truncated:
            case Invalidated:
                return truncated(attrs, status, executeAt, messageProvider);
            default:
                throw new IllegalStateException();
        }
    }

    private static final Set<MessageType> PRE_ACCEPT_TYPES =
        ImmutableSet.of(PRE_ACCEPT_REQ, BEGIN_RECOVER_REQ, PROPAGATE_PRE_ACCEPT_MSG);

    private static Command.PreAccepted preAccepted(Mutable attrs, Timestamp executeAt, Ballot promised, MessageProvider messageProvider)
    {
        Set<MessageType> witnessed = messageProvider.test(PRE_ACCEPT_TYPES);
        checkState(!witnessed.isEmpty());
        attrs.partialTxn(txnFromPreAcceptOrBeginRecover(witnessed, messageProvider));
        return Command.PreAccepted.preAccepted(attrs, executeAt, promised);
    }

    private static Command.Accepted accepted(Mutable attrs, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, MessageProvider messageProvider)
    {
        if (status.known.isDefinitionKnown())
        {
            Set<MessageType> witnessed = messageProvider.test(PRE_ACCEPT_TYPES);
            checkState(!witnessed.isEmpty());
            attrs.partialTxn(txnFromPreAcceptOrBeginRecover(witnessed, messageProvider));
        }

        if (status.known.deps.hasProposedDeps())
        {
            Accept accept = messageProvider.accept(accepted);
            attrs.partialDeps(accept.partialDeps);
        }

        return Command.Accepted.accepted(attrs, status, executeAt, promised, accepted);
    }

    private static final Set<MessageType> PRE_ACCEPT_COMMIT_TYPES =
        ImmutableSet.of(PRE_ACCEPT_REQ, BEGIN_RECOVER_REQ, PROPAGATE_PRE_ACCEPT_MSG,
                        COMMIT_SLOW_PATH_REQ, COMMIT_MAXIMAL_REQ, STABLE_FAST_PATH_REQ, STABLE_SLOW_PATH_REQ, STABLE_MAXIMAL_REQ, PROPAGATE_STABLE_MSG);

    private static Command.Committed committed(Mutable attrs, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOnProvider waitingOnProvider, MessageProvider messageProvider)
    {
        attrs = extract(status, accepted, messageProvider, (attrs0, txn, deps, i1, i2) -> attrs0.partialTxn(txn).partialDeps(deps), attrs);
        return Command.Committed.committed(attrs, status, executeAt, promised, accepted, waitingOnProvider.provide(attrs.partialDeps()));
    }

    private static final Set<MessageType> PRE_ACCEPT_COMMIT_APPLY_TYPES =
        ImmutableSet.of(PRE_ACCEPT_REQ, BEGIN_RECOVER_REQ, PROPAGATE_PRE_ACCEPT_MSG,
                        COMMIT_SLOW_PATH_REQ, COMMIT_MAXIMAL_REQ, PROPAGATE_STABLE_MSG,
                        APPLY_MINIMAL_REQ, APPLY_MAXIMAL_REQ, PROPAGATE_APPLY_MSG);

    private static Command.Executed executed(Mutable attrs, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOnProvider waitingOnProvider, MessageProvider messageProvider)
    {
        return extract(status, accepted, messageProvider, (attrs0, txn, deps, writes, result) -> {
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

    public static TxnAndDeps extractTxnAndDeps(SaveStatus status, Ballot accepted, MessageProvider messageProvider)
    {
        return extract(status, accepted, messageProvider, (i1, txn, deps, i2, i3) -> new TxnAndDeps(txn, deps), null);
    }

    private static <I, O> O extract(SaveStatus status, Ballot accepted, MessageProvider messageProvider, WithContents<I, O> withContents, I param)
    {
        // TODO (expected): first check if we have taken the normal path
        Set<MessageType> witnessed;

        switch (status.status)
        {
            case PreAccepted:
                witnessed = messageProvider.test(PRE_ACCEPT_TYPES);
                checkState(!witnessed.isEmpty());
                return withContents.apply(param, txnFromPreAcceptOrBeginRecover(witnessed, messageProvider), null, null, null);

            case AcceptedInvalidate:
            case Accepted:
            case PreCommitted:
                PartialTxn txn = null;
                PartialDeps deps = null;

                if (status.known.isDefinitionKnown())
                {
                    witnessed = messageProvider.test(PRE_ACCEPT_TYPES);
                    checkState(!witnessed.isEmpty());
                    txn = txnFromPreAcceptOrBeginRecover(witnessed, messageProvider);
                }

                if (status.known.deps.hasProposedDeps())
                {
                    Accept accept = messageProvider.accept(accepted);
                    deps = accept.partialDeps;
                }
                return withContents.apply(param, txn, deps, null, null);

            case Committed:
            case Stable:
            case ReadyToExecute:
                witnessed = messageProvider.test(PRE_ACCEPT_COMMIT_TYPES);
                if (witnessed.contains(STABLE_MAXIMAL_REQ))
                {
                    Commit commit = messageProvider.stableMaximal();
                    return withContents.apply(param, commit.partialTxn, commit.partialDeps, null, null);
                }
                if (witnessed.contains(COMMIT_MAXIMAL_REQ))
                {
                    Commit commit = messageProvider.commitMaximal();
                    return withContents.apply(param, commit.partialTxn, commit.partialDeps, null, null);
                }
                else if (witnessed.contains(PROPAGATE_STABLE_MSG))
                {
                    Propagate propagate = messageProvider.propagateStable();
                    return withContents.apply(param, propagate.partialTxn, propagate.stableDeps, null, null);
                }
                else if (witnessed.contains(COMMIT_SLOW_PATH_REQ))
                {
                    Commit commit = messageProvider.commitSlowPath();
                    return withContents.apply(param, merge(txnFromPreAcceptOrBeginRecover(witnessed, messageProvider), commit.partialTxn), commit.partialDeps, null, null);
                }
                else
                {
                    checkState(witnessed.contains(STABLE_FAST_PATH_REQ));
                    Commit commit = messageProvider.stableFastPath();
                    return withContents.apply(param, merge(txnFromPreAcceptOrBeginRecover(witnessed, messageProvider), commit.partialTxn), commit.partialDeps, null, null);
                }

            case PreApplied:
            case Applied:
                witnessed = messageProvider.test(PRE_ACCEPT_COMMIT_APPLY_TYPES);
                if (witnessed.contains(APPLY_MAXIMAL_REQ))
                {
                    Apply apply = messageProvider.applyMaximal();
                    return withContents.apply(param, apply.txn, apply.deps, apply.writes, apply.result);
                }
                else if (witnessed.contains(PROPAGATE_APPLY_MSG))
                {
                    Propagate propagate = messageProvider.propagateApply();
                    return withContents.apply(param, propagate.partialTxn, propagate.stableDeps, propagate.writes, propagate.result);
                }
                else
                {
                    checkState(witnessed.contains(APPLY_MINIMAL_REQ));
                    Apply apply = messageProvider.applyMinimal();
                    if (witnessed.contains(STABLE_MAXIMAL_REQ))
                    {
                        Commit commit = messageProvider.stableMaximal();
                        return withContents.apply(param, commit.partialTxn, commit.partialDeps, apply.writes, apply.result);
                    }
                    else if (witnessed.contains(COMMIT_MAXIMAL_REQ))
                    {
                        Commit commit = messageProvider.commitMaximal();
                        return withContents.apply(param, commit.partialTxn, commit.partialDeps, apply.writes, apply.result);
                    }
                    else if (witnessed.contains(PROPAGATE_STABLE_MSG))
                    {
                        Propagate propagate = messageProvider.propagateStable();
                        return withContents.apply(param, propagate.partialTxn, propagate.stableDeps, apply.writes, apply.result);
                    }
                    else if (witnessed.contains(COMMIT_SLOW_PATH_REQ))
                    {
                        Commit commit = messageProvider.commitSlowPath();
                        return withContents.apply(param, merge(txnFromPreAcceptOrBeginRecover(witnessed, messageProvider), commit.partialTxn), commit.partialDeps, apply.writes, apply.result);
                    }
                    else if (witnessed.contains(STABLE_FAST_PATH_REQ))
                    {
                        Commit commit = messageProvider.stableFastPath();
                        return withContents.apply(param, merge(txnFromPreAcceptOrBeginRecover(witnessed, messageProvider), commit.partialTxn), commit.partialDeps, apply.writes, apply.result);
                    }
                    else
                    {
                        return withContents.apply(param, merge(apply.txn, txnFromPreAcceptOrBeginRecover(witnessed, messageProvider)), apply.deps, apply.writes, apply.result);
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

    private static PartialTxn txnFromPreAcceptOrBeginRecover(Set<MessageType> witnessed, MessageProvider messageProvider)
    {
        if (witnessed.contains(PRE_ACCEPT_REQ))
            return messageProvider.preAccept().partialTxn;

        if (witnessed.contains(BEGIN_RECOVER_REQ))
            return messageProvider.beginRecover().partialTxn;

        if (witnessed.contains(PROPAGATE_PRE_ACCEPT_MSG))
            return messageProvider.propagatePreAccept().partialTxn;

        return null;
    }

    public interface WaitingOnProvider
    {
        WaitingOn provide(PartialDeps deps);
    }

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
