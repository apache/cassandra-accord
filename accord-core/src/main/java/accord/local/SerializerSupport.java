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
import accord.messages.ApplyThenWaitUntilApplied;
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
import static accord.messages.MessageType.COMMIT_MINIMAL_REQ;
import static accord.messages.MessageType.PRE_ACCEPT_REQ;
import static accord.messages.MessageType.PROPAGATE_APPLY_MSG;
import static accord.messages.MessageType.PROPAGATE_COMMIT_MSG;
import static accord.messages.MessageType.PROPAGATE_PRE_ACCEPT_MSG;
import static accord.primitives.PartialTxn.merge;
import static accord.utils.Invariants.checkState;

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
           COMMIT_MINIMAL_REQ, COMMIT_MAXIMAL_REQ, PROPAGATE_COMMIT_MSG);

    private static Command.Committed committed(Mutable attrs, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOnProvider waitingOnProvider, MessageProvider messageProvider)
    {
        Set<MessageType> witnessed = messageProvider.test(PRE_ACCEPT_COMMIT_TYPES);

        PartialTxn txn;
        PartialDeps deps;

        if (witnessed.contains(COMMIT_MAXIMAL_REQ))
        {
            Commit commit = messageProvider.commitMaximal();
            txn = commit.partialTxn;
            deps = commit.partialDeps;
        }
        else if (witnessed.contains(PROPAGATE_COMMIT_MSG))
        {
            Propagate propagate = messageProvider.propagateCommit();
            txn = propagate.partialTxn;
            deps = propagate.partialDeps;
        }
        else
        {
            checkState(witnessed.contains(COMMIT_MINIMAL_REQ));
            Commit commit = messageProvider.commitMinimal();
            txn = merge(txnFromPreAcceptOrBeginRecover(witnessed, messageProvider), commit.partialTxn);
            deps = commit.partialDeps;
        }

        attrs.partialTxn(txn)
             .partialDeps(deps);

        return Command.Committed.committed(attrs, status, executeAt, promised, accepted, waitingOnProvider.provide(deps));
    }

    private static final Set<MessageType> PRE_ACCEPT_COMMIT_APPLY_TYPES =
        ImmutableSet.of(PRE_ACCEPT_REQ, BEGIN_RECOVER_REQ, PROPAGATE_PRE_ACCEPT_MSG,
           COMMIT_MINIMAL_REQ, COMMIT_MAXIMAL_REQ, PROPAGATE_COMMIT_MSG,
           APPLY_MINIMAL_REQ, APPLY_MAXIMAL_REQ, PROPAGATE_APPLY_MSG);

    private static Command.Executed executed(Mutable attrs, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOnProvider waitingOnProvider, MessageProvider messageProvider)
    {
        Set<MessageType> witnessed = messageProvider.test(PRE_ACCEPT_COMMIT_APPLY_TYPES);

        PartialTxn txn;
        PartialDeps deps;
        Writes writes;
        Result result;

        if (witnessed.contains(APPLY_MAXIMAL_REQ))
        {
            Apply apply = messageProvider.applyMaximal();
            txn = apply.txn;
            deps = apply.deps;
            writes = apply.writes;
            result = apply.result;
        }
        else if (witnessed.contains(PROPAGATE_APPLY_MSG))
        {
            Propagate propagate = messageProvider.propagateApply();
            txn = propagate.partialTxn;
            deps = propagate.partialDeps;
            writes = propagate.writes;
            result = propagate.result;
        }
        else
        {
            boolean haveApplyMinimal = witnessed.contains(APPLY_MINIMAL_REQ);
            boolean haveCommitMaximal = witnessed.contains(COMMIT_MAXIMAL_REQ);

            Apply apply = null;
            Commit commit = null;
            String errorMessage = "Must have either an APPLY_MINIMAL_REQ or a COMMIT_MAXIMAL_REQ containing ApplyThenWaitUntilApplied";
            if (haveApplyMinimal)
            {
                apply = messageProvider.applyMinimal();
                writes = apply.writes;
                result = apply.result;
            }
            else if (haveCommitMaximal)
            {
                commit = messageProvider.commitMaximal();
                checkState(commit.readData instanceof ApplyThenWaitUntilApplied, errorMessage);
                ApplyThenWaitUntilApplied applyThenWaitUntilApplied = (ApplyThenWaitUntilApplied)commit.readData;
                writes = applyThenWaitUntilApplied.writes;
                result = applyThenWaitUntilApplied.txnResult;
            }
            else
            {
                throw new IllegalStateException(errorMessage);
            }

            /*
             * NOTE: If Commit has been witnessed, we'll extract deps from there;
             * Apply has an expected TO-DO to stop including deps in such case.
             */
            if (haveCommitMaximal)
            {
                if (commit == null)
                    commit = messageProvider.commitMaximal();
                txn = commit.partialTxn;
                deps = commit.partialDeps;
            }
            else if (witnessed.contains(PROPAGATE_COMMIT_MSG))
            {
                Propagate propagateCommit = messageProvider.propagateCommit();
                txn = propagateCommit.partialTxn;
                deps = propagateCommit.partialDeps;
            }
            else if (witnessed.contains(COMMIT_MINIMAL_REQ))
            {
                commit = messageProvider.commitMinimal();
                txn = merge(apply.txn, merge(commit.partialTxn, txnFromPreAcceptOrBeginRecover(witnessed, messageProvider)));
                deps = commit.partialDeps;
            }
            else
            {
                txn = merge(apply.txn, txnFromPreAcceptOrBeginRecover(witnessed, messageProvider));
                deps = apply.deps;
            }
        }

        attrs.partialTxn(txn)
             .partialDeps(deps);

        return Command.Executed.executed(attrs, status, executeAt, promised, accepted, waitingOnProvider.provide(deps), writes, result);
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
                throw new IllegalStateException("Unhandled SaveStatus: " + status);
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

    public static TxnAndDeps extractTxnAndDeps(SaveStatus status, Ballot accepted, MessageProvider messageProvider)
    {
        Set<MessageType> witnessed;

        switch (status.status)
        {
            case PreAccepted:
                witnessed = messageProvider.test(PRE_ACCEPT_TYPES);
                checkState(!witnessed.isEmpty());
                return new TxnAndDeps(txnFromPreAcceptOrBeginRecover(witnessed, messageProvider), null);
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

                return new TxnAndDeps(txn, deps);
            case Committed:
            case ReadyToExecute:
                witnessed = messageProvider.test(PRE_ACCEPT_COMMIT_TYPES);
                if (witnessed.contains(COMMIT_MAXIMAL_REQ))
                {
                    Commit commit = messageProvider.commitMaximal();
                    return new TxnAndDeps(commit.partialTxn, commit.partialDeps);
                }
                else if (witnessed.contains(PROPAGATE_COMMIT_MSG))
                {
                    Propagate propagate = messageProvider.propagateCommit();
                    return new TxnAndDeps(propagate.partialTxn, propagate.partialDeps);
                }
                else
                {
                    checkState(witnessed.contains(COMMIT_MINIMAL_REQ));
                    Commit commit = messageProvider.commitMinimal();
                    return new TxnAndDeps(merge(txnFromPreAcceptOrBeginRecover(witnessed, messageProvider), commit.partialTxn), commit.partialDeps);
                }
            case PreApplied:
            case Applied:
                witnessed = messageProvider.test(PRE_ACCEPT_COMMIT_APPLY_TYPES);
                if (witnessed.contains(APPLY_MAXIMAL_REQ))
                {
                    Apply apply = messageProvider.applyMaximal();
                    return new TxnAndDeps(apply.txn, apply.deps);
                }
                else if (witnessed.contains(PROPAGATE_APPLY_MSG))
                {
                    Propagate propagate = messageProvider.propagateApply();
                    return new TxnAndDeps(propagate.partialTxn, propagate.partialDeps);
                }
                else if (witnessed.contains(COMMIT_MAXIMAL_REQ))
                {
                    Commit commit = messageProvider.commitMaximal();
                    return new TxnAndDeps(commit.partialTxn, commit.partialDeps);
                }
                else if (witnessed.contains(PROPAGATE_COMMIT_MSG))
                {
                    Propagate propagate = messageProvider.propagateCommit();
                    return new TxnAndDeps(propagate.partialTxn, propagate.partialDeps);
                }
                else if (witnessed.contains(COMMIT_MINIMAL_REQ))
                {
                    checkState(witnessed.contains(APPLY_MINIMAL_REQ));
                    Apply apply = messageProvider.applyMinimal();
                    Commit commit = messageProvider.commitMinimal();
                    return new TxnAndDeps(merge(apply.txn, merge(commit.partialTxn, txnFromPreAcceptOrBeginRecover(witnessed, messageProvider))), commit.partialDeps);
                }
                else
                {
                    checkState(witnessed.contains(APPLY_MINIMAL_REQ));
                    Apply apply = messageProvider.applyMinimal();
                    return new TxnAndDeps(merge(apply.txn, txnFromPreAcceptOrBeginRecover(witnessed, messageProvider)), apply.deps);
                }
            case NotDefined:
            case Truncated:
            case Invalidated:
                return TxnAndDeps.EMPTY;
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

        Commit commitMinimal();

        Commit commitMaximal();

        Propagate propagateCommit();

        Apply applyMinimal();

        Apply applyMaximal();

        Propagate propagateApply();
    }
}
