package accord.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.stream.IntStream;

/**
 * Nomenclature:
 *  register: the values associated with a given key
 *  step: a logical point in the causal sequence of events for a register
 *  step index: the index of a step; since we observe a strictly growing sequence this translates directly to
 *              the length of an observed sequence for a key. This imposes a total causal order for a given register
 *              (sequences [], [1], [1,2] have step indexes of 0, 1 and 2 respectively, with 2 necessarily happening after 1 and 0)
 *  predecessor: a step for a referent key that must occur before the referring step and key.
 *               two kinds: 1) those values for keys [B,C..) at step index i for a read of key A, precede key A's step index i +1
 *                          2) those values for keys [B,C..) at step index i for a write of key A, precede key A's step index i
 *  max predecessor: the maximum predecessor that may be reached via any predecessor relation
 *
 * Ensure there are no timestamp cycles in the implied list of predecessors, i.e. that we have a strict serializable order.
 * That is, we maintain links to the maximum predecessor step for each key, at each step for each key, and see if we can
 * find a path of predecessors that would witness us.
 *
 * TODO: find and report a path when we encounter a violation
 */
public class StrictSerializabilityVerifier
{
    /**
     * A link to the maximum predecessor node for a given key reachable from the transitive closure of predecessor
     * relations from a given register's observation (i.e. for a given sequence observed for a given key).
     *
     * A predecessor is an absolute happens-before relationship. This is created either:
     *  1) by witnessing some read for key A coincident with a write for key B,
     *     therefore the write for key B happened strictly after the write for key A; or
     *  2) any value for key A witnessed alongside a step index i for key B happens before i+1 for key B.
     *
     * For every observation step index i for key A, we have an outgoing MaxPredecessor link to every key.
     * This object both maintains the current maximum predecessor step index reachable via the transitive closure
     * of happens-before relationships, but represents a back-link from that step index for the referred-to key,
     * so that when its own predecessor memoized maximum predecessor values are updated we can propagate them here.
     *
     * In essence, each node in the happens-before graph maintains a link to every possible frontier of its transitive
     * closure in the graph, so that each time that frontier is updated the internal nodes that reference it are updated
     * to the new frontier. This ensures ~computationally optimal maintenance of this transitive closure at the expense
     * of quadratic memory utilisation, but for small numbers of unique keys this remains quite manageable (i.e. steps*keys^2)
     *
     * This graph can be interpreted quite simply: if any step index for a key can reach itself (or a successor) via
     * the transitive closure of happens-before relations then there is a serializability violation.
     */
    static class MaxPredecessor implements Comparable<MaxPredecessor>
    {
        MaxPredecessor prev = this, next = this;

        // the key we are tracking predecessors for
        final int ofKey;
        // the step index we are tracking predecessors for
        Step ofStep;

        // TODO: we probably don't need this field, as it's implied by the node we point to, that we have when we enqueue refresh
        // the key we are tracking the maximum predecessor for
        final int predecessorKey;

        // the step of the predecessor we are tracking with this node
        Step predecessorStep;

        MaxPredecessor(int ofKey, Step ofStep, int predecessorKey)
        {
            this.ofKey = ofKey;
            this.ofStep = ofStep;
            this.predecessorKey = predecessorKey;
        }

        @Override
        public int compareTo(MaxPredecessor that)
        {
            if (this.ofStep != that.ofStep) return Integer.compare(this.ofStep.ofStepIndex, that.ofStep.ofStepIndex);
            else return Integer.compare(this.ofKey, that.ofKey);
        }

        /**
         * Unlink {@code push} from any list in which it presently resides, and link it to this one
         */
        void push(MaxPredecessor push)
        {
            MaxPredecessor head = this;
            // unlink push from its current list
            push.next.prev = push.prev;
            push.prev.next = push.next;
            // link push to this list
            push.next = head.next;
            push.prev = head;
            head.next = push;
            push.next.prev = push;
        }

        /**
         * Apply {@code forEach} to each element in this list
         */
        void forEach(Consumer<MaxPredecessor> forEach)
        {
            MaxPredecessor next = this.next;
            while (next != this)
            {
                forEach.accept(next);
                next = next.next;
            }
        }

        @Override
        public String toString()
        {
            return predecessorStep == null ? "" : "" + predecessorStep.ofStepIndex;
        }
    }

    static class UnknownStepPredecessor extends MaxPredecessor
    {
        final UnknownStepHolder holder;
        UnknownStepPredecessor(Step successor, UnknownStepHolder holder)
        {
            super(successor.ofKey, successor, holder.step.ofKey);
            this.holder = holder;
            this.predecessorStep = holder.step;
        }
    }

    class UnknownStepHolder implements Runnable
    {
        final List<Step> peers = new ArrayList<>();
        final int writeValue;
        final int start;
        final int end;
        final Step step;

        UnknownStepHolder(int writeValue, int start, int end, Step step)
        {
            this.start = start;
            this.end = end;
            this.writeValue = writeValue;
            this.step = step;
            step.onChange = this;
        }

        void discoveredStepIndex(int stepIndex)
        {
            Register register = registers[step.ofKey];
            step.ofStepIndex = stepIndex;
            step.onChange = null;
            register.insert(step);

            for (Step peer : peers)
            {
                if (peer.maxPeers[step.ofKey] < stepIndex)
                {
                    peer.unknownStepPeers.remove(this);
                    if (peer.unknownStepPeers.isEmpty())
                        peer.unknownStepPeers = null;
                    peer.maxPeers[step.ofKey] = stepIndex;
                    registers[peer.ofKey].onChange(peer);
                }
            }

            register.onChange(step);
        }

        @Override
        public String toString()
        {
            return "{key:" + step.ofKey + ", value:" + writeValue + '}';
        }

        @Override
        public void run()
        {
            for (Step peer : peers)
                registers[peer.ofKey].onChange(peer);
        }
    }

    /**
     * Represents the graph state for a step for a single key, and maintains backwards references to all
     * internal nodes of the graph whose maximum predecessor for this key points to this step.
     * When this step is updated, we queue all of these internal nodes to update their own max predecessors.
     */
    static class Step extends MaxPredecessor
    {
        /**
         * the maximum _step_ of the corresponding key's sequence that was witnessed alongside this step for this key
         */
        final int[] maxPeers;

        /**
         *  The maximum _step_ of the corresponding key's sequence that was witnessed by any transitive predecessor of this key for this step.
         *  That is, if we look at the directly preceding step for this key (which must by definition precede this step) and explore all of
         *  its predecessors in the same manner, what is the highest step we can reach for each key.
         */
        final MaxPredecessor[] maxPredecessors;

        // TODO: cleanup
        List<UnknownStepHolder> unknownStepPeers;
        Map<Step, UnknownStepPredecessor> unknownStepPredecessors;
        Runnable onChange;

        int ofStepIndex;

        /**
         * The next instantiated sequence's observation.
         * This may not be stepIndex+1, if we have not witnessed stepIndex+1 directly.
         * i.e. if we have witnessed [0] and [0,1,2] then 0's successor will be 2.
         * If we later witness [0,1], 0's successor will be updated to 1, whose successor will be 2.
         */
        Step successor;

        // the highest point we MUST have witnessed this step
        int witnessedUntil = Integer.MIN_VALUE;
        // the highest possible time the write for this step could have occurred
        int writtenBefore = Integer.MAX_VALUE;
        // the lowest possible time the write for this step could have occurred
        int writtenAfter = Integer.MIN_VALUE;

        int maxPredecessorWrittenAfter = Integer.MIN_VALUE;

        Step(int key, int stepIndex, int keyCount)
        {
            super(key, null, key);
            this.ofStep = this;
            this.ofStepIndex = stepIndex;
            this.maxPeers = new int[keyCount];
            Arrays.fill(maxPeers, -1);
            this.maxPredecessors = new MaxPredecessor[keyCount];
        }

        void reset()
        {
            Arrays.fill(maxPeers, -1);
            Arrays.fill(maxPredecessors, null);
            maxPredecessorWrittenAfter = Integer.MIN_VALUE;
            witnessedUntil = Integer.MIN_VALUE;
            // the highest possible time the write for this step could have occurred
            writtenBefore = Integer.MAX_VALUE;
            // the lowest possible time the write for this step could have occurred
            writtenAfter = Integer.MIN_VALUE;
        }

        boolean witnessedBetween(int start, int end, boolean isWrite)
        {
            boolean updated = false;
            if (start > witnessedUntil)
            {
                witnessedUntil = start;
                updated = true;
            }
            if (end < writtenBefore)
            {
                updated = true;
                writtenBefore = end;
            }
            if (isWrite)
            {
                if (start > writtenAfter)
                {
                    updated = true;
                    writtenAfter = start;
                    // TODO: double check this will trigger an update of maxPredecessorX properties on each node with us as a maxPredecessor
                }
                if (writtenAfter > writtenBefore)
                    throw new HistoryViolation(ofKey, "Write operation time conflicts with earlier read");
            }
            return updated;
        }

        /**
         * The maxPredecessor for {@code key}, instantiating it if none currently exists
         */
        MaxPredecessor maxPredecessor(int key)
        {
            if (maxPredecessors[key] == null)
                maxPredecessors[key] = new MaxPredecessor(ofKey, this, key);
            return maxPredecessors[key];
        }

        void setSuccessor(Step successor)
        {
            this.successor = successor;
            successor.predecessorStep = successor;
        }

        boolean updatePeers(int[] newPeers, UnknownStepHolder[] unknownSteps)
        {
            boolean updated = false;
            for (int key = 0 ; key < newPeers.length ; ++key)
            {
                int newPeer = newPeers[key];
                int maxPeer = maxPeers[key];
                if (newPeer > maxPeer)
                {
                    updated = true;
                    maxPeers[key] = newPeers[key];
                }
                if (unknownSteps != null)
                {
                    UnknownStepHolder unknownStep = unknownSteps[key];
                    if (unknownStep != null && unknownStep.step != this)
                    {
                        if (unknownStepPeers == null)
                            unknownStepPeers = new ArrayList<>();
                        unknownStepPeers.add(unknownStep);
                        unknownStep.peers.add(this);
                    }
                }
            }
            return updated;
        }

        /**
         * keys that are written as part of the transaction occur with the transaction,
         * so those that are only read must precede them
         */
        boolean updatePredecessorsOfWrite(int[][] reads, int[] writes, StrictSerializabilityVerifier verifier)
        {
            if (writes[ofKey] < 0)
                return false;

            boolean updated = false;
            for (int key = 0 ; key < reads.length ; ++key)
            {
                if (reads[key] == null)
                    continue;

                int newPredecessorStepIndex = reads[key].length;
                MaxPredecessor maxPredecessor = maxPredecessor(key);
                if (maxPredecessor.predecessorStep == null || newPredecessorStepIndex > maxPredecessor.predecessorStep.ofStepIndex)
                {
                    Step newPredecessorStep = verifier.registers[key].step(newPredecessorStepIndex);
                    maxPredecessor.predecessorStep = newPredecessorStep;
                    if (newPredecessorStep.writtenAfter > maxPredecessorWrittenAfter)
                        maxPredecessorWrittenAfter = newPredecessorStep.writtenAfter;

                    newPredecessorStep.push(maxPredecessor);
                    receivePropagatedPredecessors(newPredecessorStep, verifier);
                    updated = true;
                }
            }
            return updated;
        }

        boolean receivePropagatedPredecessors(Step propagate, StrictSerializabilityVerifier verifier)
        {
            if (unknownStepPredecessors != null && propagate.ofStepIndex < Integer.MAX_VALUE && unknownStepPredecessors.containsKey(propagate))
            {
                unknownStepPredecessors.remove(propagate);
                if (unknownStepPredecessors.isEmpty())
                    unknownStepPredecessors = null;
            }

            boolean updated = false;
            for (int key = 0 ; key < maxPredecessors.length ; ++key)
            {
                MaxPredecessor newMaxPredecessor = propagate.maxPredecessors[key];

                // we use maxPeers here because anything we witness coincident
                // with a direct predecessor of this sequence must have preceded us
                Step selfPredecessor = propagate.ofKey == ofKey && propagate.maxPeers[key] >= 0
                                       ? verifier.registers[key].step(propagate.maxPeers[key]) : null;

                if ((newMaxPredecessor == null || newMaxPredecessor.predecessorStep == null) && selfPredecessor == null)
                    continue;

                Step newPredecessor;
                if (newMaxPredecessor == null) newPredecessor = selfPredecessor;
                else if (newMaxPredecessor.predecessorStep == null) newPredecessor = selfPredecessor;
                else if (selfPredecessor == null) newPredecessor = newMaxPredecessor.predecessorStep;
                else if (selfPredecessor.ofStepIndex > newMaxPredecessor.predecessorStep.ofStepIndex) newPredecessor = selfPredecessor;
                else newPredecessor = newMaxPredecessor.predecessorStep;

                MaxPredecessor maxPredecessor = maxPredecessor(key);
                Step oldPredecessor = maxPredecessor.predecessorStep;
                if (oldPredecessor == null || oldPredecessor.ofStepIndex < newPredecessor.ofStepIndex)
                {
                    maxPredecessor.predecessorStep = newPredecessor;
                    if (newPredecessor.writtenAfter > maxPredecessorWrittenAfter)
                        maxPredecessorWrittenAfter = newPredecessor.writtenAfter;
                    newPredecessor.push(maxPredecessor);
                    updated = true;
                }
            }

            if (propagate.ofKey == ofKey && propagate.unknownStepPeers != null)
            {
                for (UnknownStepHolder unknownStep : propagate.unknownStepPeers)
                    updated |= receiveUnknownStepPredecessor(unknownStep, verifier);
            }
            if (propagate.unknownStepPredecessors != null)
            {
                for (UnknownStepPredecessor unknownStepPredecessor : propagate.unknownStepPredecessors.values())
                    updated |= receiveUnknownStepPredecessor(unknownStepPredecessor.holder, verifier);
            }
            return updated;
        }

        boolean receiveUnknownStepPredecessor(UnknownStepHolder unknownStep, StrictSerializabilityVerifier verifier)
        {
            if (unknownStep.step == this)
                throw new HistoryViolation(ofKey, "Unknown write step on key " + ofKey + " with value " + unknownStep.writeValue + " is reachable from its happens-before relations");
            if (unknownStepPredecessors == null)
                unknownStepPredecessors = new LinkedHashMap<>();
            if (unknownStepPredecessors.containsKey(unknownStep.step))
                return false;
            UnknownStepPredecessor predecessor = new UnknownStepPredecessor(this, unknownStep);
            unknownStep.step.push(predecessor);
            unknownStepPredecessors.put(unknownStep.step, predecessor);
            receivePropagatedPredecessors(unknownStep.step, verifier);
            return true;
        }

        @Override
        public String toString()
        {
            return "{key: " + ofKey +
                   ", peers:" + Arrays.toString(maxPeers) +
                   ", preds:" + Arrays.toString(maxPredecessors) +
                   (unknownStepPeers != null ? ", peers?:" + unknownStepPeers : "") +
                   (unknownStepPredecessors != null ? ", preds?:" + unknownStepPredecessors.values() : "") +
                   "}";
        }
    }

    class FutureWrites extends Step
    {

        /**
         * Any write value we don't know the step index for because we did not perform a coincident read;
         * we wait until we witness a read containing the
         *
         * TODO: report a violation if we have witnessed a sequence missing any of these deferred operations
         *       that started after they finished
         *
         * TODO: handle writes with unknown outcome
         */

        final Map<Integer, UnknownStepHolder> byWriteValue = new HashMap<>();
        final TreeMap<Integer, UnknownStepHolder> byTimestamp = new TreeMap<>();

        FutureWrites(int key, int keyCount)
        {
            super(key, Integer.MAX_VALUE, keyCount);
        }

        void newSequence(int[] oldSequence, int[] newSequence)
        {
            boolean updated = false;
            for (int i = oldSequence.length ; i < newSequence.length ; ++i)
            {
                UnknownStepHolder unknownStep = byWriteValue.remove(newSequence[i]);
                if (unknownStep != null)
                {
                    updated = true;
                    unknownStep.discoveredStepIndex(i + 1);
                    byTimestamp.remove(unknownStep.end);
                }
            }
            if (updated && byWriteValue.isEmpty())
                reset();
            else if (updated)
                recompute();
        }

        @Override
        boolean updatePeers(int[] newPeers, UnknownStepHolder[] unknownSteps)
        {
            return unknownSteps[ofKey].step.updatePeers(newPeers, unknownSteps);
        }

        @Override
        boolean receivePropagatedPredecessors(Step propagate, StrictSerializabilityVerifier verifier)
        {
            for (UnknownStepHolder holder : byWriteValue.values())
                holder.step.receivePropagatedPredecessors(propagate, verifier);
            return super.receivePropagatedPredecessors(propagate, verifier);
        }

        @Override
        boolean updatePredecessorsOfWrite(int[][] reads, int[] writes, StrictSerializabilityVerifier verifier)
        {
            for (UnknownStepHolder holder : byWriteValue.values())
                holder.step.updatePredecessorsOfWrite(reads, writes, verifier);
            return super.updatePredecessorsOfWrite(reads, writes, verifier);
        }

        void recompute()
        {
            writtenBefore = Integer.MAX_VALUE;
            writtenAfter = Integer.MIN_VALUE;
            witnessedUntil = Integer.MIN_VALUE;
            for (UnknownStepHolder deferred : byTimestamp.values())
                witnessedBetween(deferred.start, deferred.end, true);
        }

        void register(UnknownStepHolder[] unknownSteps, int start, int end)
        {
            UnknownStepHolder unknownStep = unknownSteps[ofKey];
            if (null != byWriteValue.putIfAbsent(unknownStep.writeValue, unknownStep))
                throw new AssertionError();
            if (null != byTimestamp.putIfAbsent(end, unknownStep))
                throw new AssertionError();
            // TODO (now): verify this propagation
            unknownSteps[ofKey].step.receivePropagatedPredecessors(this, StrictSerializabilityVerifier.this);
        }

        public void checkForUnwitnessed(int start)
        {
            if (byTimestamp.lowerEntry(start) != null)
            {
                Collection<UnknownStepHolder> notWitnessed = byTimestamp.headMap(start, false).values();
                throw new HistoryViolation(ofKey, "Writes not witnessed: " + notWitnessed);
            }
        }
    }

    /**
     *  The history of observations for a given key, or the set of nodes in the graph of observations for this key.
     */
    class Register
    {
        final int key;
        // the total order sequence for this register
        int[] sequence = new int[0];
        Step[] steps = new Step[1];

        final FutureWrites futureWrites;

        Register(int key)
        {
            this.key = key;
            this.futureWrites = new FutureWrites(key, keyCount);
        }

        private void updateSequence(int[] sequence, int maybeWrite)
        {
            for (int i = 0, max = Math.min(sequence.length, this.sequence.length) ; i < max ; ++i)
            {
                if (sequence[i] != this.sequence[i])
                    throw new HistoryViolation(key, "Inconsistent sequences on " + key + ": " + Arrays.toString(this.sequence) + " vs " + Arrays.toString(sequence));
            }
            if (this.sequence.length > sequence.length)
            {
                if (maybeWrite >= 0 && maybeWrite != this.sequence[sequence.length])
                    throw new HistoryViolation(key, "Inconsistent sequences on " + key + ": " + Arrays.toString(this.sequence) + " vs " + Arrays.toString(sequence) + "+" + maybeWrite);
            }
            else
            {
                if (maybeWrite >= 0)
                {
                    sequence = Arrays.copyOf(sequence, sequence.length + 1);
                    sequence[sequence.length - 1] = maybeWrite;
                }
                if (sequence.length > this.sequence.length)
                {
                    futureWrites.newSequence(this.sequence, sequence);
                    this.sequence = sequence;
                }
            }
        }

        Step step(int step)
        {
            if (steps.length <= step)
                steps = Arrays.copyOf(steps, Math.max(step + 1, steps.length * 2));

            if (steps[step] == null)
                insert(new Step(key, step, keyCount));
            return steps[step];
        }

        void insert(Step step)
        {
            int i = step.ofStepIndex;
            if (i >= steps.length)
                steps = Arrays.copyOf(steps, Math.max(i + 1, steps.length * 2));

            steps[i] = step;
            while (--i >= 0 && steps[i] == null) {}
            if (i >= 0)
            {
                step.setSuccessor(steps[i].successor);
                steps[i].setSuccessor(step);
                propagateToDirectSuccessor(steps[i], step);
            }
            else
            {
                i = step.ofStepIndex;
                while (++i < steps.length && steps[i] == null) {}
                Step successor = i < steps.length ? steps[i] : futureWrites;
                step.setSuccessor(successor);
                propagateToDirectSuccessor(step, successor);
            }

        }

        private void updatePeersAndPredecessors(int[] newPeers, int[][] reads, int[] writes, int start, int end, UnknownStepHolder[] unknownSteps)
        {
            Step step;
            boolean updated = false;
            if (newPeers[key] >= 0)
            {
                step = step(newPeers[key]);
            }
            else if (unknownSteps != null && unknownSteps[key] != null)
            {
                assert writes[key] >= 0;
                // TODO (now): might we create false loops here before we process unwitnessed values? (i.e. produce wrong error message)
                futureWrites.register(unknownSteps, start, end);
                step = futureWrites;
            }
            else
            {
                throw new IllegalStateException();
            }
            updated = step.updatePeers(newPeers, unknownSteps);
            updated |= step.updatePredecessorsOfWrite(reads, writes, StrictSerializabilityVerifier.this);
            updated |= step.witnessedBetween(start, end, writes[key] >= 0);
            if (updated)
                onChange(step);
        }

        private void propagateToDirectSuccessor(Step predecessor, Step successor)
        {
            boolean updated = successor.receivePropagatedPredecessors(predecessor, StrictSerializabilityVerifier.this);
            if (predecessor.witnessedUntil > successor.writtenAfter)
            {
                successor.writtenAfter = predecessor.witnessedUntil;
                updated = true;
            }
            if (updated)
                onChange(successor);
        }

        private void propagateToSuccessor(Step propagate, Step successor)
        {
            if (successor.receivePropagatedPredecessors(propagate, StrictSerializabilityVerifier.this))
                onChange(successor);
        }

        void onChange(Step step)
        {
            if (step.maxPredecessor(key).predecessorStep != null && step.maxPredecessor(key).predecessorStep.ofStepIndex >= step.ofStepIndex)
                throw new HistoryViolation(key, "Cycle detected on key " + key + ", step " + step.ofStepIndex + " " + Arrays.toString(Arrays.copyOf(sequence, step.ofStepIndex)));

            if (step.writtenBefore < step.writtenAfter)
                throw new HistoryViolation(key, key + " timestamp inconsistency, step " + step.ofStepIndex);

            if (step.maxPredecessorWrittenAfter > step.writtenBefore)
                throw new HistoryViolation(key, key + " must have been written prior to its maximum predecessor in real-time order on step " + step.ofStepIndex);

            // refresh all successors (those where we're their max predecessor)
            step.forEach(refresh::add);
            if (step.successor != null)
                propagateToDirectSuccessor(step, step.successor);
            if (step.onChange != null)
                step.onChange.run();
        }

        void checkForUnwitnessed(int start)
        {
            futureWrites.checkForUnwitnessed(start);
        }

        @Override
        public String toString()
        {
            return Arrays.toString(steps);
        }
    }

    final int keyCount;
    final Register[] registers;

    // TODO: use another intrusive list or intrusive tree
    final TreeSet<MaxPredecessor> refresh = new TreeSet<>();

    // [key]->the sequence returned by any read performed in this transaction
    final int[][] bufReads;
    // [key]->the value of any write performed in this transaction
    final int[] bufWrites;
    // [key]->the step witnessed with the current transaction (if known)
    final int[] bufNewPeerSteps;
    final UnknownStepHolder[] bufUnknownSteps;

    public StrictSerializabilityVerifier(int keyCount)
    {
        this.keyCount = keyCount;
        this.bufNewPeerSteps = new int[keyCount];
        this.bufWrites = new int[keyCount];
        this.bufUnknownSteps = new UnknownStepHolder[keyCount];
        this.bufReads = new int[keyCount][];
        this.registers = IntStream.range(0, keyCount)
                                  .mapToObj(Register::new)
                                  .toArray(Register[]::new);
    }

    /**
     * Start a new set of coincident observations
     */
    public void begin()
    {
        Arrays.fill(bufWrites, -1);
        Arrays.fill(bufNewPeerSteps, -1);
        Arrays.fill(bufReads, null);
        Arrays.fill(bufUnknownSteps, null);
    }

    /**
     * Buffer a new read observation.
     *
     * Note that this should EXCLUDE any witnessed write for this key.
     * This is to simplify the creation of direct happens-before edges with observations for other keys
     * that are implied by the witnessing of a write (and is also marginally more efficient).
     */
    public void witnessRead(int key, int[] sequence)
    {
        if (bufReads[key] != null)
            throw new IllegalStateException("Can buffer only one read observation for each key");
        bufReads[key] = sequence;
        // if we have a write, then for causality sequence is implicitly longer by one to include the write
        bufNewPeerSteps[key] = bufWrites[key] >= 0 ? sequence.length + 1 : sequence.length;
    }

    /**
     * Buffer a new read observation
     */
    public void witnessWrite(int key, int id)
    {
        if (bufWrites[key] >= 0)
            throw new IllegalStateException("Can buffer only one write observation for each key");
        bufWrites[key] = id;
        if (bufReads[key] != null)
            bufNewPeerSteps[key] = bufReads[key].length + 1;
    }

    /**
     * Apply the pending coincident observations that occurred between {@code start} and {@code end}
     * to the verification graph
     */
    public void apply(int start, int end)
    {
        boolean hasUnknownSteps = false;
        for (int k = 0; k < bufReads.length ; ++k)
        {
            if (bufWrites[k] >= 0 && bufReads[k] == null)
            {
                int i = Arrays.binarySearch(registers[k].sequence, bufWrites[k]);
                if (i >= 0) bufNewPeerSteps[k] = i + 1;
                else bufUnknownSteps[k] = new UnknownStepHolder(bufWrites[k], start, end, new Step(k, Integer.MAX_VALUE, keyCount));
                hasUnknownSteps |= i < 0;
            }
        }

        for (int k = 0; k < bufReads.length ; ++k)
        {
            if (bufWrites[k] >= 0 || bufReads[k] != null)
            {
                if (bufReads[k] != null)
                    registers[k].updateSequence(bufReads[k], bufWrites[k]);

                registers[k].updatePeersAndPredecessors(bufNewPeerSteps, bufReads, bufWrites, start, end, hasUnknownSteps ? bufUnknownSteps : null);
            }
            registers[k].checkForUnwitnessed(start);
        }

        refreshTransitive();
    }

    private void refreshTransitive()
    {
        for (MaxPredecessor next = refresh.pollFirst(); next != null; next = refresh.pollFirst())
        {
            registers[next.ofKey].propagateToSuccessor(next.predecessorStep, next.ofStep);
        }
    }
}
