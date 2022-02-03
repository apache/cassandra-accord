package accord.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * Ensure there are no cycles in the implied list of predecessors, i.e. that we have a serializable order.
 * That is, we maintain links to the maximum predecessor step for each key, at each step for each key.
 * In combination with a linearizability verifier for each register/partition, we verify strict-serializability.
 *
 * TODO: session serializability, i.e. use each client's sequence of events as additional happens-before relations
 * TODO: find and report a path when we encounter a violation
 */
public class SerializabilityVerifier
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

        final int ofKey;
        final int ofStepIndex;

        // TODO: we probably don't need this field, as it's implied by the node we point to, that we have when we enqueue refresh
        final int predecessorKey;
        int predecessorStepIndex;

        MaxPredecessor(int ofKey, int ofStepIndex, int predecessorKey)
        {
            this.ofKey = ofKey;
            this.ofStepIndex = ofStepIndex;
            this.predecessorKey = predecessorKey;
            this.predecessorStepIndex = -1;
        }

        @Override
        public int compareTo(MaxPredecessor that)
        {
            if (this.ofStepIndex != that.ofStepIndex) return Integer.compare(this.ofStepIndex, that.ofStepIndex);
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
            return Integer.toString(predecessorStepIndex);
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

        /**
         * The next instantiated sequence's observation.
         * This may not be stepIndex+1, if we have not witnessed stepIndex+1 directly.
         * i.e. if we have witnessed [0] and [0,1,2] then 0's successor will be 2.
         * If we later witness [0,1], 0's successor will be updated to 1, whose successor will be 2.
         */
        Step successor;

        Step(int key, int stepIndex, int keyCount)
        {
            super(key, stepIndex, key);
            this.maxPeers = new int[keyCount];
            Arrays.fill(maxPeers, -1);
            this.maxPredecessors = new MaxPredecessor[keyCount];
            this.predecessorStepIndex = stepIndex - 1;
        }

        /**
         * The maxPredecessor for {@code key}, instantiating it if none currently exists
         */
        MaxPredecessor maxPredecessor(int key)
        {
            if (maxPredecessors[key] == null)
                maxPredecessors[key] = new MaxPredecessor(ofKey, ofStepIndex, key);
            return maxPredecessors[key];
        }

        @Override
        public String toString()
        {
            return "{" + Arrays.toString(maxPeers) + ", " + Arrays.toString(maxPredecessors) + '}';
        }
    }

    /**
     *  The history of observations for a given key, or the set of nodes in the graph of observations for this key.
     *
     *  TODO: extend LinearizabilityVerifier
     */
    class Register
    {
        final int key;
        // the total order sequence for this register
        int[] sequence = new int[0];
        Step[] steps = new Step[1];

        /**
         * Any write value we don't know the step index for because we did not perform a coincident read;
         * we wait until we witness a read containing the
         */
        Map<Integer, List<Deferred>> deferred = new HashMap<>();

        Register(int key)
        {
            this.key = key;
        }

        private void updateSequence(int[] sequence, int maybeWrite)
        {
            for (int i = 0, max = Math.min(sequence.length, this.sequence.length) ; i < max ; ++i)
            {
                if (sequence[i] != this.sequence[i])
                    throw new HistoryViolation(key, "Inconsistent sequences: " + Arrays.toString(this.sequence) + " vs " + Arrays.toString(sequence));
            }
            if (this.sequence.length > sequence.length)
            {
                if (maybeWrite >= 0 && maybeWrite != this.sequence[sequence.length])
                    throw new HistoryViolation(key, "Inconsistent sequences: " + Arrays.toString(this.sequence) + " vs " + Arrays.toString(sequence) + "+" + maybeWrite);
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
                    // process any sequences deferred because we didnt know what step they occurred on
                    for (int i = this.sequence.length ; i < sequence.length ; ++i)
                    {
                        List<Deferred> deferreds = deferred.remove(i);
                        if (deferreds != null)
                        {
                            for (Deferred deferred : deferreds)
                            {
                                deferred.update(key, i + 1);
                                deferred.process(SerializabilityVerifier.this);
                            }
                        }
                    }
                    this.sequence = sequence;
                }
            }
        }

        Step step(int step)
        {
            if (steps.length <= step)
                steps = Arrays.copyOf(steps, Math.max(step + 1, steps.length * 2));

            if (steps[step] == null)
            {
                steps[step] = new Step(key, step, keyCount);
                int i = step;
                while (--i >= 0 && steps[i] == null) {}
                if (i >= 0)
                {
                    steps[step].successor = steps[i].successor;
                    steps[i].successor = steps[step];
                    updatePredecessors(steps[step], steps[i], true);
                }
                else
                {
                    i = step;
                    while (++i < steps.length && steps[i] == null) {}
                    if (i < steps.length)
                    {
                        steps[step].successor = steps[i];
                        updatePredecessors(steps[i], steps[step], true);
                    }
                }
            }
            return steps[step];
        }

        private void updatePeersAndPredecessors(int[] newPeers, int[][] reads, int[] writes)
        {
            int stepIndex = newPeers[key];
            Step step = step(stepIndex);
            boolean updated = updatePeers(step, newPeers);
            updated |= updatePredecessorsOfWrite(step, reads, writes);
            if (updated)
                onChange(step);
        }

        private boolean updatePeers(Step step, int[] newPeers)
        {
            boolean updated = false;
            for (int key = 0 ; key < keyCount ; ++key)
            {
                int newPeer = newPeers[key];
                int maxPeer = step.maxPeers[key];
                if (newPeer > maxPeer)
                {
                    updated = true;
                    step.maxPeers[key] = newPeers[key];
                }
            }
            return updated;
        }

        private void updatePredecessors(Step updateStep, Step fromStep, boolean includeSelf)
        {
            boolean updated = false;
            for (int key = 0 ; key < keyCount ; ++key)
            {
                MaxPredecessor newPredecessor = fromStep.maxPredecessors[key];
                int selfPredecessorStepIndex = includeSelf ? fromStep.maxPeers[key] : -1;
                int newPredecessorStepIndex = newPredecessor == null ? selfPredecessorStepIndex
                                                                : Math.max(selfPredecessorStepIndex, newPredecessor.predecessorStepIndex);
                MaxPredecessor maxPredecessor;
                if (newPredecessorStepIndex >= 0 && newPredecessorStepIndex > (maxPredecessor = updateStep.maxPredecessor(key)).predecessorStepIndex)
                {
                    maxPredecessor.predecessorStepIndex = newPredecessorStepIndex;
                    registers[key].step(newPredecessorStepIndex).push(maxPredecessor);
                    updated = true;
                }
            }
            if (updated)
                onChange(updateStep);
        }

        /**
         * keys that are written as part of the transaction occur with the transaction,
         * so those that are only read must precede them
         */
        private boolean updatePredecessorsOfWrite(Step step, int[][] reads, int[] writes)
        {
            if (writes[key] < 0)
                return false;

            boolean updated = false;
            for (int key = 0 ; key < writes.length ; ++key)
            {
                if (reads[key] == null)
                    continue;

                int newPredecessorStepIndex = reads[key].length;
                MaxPredecessor maxPredecessor;
                if (newPredecessorStepIndex > (maxPredecessor = step.maxPredecessor(key)).predecessorStepIndex)
                {
                    maxPredecessor.predecessorStepIndex = newPredecessorStepIndex;
                    Step fromStep = registers[key].step(newPredecessorStepIndex);
                    fromStep.push(maxPredecessor);
                    updatePredecessors(step, fromStep, false);
                    updated = true;
                }
            }
            return updated;
        }

        void onChange(Step step)
        {
            if (step.maxPredecessor(key).predecessorStepIndex >= step.ofStepIndex)
                throw new HistoryViolation(key, "Cycle detected on key " + key + ", step " + step.ofStepIndex + " " + Arrays.toString(Arrays.copyOf(sequence, step.ofStepIndex)));

            step.forEach(refresh::add);
            if (step.successor != null)
                updatePredecessors(step.successor, step, true);
        }

        void registerDeferred(int unknownStepWriteValue, Deferred deferred)
        {
            this.deferred.computeIfAbsent(unknownStepWriteValue, ignore -> new ArrayList<>())
                         .add(deferred);
        }

        @Override
        public String toString()
        {
            return Arrays.toString(steps);
        }
    }

    // writes without a corresponding read don't know their position in the total order for the register
    // once this is known we can process the implied predecessor graph for them
    private static class Deferred
    {
        final int[] newPeers;
        final int[][] reads;
        final int[] writes;

        Deferred(int[] newPeers, int[][] reads, int[] writes)
        {
            this.newPeers = newPeers;
            this.reads = reads;
            this.writes = writes;
        }

        void update(int key, int step)
        {
            newPeers[key] = step;
        }

        void process(SerializabilityVerifier verifier)
        {
            for (int k = 0; k < newPeers.length ; ++k)
            {
                if (newPeers[k] >= 0)
                    verifier.registers[k].updatePeersAndPredecessors(newPeers, reads, writes);
            }
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

    public SerializabilityVerifier(int keyCount)
    {
        this.keyCount = keyCount;
        this.bufNewPeerSteps = new int[keyCount];
        this.bufWrites = new int[keyCount];
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
            bufNewPeerSteps[key] = bufReads[key].length;
    }

    /**
     * Apply the pending coincident observations to the verification graph
     */
    public void apply()
    {
        for (int k = 0; k < bufReads.length ; ++k)
        {
            if (bufWrites[k] >= 0 && bufReads[k] == null)
            {
                int i = Arrays.binarySearch(registers[k].sequence, bufWrites[k]);
                if (i >= 0)
                    bufNewPeerSteps[k] = i + 1;
            }
        }

        Deferred deferred = null;
        for (int k = 0; k < bufReads.length ; ++k)
        {
            if (bufReads[k] != null)
                registers[k].updateSequence(bufReads[k], bufWrites[k]);

            if (bufNewPeerSteps[k] >= 0)
            {
                registers[k].updatePeersAndPredecessors(bufNewPeerSteps, bufReads, bufWrites);
            }
            else if (bufWrites[k] >= 0)
            {
                if (deferred == null)
                    deferred = new SerializabilityVerifier.Deferred(bufNewPeerSteps.clone(), bufReads.clone(), bufWrites.clone());
                registers[k].registerDeferred(bufWrites[k], deferred);
            }
        }

        refreshTransitive();
    }

    private void refreshTransitive()
    {
        for (MaxPredecessor next = refresh.pollFirst(); next != null; next = refresh.pollFirst())
        {
            registers[next.ofKey].updatePredecessors(registers[next.ofKey].step(next.ofStepIndex),
                                                     registers[next.predecessorKey].step(next.predecessorStepIndex), false);
        }
    }
}
