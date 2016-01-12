package io.stat.nabuproject.enki.integration.balance;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import javax.lang.model.type.NullType;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

/**
 * Describes a combination of assignments and unassignments of AssignmentType,
 * which Context can consume.
 *
 * @param <Assignment> the kind of Assignments that the Context consumes
 *                     they should ideally have a solid implementation of
 *                     equals(Object) and hashCode()
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@RequiredArgsConstructor(access= AccessLevel.PRIVATE)
public class AssignmentDelta<Context extends AssignmentContext<Assignment>, Assignment> {
    private final @Getter Context context;
    private final @Getter ImmutableSet<Assignment> toStart;
    private final @Getter ImmutableSet<Assignment> toStop;
    private final @Getter int weight;

    public static <Context extends AssignmentContext<Assignment>, Assignment> Builder<Context, Assignment> builder(Context c, Set<Assignment> existingTasks, Random random) {
        return new Builder<>(c, ImmutableSet.copyOf(existingTasks), random);
    }

    /**
     * like with {@link AssignmentDelta#equals(Object)}, the hash code
     * should simply be affected by the class of the context.
     */
    @Override
    public int hashCode() {
        Class<?> ctxClass = (context != null) ? context.getClass() : NullType.class;
        return Objects.hash(context, ctxClass, AssignmentDelta.class);
    }

    /**
     * Equality of a Delta is determined based on the context, since we just
     * want to restrict Sets of deltas to having only one delta per
     * context affected.
     * @param obj the object to compare equality for
     * @return whether or not this delta affects the same context as obj, if obj
     * is an AssignmentDelta, otherwise false.
     */
    @Override
    public boolean equals(Object obj) {
        return this == obj
                ||    (obj != null
                    && obj instanceof AssignmentDelta
                    && this.context.equals(((AssignmentDelta) obj).getContext()));
    }

    /**
     * How many start operations this delta involves
     * @return toStart.size()
     */
    public int getStartCount() {
        return toStart.size();
    }

    /**
     * How many stop operations this delta involves
     * @return toStop.size()
     */
    public int getStopCount() {
        return toStop.size();
    }

    static class Builder<Context extends AssignmentContext<Assignment>, Assignment> {
        private final @Getter Context context;
        private final @Getter int startWeight;
        private final ImmutableSet<Assignment> startedWith;
        private final Set<Assignment> toStart;
        private final Set<Assignment> toStop;
        private @Getter int weightWithChanges;
        private final @Getter Random random;

        private Builder(Context context, ImmutableSet<Assignment> startedWith, Random random) {
            this.context = context;
            this.startedWith = startedWith;
            this.toStart = Sets.newHashSet();
            this.toStop  = Sets.newHashSet();
            this.random  = random;

            this.startWeight = this.startedWith.size();
            this.weightWithChanges = this.startWeight;
        }

        /**
         * Schedules an assignment to be started when the resultant AssignmentDelta is executed.<br>
         * You can call this method if and only if:
         * <ol>
         *     <li>the assignment is not already queued for starting, AND</li>
         *     <li>the worker did not already have this assignment when the balance started OR it
         *     was scheduled to stop prior to <tt>assign</tt> being called.</li>
         * </ol>
         *
         * If the worker was scheduled to stop the <tt>assignment</tt> on this rebalance by calling <tt>unassign</tt> earlier,
         * this call will effectively negate that (i.e., remove the assignment from the stop queue).
         *
         * @param assignment the assignment to start processing when the delta is executed.
         */
        public void assign(Assignment assignment) {
            boolean startedWithAssignment = startedWith.contains(assignment);
            boolean wasQueuedForStart     = toStart.contains(assignment);
            boolean wasQueuedForStop      = toStop.contains(assignment);

            // make sure something wonky isn't happening, like assigning
            // a task that the context was already doing at the start of
            // the balance, or reassigning it the same task without unassigning it first.
            //
            // Only two scenarios can happen
            // 1) The context started the balance already having the assignment, and was told to stop
            //    as part of the rebalance, and the balancer is undoing that decision
            // 2) It never had the assignment in the first place.
            assert !wasQueuedForStart : "Tried to queue an assignment for starting twice in a row!";

            assert ((startedWithAssignment && wasQueuedForStop)
                            || !startedWithAssignment) : "Tried to queue an assignment for starting that is already running!";

            if(wasQueuedForStop) {
                toStop.remove(assignment);
            }

            if(!startedWithAssignment) {
                toStart.add(assignment);
            }

            weightWithChanges += 1;
        }

        /**
         * Schedules an assignment to be stopped when the resultant AssignmentDelta is executed.<br>
         * You can call this method if and only if:
         * <ol>
         *     <li>the assignment is not already queued for stopping, AND</li>
         *     <li>the worker already had this assignment when the balance started OR it was scheduled to start
         *         prior to <tt>unassign</tt> being called. If the job was scheduled to start</li>
         * </ol>
         *
         * If the worker was scheduled to start the <tt>assignment</tt> on this rebalance by calling <tt>assign</tt> earlier,
         * this call will effectively negate that (i.e., remove the assignment from the start queue).
         * @param assignment the assignment to stop processing when the delta.
         */
        public void unassign(Assignment assignment) {
            boolean startedWithAssignment = startedWith.contains(assignment);
            boolean wasQueuedForStart     = toStart.contains(assignment);
            boolean wasQueuedForStop      = toStop.contains(assignment);

            // ensure nothing weird happens
            // like unassigning a task that was never assigned.
            //
            // A task can only be unassigned if
            // 1) The context had the assignment when the balance started
            // 2) The context did not have the assignment, but the balancer assigned it
            //    prior to this unassign call and as is now changing its mind
            assert !wasQueuedForStop : "Tried to queue an assignment for stopping twice in a row!";
            assert (wasQueuedForStart || startedWithAssignment) : "Tried to queue an assignment for stopping that is not running!";

            if(wasQueuedForStart) {
                toStart.remove(assignment);
            }

            if(startedWithAssignment) {
                toStop.add(assignment);
            }

            weightWithChanges -= 1;
        }

        /**
         * Finds the {@literal "best"} task to unassign from this worker.
         * todo: an actual heuristic
         * @return the Assignment that was unassigned.
         */
        public Assignment unassignBestFit() {
            assert toStart.size() + startedWith.size() > 0 : "Tried to unassignBestFit from a worker with no assigned tasks!";
            Assignment candidate;
            if(!toStart.isEmpty()) {
                candidate = toStart.stream().skip(random.nextInt(toStart.size())).findFirst().get();
                assert candidate != null : "Pulling a random candidate from a non-empty start list failed for some inexplicable reason";
            } else {
                candidate = startedWith.stream().skip(random.nextInt(startedWith.size())).findFirst().get();
            }

            unassign(candidate);
            return candidate;
        }

        /**
         * @return the sum of start and stop operations in this delta.
         */
        public int getChangeCount() {
            int totalChanges = (toStart.size() + toStop.size());
            assert totalChanges >= 0 : "The sum of changes of this AssignmentDelta is a negative number, which is impossible.";
            return totalChanges;
        }

        /**
         * @return whether or not this delta has any starts or stops associated with it.
         */
        public boolean hasChanges() {
            return getChangeCount() > 0;
        }

        /**
         * Get the immutable AssignmentDelta that describes all the operations this builder performed.
         * @return an AssignmentDelta&lt;Assignment&gt;
         */
        public AssignmentDelta<Context, Assignment> build() {
            assert getWeightWithChanges() >= 0 : "Building this AssignmentDelta will result in an AD with negative changes, which is impossible.";
            return new AssignmentDelta<>(context, ImmutableSet.copyOf(toStart), ImmutableSet.copyOf(toStop), weightWithChanges);
        }

        /**
         * Equality of a Delta is determined based on the context, since we just
         * want to restrict Sets of deltas to having only one delta per
         * context affected.
         * @param obj the object to compare equality for
         * @return whether or not this delta affects the same context as obj, if obj
         * is an AssignmentDelta, otherwise false.
         */
        @Override
        public boolean equals(Object obj) {
            return this == obj
                    ||    (obj != null
                    && obj instanceof AssignmentDelta.Builder
                    && this.context.equals(((AssignmentDelta.Builder) obj).getContext()));
        }

        /**
         * like with {@link AssignmentDelta.Builder#equals(Object)}, the hash code
         * should simply be affected by the class of the context.
         */
        @Override
        public int hashCode() {
            Class<?> ctxClass = (context != null) ? context.getClass() : NullType.class;
            return Objects.hash(context, ctxClass, AssignmentDelta.Builder.class);
        }

        /**
         * How many starts will be needed to execute the delta this
         * builder will build assuming no other changes are applied
         * @return how many pending starts in the builder
         */
        public int getStartCount() {
            return toStart.size();
        }

        /**
         * How many stops will be needed to execute the delta this
         * builder will build assuming no other changes are applied
         * @return how many pending stops in the builder
         */
        public int getStopCount() {
            return toStop.size();
        }

        /**
         * Collates all assignments represented by this Delta builder
         * into a readable stirng
         * @return
         */
        public String collateAssignmentsReadably() {
            Set<Assignment> collated = Sets.newHashSet(startedWith);
            collated.addAll(toStart);
            collated.removeAll(toStop);
            return context.collateAssignmentsReadably(collated);
        }
    }
}
