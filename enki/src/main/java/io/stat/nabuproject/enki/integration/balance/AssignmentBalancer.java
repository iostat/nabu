package io.stat.nabuproject.enki.integration.balance;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Calculates optimal task distribution without unassigning all tasks
 * and round-robining the set of all tasks across the set of all works.
 *
 * Probably not thread-safe. Should never be reused (mostly because connection states
 * could even change while balancing, and you'd be forced to rebalance anyway after an attemp
 * to execute some AssignmentDelta fails).
 *
 * Also needs to be optimized. It's probably too safe atm.
 * Also probably needs to start using HPPC or Koloboke or something.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public class AssignmentBalancer<Context extends AssignmentContext, Assignment> {
    private Map<Integer, Set<AssignmentDelta.Builder<Context, Assignment>>> buckets;
    private static <C, A> Set<AssignmentDelta.Builder<C, A>> CREATE_BUCKET() {
        return Sets.newConcurrentHashSet();
    }
    private boolean isBeingReused;
    private final Random random;

    public AssignmentBalancer() {
        this(new Random(System.nanoTime()));
    }

    /**
     * Create a new assignment balancer. The balancer should NOT be reused.
     * @param random the RNG to use for shuffling and such.
     */
    public AssignmentBalancer(Random random) {
        this.buckets = new HashMap<>();
        this.isBeingReused = false;
        this.random = random;
    }

    public void addWorker(Context c, Set<Assignment> assignments) {
        assertNotBeingReused();
        assert !contextIsAlreadyTracked(c) : "Tried to add a worker into the balancer that's already being tracked!";

        AssignmentDelta.Builder<Context, Assignment> ad = AssignmentDelta.builder(c, assignments, random);
        int bucket = ad.getWeightWithChanges();

        getOrCreateBucket(bucket).add(ad);
    }

    /**
     * Ensures that this AssignmentBalancer hasn't already balanced once.
     */
    private void assertNotBeingReused() {
        assert !isBeingReused : "Tried to reuse an AssignmentBalancer!";
    }

    /**
     * Asserts that the bucket you are trying to get exists, and then returns it.
     * This should ALWAYS be used when you are reading from a specific bucket.
     * @param number the number of the bucket
     * @return the bucket for that number, or a spectacular AssertError if the bucket doesn't exist
     */
    private Set<AssignmentDelta.Builder<Context, Assignment>> getBucket(int number) {
        assertNotBeingReused();
        assert buckets.containsKey(number) : "Tried to read from a bucket that doesn't exist!";
        return buckets.get(number);
    }

    /**
     * Gets or creates a bucket with the specified number. This should be used
     * ALWAYS when you are inserting into a bucket.
     * @param number the number of the bucket
     * @return the newly created or existing bucket for <tt>number</tt>
     */
    private Set<AssignmentDelta.Builder<Context, Assignment>> getOrCreateBucket(int number) {
        assertNotBeingReused();
        if(buckets.containsKey(number)) {
            return buckets.get(number);
        } else {
            Set<AssignmentDelta.Builder<Context, Assignment>> newSet = CREATE_BUCKET();
            buckets.put(number, newSet);
            return newSet;
        }
    }

    /**
     * Checks whether or not the specified Context is already being tracked by this balancer.
     * Used internally before assertions to ensure there are no duplicates.
     * @param c the context to check
     * @return whether or not the context is already tracked.
     */
    private boolean contextIsAlreadyTracked(Context c) {
        assertNotBeingReused();
        if(anyMatch(c::equals)) {
            logger.warn("contextIsAlreadyTracked returned true. This should NOT happen.");
            return true;
        }
        return false;
    }

    /**
     * Use this when you perform any operation on a AssignmentDelta. It will remove the
     * delta from its old bucket (and make sure it WAS in the bucket it was supposed to be in),
     * run your operation, and then place it into the new bucket.
     *
     * The reason we can't do the update and replace afterwards is that the equality might change, and depending on
     * the implementation of Set that's used, means we wont be able to get the old AssignmentDelta out.
     *
     * @param ad the AssignmentDelta to operate on
     * @param op the operation to perform on the AssignmentDelta, or a spectacular AssertError if cd was not where it was supposed to be
     */
    private void operateOnAD(AssignmentDelta.Builder<Context, Assignment> ad, Consumer<AssignmentDelta.Builder<Context, Assignment>> op) {
        assertNotBeingReused();
        int oldBucket = ad.getWeightWithChanges();
        boolean wasInBucket = getBucket(oldBucket).remove(ad);

        assert wasInBucket : "Tried to operate on a AssignmentDelta that was not in the appropriate bucket!";

        op.accept(ad);

        int newBucket = ad.getWeightWithChanges();
        getOrCreateBucket(newBucket).add(ad);

        if(getBucket(oldBucket).size() == 0) {
            buckets.remove(oldBucket);
        }
    }

    private Stream<AssignmentDelta.Builder<Context, Assignment>> streamAllBuckets() {
        Stream<AssignmentDelta.Builder<Context, Assignment>> ret = Stream.empty();
        for(Set<AssignmentDelta.Builder<Context, Assignment>> bucket : buckets.values()) {
            ret = Stream.concat(ret, bucket.stream());
        }

        return ret;
    }

    private boolean anyMatch(Predicate<AssignmentDelta.Builder<Context, Assignment>> p) {
        for(Integer i : buckets.keySet()) {
            Set<AssignmentDelta.Builder<Context, Assignment>> members = buckets.get(i);
            for(AssignmentDelta.Builder<Context, Assignment> ad : members) {
                if(p.test(ad)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Perform the rebalance operation. After this, this balancer CANNOT be reused.
     * @return A list of AssignmentDelta tasks that need to be executed.
     */
    public List<AssignmentDelta<Context, Assignment>> balance(Set<Assignment> unassignedTasks) {
        assertNotBeingReused();
        int originalUnassignedTasksSize = unassignedTasks.size();

        ImmutableMap<Integer, Set<AssignmentDelta.Builder<Context, Assignment>>> copyOfBuckets = ImmutableMap.copyOf(buckets);

        Map<Integer, Integer> bucketSizes = Maps.newHashMap();
        int totalWorkers = 0;
        int totalAssignedAtStart   = 0;

        // step 1) reassign tasks as needed so that pre-existing workload is relatively even.
        //        1a) figure out total number of assigned tasks and workers
        for(Map.Entry<Integer, Set<AssignmentDelta.Builder<Context, Assignment>>> entry : copyOfBuckets.entrySet()) {
            int taskCount  = entry.getKey();
            int bucketSize = entry.getValue().size();

            totalAssignedAtStart += (taskCount * bucketSize);
            totalWorkers += bucketSize;

            bucketSizes.put(taskCount, bucketSize);
        }

        // nothing fancy, just a ceil division without floating casts)
        int idealTasksPerWorker = (totalAssignedAtStart / totalWorkers + (totalAssignedAtStart % totalWorkers == 0 ? 0 : 1));

        //        1b) remove tasks from workers who have more than the average pre-existing-workload amount.
        List<Assignment> pulledFromAboveIdeal = Lists.newLinkedList();
        for(Map.Entry<Integer, Set<AssignmentDelta.Builder<Context, Assignment>>> entry : copyOfBuckets.entrySet()) {
            if(entry.getKey() > idealTasksPerWorker) {
                int toRemove = entry.getKey() - idealTasksPerWorker;
                for(AssignmentDelta.Builder<Context, Assignment> adb : entry.getValue()) {
                    operateOnAD(adb, ad -> {
                        for(int i = 0; i < toRemove; i++) {
                            Assignment removed = ad.unassignBestFit();
                            pulledFromAboveIdeal.add(removed);
                        }
                    });
                }
            }
        }

        //        1c) distribute the work pulled from above-average workers to below average ones
        // figure out how much each less-loaded worker needs to have
        // so they're all even.
        AtomicInteger workersBelowIdealAI = new AtomicInteger(0);
        AtomicInteger tasksInWorkersLTIAI = new AtomicInteger(0);
        ImmutableSet.Builder<AssignmentDelta.Builder<Context, Assignment>> keptLessLoadedBucketsBuilder = ImmutableSet.builder();
        copyOfBuckets.entrySet()
                     .stream()
                     .filter(e -> e.getKey() < idealTasksPerWorker)
                     .forEach((kv) -> {
                         Set<AssignmentDelta.Builder<Context, Assignment>> keptBucket = kv.getValue();
                         int numWorkers = keptBucket.size();
                         int numTasks   = kv.getKey();
                         workersBelowIdealAI.addAndGet(numWorkers);
                         tasksInWorkersLTIAI.addAndGet(numWorkers * numTasks);
                         keptLessLoadedBucketsBuilder.addAll(keptBucket);
                     });

        // sum total outstanding tasks to be given to the below-average workers
        int totalLTIWGoalTasks = tasksInWorkersLTIAI.get() + pulledFromAboveIdeal.size();
        // get how many workers are below average in total
        int workersBelowIdeal  = workersBelowIdealAI.get();

        // either there's at least one worker below the ideal, or there are
        // were no tasks plucked from the "above average" bucket.
        assert (totalLTIWGoalTasks == 0 && workersBelowIdeal == 0) || (workersBelowIdeal >= 1) : "Math has lost all meaning.";

        // sometimes you'll have an optimal distribution, in this case you can be sure
        if(workersBelowIdeal != 0) {
            int targetTasksForLessLoadedWorkers = (totalLTIWGoalTasks / workersBelowIdeal + (totalLTIWGoalTasks % workersBelowIdeal == 0 ? 0 : 1));
            Set<AssignmentDelta.Builder<Context, Assignment>> keptLessLoadedBuckets = keptLessLoadedBucketsBuilder.build();
            for(AssignmentDelta.Builder<Context, Assignment> worker : keptLessLoadedBuckets) {
                int tasksToAssign = targetTasksForLessLoadedWorkers - worker.getWeightWithChanges();
                operateOnAD(worker, ad -> {
                    for(int i = 0; i < tasksToAssign && pulledFromAboveIdeal.size() != 0; i++) {
                        Assignment randomAssgn = pulledFromAboveIdeal.remove(random.nextInt(pulledFromAboveIdeal.size()));
                        ad.assign(randomAssgn);
                    }
                });
            }
        }


        // this should never happen at this point
        assert pulledFromAboveIdeal.size() == 0 : "tasks pulled from above average workers, but there are no workers below average!";

        // from this point on, we can work in a round-robin manner, wherein we just take the remaining tasks, and put them into a node
        // with the smallest amount of tasks assigned to it, but not backed by an immutable map, so we always have the closest packing
        // the only remaining tasks unassigned are tasks that were unassigned at the start of the rebalance.
        List<Assignment> unassignedAsList = Lists.newArrayList(unassignedTasks);
        while(unassignedAsList.size() != 0) {
            Assignment nextUnassigned = unassignedAsList.remove(random.nextInt(unassignedAsList.size()));

            Set<AssignmentDelta.Builder<Context, Assignment>> smallestBucket = buckets.get(Collections.min(buckets.keySet()));
            AssignmentDelta.Builder<Context, Assignment> lowestWorker = smallestBucket.stream()
                                                                                      .skip(random.nextInt(smallestBucket.size()))
                                                                                      .findFirst()
                                                                                      .get();
            assert lowestWorker != null : "Somehow got a null worker from a sub-bucket?";

            operateOnAD(lowestWorker, worker -> {
                worker.assign(nextUnassigned);
            });
        }

        StringBuilder sb = new StringBuilder("\nSTARTED WITH ").append(originalUnassignedTasksSize).append(" UNASSIGNED TASKS\n");
        sb.append(totalAssignedAtStart).append(" ASSIGNED AT START AND ").append(totalWorkers).append(" TOTAL WORKERS\n");
        sb.append("LOAD DISTRIBUTION\n");
        sb.append(Strings.padStart("WORKER NAME", 60, ' '));
        sb.append(Strings.padStart("TASK COUNT", 20, ' '));
        sb.append(Strings.padStart("DELTA +", 10, ' '));
        sb.append(Strings.padStart("DELTA -", 10, ' '));
        sb.append("\n");

        buckets.keySet().stream().sorted().forEach(key -> {
            int taskCount = key;
            Set<AssignmentDelta.Builder<Context, Assignment>> workersInBucket = buckets.get(key);

            workersInBucket.forEach(builder -> {
                sb.append(Strings.padStart(builder.getContext().getDescription(), 60, ' '))
                        .append(Strings.padStart(Integer.toString(taskCount), 20, ' '))
                        .append(Strings.padStart(Integer.toString(builder.getStartCount()), 10, ' '))
                        .append(Strings.padStart(Integer.toString(builder.getStopCount()), 10, ' '))
                        .append("\n");
            });
        });

        logger.info(sb.toString());


        List<AssignmentDelta<Context, Assignment>> ret = streamAllBuckets()
                                                            .filter(AssignmentDelta.Builder::hasChanges)
                                                            .map(AssignmentDelta.Builder::build)
                                                            .collect(Collectors.toList());

        this.isBeingReused = true;
        return ret;
    }
}
