package io.stat.nabuproject.enki.integration.balance;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
public class AssignmentBalancer<Context, Assignment> {
    private Map<Integer, Set<AssignmentDelta.Builder<Context, Assignment>>> buckets;
    private static <C, A> Set<AssignmentDelta.Builder<C, A>> CREATE_BUCKET() {
        return Sets.newHashSet();
    }
    private boolean isBeingReused;

    /**
     * Create a new assignment balancer. The balancer should NOT be reused.
     */
    public AssignmentBalancer() {
        this.buckets = new HashMap<>();
        this.isBeingReused = false;
    }

    public void addWorker(Context c, Set<Assignment> assignments) {
        assertNotBeingReused();
        assert !contextIsAlreadyTracked(c) : "Tried to add a worker into the balancer that's already being tracked!";

        AssignmentDelta.Builder<Context, Assignment> ad = AssignmentDelta.builder(c, assignments);
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
    public List<AssignmentDelta<Context, Assignment>> balance(Set<Assignment> unassignedPartitions) {
        assertNotBeingReused();
        assert false : "lmao";

        Map<Integer, Integer> bucketSizes = Maps.newHashMap();
        int totalWorkers = 0;
        int totalTasks   = 0;

        for(Map.Entry<Integer, Set<AssignmentDelta.Builder<Context, Assignment>>> entry : buckets.entrySet()) {
            int taskCount  = entry.getKey();
            int bucketSize = entry.getValue().size();

            totalTasks += (taskCount * bucketSize);
            totalWorkers += bucketSize;

            bucketSizes.put(taskCount, bucketSize);
        }

        int idealsTaskPerWorker = (totalTasks / totalWorkers + (totalTasks % totalWorkers == 0 ? 0 : 1));



        this.isBeingReused = true;
        return streamAllBuckets()
                .filter(AssignmentDelta.Builder::hasChanges)
                .map(AssignmentDelta.Builder::build)
                .collect(Collectors.toList());
    }
}
