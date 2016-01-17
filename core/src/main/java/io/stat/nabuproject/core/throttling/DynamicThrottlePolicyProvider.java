package io.stat.nabuproject.core.throttling;

import com.google.inject.Inject;
import io.stat.nabuproject.core.DynamicComponent;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Allows you to change who the actual ThrottlePolicyProvider is at runtime.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
class DynamicThrottlePolicyProvider implements ThrottlePolicyProvider, DynamicComponent<ThrottlePolicyProvider> {
    private AtomicReference<ThrottlePolicyProvider> backingProvider;

    @Inject
    public DynamicThrottlePolicyProvider(ThrottlePolicyProvider initialSource) {
        this.backingProvider = new AtomicReference<>(initialSource);
    }
    /**
     * Replaces the existing backing provider with the specified one.
     */
    @Override
    public void replaceInstance(ThrottlePolicyProvider newTPP) {
        backingProvider.set(newTPP);
    }

    @Override
    public ThrottlePolicyProvider getCurrentInstance() {
        return backingProvider.get();
    }

    @Override
    public List<ThrottlePolicy> getThrottlePolicies() {
        return backingProvider.get().getThrottlePolicies();
    }

    @Override
    public boolean doesIndexHaveTP(String indexName) {
        return backingProvider.get().doesIndexHaveTP(indexName);
    }

    @Override
    public ThrottlePolicy getTPForIndex(String indexName) {
        return backingProvider.get().getTPForIndex(indexName);
    }
}
