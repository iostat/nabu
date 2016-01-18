package io.stat.nabuproject.core.throttling;

import com.google.inject.Inject;
import io.stat.nabuproject.core.DynamicComponent;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Allows you to change who the actual ThrottlePolicyProvider is at runtime.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
class DynamicThrottlePolicyProvider implements ThrottlePolicyProvider, DynamicComponent<ThrottlePolicyProvider> {
    private final ThrottlePolicyProvider defaultProvider;
    private AtomicReference<ThrottlePolicyProvider> backingProvider;

    @Inject
    public DynamicThrottlePolicyProvider(ThrottlePolicyProvider initialSource) {
        this.backingProvider = new AtomicReference<>(initialSource);
        this.defaultProvider = initialSource;
        logger.info("Created a DynamicTPP with default source of class {}", initialSource.getClass());
    }
    /**
     * Replaces the existing backing provider with the specified one.
     */
    @Override
    public void replaceInstance(ThrottlePolicyProvider newTPP) {
        logger.info("Replacing ThrottlePolicyProvider with an instance of {}", newTPP.getClass());
        newTPP.seedTPCLs(backingProvider.get().getAllTPCLs());
        backingProvider.set(newTPP);
    }

    @Override
    public void setToDefaultInstance() {
        backingProvider.set(defaultProvider);
    }

    @Override
    public ThrottlePolicyProvider getCurrentInstance() {
        return backingProvider.get();
    }

    @Override
    public Class<? extends ThrottlePolicyProvider> getClassOfDefaultInstance() {
        return defaultProvider.getClass();
    }

    @Override
    public ThrottlePolicyProvider getDefaultInstance() {
        return defaultProvider;
    }

    @Override
    public List<ThrottlePolicy> getThrottlePolicies() {
        return backingProvider.get().getThrottlePolicies();
    }

    @Override
    public List<AtomicReference<ThrottlePolicy>> getTPReferences() {
        return backingProvider.get().getTPReferences();
    }

    @Override
    public boolean doesIndexHaveTP(String indexName) {
        return backingProvider.get().doesIndexHaveTP(indexName);
    }

    @Override
    public AtomicReference<ThrottlePolicy> getTPForIndex(String indexName) {
        return backingProvider.get().getTPForIndex(indexName);
    }

    @Override
    public void registerThrottlePolicyChangeListener(ThrottlePolicyChangeListener tpcl) {
        backingProvider.get().registerThrottlePolicyChangeListener(tpcl);
    }

    @Override
    public void deregisterThrottlePolicyChangeListener(ThrottlePolicyChangeListener tpcl) {
        backingProvider.get().deregisterThrottlePolicyChangeListener(tpcl);
    }
}
