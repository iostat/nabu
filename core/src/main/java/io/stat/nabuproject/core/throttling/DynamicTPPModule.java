package io.stat.nabuproject.core.throttling;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import io.stat.nabuproject.core.DynamicComponent;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Guice module for the dynamic throttle policy provider.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class DynamicTPPModule extends AbstractModule {
    public final Class<? extends ThrottlePolicyProvider> initialTPP;
    private final AtomicReference<DynamicThrottlePolicyProvider> dttpRef;

    /**
     * Create a new DynamicTPPModule, which has a Provider that instantiates
     * a DynamicTPP with an instance of initialTPP that gets injected.
     * @param initialTPP the initial backing ThrottlePolicyProvider to inject.
     */
    public DynamicTPPModule(Class<? extends ThrottlePolicyProvider> initialTPP) {
        super();
        this.initialTPP = initialTPP;
        dttpRef = new AtomicReference<>(null);
    }

    @Override
    protected void configure() {
        bind(new TypeLiteral<DynamicComponent<ThrottlePolicyProvider>>() {}).to(DynamicThrottlePolicyProvider.class);
        bind(ThrottlePolicyProvider.class).to(DynamicThrottlePolicyProvider.class);
    }

    @Provides synchronized DynamicThrottlePolicyProvider getDTTPWithInitialTPP(Injector injector) {
        if(dttpRef.get() == null) {
            dttpRef.set(new DynamicThrottlePolicyProvider(injector.getInstance(initialTPP)));
        }
        return dttpRef.get();
    }
}
