package io.stat.nabuproject.core;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;
import com.google.inject.spi.InjectionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * A Guice module that we use to hook into Guice's injections,
 * and build a component dependency DAG for determine startup and shutdown
 * order for Components
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public class StarterModule extends AbstractModule {
    @Override
    protected void configure() {
        bindListener(Matchers.any(), new ComponentTypeListener());
    }

    public static class ComponentTypeListener implements TypeListener {
        @Override
        public <I> void hear(TypeLiteral<I> type, TypeEncounter<I> encounter) {
            Class<? super I> heard = type.getRawType();
            if(Component.class.isAssignableFrom(heard)) {
                encounter.register(new ComponentInjectionListener(heard));
                logger.info("heard {}", heard);
            }
        }

        @RequiredArgsConstructor
        public class ComponentInjectionListener implements InjectionListener {
            private final Class<?> injectedInto;
            @Override
            public void afterInjection(Object injectee) {
                logger.info("afterInjection {} => {}", injectedInto, injectee);
            }
        }
    }
}
