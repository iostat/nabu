package io.stat.nabuproject.nabu.elasticsearch;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

/**
 * The Guice module for the implementation of the
 * NabuCommand &lt;-&gt; ES writer bridge.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class CommandESWriterModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(CommandWriterImpl.class).in(Singleton.class);
        bind(NabuCommandESWriter.class).to(CommandWriterImpl.class);
    }
}
