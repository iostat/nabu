package io.stat.nabu.config;

import io.stat.nabu.core.Component;

/**
 * Created by io on 12/26/15. io is an asshole for
 * not giving writing documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public abstract class ConfigurationProvider extends Component {
    public abstract String getEnv();

    public abstract String getListenAddress();

    public abstract int getListenPort();

    public abstract int getAcceptorThreads();

    public abstract int getWorkerThreads();

    public abstract String getESHome();

    public abstract boolean isESHTTPEnabled();

    public abstract int getESHTTPPort();

    public abstract String[] getKafkaBrokers();
}
