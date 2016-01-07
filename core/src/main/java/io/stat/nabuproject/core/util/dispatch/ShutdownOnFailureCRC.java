package io.stat.nabuproject.core.util.dispatch;

import io.stat.nabuproject.core.IComponent;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link CallbackReducerCallback} which shuts down the specified component
 * when a failure is detected. To prevent the possibility of its own worker executor
 * shutting down as it dispatches the shutdown, it creates a new Thread for that action.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public final class ShutdownOnFailureCRC extends CallbackReducerCallback {
    private final IComponent target;
    private final @Getter String name;

    public ShutdownOnFailureCRC(IComponent target, String name) {
        this.target = target;
        this.name = name;
    }

    @Override
    public void failedWithThrowable(Throwable t) {
        performShutdown();
    }

    @Override
    public void failed() {
        performShutdown();
    }

    @Override
    public void success() { /* no-op */ }

    private void performShutdown() {
        Thread t = new Thread(target::shutdown);
        t.setName("ShutdownOnFailerCRC-"+name);
        t.start();
    }
}
