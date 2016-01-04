package io.stat.nabuproject.core.util.dispatch;

/**
 * Runs the given FutureCollectorTask, then runs the
 * given CallbackReducerCallback.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
class CallbackReducerRunner implements Runnable {
    private final CallbackReducerCallback crc;
    private final FutureCollectorTask fct;

    CallbackReducerRunner(CallbackReducerCallback crc, FutureCollectorTask fct) {
        this.crc = crc;
        this.fct = fct;
    }

    @Override
    public void run() {
        boolean result = false;
        try {
            result = fct.call();
        } catch (Throwable t) {
            crc.failedWithThrowable(t);
        }

        if (!result) {
            crc.failed();
        } else {
            crc.success();
        }
    }
}
