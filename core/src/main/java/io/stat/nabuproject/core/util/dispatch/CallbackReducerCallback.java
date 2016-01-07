package io.stat.nabuproject.core.util.dispatch;

/**
 * A set of callbacks which respond to whether or not the callbacks that a AsyncListenerDispatcher
 * was told run failed and how they failed if they did.
 */
public abstract class CallbackReducerCallback {

    /**
     * Called if any callback threw something.
     * @param t what a callback threw.
     */
    public abstract void failedWithThrowable(Throwable t);

    /**
     * Called if not all callbacks ran successfully.
     */
    public abstract void failed();

    /**
     * Called if all callbacks executed successfully
     */
    public abstract void success();

    /**
     * Get the name of the callback this CallbackReducerCallback would have reduced.
     */
    public abstract String getName();
}
