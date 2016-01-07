package io.stat.nabuproject.core.util.dispatch;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * For every future that it is assigned to run, it will see if the future failed.
 * A future's failure is determined by whether or not it threw an Exception, or if it
 * returned null (kind of impossible) or false. In the case of the former failure case, it
 * is called an "exceptional failure"
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
final class FutureCollectorTask implements Callable<Boolean> {
    final List<Future<Boolean>> futuresToCollect;
    private final String callbackName;

    FutureCollectorTask(String callbackName, List<Future<Boolean>> futuresToCollect) {
        this.callbackName = callbackName;
        this.futuresToCollect = futuresToCollect;
    }

    @Override
    public Boolean call() throws Exception {
        for(Future<Boolean> f : futuresToCollect) {
            Boolean thisResult;
            try {
                thisResult = f.get();
            } catch(Exception e) {
                logger.error(callbackName + " :: FCT.get() failed for future " + f.toString() + " with exception ", e);
                throw e;
            }

            if(thisResult == null || !thisResult) {
                return false;
            }
        }
        return true;
    }
}
