package io.stat.nabuproject.core.util;

import lombok.extern.slf4j.Slf4j;
import sun.management.VMManagement;
import sun.misc.Signal;
import sun.misc.SignalHandler;
import sun.misc.Unsafe;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Hashtable;

// because REAL programmers use undocumented internal APIs that
// even make the compiler bitch at you for using them!

/**
 * Here be dragons.
 *
 * Provides helper functions to peer into parts of the JVM that noone is supposed to touch.
 * This uses the internal {@link sun.misc} packages to do things like getting the PID of the
 * process, and registering UNIX signal handlers.
 */
@Slf4j
public final class JVMHackery {
    private static Hashtable<Integer, Signal> _jvmSignals;
    private static int _jvmPid = 0; // cause nothing can be PID 0 except the kernel... right?
    private static Unsafe _theUnsafe; // mother of god...

    /**
     * Gets the system PID that this JVM is running on.
     * @return the pid of the JVM process
     */
    public static int getPid() {
        if(_jvmPid == 0) {
            _fetchPid();
        }

        return _jvmPid;
    }

    /**
     * Attempts to register a UNIX signal handler for signals sent to the JVM process.
     * If the JVM does not support listening to this signals, a simple warning will be logged
     * and nothing will happen.
     *
     * Example:
     * <code>
     *     JVMHackery.addJvmSignalHandler("HUP", signal -&gt; {
     *         System.out.println("received a SIGHUP!");
     *     });
     * </code>
     * @param name the name of the signal to listen to. case sensitive and platform dependent. drop the "SIG" part
     * @param handler a {@link sun.misc.SignalHandler} to handle this signal.
     */
    public static void addJvmSignalHandler(String name, SignalHandler handler) {
        for(Signal s : getJvmSignals().values()) {
            if(s.getName().equals(name)) {
                Signal.handle(new Signal(name), handler);
                return;
            }
        }

        logger.warn("Could not register a handler for SIG{}. Some sorcery might not be available...", name);
    }

    private static Unsafe getTheUnsafe() {
        if(_theUnsafe == null) {
            try {
                Field theRealUnsafeField = Unsafe.class.getDeclaredField("theUnsafe");
                boolean wasAccessible = theRealUnsafeField.isAccessible();

                theRealUnsafeField.setAccessible(true);
                _theUnsafe = (Unsafe)theRealUnsafeField.get(null);
                theRealUnsafeField.setAccessible(wasAccessible);

            } catch(NoSuchFieldException | IllegalAccessException e) {
                logger.error("Couldn't access sun.misc.Unsafe.theUnsafe. Config system may not work...", e);
            }
        }

        return _theUnsafe;
    }

    @SuppressWarnings("unchecked")
    public static <T> T createUnsafeInstance(Class<T> klazz) {
        try {
            return (T) getTheUnsafe().allocateInstance(klazz);
        } catch(InstantiationException e) {
            logger.error("InstantiationException was thrown when trying to unsafely allocate an instance of {}. Enjoy your NPE!", klazz.getCanonicalName(), e);
            return null;
        }
    }

    private static Hashtable<Integer, Signal> getJvmSignals() {
        if(_jvmSignals == null) {
            _fetchJvmSignals();
        }

        return _jvmSignals;
    }

    @SuppressWarnings("unchecked")
    private static void _fetchJvmSignals() {
        try {
            Field signalDotSignals = Signal.class.getDeclaredField("signals");
            boolean wasSignalsAccessible = signalDotSignals.isAccessible();
            signalDotSignals.setAccessible(true);
            _jvmSignals = (Hashtable)signalDotSignals.get(null);
            signalDotSignals.setAccessible(wasSignalsAccessible);
            logger.info("This JVM supports the following signals: {}", _jvmSignals);
        } catch (Exception e) {
            logger.warn("Couldn't get a list of supported JVM signals. Magic might not be available.");
            _jvmSignals = new Hashtable<>(0);
        }
    }

    @SuppressWarnings("unchecked")
    private static void _fetchPid() {
        try {
            // extreme sorcery ahead...
            int thePid;

            RuntimeMXBean rtmx = ManagementFactory.getRuntimeMXBean();
            VMManagement sunVMM;

            Field sunVMMInstance = rtmx.getClass().getDeclaredField("jvm");
            boolean wasSunVMMAccessible = sunVMMInstance.isAccessible();
            sunVMMInstance.setAccessible(true);
            sunVMM = ((VMManagement)sunVMMInstance.get(rtmx));
            sunVMMInstance.setAccessible(wasSunVMMAccessible);

            Method sunVMMGetProcessIdMethod = sunVMM.getClass().getDeclaredMethod("getProcessId");
            boolean wasGetPidAccessible = sunVMMGetProcessIdMethod.isAccessible();
            sunVMMGetProcessIdMethod.setAccessible(true);

            thePid = (Integer)sunVMMGetProcessIdMethod.invoke(sunVMM);

            sunVMMGetProcessIdMethod.setAccessible(wasGetPidAccessible);

            _jvmPid = thePid;
            return;
        } catch(Exception e) {
            logger.error("Could not get the JVM's PID. gl;hf.", e);
        }

        _jvmPid = -1;
    }
}
