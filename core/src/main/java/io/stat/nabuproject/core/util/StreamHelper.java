package io.stat.nabuproject.core.util;

import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 * Hunts for files in the classpath, system path, etc. and gets Stream to them
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public final class StreamHelper {
    public static InputStream findResource(String filename) throws FileNotFoundException {
        InputStream ret;

        logger.trace("Attempting to find {} as a resource in the system classloader...");
        ret = ClassLoader.getSystemClassLoader().getResourceAsStream(filename);

        // todo: probably needs better resource hunting, lmao...
        if(ret != null) {
            return ret;
        } else {
            throw new FileNotFoundException("Could not find " + filename + " anywhere...");
        }
    }
}
