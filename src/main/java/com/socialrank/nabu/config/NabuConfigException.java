package com.socialrank.nabu.config;

import lombok.ToString;

/**
 * Created by io on 12/21/15. (929) 253-6977 $50/hr
 */
public class NabuConfigException extends Exception {
    public NabuConfigException() { super(); }
    public NabuConfigException(String message) { super(message); }
    public NabuConfigException(Throwable cause) { super(cause); }
    public NabuConfigException(String message, Throwable cause) { super(message, cause); }
}
