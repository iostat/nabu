package com.socialrank.nabu.common;

import lombok.Getter;

/**
 * Created by io on 12/22/15. (929) 253-6977 $50/hr
 */
public enum NabuCommandType {
    INDEX(0),
    UPDATE(1);

    @Getter byte code;

    NabuCommandType(int code) {
        this.code = ((byte) code);
    }
}
