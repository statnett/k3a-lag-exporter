package io.statnett.k3a.lagexporter.utils;

import org.apache.kafka.common.errors.TimeoutException;

public final class Exceptions {

    public static TimeoutException findTimeoutException(final Throwable t) {
        Throwable current = t;
        while (current != null) {
            if (TimeoutException.class.isAssignableFrom(current.getClass())) {
                return (TimeoutException) current;
            }
            if (current == current.getCause()) {
                break;
            }
            current = current.getCause();
        }
        return null;
    }

}
