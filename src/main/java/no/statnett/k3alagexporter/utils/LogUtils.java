package no.statnett.k3alagexporter.utils;

public final class LogUtils {

    private LogUtils() {
    }

    /**
     * Sets up logging to a single line when using Java Logging.
     * Must be called before any log-related class is instantiated.
     */
    public static void enableSingleLineLogging() {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                           "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS %4$s %2$s: %5$s%6$s%n");
    }
}
