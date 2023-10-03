package no.statnett.k3alagexporter.utils;

public final class LogUtils {

    public static final String LOG_FORMAT_PROPERTY = "java.util.logging.SimpleFormatter.format";

    private LogUtils() {
    }

    /**
     * Sets up logging to a single line when using Java Logging.
     * Must be called before any log-related class is instantiated.
     */
    public static void enableSingleLineLogging() {
        if (System.getProperty(LOG_FORMAT_PROPERTY) == null) {
            System.setProperty(LOG_FORMAT_PROPERTY, "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS %4$s %2$s: %5$s%6$s%n");
        }
    }
}
