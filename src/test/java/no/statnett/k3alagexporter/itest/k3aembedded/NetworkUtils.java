package no.statnett.k3alagexporter.itest.k3aembedded;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Random;

final class NetworkUtils {

    private static final int FIRST_RANDOM_PORT = 10000;
    private static final int LAST_RANDOM_PORT = 35000;
    private static final int NUM_PORTS_IN_RANGE = LAST_RANDOM_PORT - FIRST_RANDOM_PORT + 1;
    private static final Random RND = new Random();

    private NetworkUtils() {
    }

    public static int getRandomAvailablePort() {
        for (int q = 2 * NUM_PORTS_IN_RANGE; q >= 0; q--) {
            final int port = getRandomPort();
            if (isAvailablePort(port)) {
                return port;
            }
        }
        throw new RuntimeException("Giving up finding a free port");
    }

    private static int getRandomPort() {
        return FIRST_RANDOM_PORT + RND.nextInt(NUM_PORTS_IN_RANGE);
    }

    private static boolean isAvailablePort(final int port) {
        try (final ServerSocket ignored = new ServerSocket(port, -1, InetAddress.getLocalHost())) {
            return true;
        } catch (final IOException e) {
            return false;
        }
    }

}
