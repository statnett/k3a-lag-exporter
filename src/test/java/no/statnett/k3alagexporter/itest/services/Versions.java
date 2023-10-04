package no.statnett.k3alagexporter.itest.services;

/**
 * Contains versions of container images used for testing. All in one place to
 * make it easier to keep them updated.
 */
final class Versions {

    // renovate: datasource=docker depName=confluentinc/cp-kafka
    static final String CONFLUENT_VERSION = "7.5.0";

    private Versions() {
    }

}
