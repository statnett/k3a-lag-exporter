package no.statnett.k3alagexporter.itest.k3aembedded;

import java.io.File;

final class FileUtils {

    private FileUtils() {
    }

    public static void deleteRecursively(final File directory) {
        final File[] filesAndDirectories = directory.listFiles();
        if (filesAndDirectories != null) {
            for (final File containedFileOrDirectory : filesAndDirectories) {
                if (containedFileOrDirectory.isDirectory()) {
                    deleteRecursively(containedFileOrDirectory);
                }
                delete(containedFileOrDirectory);
            }
        }
        delete(directory);
    }

    private static void delete(final File fileOrDirectory) {
        if (!fileOrDirectory.delete()) {
            System.err.println("Unable to delete \"" + fileOrDirectory + "\". Ignoring.");
        }
    }

}
