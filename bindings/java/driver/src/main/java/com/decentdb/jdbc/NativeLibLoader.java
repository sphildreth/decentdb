package com.decentdb.jdbc;

import java.io.*;
import java.net.URL;
import java.nio.file.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * Loads the DecentDB JNI native library at most once per JVM.
 *
 * Loading strategy (in order):
 * 1. If {@code DECENTDB_NATIVE_LIB} environment variable is set, load from that path.
 * 2. If {@code decentdb.native.lib.dir} system property is set, search that directory.
 * 3. Extract OS/arch-specific library from the JAR to a versioned temp file and load it.
 */
public final class NativeLibLoader {
    private static final Logger LOG = Logger.getLogger(NativeLibLoader.class.getName());

    private static final AtomicBoolean loaded = new AtomicBoolean(false);
    static volatile Throwable loadError = null;

    private static final String VERSION = DecentDBDriver.DRIVER_VERSION;

    private NativeLibLoader() {}

    public static synchronized void ensureLoaded() throws UnsatisfiedLinkError {
        if (loaded.get()) {
            if (loadError != null) {
                throw new UnsatisfiedLinkError("DecentDB native library failed to load: " + loadError.getMessage());
            }
            return;
        }
        try {
            doLoad();
            loaded.set(true);
        } catch (Exception | UnsatisfiedLinkError e) {
            loadError = e;
            loaded.set(true);
            if (e instanceof UnsatisfiedLinkError) throw (UnsatisfiedLinkError) e;
            throw new UnsatisfiedLinkError("Failed to load DecentDB native library: " + e.getMessage());
        }
    }

    private static void doLoad() throws IOException {
        // 1. Environment variable override
        String envPath = System.getenv("DECENTDB_NATIVE_LIB");
        if (envPath != null && !envPath.isEmpty()) {
            LOG.fine("Loading native library from DECENTDB_NATIVE_LIB: " + envPath);
            System.load(envPath);
            return;
        }

        // 2. System property directory
        String propDir = System.getProperty("decentdb.native.lib.dir");
        if (propDir != null && !propDir.isEmpty()) {
            File libFile = findLibInDir(new File(propDir));
            if (libFile != null) {
                LOG.fine("Loading native library from system property dir: " + libFile);
                System.load(libFile.getAbsolutePath());
                return;
            }
        }

        // 3. Extract from JAR
        loadFromJar();
    }

    private static File findLibInDir(File dir) {
        String[] candidates = libCandidateNames();
        for (String name : candidates) {
            File f = new File(dir, name);
            if (f.exists() && f.canRead()) return f;
        }
        return null;
    }

    private static void loadFromJar() throws IOException {
        String osArch = osArchDir();
        String resourcePath = "/native/" + osArch + "/" + primaryLibName();
        URL url = NativeLibLoader.class.getResource(resourcePath);
        if (url == null) {
            // Fallback: try to load from system library path
            try {
                System.loadLibrary("decentdb_jni");
                return;
            } catch (UnsatisfiedLinkError e) {
                throw new UnsatisfiedLinkError(
                    "DecentDB native library not found in JAR at '" + resourcePath +
                    "' and not on system library path. " +
                    "Set DECENTDB_NATIVE_LIB environment variable to the library path, or " +
                    "set decentdb.native.lib.dir system property to the directory containing the library."
                );
            }
        }

        // Extract to a versioned temp file for atomic, concurrent-safe loading
        String libName = "decentdb_jni-" + VERSION + "-" + osArch.replace("/", "_");
        File tempDir = new File(System.getProperty("java.io.tmpdir"), "decentdb-native");
        tempDir.mkdirs();
        File dest = new File(tempDir, libName + libSuffix());

        if (!dest.exists()) {
            // Write to a .tmp file first, then rename atomically
            File tmp = new File(tempDir, libName + libSuffix() + ".tmp");
            try (InputStream in = url.openStream();
                 FileOutputStream out = new FileOutputStream(tmp)) {
                byte[] buf = new byte[65536];
                int n;
                while ((n = in.read(buf)) != -1) out.write(buf, 0, n);
            }
            // Atomic rename; if it fails because dest exists (race), that's fine
            tmp.renameTo(dest);
        }

        // Extract core DecentDB library next to the JNI library (best-effort).
        // The JNI library is typically linked against the core library; keeping
        // them in the same directory avoids requiring java.library.path tweaks.
        for (String dep : dependentLibCandidateNames()) {
            extractIfPresent(tempDir, osArch, dep);
        }

        LOG.fine("Loading native library from extracted: " + dest);
        System.load(dest.getAbsolutePath());
    }

    private static void extractIfPresent(File tempDir, String osArch, String fileName) throws IOException {
        String resourcePath = "/native/" + osArch + "/" + fileName;
        URL url = NativeLibLoader.class.getResource(resourcePath);
        if (url == null) return;

        File dest = new File(tempDir, fileName);
        if (dest.exists()) return;

        File tmp = new File(tempDir, fileName + ".tmp");
        try (InputStream in = url.openStream();
             FileOutputStream out = new FileOutputStream(tmp)) {
            byte[] buf = new byte[65536];
            int n;
            while ((n = in.read(buf)) != -1) out.write(buf, 0, n);
        }
        tmp.renameTo(dest);
    }

    private static String osArchDir() {
        String os = System.getProperty("os.name", "").toLowerCase();
        String arch = System.getProperty("os.arch", "").toLowerCase();
        String osDir;
        if (os.contains("linux")) osDir = "linux";
        else if (os.contains("mac") || os.contains("darwin")) osDir = "darwin";
        else if (os.contains("win")) osDir = "windows";
        else osDir = "unknown";

        String archDir;
        if (arch.equals("amd64") || arch.equals("x86_64")) archDir = "x86_64";
        else if (arch.equals("aarch64") || arch.equals("arm64")) archDir = "aarch64";
        else archDir = arch;

        return osDir + "-" + archDir;
    }

    private static String primaryLibName() {
        String os = System.getProperty("os.name", "").toLowerCase();
        if (os.contains("win")) return "decentdb_jni.dll";
        if (os.contains("mac") || os.contains("darwin")) return "libdecentdb_jni.dylib";
        return "libdecentdb_jni.so";
    }

    private static String libSuffix() {
        String name = primaryLibName();
        int dot = name.lastIndexOf('.');
        return dot >= 0 ? name.substring(dot) : "";
    }

    private static String[] libCandidateNames() {
        return new String[]{
            "libdecentdb_jni.so",
            "libdecentdb_jni.dylib",
            "decentdb_jni.dll",
        };
    }

    private static String[] dependentLibCandidateNames() {
        // These are extracted best-effort; the exact core library filename varies by OS/build.
        return new String[]{
            "libc_api.so",
            "libc_api.dylib",
            "libc_api.dll",
            "c_api.dll",
            "decentdb.dll",
        };
    }
}
