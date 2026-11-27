package com.huawei.services.runtime;

/**
 * Compile-time stub of the FunctionGraph Java runtime {@code Context} interface.
 *
 * <p>The real implementation is supplied by the FunctionGraph runtime at execution time,
 * so this stub only declares the methods actually referenced by this project. It is
 * excluded from the shaded deployment JAR (see pom.xml).</p>
 */
public interface Context {

    /** Returns the runtime logger used by the function. */
    RuntimeLogger getLogger();

    /** Reads a user-defined configuration item injected into the function. */
    String getUserData(String key);
}
