package com.huawei.services.runtime;

/**
 * Compile-time stub of the FunctionGraph Java runtime logger.
 *
 * <p>The real implementation is supplied by the FunctionGraph runtime at execution time.
 * Excluded from the shaded deployment JAR (see pom.xml).</p>
 */
public interface RuntimeLogger {

    /** Writes a log line to the function's log stream. */
    void log(String message);
}
