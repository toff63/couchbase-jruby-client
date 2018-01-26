/*
 * Copyright (c) 2014 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.client.jruby.env;

import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.jruby.Cluster;

import java.util.concurrent.TimeUnit;

/**
 * @author Sergey Avseyev
 */
public class DefaultCouchbaseEnvironment extends DefaultCoreEnvironment implements CouchbaseEnvironment {
    public static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(CouchbaseEnvironment.class);
    public static final long CONNECT_TIMEOUT = TimeUnit.SECONDS.toMillis(5);
    public static final long KV_TIMEOUT = 2500;
    public static final long MANAGEMENT_TIMEOUT = TimeUnit.SECONDS.toMillis(75);
    public static final long DISCONNECT_TIMEOUT = TimeUnit.SECONDS.toMillis(5);
    private static final long VIEW_TIMEOUT = TimeUnit.SECONDS.toMillis(75);
    public static String PACKAGE_NAME_AND_VERSION = "couchbase-jruby-client";

    /**
     * Sets up the package version and user agent.
     *
     * Note that because the class loader loads classes on demand, one class from the package
     * is loaded upfront.
     */
    static {
        try {
            Class<Cluster> facadeClass = Cluster.class;
            if (facadeClass == null) {
                throw new IllegalStateException("Could not locate Cluster");
            }

            Package pkg = Package.getPackage("com.couchbase.client.jruby");
            String version = pkg.getSpecificationVersion();
            String gitVersion = pkg.getImplementationVersion();
            PACKAGE_NAME_AND_VERSION = String.format("couchbase-jruby-core/%s (git: %s)",
                    version == null ? "unknown" : version, gitVersion == null ? "unknown" : gitVersion);

            USER_AGENT = String.format("%s (%s/%s %s; %s %s)",
                    PACKAGE_NAME_AND_VERSION,
                    System.getProperty("os.name"),
                    System.getProperty("os.version"),
                    System.getProperty("os.arch"),
                    System.getProperty("java.vm.name"),
                    System.getProperty("java.runtime.version")
            );
        } catch (Exception ex) {
            LOGGER.info("Could not set up user agent and packages, defaulting.", ex);
        }
    }

    private final long managementTimeout;
    private final long connectTimeout;
    private final long kvTimeout;
    private final long disconnectTimeout;
    private final long viewTimeout;

    private DefaultCouchbaseEnvironment(final Builder builder) {
        super(builder);
        DefaultCouchbaseEnvironment env = builder.build();
        connectTimeout = longPropertyOr("connectTimeout", env.connectTimeout());
        kvTimeout = longPropertyOr("kvTimeout", env.kvTimeout());
        viewTimeout = longPropertyOr("viewTimeout", env.viewTimeout());
        managementTimeout = longPropertyOr("managementTimeout", env.managementTimeout());
        disconnectTimeout = longPropertyOr("disconnectTimeout", env.disconnectTimeout());
    }

    /**
     * Creates a {@link CouchbaseEnvironment} with default settings applied.
     *
     * @return a {@link DefaultCouchbaseEnvironment} with default settings.
     */
    public static DefaultCouchbaseEnvironment create() {
        return builder().build();
    }

    /**
     * Returns the {@link Builder} to customize environment settings.
     *
     * @return the {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public long connectTimeout() {
        return connectTimeout;
    }

    @Override
    public long kvTimeout() {
        return kvTimeout;
    }

    @Override
    public long managementTimeout() {
        return managementTimeout;
    }

    @Override
    public long viewTimeout() {
        return viewTimeout;
    }

    @Override
    public long disconnectTimeout() {
        return disconnectTimeout;
    }

    public static class Builder extends DefaultCoreEnvironment.Builder {

        private long kvTimeout = KV_TIMEOUT;
        private long connectTimeout = CONNECT_TIMEOUT;
        private String packageNameAndVersion = PACKAGE_NAME_AND_VERSION;
        private long managementTimeout = MANAGEMENT_TIMEOUT;
        private long disconnectTimeout = DISCONNECT_TIMEOUT;
        private long viewTimeout = VIEW_TIMEOUT;

//        @Override
//        public long connectTimeout() {
//            return connectTimeout;
//        }

        public Builder connectTimeout(long connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

//        @Override
//        public long disconnectTimeout() {
//            return disconnectTimeout;
//        }

        public Builder disconnectTimeout(long disconnectTimeout) {
            this.disconnectTimeout = disconnectTimeout;
            return this;
        }
//        @Override
//        public long kvTimeout() {
//            return kvTimeout;
//        }

        public Builder kvTimeout(long kvTimeout) {
            this.kvTimeout = kvTimeout;
            return this;
        }

//        @Override
//        public long managementTimeout() {
//            return managementTimeout;
//        }
//
//        @Override
//        public long viewTimeout() {
//            return viewTimeout;
//        }

        public Builder managementTimeout(long managementTimeout) {
            this.managementTimeout = managementTimeout;
            return this;
        }
        @Override
        public DefaultCouchbaseEnvironment build() {
            return new DefaultCouchbaseEnvironment(this);
        }

        @Override
        public Builder packageNameAndVersion(final String packageNameAndVersion) {
            super.packageNameAndVersion(packageNameAndVersion);
            return this;
        }

//        @Override
//        public String packageNameAndVersion() {
//            return packageNameAndVersion;
//        }
    }
}
