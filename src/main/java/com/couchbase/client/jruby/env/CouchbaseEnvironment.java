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

import com.couchbase.client.core.env.CoreEnvironment;

/**
 * @author Sergey Avseyev
 */
public interface CouchbaseEnvironment extends CoreEnvironment {
    /**
     * The default timeout for connect operations, set to {@link DefaultCouchbaseEnvironment#CONNECT_TIMEOUT}.
     *
     * @return the default connect timeout.
     */
    long connectTimeout();

    /**
     * The default timeout for binary (key/value) operations, set to {@link DefaultCouchbaseEnvironment#KV_TIMEOUT}.
     *
     * @return the default binary timeout.
     */
    long kvTimeout();

    /**
     * The default timeout for management operations, set to {@link DefaultCouchbaseEnvironment#MANAGEMENT_TIMEOUT}.
     *
     * @return the default management timeout.
     */
    long managementTimeout();

    /**
     * The default timeout for view operations, set to {@link DefaultCouchbaseEnvironment#VIEW_TIMEOUT}.
     *
     * @return the default view timeout.
     */
    long viewTimeout();

    /**
     * The default timeout for disconnect operations, set to {@link DefaultCouchbaseEnvironment#DISCONNECT_TIMEOUT}.
     *
     * @return the default disconnect timeout.
     */
    long disconnectTimeout();
}
