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

package com.couchbase.client.jruby;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.config.FlushRequest;
import com.couchbase.client.core.message.config.FlushResponse;
import com.couchbase.client.core.message.kv.GetRequest;
import com.couchbase.client.core.message.kv.GetResponse;
import com.couchbase.client.core.message.kv.UpsertRequest;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.jruby.env.CouchbaseEnvironment;
import com.couchbase.client.jruby.error.FlushDisabledException;
import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyObject;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import rx.Observable;
import rx.functions.Func1;

import java.util.concurrent.TimeUnit;

/**
 * @author Sergey Avseyev
 */
public class BucketManager extends RubyObject {
    private final ClusterFacade core;
    private final String bucket;
    private final String password;
    private final CouchbaseEnvironment environment;

    public BucketManager(Ruby runtime, RubyClass metaClass) {
        this(runtime, metaClass, null, null, null, null);
    }

    public BucketManager(final Ruby runtime, final RubyClass metaClass, final CouchbaseEnvironment environment,
                         final ClusterFacade core, final String bucket, final String password) {
        super(runtime, metaClass);
        this.environment = environment;
        this.core = core;
        this.bucket = bucket;
        this.password = password;
    }

    @JRubyMethod(name = "flush")
    public IRubyObject flush(ThreadContext context) {
        final long timeout = environment.managementTimeout();
        return flushAsync(context)
                .timeout(timeout, TimeUnit.MILLISECONDS)
                .toBlocking()
                .single();
    }

    private Observable<IRubyObject> flushAsync(final ThreadContext context) {
        final Ruby runtime = context.getRuntime();
        final String markerKey = "__flush_marker";
        return core
                .send(new UpsertRequest(markerKey, Unpooled.copiedBuffer(markerKey, CharsetUtil.UTF_8), bucket))
                .flatMap(new Func1<CouchbaseResponse, Observable<FlushResponse>>() {
                    @Override
                    public Observable<FlushResponse> call(CouchbaseResponse res) {
                        return core.send(new FlushRequest(bucket, password));
                    }
                }).flatMap(new Func1<FlushResponse, Observable<IRubyObject>>() {
                    @Override
                    public Observable<IRubyObject> call(FlushResponse flushResponse) {
                        if (flushResponse.status() == ResponseStatus.FAILURE) {
                            if (flushResponse.content().contains("disabled")) {
                                return Observable.error(new FlushDisabledException("Flush is disabled for this bucket."));
                            } else {
                                return Observable.error(new CouchbaseException("Flush failed because of: "
                                        + flushResponse.content()));
                            }
                        }
                        if (flushResponse.isDone()) {
                            return Observable.just((IRubyObject) runtime.getTrue());
                        }
                        while (true) {
                            GetResponse res = core.<GetResponse>send(new GetRequest(markerKey, bucket)).toBlocking().single();
                            if (res.status() == ResponseStatus.NOT_EXISTS) {
                                return Observable.just((IRubyObject) runtime.getTrue());
                            }
                        }
                    }
                });
    }
}
