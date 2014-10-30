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
import com.couchbase.client.core.CouchbaseCore;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.cluster.DisconnectRequest;
import com.couchbase.client.core.message.cluster.DisconnectResponse;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.jruby.env.CouchbaseEnvironment;
import com.couchbase.client.jruby.env.DefaultCouchbaseEnvironment;
import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyClass;
import org.jruby.RubyObject;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import rx.Observable;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Sergey Avseyev
 */
@JRubyClass(name = "Couchbase::Cluster")
public class Cluster extends RubyObject {
    private final ClusterFacade core;
    private final RubyClass bucketClass;
    private final CouchbaseEnvironment environment;

    public Cluster(Ruby runtime, RubyClass metaClass) {
        super(runtime, metaClass);
        bucketClass = runtime.getModule("Couchbase").getClass("Bucket");
        environment = DefaultCouchbaseEnvironment.create();
        core = new CouchbaseCore(environment);
    }

    @JRubyMethod(name = "initialize", optional = 1)
    public IRubyObject initialize(ThreadContext context, IRubyObject[] args) {
        List<String> seedNodes = new ArrayList<String>();
        if (args.length > 0) {
            IRubyObject nodes = args[0];
            nodes.checkArrayType();
            for (IRubyObject node : ((RubyArray) nodes).toJavaArray()) {
                seedNodes.add(node.asJavaString());
            }
        }
        if (seedNodes.isEmpty()) {
            seedNodes.add("127.0.0.1");
        }
        SeedNodesRequest request = new SeedNodesRequest(seedNodes);
        core.send(request).toBlocking().single();
        return context.nil;
    }

    @JRubyMethod(name = "disconnect")
    public IRubyObject disconnect(ThreadContext context) {
        final long timeout = environment.disconnectTimeout();
        final Ruby runtime = context.getRuntime();
        return disconnectAsync()
                .timeout(timeout, TimeUnit.MILLISECONDS)
                .toBlocking()
                .single() ? runtime.getTrue() : runtime.getFalse();
    }

    private Observable<Boolean> disconnectAsync() {
        return core
                .<DisconnectResponse>send(new DisconnectRequest())
                .flatMap(new Func1<DisconnectResponse, Observable<Boolean>>() {
                    @Override
                    public Observable<Boolean> call(DisconnectResponse disconnectResponse) {
                        return environment.shutdown();
                    }
                });
    }

    @JRubyMethod(name = "open_bucket", optional = 2)
    public IRubyObject openBucket(final ThreadContext context, final IRubyObject[] args) {
        String name = "default";
        String password = "";

        if (args.length > 0) {
            name = args[0].asJavaString();
        }
        if (args.length > 1) {
            password = args[1].asJavaString();
        }
        final long timeout = environment.connectTimeout();
        return openBucketAsync(context.getRuntime(), name, password)
                .timeout(timeout, TimeUnit.MILLISECONDS)
                .toBlocking()
                .single();
    }

    private Observable<Bucket> openBucketAsync(final Ruby runtime, final String name, final String password) {
        final String pass = password == null ? "" : password;

        return core
                .send(new OpenBucketRequest(name, password))
                .map(new Func1<CouchbaseResponse, Bucket>() {
                    @Override
                    public Bucket call(CouchbaseResponse response) {
                        if (response.status() != ResponseStatus.SUCCESS) {
                            throw new CouchbaseException("Could not open bucket.");
                        }
                        return new Bucket(runtime, bucketClass, environment, core, name, pass);
                    }
                }).onErrorReturn(new Func1<Throwable, Bucket>() {
                    @Override
                    public Bucket call(Throwable throwable) {
                        if (throwable instanceof CouchbaseException) {
                            throw (CouchbaseException) throwable;
                        }
                        throw new CouchbaseException(throwable);
                    }
                });
    }
}
