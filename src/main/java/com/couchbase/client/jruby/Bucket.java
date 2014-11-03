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
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.cluster.CloseBucketRequest;
import com.couchbase.client.core.message.cluster.CloseBucketResponse;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.kv.*;
import com.couchbase.client.core.message.observe.Observe;
import com.couchbase.client.core.message.view.ViewQueryRequest;
import com.couchbase.client.core.message.view.ViewQueryResponse;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.jruby.env.CouchbaseEnvironment;
import com.couchbase.client.jruby.error.CASMismatchException;
import com.couchbase.client.jruby.error.DocumentAlreadyExistsException;
import com.couchbase.client.jruby.error.DocumentDoesNotExistException;
import com.couchbase.client.jruby.error.DurabilityException;
import org.jruby.*;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import rx.Observable;
import rx.functions.Func1;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author Sergey Avseyev
 */
@JRubyClass(name = "Couchbase::Bucket")
public class Bucket extends RubyObject {
    private final ClusterFacade core;
    private final String password;
    private final String bucket;
    private final CouchbaseEnvironment environment;
    private final Transcoder transcoder;
    private final RubyClass documentClass;
    private final RubyClass bucketManagerClass;
    private final RubyClass viewResultClass;
    private final RubySymbol symPersistTo;
    private final RubySymbol symReplicateTo;
    private final RubySymbol symInitial;
    private final RubySymbol symExpiry;
    private final RubySymbol symAll;
    private final RubySymbol symStale;
    private final RubySymbol symOk;
    private final RubySymbol symUpdateAfter;
    private final RubySymbol symDebug;
    private final RubySymbol symSkip;
    private final RubySymbol symGroupLevel;
    private final RubySymbol symGroup;
    private final RubySymbol symOnError;
    private final RubySymbol symDescending;
    private final RubySymbol symInclusiveEnd;
    private final RubySymbol symStartkey;
    private final RubySymbol symStartkeyDocid;
    private final RubySymbol symEndkey;
    private final RubySymbol symEndkeyDocid;
    private final RubySymbol symKeys;
    private final RubySymbol symKey;
    private final RubySymbol symBBox;
    private final RubySymbol symReduce;
    private final RubyModule multiJsonModule;

    public Bucket(final Ruby runtime, final RubyClass metaClass) {
        this(runtime, metaClass, null, null, null, null);
    }

    public Bucket(final Ruby runtime, final RubyClass metaClass, final CouchbaseEnvironment environment,
                  final ClusterFacade core, final String bucket, final String password) {
        super(runtime, metaClass);
        this.environment = environment;
        this.core = core;
        this.bucket = bucket;
        this.password = password;
        symInitial = runtime.newSymbol("initial");
        symExpiry = runtime.newSymbol("expiry");
        symPersistTo = runtime.newSymbol("persist_to");
        symReplicateTo = runtime.newSymbol("replicate_to");
        symAll = runtime.newSymbol("all");
        symStale = runtime.newSymbol("stale");
        symOk = runtime.newSymbol("ok");
        symUpdateAfter = runtime.newSymbol("update_after");
        symDebug = runtime.newSymbol("debug");
        symSkip = runtime.newSymbol("skip");
        symGroupLevel = runtime.newSymbol("group_level");
        symGroup = runtime.newSymbol("group");
        symOnError = runtime.newSymbol("on_error");
        symDescending = runtime.newSymbol("descending");
        symInclusiveEnd = runtime.newSymbol("inclusive_end");
        symStartkey = runtime.newSymbol("start_key");
        symStartkeyDocid = runtime.newSymbol("startkey_docid");
        symEndkey = runtime.newSymbol("endkey");
        symEndkeyDocid = runtime.newSymbol("endkey_docid");
        symKeys = runtime.newSymbol("keys");
        symKey = runtime.newSymbol("key");
        symBBox = runtime.newSymbol("bbox");
        symReduce = runtime.newSymbol("reduce");
        documentClass = runtime.getModule("Couchbase").getClass("Document");
        viewResultClass = runtime.getModule("Couchbase").getClass("ViewResult");
        bucketManagerClass = runtime.getModule("Couchbase").getClass("BucketManager");
        multiJsonModule = runtime.getModule("MultiJson");
        transcoder = new Transcoder(documentClass, multiJsonModule);
    }

    @JRubyMethod(name = "bucket_manager")
    public IRubyObject bucketManager(final ThreadContext context) {
        return new BucketManager(context.getRuntime(), bucketManagerClass, environment, core, bucket, password);
    }

    @JRubyMethod(name = "get")
    public IRubyObject get(final ThreadContext context, final IRubyObject id) {
        final long timeout = environment.kvTimeout();
        return get(context, id.asJavaString())
                .timeout(timeout, TimeUnit.MILLISECONDS)
                .toBlocking()
                .singleOrDefault(context.nil);
    }

    private Observable<IRubyObject> get(final ThreadContext context, final String id) {
        return core
                .<GetResponse>send(new GetRequest(id, bucket))
                .filter(new Func1<GetResponse, Boolean>() {
                    @Override
                    public Boolean call(GetResponse getResponse) {
                        return getResponse.status() == ResponseStatus.SUCCESS;
                    }
                })
                .map(new Func1<GetResponse, IRubyObject>() {
                    @Override
                    public IRubyObject call(final GetResponse response) {
                        return newDocument(context, id, response.cas(), 0, response.content(), response.flags());
                    }
                });
    }

    @JRubyMethod(name = "get_from_replica")
    public IRubyObject getFromReplica(final ThreadContext context, final IRubyObject id, final IRubyObject replica) {
        final long timeout = environment.kvTimeout();
        int repl;

        if (replica.eql(symAll)) {
            repl = -1;
        } else {
            repl = (int) ((RubyFixnum) replica).getLongValue();
            if (repl < 1 || repl > 3) {
                throw context.getRuntime().newArgumentError("replica should be in range (1..3) or :all");
            }
        }
        return get(context, id.asJavaString())
                .timeout(timeout, TimeUnit.MILLISECONDS)
                .toBlocking()
                .singleOrDefault(context.nil);
    }

    public Observable<IRubyObject> getFromReplica(final ThreadContext context, final String id, final int replica) {
        Observable<GetResponse> incoming;
        if (replica == -1) {
            incoming = core
                    .<GetClusterConfigResponse>send(new GetClusterConfigRequest())
                    .map(new Func1<GetClusterConfigResponse, Integer>() {
                        @Override
                        public Integer call(GetClusterConfigResponse response) {
                            CouchbaseBucketConfig conf = (CouchbaseBucketConfig) response.config().bucketConfig(bucket);
                            return conf.numberOfReplicas();
                        }
                    }).flatMap(new Func1<Integer, Observable<BinaryRequest>>() {
                        @Override
                        public Observable<BinaryRequest> call(Integer max) {
                            List<BinaryRequest> requests = new ArrayList<BinaryRequest>();

                            requests.add(new GetRequest(id, bucket));
                            for (int i = 1; i <= max; i++) {
                                requests.add(new ReplicaGetRequest(id, bucket, (short)i));
                            }
                            return Observable.from(requests);
                        }
                    }).flatMap(new Func1<BinaryRequest, Observable<GetResponse>>() {
                        @Override
                        public Observable<GetResponse> call(BinaryRequest req) {
                            return core.send(req);
                        }
                    });
        } else {
            incoming = core.send(new ReplicaGetRequest(id, bucket, (short) replica));
        }

        return incoming
                .filter(new Func1<GetResponse, Boolean>() {
                    @Override
                    public Boolean call(GetResponse getResponse) {
                        return getResponse.status() == ResponseStatus.SUCCESS;
                    }
                })
                .map(new Func1<GetResponse, IRubyObject>() {
                    @Override
                    public IRubyObject call(final GetResponse response) {
                        return newDocument(context, id, response.cas(), 0, response.content(), response.flags());
                    }
                });
    }

    @JRubyMethod(name = "insert", required = 1, optional = 1)
    public IRubyObject insert(final ThreadContext context, final IRubyObject[] args) {
        final long timeout = environment.kvTimeout();
        final IRubyObject document = args[0];
        Observe.PersistTo persistTo = Observe.PersistTo.NONE;
        Observe.ReplicateTo replicateTo = Observe.ReplicateTo.NONE;
        if (!documentClass.isInstance(document)) {
            throw context.getRuntime().newTypeError("Expected Couchbase::Document or descendant");
        }
        if (args.length == 2 && args[1] instanceof RubyHash) {
            RubyHash options = (RubyHash) args[1];
            assertOptions(context, options, symPersistTo, symReplicateTo);
            if (options.containsKey(symPersistTo)) {
                persistTo = getPersistToOption(context, options);
            }
            if (options.containsKey(symReplicateTo)) {
                replicateTo = getReplicateToOption(context, options);
            }
        }
        return insert(context, (Document) document, persistTo, replicateTo)
                .timeout(timeout, TimeUnit.MILLISECONDS)
                .toBlocking()
                .single();
    }

    private Observable<IRubyObject> insert(final ThreadContext context, final Document document,
                                           final Observe.PersistTo persistTo,
                                           final Observe.ReplicateTo replicateTo) {
        final Tuple2<ByteBuf, Integer> blob = transcoder.dump(context, document);
        final Observable<IRubyObject> observable = core
                .<InsertResponse>send(new InsertRequest(document.id(context), blob.value1(), document.expiry(context), blob.value2(), bucket))
                .flatMap(new Func1<InsertResponse, Observable<IRubyObject>>() {
                    @Override
                    public Observable<IRubyObject> call(InsertResponse response) {
                        if (response.status() == ResponseStatus.EXISTS) {
                            return Observable.error(new DocumentAlreadyExistsException());
                        }
                        return Observable.just(newDocument(context, document.id(context), response.cas(), document.expiry(context), document.content(context)));
                    }
                });
        if (replicateTo == Observe.ReplicateTo.NONE && persistTo == Observe.PersistTo.NONE) {
            return observable;
        } else {
            return observable.flatMap(new Func1<IRubyObject, Observable<IRubyObject>>() {
                @Override
                public Observable<IRubyObject> call(final IRubyObject object) {
                    Document doc = (Document) object;
                    return Observe
                            .call(core, bucket, doc.id(context), doc.cas(context), false, persistTo, replicateTo)
                            .map(new Func1<Boolean, IRubyObject>() {
                                @Override
                                public IRubyObject call(Boolean aBoolean) {
                                    return object;
                                }
                            }).onErrorResumeNext(new Func1<Throwable, Observable<IRubyObject>>() {
                                @Override
                                public Observable<IRubyObject> call(Throwable throwable) {
                                    return Observable.error(new DurabilityException("Durability constraint failed.", throwable));
                                }
                            });
                }
            });
        }
    }

    @JRubyMethod(name = "upsert", required = 1, optional = 1)
    public IRubyObject upsert(final ThreadContext context, final IRubyObject[] args) {
        final long timeout = environment.kvTimeout();
        final IRubyObject document = args[0];
        Observe.PersistTo persistTo = Observe.PersistTo.NONE;
        Observe.ReplicateTo replicateTo = Observe.ReplicateTo.NONE;
        if (!documentClass.isInstance(document)) {
            throw context.getRuntime().newTypeError("Expected Couchbase::Document or descendant");
        }
        if (args.length == 2 && args[1] instanceof RubyHash) {
            RubyHash options = (RubyHash) args[1];
            assertOptions(context, options, symPersistTo, symReplicateTo);
            if (options.containsKey(symPersistTo)) {
                persistTo = getPersistToOption(context, options);
            }
            if (options.containsKey(symReplicateTo)) {
                replicateTo = getReplicateToOption(context, options);
            }
        }
        return upsert(context, (Document) document, persistTo, replicateTo)
                .timeout(timeout, TimeUnit.MILLISECONDS)
                .toBlocking()
                .single();
    }

    private Observable<IRubyObject> upsert(final ThreadContext context, final Document document,
                                           final Observe.PersistTo persistTo,
                                           final Observe.ReplicateTo replicateTo) {
        final Tuple2<ByteBuf, Integer> blob = transcoder.dump(context, document);
        final Observable<IRubyObject> observable = core
                .<UpsertResponse>send(new UpsertRequest(document.id(context), blob.value1(), document.expiry(context), blob.value2(), bucket))
                .flatMap(new Func1<UpsertResponse, Observable<IRubyObject>>() {
                    @Override
                    public Observable<IRubyObject> call(UpsertResponse response) {
                        if (response.status() == ResponseStatus.EXISTS) {
                            return Observable.error(new CASMismatchException());
                        }
                        return Observable.just(newDocument(context, document.id(context), response.cas(), document.expiry(context), document.content(context)));
                    }
                });

        if (replicateTo == Observe.ReplicateTo.NONE && persistTo == Observe.PersistTo.NONE) {
            return observable;
        } else {
            return observable.flatMap(new Func1<IRubyObject, Observable<IRubyObject>>() {
                @Override
                public Observable<IRubyObject> call(final IRubyObject object) {
                    Document doc = (Document) object;
                    return Observe
                            .call(core, bucket, doc.id(context), doc.cas(context), false, persistTo, replicateTo)
                            .map(new Func1<Boolean, IRubyObject>() {
                                @Override
                                public IRubyObject call(Boolean aBoolean) {
                                    return object;
                                }
                            }).onErrorResumeNext(new Func1<Throwable, Observable<IRubyObject>>() {
                                @Override
                                public Observable<IRubyObject> call(Throwable throwable) {
                                    return Observable.error(new DurabilityException("Durability constraint failed.", throwable));
                                }
                            });
                }
            });
        }
    }

    @JRubyMethod(name = "replace", required = 1, optional = 1)
    public IRubyObject replace(final ThreadContext context, final IRubyObject[] args) {
        final long timeout = environment.kvTimeout();
        final IRubyObject document = args[0];
        Observe.PersistTo persistTo = Observe.PersistTo.NONE;
        Observe.ReplicateTo replicateTo = Observe.ReplicateTo.NONE;
        if (!documentClass.isInstance(document)) {
            throw context.getRuntime().newTypeError("Expected Couchbase::Document or descendant");
        }
        if (args.length == 2 && args[1] instanceof RubyHash) {
            RubyHash options = (RubyHash) args[1];
            assertOptions(context, options, symPersistTo, symReplicateTo);
            if (options.containsKey(symPersistTo)) {
                persistTo = getPersistToOption(context, options);
            }
            if (options.containsKey(symReplicateTo)) {
                replicateTo = getReplicateToOption(context, options);
            }
        }
        return upsert(context, (Document) document, persistTo, replicateTo)
                .timeout(timeout, TimeUnit.MILLISECONDS)
                .toBlocking()
                .single();
    }

    private Observable<IRubyObject> replace(final ThreadContext context, final Document document,
                                            final Observe.PersistTo persistTo,
                                            final Observe.ReplicateTo replicateTo) {
        final Tuple2<ByteBuf, Integer> blob = transcoder.dump(context, document);
        Observable<IRubyObject> observable = core
                .<ReplaceResponse>send(new ReplaceRequest(document.id(context), blob.value1(), document.cas(context), document.expiry(context), blob.value2(), bucket))
                .flatMap(new Func1<ReplaceResponse, Observable<IRubyObject>>() {
                    @Override
                    public Observable<IRubyObject> call(ReplaceResponse response) {
                        if (response.status() == ResponseStatus.NOT_EXISTS) {
                            return Observable.error(new DocumentDoesNotExistException());
                        }
                        if (response.status() == ResponseStatus.EXISTS) {
                            return Observable.error(new CASMismatchException());
                        }
                        return Observable.just(newDocument(context, document.id(context), response.cas(), document.expiry(context), document.content(context)));
                    }
                });

        if (replicateTo == Observe.ReplicateTo.NONE && persistTo == Observe.PersistTo.NONE) {
            return observable;
        } else {
            return observable.flatMap(new Func1<IRubyObject, Observable<IRubyObject>>() {
                @Override
                public Observable<IRubyObject> call(final IRubyObject object) {
                    Document doc = (Document) object;
                    return Observe
                            .call(core, bucket, doc.id(context), doc.cas(context), false, persistTo, replicateTo)
                            .map(new Func1<Boolean, IRubyObject>() {
                                @Override
                                public IRubyObject call(Boolean aBoolean) {
                                    return object;
                                }
                            }).onErrorResumeNext(new Func1<Throwable, Observable<IRubyObject>>() {
                                @Override
                                public Observable<IRubyObject> call(Throwable throwable) {
                                    return Observable.error(new DurabilityException("Durability constraint failed.", throwable));
                                }
                            });
                }
            });
        }
    }

    @JRubyMethod(name = "counter", required = 2, optional = 1)
    public IRubyObject counter(final ThreadContext context, final IRubyObject[] args) {
        final long timeout = environment.kvTimeout();
        String id;
        long delta, initial = 0;
        int expiry = 0;

        id = args[0].convertToString().asJavaString();
        delta = args[1].convertToInteger().getLongValue();
        if (args.length == 3 && args[2] instanceof RubyHash) {
            RubyHash options = (RubyHash) args[2];
            assertOptions(context, options, symInitial, symExpiry);
            if (options.containsKey(symInitial)) {
                initial = ((RubyFixnum) options.op_aref(context, symInitial)).getLongValue();
            }
            if (options.containsKey(symExpiry)) {
                expiry = (int) ((RubyFixnum) options.op_aref(context, symExpiry)).getLongValue();
            }
        }
        return counter(context, id, delta, initial, expiry)
                .timeout(timeout, TimeUnit.MILLISECONDS)
                .toBlocking()
                .single();
    }

    public Observable<IRubyObject> counter(final ThreadContext context, final String id, final long delta, final long initial, final int expiry) {
        final Ruby runtime = context.getRuntime();
        return core
                .<CounterResponse>send(new CounterRequest(id, initial, delta, expiry, bucket))
                .flatMap(new Func1<CounterResponse, Observable<IRubyObject>>() {
                    @Override
                    public Observable<IRubyObject> call(CounterResponse response) {
                        return Observable.just(newDocument(context, id, response.cas(), expiry, runtime.newFixnum(response.value())));
                    }
                });
    }

    @JRubyMethod(name = "get_and_touch")
    public IRubyObject getAndTouch(final ThreadContext context, final IRubyObject id, final IRubyObject expiry) {
        final long timeout = environment.kvTimeout();
        return getAndTouch(context, id.asJavaString(), (int) expiry.convertToInteger().getLongValue())
                .timeout(timeout, TimeUnit.MILLISECONDS)
                .toBlocking()
                .singleOrDefault(context.nil);
    }

    private Observable<IRubyObject> getAndTouch(final ThreadContext context, final String id, int expiry) {
        return core
                .<GetResponse>send(new GetRequest(id, bucket, false, true, expiry))
                .filter(new Func1<GetResponse, Boolean>() {
                    @Override
                    public Boolean call(GetResponse getResponse) {
                        return getResponse.status() == ResponseStatus.SUCCESS;
                    }
                })
                .map(new Func1<GetResponse, IRubyObject>() {
                    @Override
                    public IRubyObject call(final GetResponse response) {
                        return newDocument(context, id, response.cas(), 0, response.content(), response.flags());
                    }
                });
    }


    @JRubyMethod(name = "get_and_lock")
    public IRubyObject getAndLock(final ThreadContext context, final IRubyObject id, final IRubyObject expiry) {
        final long timeout = environment.kvTimeout();
        return getAndLock(context, id.asJavaString(), (int) expiry.convertToInteger().getLongValue())
                .timeout(timeout, TimeUnit.MILLISECONDS)
                .toBlocking()
                .singleOrDefault(context.nil);
    }

    private Observable<IRubyObject> getAndLock(final ThreadContext context, final String id, int lockTime) {
        return core
                .<GetResponse>send(new GetRequest(id, bucket, true, false, lockTime))
                .filter(new Func1<GetResponse, Boolean>() {
                    @Override
                    public Boolean call(GetResponse getResponse) {
                        return getResponse.status() == ResponseStatus.SUCCESS;
                    }
                })
                .map(new Func1<GetResponse, IRubyObject>() {
                    @Override
                    public IRubyObject call(final GetResponse response) {
                        return newDocument(context, id, response.cas(), 0, response.content(), response.flags());
                    }
                });
    }

    @JRubyMethod(name = "unlock")
    public IRubyObject unlock(final ThreadContext context, final IRubyObject document) {
        final long timeout = environment.kvTimeout();
        if (!documentClass.isInstance(document)) {
            throw context.getRuntime().newTypeError("Expected Couchbase::Document or descendant");
        }
        return unlock(context, (Document) document)
                .timeout(timeout, TimeUnit.MILLISECONDS)
                .toBlocking()
                .single();
    }

    private Observable<IRubyObject> unlock(final ThreadContext context, final Document document) {
        final Ruby runtime = context.getRuntime();
        return core
                .<UnlockResponse>send(new UnlockRequest(document.id(context), document.cas(context), bucket))
                .flatMap(new Func1<UnlockResponse, Observable<IRubyObject>>() {
                    @Override
                    public Observable<IRubyObject> call(UnlockResponse response) {
                        if (response.status() == ResponseStatus.NOT_EXISTS) {
                            return Observable.error(new DocumentDoesNotExistException());
                        }
                        if (response.status() == ResponseStatus.FAILURE) {
                            return Observable.error(new CASMismatchException());
                        }
                        return Observable.just((IRubyObject) (response.status().isSuccess() ? runtime.getTrue() : runtime.getFalse()));
                    }
                });
    }

    @JRubyMethod(name = "touch")
    public IRubyObject touch(final ThreadContext context, final IRubyObject id, final IRubyObject expiry) {
        final long timeout = environment.kvTimeout();
        return touch(context, id.asJavaString(), (int) expiry.convertToInteger().getLongValue())
                .timeout(timeout, TimeUnit.MILLISECONDS)
                .toBlocking()
                .single();
    }

    private Observable<IRubyObject> touch(final ThreadContext context, final String id, final int expiry) {
        final Ruby runtime = context.getRuntime();
        return core
                .<TouchResponse>send(new TouchRequest(id, expiry, bucket))
                .flatMap(new Func1<TouchResponse, Observable<IRubyObject>>() {
                    @Override
                    public Observable<IRubyObject> call(TouchResponse response) {
                        if (response.status() == ResponseStatus.NOT_EXISTS) {
                            return Observable.error(new DocumentDoesNotExistException());
                        }
                        return Observable.just((IRubyObject) (response.status().isSuccess() ? runtime.getTrue() : runtime.getFalse()));
                    }
                });
    }

    @JRubyMethod(name = "append", required = 1, optional = 1)
    public IRubyObject append(final ThreadContext context, final IRubyObject[] args) {
        final long timeout = environment.kvTimeout();
        final IRubyObject document = args[0];
        Observe.PersistTo persistTo = Observe.PersistTo.NONE;
        Observe.ReplicateTo replicateTo = Observe.ReplicateTo.NONE;
        if (!documentClass.isInstance(document)) {
            throw context.getRuntime().newTypeError("Expected Couchbase::Document or descendant");
        }
        if (args.length == 2 && args[1] instanceof RubyHash) {
            RubyHash options = (RubyHash) args[1];
            assertOptions(context, options, symPersistTo, symReplicateTo);
            if (options.containsKey(symPersistTo)) {
                persistTo = getPersistToOption(context, options);
            }
            if (options.containsKey(symReplicateTo)) {
                replicateTo = getReplicateToOption(context, options);
            }
        }
        return append(context, (Document) document, persistTo, replicateTo)
                .timeout(timeout, TimeUnit.MILLISECONDS)
                .toBlocking()
                .single();
    }

    private Observable<IRubyObject> append(final ThreadContext context, final Document document,
                                           final Observe.PersistTo persistTo,
                                           final Observe.ReplicateTo replicateTo) {
        final Tuple2<ByteBuf, Integer> blob = transcoder.dump(context, document);
        Observable<IRubyObject> observable = core
                .<AppendResponse>send(new AppendRequest(document.id(context), document.cas(context), blob.value1(), bucket))
                .flatMap(new Func1<AppendResponse, Observable<IRubyObject>>() {
                    @Override
                    public Observable<IRubyObject> call(AppendResponse response) {
                        if (response.status() == ResponseStatus.FAILURE) {
                            return Observable.error(new DocumentDoesNotExistException());
                        }
                        return Observable.just(newDocument(context, document.id(context), response.cas(), document.expiry(context), document.content(context)));
                    }
                });

        if (replicateTo == Observe.ReplicateTo.NONE && persistTo == Observe.PersistTo.NONE) {
            return observable;
        } else {
            return observable.flatMap(new Func1<IRubyObject, Observable<IRubyObject>>() {
                @Override
                public Observable<IRubyObject> call(final IRubyObject object) {
                    Document doc = (Document) object;
                    return Observe
                            .call(core, bucket, doc.id(context), doc.cas(context), false, persistTo, replicateTo)
                            .map(new Func1<Boolean, IRubyObject>() {
                                @Override
                                public IRubyObject call(Boolean aBoolean) {
                                    return object;
                                }
                            }).onErrorResumeNext(new Func1<Throwable, Observable<IRubyObject>>() {
                                @Override
                                public Observable<IRubyObject> call(Throwable throwable) {
                                    return Observable.error(new DurabilityException("Durability constraint failed.", throwable));
                                }
                            });
                }
            });
        }
    }

    @JRubyMethod(name = "prepend", required = 1, optional = 1)
    public IRubyObject prepend(final ThreadContext context, final IRubyObject[] args) {
        final long timeout = environment.kvTimeout();
        final IRubyObject document = args[0];
        Observe.PersistTo persistTo = Observe.PersistTo.NONE;
        Observe.ReplicateTo replicateTo = Observe.ReplicateTo.NONE;
        if (!documentClass.isInstance(document)) {
            throw context.getRuntime().newTypeError("Expected Couchbase::Document or descendant");
        }
        if (args.length == 2 && args[1] instanceof RubyHash) {
            RubyHash options = (RubyHash) args[1];
            assertOptions(context, options, symPersistTo, symReplicateTo);
            if (options.containsKey(symPersistTo)) {
                persistTo = getPersistToOption(context, options);
            }
            if (options.containsKey(symReplicateTo)) {
                replicateTo = getReplicateToOption(context, options);
            }
        }
        return prepend(context, (Document) document, persistTo, replicateTo)
                .timeout(timeout, TimeUnit.MILLISECONDS)
                .toBlocking()
                .single();
    }

    private Observable<IRubyObject> prepend(final ThreadContext context, final Document document,
                                            final Observe.PersistTo persistTo,
                                            final Observe.ReplicateTo replicateTo) {
        final Tuple2<ByteBuf, Integer> blob = transcoder.dump(context, document);
        Observable<IRubyObject> observable = core
                .<PrependResponse>send(new PrependRequest(document.id(context), document.cas(context), blob.value1(), bucket))
                .flatMap(new Func1<PrependResponse, Observable<IRubyObject>>() {
                    @Override
                    public Observable<IRubyObject> call(PrependResponse response) {
                        if (response.status() == ResponseStatus.FAILURE) {
                            return Observable.error(new DocumentDoesNotExistException());
                        }
                        return Observable.just(newDocument(context, document.id(context), response.cas(), document.expiry(context), document.content(context)));
                    }
                });

        if (replicateTo == Observe.ReplicateTo.NONE && persistTo == Observe.PersistTo.NONE) {
            return observable;
        } else {
            return observable.flatMap(new Func1<IRubyObject, Observable<IRubyObject>>() {
                @Override
                public Observable<IRubyObject> call(final IRubyObject object) {
                    Document doc = (Document) object;
                    return Observe
                            .call(core, bucket, doc.id(context), doc.cas(context), false, persistTo, replicateTo)
                            .map(new Func1<Boolean, IRubyObject>() {
                                @Override
                                public IRubyObject call(Boolean aBoolean) {
                                    return object;
                                }
                            }).onErrorResumeNext(new Func1<Throwable, Observable<IRubyObject>>() {
                                @Override
                                public Observable<IRubyObject> call(Throwable throwable) {
                                    return Observable.error(new DurabilityException("Durability constraint failed.", throwable));
                                }
                            });
                }
            });
        }
    }

    @JRubyMethod(name = "remove", required = 1, optional = 1)
    public IRubyObject remove(final ThreadContext context, final IRubyObject[] args) {
        final long timeout = environment.kvTimeout();
        final IRubyObject document = args[0];
        Observe.PersistTo persistTo = Observe.PersistTo.NONE;
        Observe.ReplicateTo replicateTo = Observe.ReplicateTo.NONE;
        if (!documentClass.isInstance(document)) {
            throw context.getRuntime().newTypeError("Expected Couchbase::Document or descendant");
        }
        if (args.length == 2 && args[1] instanceof RubyHash) {
            RubyHash options = (RubyHash) args[1];
            assertOptions(context, options, symPersistTo, symReplicateTo);
            if (options.containsKey(symPersistTo)) {
                persistTo = getPersistToOption(context, options);
            }
            if (options.containsKey(symReplicateTo)) {
                replicateTo = getReplicateToOption(context, options);
            }
        }
        return remove(context, (Document) document, persistTo, replicateTo)
                .timeout(timeout, TimeUnit.MILLISECONDS)
                .toBlocking()
                .single();
    }

    private Observable<IRubyObject> remove(final ThreadContext context, final Document document,
                                           final Observe.PersistTo persistTo,
                                           final Observe.ReplicateTo replicateTo) {
        Observable<IRubyObject> observable = core
                .<RemoveResponse>send(new RemoveRequest(document.id(context), document.cas(context), bucket))
                .flatMap(new Func1<RemoveResponse, Observable<IRubyObject>>() {
                    @Override
                    public Observable<IRubyObject> call(RemoveResponse response) {
                        if (response.status() == ResponseStatus.FAILURE) {
                            return Observable.error(new DocumentDoesNotExistException());
                        }
                        return Observable.just(newDocument(context, document.id(context), response.cas(), document.expiry(context), document.content(context)));
                    }
                });

        if (replicateTo == Observe.ReplicateTo.NONE && persistTo == Observe.PersistTo.NONE) {
            return observable;
        } else {
            return observable.flatMap(new Func1<IRubyObject, Observable<IRubyObject>>() {
                @Override
                public Observable<IRubyObject> call(final IRubyObject object) {
                    Document doc = (Document) object;
                    return Observe
                            .call(core, bucket, doc.id(context), doc.cas(context), true, persistTo, replicateTo)
                            .map(new Func1<Boolean, IRubyObject>() {
                                @Override
                                public IRubyObject call(Boolean aBoolean) {
                                    return object;
                                }
                            }).onErrorResumeNext(new Func1<Throwable, Observable<IRubyObject>>() {
                                @Override
                                public Observable<IRubyObject> call(Throwable throwable) {
                                    return Observable.error(new DurabilityException("Durability constraint failed.", throwable));
                                }
                            });
                }
            });
        }
    }

    @JRubyMethod(name = "close")
    public IRubyObject close(final ThreadContext context) {
        final long timeout = environment.managementTimeout();
        return closeAsync(context)
                .timeout(timeout, TimeUnit.MILLISECONDS)
                .toBlocking()
                .single();
    }

    private Observable<IRubyObject> closeAsync(final ThreadContext context) {
        final Ruby runtime = context.getRuntime();
        return core
                .<CloseBucketResponse>send(new CloseBucketRequest(bucket))
                .flatMap(new Func1<CloseBucketResponse, Observable<IRubyObject>>() {
                    @Override
                    public Observable<IRubyObject> call(CloseBucketResponse response) {
                        return Observable.just((IRubyObject) (response.status().isSuccess() ? runtime.getTrue() : runtime.getFalse()));
                    }
                });
    }

    @JRubyMethod(name = "query", required = 2, optional = 1)
    public IRubyObject query(final ThreadContext context, final IRubyObject[] args) {
        final long timeout = environment.viewTimeout();
        final String design = args[0].asJavaString();
        final String view = args[1].asJavaString();
        final StringBuilder query = new StringBuilder();

        if (args.length == 3 && args[2] instanceof RubyHash) {
            RubyHash options = (RubyHash) args[2];
            getQueryParams(context, options, query);
        }
        return query(context, design, view, false, query.toString())
                .timeout(timeout, TimeUnit.MILLISECONDS)
                .toBlocking()
                .single();
    }

    public Observable<IRubyObject> query(final ThreadContext context, final String design, final String view,
                                         final boolean isDevelopment, final String query) {
        final Ruby runtime = context.getRuntime();
        final ViewQueryRequest request = new ViewQueryRequest(design, view, isDevelopment, query, bucket, password);
        return core.<ViewQueryResponse>send(request)
                .flatMap(new Func1<ViewQueryResponse, Observable<IRubyObject>>() {
                    @Override
                    public Observable<IRubyObject> call(final ViewQueryResponse response) {
                        String info = response.info().toBlocking().single().toString(CharsetUtil.UTF_8);
                        List<String> rows = response.rows().toList().map(new Func1<List<ByteBuf>, List<String>>() {
                            @Override
                            public List<String> call(List<ByteBuf> rows) {
                                List<String> res = new ArrayList<String>();
                                for (ByteBuf row : rows) {
                                    res.add(row.toString(CharsetUtil.UTF_8));
                                }
                                return res;
                            }
                        }).toBlocking().single();
                        return Observable.just((IRubyObject) new ViewResult(runtime, viewResultClass, response.status(), info, rows, null, null));
                    }
                });
    }

    private Observe.PersistTo getPersistToOption(final ThreadContext context, final RubyHash options) {
        final RubyFixnum val = (RubyFixnum) options.op_aref(context, symPersistTo);
        switch ((int) val.getLongValue()) {
            case 0:
                return Observe.PersistTo.NONE;
            case 1:
                return Observe.PersistTo.MASTER;
            case 2:
                return Observe.PersistTo.TWO;
            case 3:
                return Observe.PersistTo.THREE;
            case 4:
                return Observe.PersistTo.FOUR;
            default:
                throw context.getRuntime().newArgumentError("persist_to should be in range (0..4)");
        }
    }

    private Observe.ReplicateTo getReplicateToOption(final ThreadContext context, final RubyHash options) {
        final RubyFixnum val = (RubyFixnum) options.op_aref(context, symReplicateTo);
        switch ((int) val.getLongValue()) {
            case 0:
                return Observe.ReplicateTo.NONE;
            case 1:
                return Observe.ReplicateTo.ONE;
            case 2:
                return Observe.ReplicateTo.TWO;
            case 3:
                return Observe.ReplicateTo.THREE;
            default:
                throw context.getRuntime().newArgumentError("replicate_to should be in range (0..3)");
        }
    }

    private void assertOptions(final ThreadContext context, final RubyHash options, IRubyObject... knownKeys) {
        Ruby runtime = context.getRuntime();
        Set keys = new HashSet(options.keySet());
        for (IRubyObject knownKey : knownKeys) {
            keys.remove(knownKey);
        }
        if (!keys.isEmpty()) {
            RubyArray unknownKeys = runtime.newArray();
            unknownKeys.addAll(keys);
            throw runtime.newArgumentError("unknown option(s): " + unknownKeys);
        }
    }

    private void getQueryParams(ThreadContext context, RubyHash options, StringBuilder query) {
        final Ruby runtime = context.getRuntime();

        assertOptions(context, options, symStale, symDebug, symSkip, symGroupLevel, symGroup, symOnError,
                symDescending, symInclusiveEnd, symStartkey, symStartkeyDocid, symEndkey, symEndkeyDocid,
                symKeys, symKey, symBBox, symReduce);
        if (options.containsKey(symStale)) {
            IRubyObject opt = options.op_aref(context, symStale);
            String val;
            if (opt == runtime.getFalse()) {
                val = "false";
            } else if (opt == symOk) {
                val = "ok";
            } else if (opt == symUpdateAfter) {
                val = "update_after";
            } else {
                throw runtime.newArgumentError("Invalid value for :stale, should be false, :ok or :update_after");
            }
            query.append("&stale=" + val);
        }
        if (options.containsKey(symDebug)) {
            IRubyObject opt = options.op_aref(context, symDebug);
            query.append("&debug=" + opt.isTrue());
        }
        if (options.containsKey(symGroup)) {
            IRubyObject opt = options.op_aref(context, symGroup);
            query.append("&group=" + opt.isTrue());
        }
        if (options.containsKey(symOnError)) {
            IRubyObject opt = options.op_aref(context, symOnError);
            query.append("&on_error=" + opt.isTrue());
        }
        if (options.containsKey(symReduce)) {
            IRubyObject opt = options.op_aref(context, symReduce);
            query.append("&reduce=" + opt.isTrue());
        }
        if (options.containsKey(symDescending)) {
            IRubyObject opt = options.op_aref(context, symDescending);
            query.append("&descending=" + opt.isTrue());
        }
        if (options.containsKey(symSkip)) {
            IRubyObject opt = options.op_aref(context, symSkip);
            long val = ((RubyFixnum) opt).getLongValue();
            query.append("&skip=" + val);
        }
        if (options.containsKey(symGroupLevel)) {
            IRubyObject opt = options.op_aref(context, symGroupLevel);
            long val = ((RubyFixnum) opt).getLongValue();
            query.append("&group_level=" + val);
        }
        if (options.containsKey(symDescending)) {
            IRubyObject opt = options.op_aref(context, symDescending);
            query.append("&descending=" + opt.isTrue());
        }
        if (options.containsKey(symStartkey)) {
            IRubyObject opt = options.op_aref(context, symStartkey);
            query.append("&startkey=" + toJson(context, opt));
        }
        if (options.containsKey(symStartkeyDocid)) {
            IRubyObject opt = options.op_aref(context, symStartkeyDocid);
            query.append("&startkey_docid=" + toJson(context, opt));
        }
        if (options.containsKey(symEndkey)) {
            IRubyObject opt = options.op_aref(context, symEndkey);
            query.append("&endkey=" + toJson(context, opt));
        }
        if (options.containsKey(symEndkeyDocid)) {
            IRubyObject opt = options.op_aref(context, symEndkeyDocid);
            query.append("&endkey_docid=" + toJson(context, opt));
        }
        if (options.containsKey(symKeys)) {
            IRubyObject opt = options.op_aref(context, symKeys);
            query.append("&keys=" + toJson(context, opt));
        }
        if (options.containsKey(symKey)) {
            IRubyObject opt = options.op_aref(context, symKey);
            query.append("&key=" + toJson(context, opt));
        }
        if (options.containsKey(symBBox)) {
            IRubyObject opt = options.op_aref(context, symBBox);
            query.append("&bbox=" + toJson(context, opt));
        }
        query.replace(0, 1, "");
    }

    private String toJson(ThreadContext context, IRubyObject object) {
        try {
            return URLEncoder.encode(multiJsonModule.callMethod("dump", object).asJavaString(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Could not prepare view argument: " + e);
        }
    }

    private IRubyObject newDocument(ThreadContext context, String id, long cas, int i, IRubyObject content) {
        final Ruby runtime = context.getRuntime();
        return new Document(runtime, documentClass, id, cas, 0, content);
    }

    private IRubyObject newDocument(ThreadContext context, String id, long cas, int i, ByteBuf content, int flags) {
        final Ruby runtime = context.getRuntime();
        return new Document(runtime, documentClass, id, cas, 0,
                transcoder.load(context, content.toString(CharsetUtil.UTF_8), flags));
    }
}
