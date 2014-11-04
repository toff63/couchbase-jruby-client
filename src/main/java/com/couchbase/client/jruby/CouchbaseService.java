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

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyModule;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.BasicLibraryService;

import java.io.IOException;

/**
 * @author Sergey Avseyev
 */
public class CouchbaseService implements BasicLibraryService {
    @Override
    public boolean basicLoad(Ruby runtime) throws IOException {
        if (runtime.getModule("MultiJson") == null) {
            throw runtime.newRuntimeError("Cannot found MultiJson module");
        }
        RubyModule couchbase = runtime.defineModule("Couchbase");

        couchbase.defineClassUnder("Cluster", runtime.getObject(), new ObjectAllocator() {
            public IRubyObject allocate(Ruby ruby, RubyClass rubyClass) {
                return new Cluster(ruby, rubyClass);
            }
        }).defineAnnotatedMethods(Cluster.class);

        couchbase.defineClassUnder("Bucket", runtime.getObject(), new ObjectAllocator() {
            public IRubyObject allocate(Ruby ruby, RubyClass rubyClass) {
                return new Bucket(ruby, rubyClass);
            }
        }).defineAnnotatedMethods(Bucket.class);

        couchbase.defineClassUnder("BucketManager", runtime.getObject(), new ObjectAllocator() {
            public IRubyObject allocate(Ruby ruby, RubyClass rubyClass) {
                return new BucketManager(ruby, rubyClass);
            }
        }).defineAnnotatedMethods(BucketManager.class);

        RubyClass document = couchbase.defineClassUnder("Document", runtime.getObject(), new ObjectAllocator() {
            public IRubyObject allocate(Ruby ruby, RubyClass rubyClass) {
                return new Document(ruby, rubyClass);
            }
        });
        document.attr_reader(runtime.getCurrentContext(),
                new IRubyObject[]{
                        runtime.newSymbol("id"),
                        runtime.newSymbol("cas"),
                        runtime.newSymbol("expiry"),
                        runtime.newSymbol("content"),
                        runtime.newSymbol("transcode"),
                });
        document.defineAnnotatedMethods(Document.class);
        RubyClass view_result = couchbase.defineClassUnder("ViewResult", runtime.getObject(), new ObjectAllocator() {
            public IRubyObject allocate(Ruby ruby, RubyClass rubyClass) {
                return new ViewResult(ruby, rubyClass);
            }
        });
        view_result.attr_reader(runtime.getCurrentContext(),
                new IRubyObject[]{
                        runtime.newSymbol("status"),
                        runtime.newSymbol("info"),
                        runtime.newSymbol("rows"),
                        runtime.newSymbol("errors"),
                        runtime.newSymbol("debug"),
                });
        view_result.defineAnnotatedMethods(ViewResult.class);

        return true;
    }
}
