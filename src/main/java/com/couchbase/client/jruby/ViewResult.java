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

import com.couchbase.client.core.message.ResponseStatus;
import org.jruby.*;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Sergey Avseyev
 */
@JRubyClass(name = "Couchbase::ViewResult")
public class ViewResult extends RubyObject {
    private final RubySymbol ivInfo;
    private final RubySymbol ivRows;
    private final RubySymbol ivErrors;
    private final RubySymbol ivDebug;
    private final RubySymbol ivStatus;
    private final RubySymbol symInfo;
    private final RubySymbol symRows;
    private final RubySymbol symErrors;
    private final RubySymbol symDebug;
    private final RubySymbol symStatus;
    private final RubySymbol symExists;
    private final RubySymbol symFailure;
    private final RubySymbol symNotExists;
    private final RubySymbol symRetry;
    private final RubySymbol symSuccess;
    private final RubyModule multiJsonModule;

    public ViewResult(Ruby runtime, RubyClass metaClass) {
        this(runtime, metaClass, ResponseStatus.SUCCESS, null, null, null, null);
    }

    public ViewResult(Ruby runtime, RubyClass metaClass, ResponseStatus status, String info, List<String> rows, String errors, String debug) {
        super(runtime, metaClass);
        multiJsonModule = runtime.getModule("MultiJson");
        ivInfo = runtime.newSymbol("@info");
        ivRows = runtime.newSymbol("@rows");
        ivErrors = runtime.newSymbol("@errors");
        ivDebug = runtime.newSymbol("@debug");
        ivStatus = runtime.newSymbol("@status");
        symInfo = runtime.newSymbol("info");
        symRows = runtime.newSymbol("rows");
        symErrors = runtime.newSymbol("errors");
        symDebug = runtime.newSymbol("debug");
        symStatus = runtime.newSymbol("status");
        symExists = runtime.newSymbol("exists");
        symFailure = runtime.newSymbol("failure");
        symNotExists = runtime.newSymbol("not_exists");
        symRetry = runtime.newSymbol("retry");
        symSuccess = runtime.newSymbol("success");

        RubyArray rowsAry = null;
        if (rows != null) {
            rowsAry = runtime.newArray();
            for (String row : rows) {
                rowsAry.add(loadJson(runtime, row));
            }
        }
        RubySymbol statusSym;
        switch (status) {
            case EXISTS:
                statusSym = symExists;
                break;
            case FAILURE:
                statusSym = symFailure;
                break;
            case NOT_EXISTS:
                statusSym = symNotExists;
                break;
            case RETRY:
                statusSym = symRetry;
                break;
            case SUCCESS:
                statusSym = symSuccess;
                break;
            default:
                throw runtime.newArgumentError("unknown status code: " + status);
        }

        initialize(runtime.getCurrentContext(),
                new IRubyObject[]{
                        statusSym,
                        info == null ? runtime.getNil() : loadJson(runtime, info),
                        rows == null ? runtime.getNil() : rowsAry,
                        errors == null ? runtime.getNil() : loadJson(runtime, errors),
                        debug == null ? runtime.getNil() : loadJson(runtime, debug),
                });
    }

    @JRubyMethod(name = "initialize", optional = 5)
    public IRubyObject initialize(ThreadContext context, IRubyObject[] args) {
        Ruby runtime = context.getRuntime();
        if (args.length == 1 && args[0] instanceof RubyHash) {
            RubyHash attrs = (RubyHash) args[0];
            IRubyObject val;
            if (attrs.containsKey(symStatus)) {
                instance_variable_set(ivStatus, attrs.op_aref(context, symStatus));
            }
            if (attrs.containsKey(symInfo)) {
                instance_variable_set(ivInfo, attrs.op_aref(context, symInfo));
            }
            if (attrs.containsKey(symRows)) {
                instance_variable_set(ivRows, attrs.op_aref(context, symRows));
            }
            if (attrs.containsKey(symErrors)) {
                instance_variable_set(ivErrors, attrs.op_aref(context, symErrors));
            }
            if (attrs.containsKey(symDebug)) {
                instance_variable_set(ivDebug, attrs.op_aref(context, symDebug));
            }
        } else {
            if (args.length > 0) {
                instance_variable_set(ivStatus, args[0]);
            }
            if (args.length > 1) {
                instance_variable_set(ivInfo, args[1]);
            }
            if (args.length > 2) {
                instance_variable_set(ivRows, args[2]);
            }
            if (args.length > 3) {
                instance_variable_set(ivErrors, args[3]);
            }
            if (args.length > 4) {
                instance_variable_set(ivDebug, args[4]);
            }
        }
        return context.nil;
    }

    @JRubyMethod(name = "success?")
    public IRubyObject success(final ThreadContext context) {
        Ruby runtime = context.getRuntime();
        return instance_variable_get(context, ivStatus) == symSuccess ? runtime.getTrue() : runtime.getFalse();
    }

    public String info(ThreadContext context) {
        IRubyObject val = instance_variable_get(context, ivInfo);
        if (val.isNil()) {
            return null;
        } else {
            return val.asJavaString();
        }
    }

    public List<String> rows(ThreadContext context) {
        IRubyObject val = instance_variable_get(context, ivRows);
        List<String> res = new ArrayList<>();
        if (!val.isNil() && val instanceof RubyArray) {
            for (Object obj : ((RubyArray) val)) {
                res.add(((IRubyObject) obj).asJavaString());
            }
            return res;
        }
        return res;
    }

    public String errors(ThreadContext context) {
        IRubyObject val = instance_variable_get(context, ivErrors);
        if (val.isNil()) {
            return null;
        } else {
            return val.asJavaString();
        }
    }

    public String debug(ThreadContext context) {
        IRubyObject val = instance_variable_get(context, ivDebug);
        if (val.isNil()) {
            return null;
        } else {
            return val.asJavaString();
        }
    }

    private IRubyObject loadJson(Ruby runtime, String blob) {
        if (blob == null || blob.isEmpty()) {
            return runtime.getNil();
        } else {
            return multiJsonModule.callMethod("load", runtime.newString(blob));
        }
    }
}
