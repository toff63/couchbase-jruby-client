package com.couchbase.client.ruby;


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
import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyFixnum;
import org.jruby.RubyHash;
import org.jruby.RubyModule;
import org.jruby.RubyNumeric;
import org.jruby.RubyObject;
import org.jruby.RubyString;
import org.jruby.RubySymbol;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

import com.couchbase.client.java.document.RawJsonDocument;

/**
 * @author Sergey Avseyev
 */
@JRubyClass(name = "Couchbase::Document")
public class Document extends RubyObject {


	private static final long serialVersionUID = -8381537854469018976L;
	private final RubySymbol ivId;
    private final RubySymbol ivContent;
    private final RubySymbol ivCas;
    private final RubySymbol ivExpiry;
    private final RubySymbol ivTranscode;
    private final RubySymbol symId;
    private final RubySymbol symContent;
    private final RubySymbol symCas;
    private final RubySymbol symExpiry;
    private final RubySymbol symTranscode;
    private final RubyModule multiJsonModule;
    private final RubyModule parseErrorClass;
    
    public Document(Ruby runtime, RubyClass metaClass) {
        this(runtime, metaClass, null, 0, 0, (IRubyObject) null);
    }
    
    public Document(Ruby runtime, RubyClass metaClass, String id, long cas, int expiry, String json) {
        super(runtime, metaClass);
        multiJsonModule = runtime.getModule("MultiJson");
        parseErrorClass = multiJsonModule.getClass("ParseError");
        IRubyObject content =  multiJsonModule.callMethod("load", RubyString.newString(runtime, json));
        
        ivId = runtime.newSymbol("@id");
        ivContent = runtime.newSymbol("@content");
        ivCas = runtime.newSymbol("@cas");
        ivExpiry = runtime.newSymbol("@ttl");
        ivTranscode = runtime.newSymbol("@transcode");
        symId = runtime.newSymbol("id");
        symContent = runtime.newSymbol("content");
        symCas = runtime.newSymbol("cas");
        symExpiry = runtime.newSymbol("ttl");
        symTranscode = runtime.newSymbol("transcode");
        initialize(runtime.getCurrentContext(),
                new IRubyObject[]{
                        id == null ? runtime.getNil() : RubyString.newString(runtime, id),
                        content == null ? runtime.getNil() : content,
                        RubyFixnum.newFixnum(runtime, cas),
                        RubyFixnum.newFixnum(runtime, expiry),
                        runtime.getTrue()
                });    }
    
    public Document(Ruby runtime, RubyClass metaClass, String id, long cas, int expiry, IRubyObject content) {
        super(runtime, metaClass);
        multiJsonModule = runtime.getModule("MultiJson");
        parseErrorClass = multiJsonModule.getClass("ParseError");
        
        ivId = runtime.newSymbol("@id");
        ivContent = runtime.newSymbol("@content");
        ivCas = runtime.newSymbol("@cas");
        ivExpiry = runtime.newSymbol("@ttl");
        ivTranscode = runtime.newSymbol("@transcode");
        symId = runtime.newSymbol("id");
        symContent = runtime.newSymbol("content");
        symCas = runtime.newSymbol("cas");
        symExpiry = runtime.newSymbol("ttl");
        symTranscode = runtime.newSymbol("transcode");
        initialize(runtime.getCurrentContext(),
                new IRubyObject[]{
                        id == null ? runtime.getNil() : RubyString.newString(runtime, id),
                        content == null ? runtime.getNil() : content,
                        RubyFixnum.newFixnum(runtime, cas),
                        RubyFixnum.newFixnum(runtime, expiry),
                        runtime.getTrue()
                });
    }

    @JRubyMethod(name = "initialize", optional = 4)
    public IRubyObject initialize(ThreadContext context, IRubyObject[] args) {
        if (args.length == 1 && args[0] instanceof RubyHash) {
            RubyHash attrs = (RubyHash) args[0];
            if (attrs.containsKey(symId)) {
                instance_variable_set(ivId, attrs.op_aref(context, symId));
            }
            if (attrs.containsKey(symContent)) {
                instance_variable_set(ivContent, attrs.op_aref(context, symContent));
            }
            if (attrs.containsKey(symCas)) {
                instance_variable_set(ivCas, attrs.op_aref(context, symCas));
            }
            if (attrs.containsKey(symExpiry)) {
                instance_variable_set(ivExpiry, attrs.op_aref(context, symExpiry));
            }
            if (attrs.containsKey(symTranscode)) {
                instance_variable_set(ivTranscode, attrs.op_aref(context, symTranscode));
            }
        } else {
            if (args.length > 0) {
                instance_variable_set(ivId, args[0]);
            }
            if (args.length > 1) {
                instance_variable_set(ivContent, args[1]);
            }
            if (args.length > 2) {
                instance_variable_set(ivCas, args[2]);
            }
            if (args.length > 3) {
                instance_variable_set(ivExpiry, args[3]);
            }
            if (args.length > 4) {
                instance_variable_set(ivTranscode, args[4]);
            }
        }
        metaClass.addReadWriteAttribute(context, "id");
        metaClass.addReadWriteAttribute(context, "content");
        metaClass.addReadWriteAttribute(context, "cas");
        metaClass.addReadWriteAttribute(context, "expiry");
        return context.nil;
    }

    public String id(ThreadContext context) {
        IRubyObject val = instance_variable_get(context, ivId);
        if (val.isNil()) {
            return null;
        } else {
            return val.asJavaString();
        }
    }

    public IRubyObject content(ThreadContext context) {
    	return instance_variable_get(context, ivContent);
    }

    public long cas(ThreadContext context) {
        IRubyObject val = instance_variable_get(context, ivCas);
        if (val.isNil()) {
            return 0;
        } else {
            return ((RubyNumeric) val).getLongValue();
        }
    }

    public int expiry(ThreadContext context) {
        IRubyObject val = instance_variable_get(context, ivExpiry);
        if (val.isNil()) {
            return 0;
        } else {
            return (int) ((RubyNumeric) val).getLongValue();
        }
    }

    public boolean transcode(ThreadContext context) {
        return instance_variable_get(context, ivTranscode).isTrue();
    }
    
    public RawJsonDocument toJavaDocument(ThreadContext context){
        String content =  multiJsonModule.callMethod("dump", content(context)).asJavaString();
    	return RawJsonDocument.create(id(context), expiry(context), content, cas(context));
    }
}