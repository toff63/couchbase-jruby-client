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

import com.couchbase.client.core.lang.Tuple;
import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyModule;
import org.jruby.exceptions.RaiseException;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

/**
 * @author Sergey Avseyev
 */
public class Transcoder {
    public static final int COMMON_FORMAT_MASK = 0x0F000000;
    public static final int JSON_COMMON_FLAGS = 2 << 24;
    public static final int JSON_LEGACY_FLAGS = 0;
    public static final int JSON_COMPAT_FLAGS = JSON_COMMON_FLAGS | JSON_LEGACY_FLAGS;
    private final RubyModule transcoderModule;
    private final RubyModule parseErrorClass;
    private final RubyClass documentClass;

    public Transcoder(RubyClass documentClass, RubyModule transcoderModule) {
       this.transcoderModule =  transcoderModule;
       this.documentClass = documentClass;
       parseErrorClass = transcoderModule.getClass("ParseError");
    }

    public Tuple2<ByteBuf, Integer> dump(ThreadContext context, Document object) {
        if (object.transcode(context)) {
            String content = transcoderModule.callMethod("dump", object.content(context)).asJavaString();
            return Tuple.create(Unpooled.copiedBuffer(content, CharsetUtil.UTF_8), JSON_COMPAT_FLAGS);
        } else {
            return Tuple.create(Unpooled.copiedBuffer(object.content(context).asJavaString(), CharsetUtil.UTF_8), JSON_COMPAT_FLAGS);
        }
    }

    public IRubyObject load(ThreadContext context, String blob, int flags) {
        Ruby runtime = context.getRuntime();
        IRubyObject rubyBlob = runtime.newString(blob);
        if ((flags & COMMON_FORMAT_MASK) == JSON_COMMON_FLAGS || flags == JSON_LEGACY_FLAGS) {
            try {
                return transcoderModule.callMethod("load", rubyBlob);
            } catch (RaiseException ex) {
                if (parseErrorClass.isInstance(ex.getException())) {
                    return rubyBlob;
                } else {
                    throw ex;
                }
            }
        } else {
            return rubyBlob;
        }
    }
}
