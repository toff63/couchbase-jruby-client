package com.couchbase.client.ruby;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyBoolean;
import org.jruby.RubyClass;
import org.jruby.RubyFixnum;
import org.jruby.RubyObject;
import org.jruby.RubyString;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

import com.couchbase.client.java.document.JsonLongDocument;
import com.couchbase.client.java.document.RawJsonDocument;

import rx.Observable;

@JRubyClass(name = "Couchbase::Bucket")
public class Bucket extends RubyObject {

	private static final long serialVersionUID = 1L;
	private com.couchbase.client.java.Bucket bucket;
	private final RubyClass documentClass;
	

	public Bucket(final Ruby runtime, final RubyClass metaClass) {
		this(runtime, metaClass,() -> null);
	}

	public Bucket(Ruby runtime, RubyClass metaClass, Supplier<com.couchbase.client.java.Bucket> constructor) {
		super(runtime, metaClass);
		bucket = constructor.get();
		documentClass = runtime.getModule("Couchbase").getClass("Document");
	}

	public Bucket(Ruby runtime, RubyClass bucketClass, String name, Function<String, com.couchbase.client.java.Bucket> constructor) {
		super(runtime, bucketClass);
		bucket = constructor.apply(name);
		documentClass = runtime.getModule("Couchbase").getClass("Document");
	}

	public Bucket(Ruby runtime, RubyClass bucketClass, long timeout,  Function<Long, com.couchbase.client.java.Bucket> constructor) {
		super(runtime, bucketClass);
		bucket = constructor.apply(timeout);
		documentClass = runtime.getModule("Couchbase").getClass("Document");
	}

	public Bucket(Ruby runtime, RubyClass bucketClass, String name, long timeout, BiFunction<String, Long, com.couchbase.client.java.Bucket> constructor) {
		super(runtime, bucketClass);
		bucket = constructor.apply(name, timeout);
		documentClass = runtime.getModule("Couchbase").getClass("Document");
	}

	public Bucket(Ruby runtime, RubyClass bucketClass, String name, String password, long timeout, Function<String, BiFunction<String, Long, com.couchbase.client.java.Bucket>> constructor) {
		super(runtime, bucketClass);
		bucket = constructor.apply(name).apply(password, timeout);
		documentClass = runtime.getModule("Couchbase").getClass("Document");
	}

	public Bucket(Ruby runtime, RubyClass bucketClass, String name, String password, BiFunction<String, String, com.couchbase.client.java.Bucket> constructor) {
		super(runtime, bucketClass);
		bucket = constructor.apply(name, password);
		documentClass = runtime.getModule("Couchbase").getClass("Document");
	}

	@JRubyMethod(name = "insert", required = 1)
	public IRubyObject insert(ThreadContext context, IRubyObject document) {
		assertDocumentType(context, document);
		RawJsonDocument doc = ((com.couchbase.client.ruby.Document) document).toJavaDocument(context);
		Function<RawJsonDocument, RawJsonDocument> insert = getInsert(doc.cas());
		RawJsonDocument d = insert.apply(doc);
		return new com.couchbase.client.ruby.Document(context.runtime, documentClass, d.id(), d.cas(), d.expiry(), d.content());
	}


	@JRubyMethod(name = "incr", required = 2)
	public IRubyObject increment(ThreadContext context, RubyString key, IRubyObject rubyDelta) {
		Long delta = ((RubyFixnum) rubyDelta).getLongValue();
		JsonLongDocument d = bucket.counter(key.asJavaString(), delta);
		return new com.couchbase.client.ruby.Document(context.runtime, documentClass, d.id(), d.cas(), d.expiry(), String.valueOf(d.content()));
	}
	
	private Function<RawJsonDocument, RawJsonDocument> getInsert(long cas) {
		if (cas == 0) return doc -> bucket.upsert(doc);
		else return doc -> bucket.replace(doc);
	}

	private void assertDocumentType(ThreadContext context, IRubyObject document) {
		Boolean isDocument = documentClass.isInstance(document);
		if (!isDocument) {
			throw context.getRuntime().newTypeError("Expected Couchbase::Document or descendant");
		}
	}
	
	@JRubyMethod(name = "get", required = 1)
	public IRubyObject get(ThreadContext context, RubyString id) {
		RawJsonDocument json = bucket.get(id.asJavaString(), RawJsonDocument.class);
		if (json == null) return context.nil;
		return new com.couchbase.client.ruby.Document(context.runtime, documentClass, json.id(), json.cas(), json.expiry(), json.content());
	}
	
	@JRubyMethod(name = "mget", required = 1)
	public IRubyObject mget(ThreadContext context, IRubyObject rubyIds) {
		@SuppressWarnings("unchecked")
		List<String> ids = (List<String>) rubyIds;
		List<Document> docs = Observable.from(ids)
				.map(id -> bucket.get(id, RawJsonDocument.class))
				.map(json -> new com.couchbase.client.ruby.Document(context.runtime, documentClass, json.id(), json.cas(), json.expiry(), json.content()))
				.toList()
				.toBlocking()
				.single();
		return RubyArray.newArray(context.runtime,  docs);
	}


	@JRubyMethod(name = "close")
	public IRubyObject close(final ThreadContext context) {
		return RubyBoolean.newBoolean(context.getRuntime(), bucket.close());
	}

}
