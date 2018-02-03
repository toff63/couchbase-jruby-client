package com.couchbase.client.ruby;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyBoolean;
import org.jruby.RubyClass;
import org.jruby.RubyHash;
import org.jruby.RubyInteger;
import org.jruby.RubyObject;
import org.jruby.RubyString;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.transcoder.RawJsonTranscoder;
import com.couchbase.client.java.transcoder.Transcoder;

@JRubyClass(name = "Couchbase::Cluster")
public class Cluster extends RubyObject {

	private static final long serialVersionUID = -5261512637534037450L;
	private CouchbaseCluster cluster;
	private List<Transcoder<? extends Document, ?>> transcoders = Collections.singletonList(new RawJsonTranscoder());

	public Cluster(Ruby runtime, RubyClass metaClass) {
		super(runtime, metaClass);
	}

    @SuppressWarnings("unchecked")
	@JRubyMethod(name = "initialize", optional = 1)
    public IRubyObject initialize(ThreadContext context, IRubyObject[] args) {
    	if (args.length == 0) {
    		this.cluster = CouchbaseCluster.create();
    		return context.nil;
    	}
        if (args.length > 0) {
            List<String> seedNodes = new ArrayList<String>();
            IRubyObject nodes = args[0];
            nodes.checkArrayType();
            for (IRubyObject node : ((RubyArray<IRubyObject>) nodes).toJavaArray()) {
                seedNodes.add(node.asJavaString());
            }
    		this.cluster = CouchbaseCluster.create(seedNodes);
    		return context.nil;
        }
        return context.nil;
    }
    
    public static void main(String[] args) {
    	CouchbaseCluster c = CouchbaseCluster.create();
    	c.authenticate("Administrator", "password");
    	String password = null;
		com.couchbase.client.java.Bucket b = c.openBucket("default");
		b.close();
	}
    
    @JRubyMethod(name = "authenticate", required = 2)
    public IRubyObject authenticate(ThreadContext context, RubyString userName, RubyString password) {
    	cluster.authenticate(userName.asJavaString(), password.asJavaString());
    	return context.nil;
    }
    
    @JRubyMethod(name = "disconnect", optional = 1)
	public IRubyObject disconnect(ThreadContext context, IRubyObject timeout) {
    	if (!(timeout instanceof RubyInteger)) return RubyBoolean.newBoolean(context.getRuntime(), this.cluster.disconnect());
    	Long javaTimeout = ((RubyInteger) timeout).getLongValue();
    	return RubyBoolean.newBoolean(context.getRuntime(), this.cluster.disconnect(javaTimeout, TimeUnit.SECONDS));
	}
    
    @JRubyMethod(name = "bucket", optional = 1)
    public IRubyObject openBucket(ThreadContext context, IRubyObject rubyOptions) {
    	Ruby runtime = context.getRuntime();
    	RubyClass bucketClass = runtime.getModule("Couchbase").getClass("Bucket");
    	Boolean receivedHash = rubyOptions instanceof RubyHash;
    	if (rubyOptions == null || !receivedHash) return openBucket(runtime, bucketClass);
    	RubyHash options = (RubyHash) rubyOptions;
    	if (options.isEmpty()) return openBucket(context.getRuntime(), bucketClass);
    	String name = (String) options.get(runtime.newSymbol("name"));
    	String password = (String) options.get(runtime.newSymbol("password"));
    	Long timeout = (Long) options.get(runtime.newSymbol("timeout"));
    	if (name != null && password != null && timeout != null)
    		return openBucket(runtime, bucketClass, name, password, timeout);
    	if (name != null && password != null) return openBucket(runtime, bucketClass, name, password);
    	if (name != null && timeout != null) return openBucket(runtime, bucketClass, name, timeout);
    	if (name != null) return openBucket(runtime, bucketClass, name);
    	if (timeout != null) return openBucket(runtime, bucketClass, timeout);
    	return openBucket(runtime, bucketClass);
    }
    
    public Bucket openBucket(Ruby runtime, RubyClass bucketClass) {
    	return new Bucket(runtime, bucketClass, () -> cluster.openBucket("default", transcoders));
    }

	public Bucket openBucket(Ruby runtime, RubyClass bucketClass, String bucketName) {
		return new Bucket(runtime, bucketClass, bucketName, (String name) -> cluster.openBucket(name, transcoders));
	}
	
	public Bucket openBucket(Ruby runtime, RubyClass bucketClass, long timeout) {
		return new Bucket(runtime, bucketClass, timeout, (Long t) -> cluster.openBucket("default", transcoders, t, TimeUnit.SECONDS));
	}

	public Bucket openBucket(Ruby runtime, RubyClass bucketClass, String name, long timeout) {
		return new Bucket(runtime, bucketClass, name, timeout, (String n, Long t) -> cluster.openBucket(n, transcoders, t, TimeUnit.SECONDS));

	}

	public Bucket openBucket(Ruby runtime, RubyClass bucketClass, String name, String password) {
		return new Bucket(runtime, bucketClass, name, password, (String n, String p) -> cluster.openBucket(n, p, transcoders));

	}

	public Bucket openBucket(Ruby runtime, RubyClass bucketClass, String name, String password, long timeout) {
		Function<String, BiFunction<String, Long, com.couchbase.client.java.Bucket>> constructor = (n) -> (p, t) -> cluster.openBucket(n, p, transcoders, t, TimeUnit.SECONDS);
		return new Bucket(runtime, bucketClass, name, password, timeout, constructor);
	}


}
