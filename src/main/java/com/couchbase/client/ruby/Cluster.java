package com.couchbase.client.ruby;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyBoolean;
import org.jruby.RubyClass;
import org.jruby.RubyObject;
import org.jruby.RubyString;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.anno.JRubyModule;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.internal.DiagnosticsReport;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.auth.Authenticator;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.transcoder.Transcoder;

@JRubyClass(name = "Com::Couchbase::Client::Ruby::Couchbase::Cluster")
public class Cluster extends RubyObject implements com.couchbase.client.java.Cluster {

	private static final long serialVersionUID = -5261512637534037450L;
	private CouchbaseCluster cluster;
	
	public Cluster(Ruby runtime, RubyClass metaClass) {
		super(runtime, metaClass);
	}

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
            for (IRubyObject node : ((RubyArray) nodes).toJavaArray()) {
                seedNodes.add(node.asJavaString());
            }
    		this.cluster = CouchbaseCluster.create(seedNodes);
    		return context.nil;
        }
        return context.nil;
    }
    
    @JRubyMethod(name = "authenticate", required = 2)
    public IRubyObject authenticate(ThreadContext context, RubyString userName, RubyString password) {
    	cluster.authenticate(userName.asJavaString(), password.asJavaString());
    	return context.nil;
    }
    
    @JRubyMethod(name = "disconnect")
	public IRubyObject disconnect(ThreadContext context) {
		return RubyBoolean.newBoolean(context.getRuntime(), this.cluster.disconnect());
	}

	@Override
	public Boolean disconnect() {
		return this.cluster.disconnect();
	}

	@Override
	public Bucket openBucket() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Bucket openBucket(long timeout, TimeUnit timeUnit) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Bucket openBucket(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Bucket openBucket(String name, long timeout, TimeUnit timeUnit) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Bucket openBucket(String name, List<Transcoder<? extends Document, ?>> transcoders) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Bucket openBucket(String name, List<Transcoder<? extends Document, ?>> transcoders, long timeout,
			TimeUnit timeUnit) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Bucket openBucket(String name, String password) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Bucket openBucket(String name, String password, long timeout, TimeUnit timeUnit) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Bucket openBucket(String name, String password, List<Transcoder<? extends Document, ?>> transcoders) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Bucket openBucket(String name, String password, List<Transcoder<? extends Document, ?>> transcoders,
			long timeout, TimeUnit timeUnit) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public N1qlQueryResult query(N1qlQuery query) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public N1qlQueryResult query(N1qlQuery query, long timeout, TimeUnit timeUnit) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ClusterManager clusterManager(String username, String password) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ClusterManager clusterManager() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public Boolean disconnect(long timeout, TimeUnit timeUnit) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ClusterFacade core() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public com.couchbase.client.java.Cluster authenticate(Authenticator auth) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public com.couchbase.client.java.Cluster authenticate(String username, String password) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DiagnosticsReport diagnostics() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DiagnosticsReport diagnostics(String reportId) {
		// TODO Auto-generated method stub
		return null;
	}

}
