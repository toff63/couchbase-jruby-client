package com.couchbase.client.ruby;

import java.io.IOException;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyModule;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.BasicLibraryService;

public class CouchbaseService implements BasicLibraryService {

	@Override
	public boolean basicLoad(Ruby runtime) throws IOException {
		RubyModule couchbase = runtime.defineModule("Couchbase");

		couchbase.defineClassUnder("Cluster", runtime.getObject(), new ObjectAllocator() {
			public IRubyObject allocate(Ruby ruby, RubyClass rubyClass) {
				return new Cluster(ruby, rubyClass);
			}
		}).defineAnnotatedMethods(Cluster.class);
		return true;
	}

}
