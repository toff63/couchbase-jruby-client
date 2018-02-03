package com.couchbase.client.ruby;

import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.java.document.Document;

public class JDocument implements Document<String> {

	private String id;
	private long cas;
	private int expiry;
	private String content;

	public JDocument(String id, long cas, int expiry, String content) {
		this.id = id;
		this.cas = cas;
		this.expiry = expiry;
		this.content = content;
	}
	
	@Override
	public String id() {
		return id;
	}

	@Override
	public String content() {
		return content;
	}

	@Override
	public long cas() {
		return cas;
	}

	@Override
	public int expiry() {
		return expiry;
	}

	@Override
	public MutationToken mutationToken() {
		return null;
	}

}
