describe Couchbase::Bucket, :cluster => true do
  before(:all) { @cluster.open_bucket("default").bucket_manager.flush }

  subject(:bucket) { @cluster.open_bucket("default") }

  specify 'get nonexistent with default' do
    expect(bucket.get('i-dont-exist')).to be_nil
  end

  specify 'double insert' do
    doc = Couchbase::Document.new('double-insert', '{"hello": "world"}')
    bucket.insert(doc)
    expect { bucket.insert(doc) }.to raise_error(Java::ComCouchbaseClientJrubyError::DocumentAlreadyExistsException)
  end

  specify 'insert and get' do
    bucket.insert(Couchbase::Document.new('insert', '{"hello": "world"}'))
    doc = bucket.get('insert')
    expect(doc.id).to eq('insert')
    expect(doc.content).to eq('{"hello": "world"}')
  end

  specify 'upsert and get' do
    bucket.upsert(Couchbase::Document.new('upsert', '{"hello": "world"}'))
    doc = bucket.get('upsert')
    expect(doc.id).to eq('upsert')
    expect(doc.content).to eq('{"hello": "world"}')
  end

  specify 'upsert and replace' do
    bucket.upsert(Couchbase::Document.new('upsert-r', '{"hello": "world"}'))
    doc = bucket.get('upsert-r')
    expect(doc.id).to eq('upsert-r')
    expect(doc.content).to eq('{"hello": "world"}')

    bucket.replace(Couchbase::Document.new('upsert-r', '{"hello": "replaced"}'))
    doc = bucket.get('upsert-r')
    expect(doc.id).to eq('upsert-r')
    expect(doc.content).to eq('{"hello": "replaced"}')
  end

  specify 'increment from counter' do
    doc1 = bucket.counter('incr-key', 10, :initial => 0)
    expect(doc1.content).to eq('0')

    doc2 = bucket.counter('incr-key', 10)
    expect(doc2.content).to eq('10')

    doc3 = bucket.counter('incr-key', 10)
    expect(doc3.content).to eq('20')

    expect(doc1.cas).not_to eq(doc2.cas)
    expect(doc1.cas).not_to eq(doc3.cas)
    expect(doc2.cas).not_to eq(doc3.cas)
  end

  specify 'decrement from counter' do
    doc1 = bucket.counter('decr-key', -10, :initial => 100)
    expect(doc1.content).to eq('100')

    doc2 = bucket.counter('decr-key', -10)
    expect(doc2.content).to eq('90')

    doc3 = bucket.counter('decr-key', -10)
    expect(doc3.content).to eq('80')

    expect(doc1.cas).not_to eq(doc2.cas)
    expect(doc1.cas).not_to eq(doc3.cas)
    expect(doc2.cas).not_to eq(doc3.cas)
  end

  specify 'get and touch' do
    upsert = bucket.upsert(Couchbase::Document.new('get-and-touch', '{"k": "v"}', 0, 3))
    expect(upsert).not_to be_nil
    expect(upsert.id).to eq('get-and-touch')

    sleep(2)

    touched = bucket.get_and_touch('get-and-touch', 3)
    expect(touched).not_to be_nil
    expect(touched.content).to eq('{"k": "v"}')

    sleep(2)

    got = bucket.get('get-and-touch')
    expect(got).not_to be_nil
    expect(got.content).to eq('{"k": "v"}')
  end

  specify 'get and lock' do
    upsert = bucket.upsert(Couchbase::Document.new('get-and-lock', '{"k": "v"}'))
    expect(upsert).not_to be_nil
    expect(upsert.id).to eq('get-and-lock')

    locked = bucket.get_and_lock('get-and-lock', 2)
    expect(locked.content).to eq('{"k": "v"}')

    expect do
      bucket.upsert(Couchbase::Document.new('get-and-lock', '{"k": "v"}'))
    end.to raise_error(Java::ComCouchbaseClientJrubyError::CASMismatchException)

    sleep(3)

    bucket.upsert(Couchbase::Document.new('get-and-lock', '{"k": "v"}'))
  end

  specify 'unlock' do
    upsert = bucket.upsert(Couchbase::Document.new('unlock', '{"k": "v"}'))
    expect(upsert).not_to be_nil
    expect(upsert.id).to eq('unlock')

    locked = bucket.get_and_lock('unlock', 15)
    expect(locked.content).to eq('{"k": "v"}')

    expect do
      bucket.upsert(Couchbase::Document.new('unlock', '{"k": "v"}'))
    end.to raise_error(Java::ComCouchbaseClientJrubyError::CASMismatchException)

    unlocked = bucket.unlock(locked)
    expect(unlocked).to be_true

    bucket.upsert(Couchbase::Document.new('unlock', '{"k": "v"}'))
  end

  specify 'touch' do
    upsert = bucket.upsert(Couchbase::Document.new(:id => 'touch', :content => '{"k": "v"}', :expiry => 3))
    expect(upsert).not_to be_nil
    expect(upsert.id).to eq('touch')

    sleep(2)

    touched = bucket.touch('touch', 3)
    expect(touched).to be_true

    sleep(2)

    loaded = bucket.get('touch')
    expect(loaded.content).to eq('{"k": "v"}')
  end

  specify 'append string' do
    upsert = bucket.upsert(Couchbase::Document.new(:id => 'append', :content => 'foo'))
    expect(upsert).not_to be_nil
    expect(upsert.id).to eq('append')

    bucket.append(Couchbase::Document.new(:id => 'append', :content => 'bar'))
    appended = bucket.get('append')
    expect(appended).not_to be_nil
    expect(appended.content).to eq('foobar')
  end

  specify 'prepend string' do
    upsert = bucket.upsert(Couchbase::Document.new(:id => 'prepend', :content => 'foo'))
    expect(upsert).not_to be_nil
    expect(upsert.id).to eq('prepend')

    bucket.prepend(Couchbase::Document.new(:id => 'prepend', :content => 'bar'))
    appended = bucket.get('prepend')
    expect(appended).not_to be_nil
    expect(appended.content).to eq('barfoo')
  end

  specify 'remove' do
    upsert = bucket.upsert(Couchbase::Document.new(:id => 'remove', :content => '{"k": "v"}'))
    expect(upsert).not_to be_nil
    expect(upsert.id).to eq('remove')

    removed = bucket.remove(Couchbase::Document.new(:id => 'remove'))
    expect(removed.cas).not_to be_nil
    expect(removed.cas).not_to eq(upsert.cas)
    expect(bucket.get('remove')).to be_nil
  end

  specify 'upsert with persistence' do
    bucket.upsert(Couchbase::Document.new(:id => 'persist-to-master', :content => '{"k": "v"}'),
                  :persist_to => 1, :replicate_to => 0)
  end

  specify 'remove with peristence' do
    bucket.upsert(Couchbase::Document.new(:id => 'persist-to-master', :content => '{"k": "v"}'),
                  :persist_to => 1, :replicate_to => 0)
    bucket.remove(Couchbase::Document.new(:id => 'persist-to-master'),
                  :persist_to => 1, :replicate_to => 0)
  end

  specify 'upsert and get from replica' do
    pending 'flush too slow in multi-node cluster'
    bucket.upsert(Couchbase::Document.new(:id => 'upsert-and-getreplica', :content => '{"k": "v"}'),
                  :replicate_to => 1)
    doc = bucket.get_from_replica('upsert-and-getreplica', 1)
    expect(doc.id).to eq('upsert-and-getreplica')
    expect(doc.content).to eq('{"k": "v"}')
  end

  specify 'upsert with unknown option' do
    expect do
      bucket.upsert(Couchbase::Document.new(:id => 'unknown-option', :content => '{"k": "v"}'),
                    :unknown_option => 'foo')
    end.to raise_error(ArgumentError)
  end

  specify 'close' do
    expect(bucket.close).to be_true
    expect do
      bucket.upsert(Couchbase::Document.new(:id => 'unknown-option', :content => '{"k": "v"}'))
    end.to raise_error(Java::ComCouchbaseClientCore::BucketClosedException)
  end
end
