require_relative '../spec_helper'
require 'multi_json'

describe 'Couchbase::Bucket', cluster: true do
  it 'should be able to insert data' do
   bucket = @cluster.bucket
    doc = Couchbase::Document.new('id-1', {'name' => 'john'})
    p doc
    doc2 = bucket.insert(doc)
    p doc2
    bucket.close
  end
  
  it 'should be able to insert data with expiry' do
   bucket = @cluster.bucket
    doc = Couchbase::Document.new(id: 'id-1', content: {'name' => 'john'}, ttl: 2)
    p doc
    doc2 = bucket.insert(doc)
    sleep 4
    doc3 = bucket.get('id-1')
    p doc3
    bucket.close
  end
  
  it 'should be able to get data' do
   bucket = @cluster.bucket
    doc = Couchbase::Document.new('id-1', {'name' => 'john'})
    doc2 = bucket.insert(doc)
    doc3 = bucket.get('id-1')
    bucket.close
  end
  
  it 'should  support bulk get' do
   bucket = @cluster.bucket
    [Couchbase::Document.new('id-1', {'name' => 'john'}), Couchbase::Document.new('id-2', {'name' => 'john2'})].each {|d| bucket.insert(d)}
    doc1, doc2 = bucket.mget(['id-1', 'id-2'])
    expect(doc1.content).to eq({'name' => 'john'})
    expect(doc2.content).to eq({'name' => 'john2'})
    bucket.close
  end
  
  it 'should let you update data' do
   bucket = @cluster.bucket
    doc = Couchbase::Document.new('id-1', {'name' => 'john'})
    doc2 = bucket.insert(doc)
    doc3 = bucket.insert(doc2)
    doc4 = bucket.get('id-1')
    expect(doc3.cas).to eq(doc4.cas)
    bucket.close
  end
  
  it 'should let you create and increment counters' do
   bucket = @cluster.bucket
    doc = Couchbase::Document.new('id-counter', 1)
    bucket.insert(doc)
    doc1 = bucket.incr('id-counter', 1)
    expect(doc1.content).to eq(2)
    bucket.close
  end
end