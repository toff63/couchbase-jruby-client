require_relative '../spec_helper'
require 'multi_json'

describe 'Couchbase::Bucket', cluster: true do
  it 'should be able to insert data' do
   bucket = @cluster.bucket
    doc = Couchbase::Document.new('id-1', MultiJson.dump( {'name' => 'john'}))
    p doc
    doc2 = bucket.insert(doc)
    p doc2
    bucket.close
  end
  
  it 'should be able to get data' do
   bucket = @cluster.bucket
    doc = Couchbase::Document.new('id-1', MultiJson.dump( {'name' => 'john'}))
    doc2 = bucket.insert(doc)
    doc3 = bucket.get('id-1')
    bucket.close
  end
end