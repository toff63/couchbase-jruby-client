describe Couchbase::Cluster, :cluster => true do
  describe '#open_bucket' do
    specify { expect(@cluster.open_bucket).to be_instance_of(Couchbase::Bucket) }
    specify { expect(@cluster.open_bucket('default')).to be_instance_of(Couchbase::Bucket) }
    specify { expect(@cluster.open_bucket('default', '')).to be_instance_of(Couchbase::Bucket) }
  end
end
