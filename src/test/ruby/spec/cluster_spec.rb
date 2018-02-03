require_relative '../spec_helper'

describe 'Couchbase::Cluster', cluster: true do
# it 'should allow you to authenticate' do
#    @cluster.authenticate('Administrator', 'password')
#  end
  
  it 'should allow to connect to default bucket' do
    bucket = @cluster.bucket
    bucket.close
  end
  
  it 'should allow to connect to specific bucket' do
    bucket = @cluster.bucket name: 'beer-sample'
    bucket.close
  end
  
  it 'should allow to connect to default bucket with specified timeout' do
    bucket = @cluster.bucket timeout: 5
    bucket.close
  end
  
  it 'should allow to connect to default bucket with specified timeout' do
    bucket = @cluster.bucket timeout: 5, name: 'default'
    bucket.close
  end
end