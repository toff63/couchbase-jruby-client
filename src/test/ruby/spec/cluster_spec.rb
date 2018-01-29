require_relative '../spec_helper'

describe 'Couchbase::Cluster', cluster: true do
  it 'should allow you to authenticate' do
    @cluster.authenticate('Administrator', 'password')
  end
end