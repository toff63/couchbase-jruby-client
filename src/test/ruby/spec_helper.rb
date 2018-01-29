require 'com/couchbase/client/ruby/couchbase'

RSpec.configure do |config|
  config.run_all_when_everything_filtered = true
  config.order = 'random'
  config.before :all, :cluster => true do
    @cluster = Couchbase::Cluster.new
  end
  config.after :all, :cluster => true do
    @cluster.disconnect if @cluster
    @cluster = nil
  end
end