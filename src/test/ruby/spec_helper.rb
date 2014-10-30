require 'java'
require 'multi_json'
require 'com/couchbase/client/jruby/couchbase'

RSpec.configure do |config|
  config.treat_symbols_as_metadata_keys_with_true_values = true
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
