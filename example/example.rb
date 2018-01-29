require_relative 'ext/jruby-client-all.jar'

cluster = Couchbase::Cluster.new
cluster.authenticate('Administrator', 'password')
cluster.disconnect
