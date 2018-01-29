require_relative 'jars/jruby-client-all.jar'

cluster = Couchbase::Cluster.new
cluster.authenticate('Administrator', 'password')
cluster.disconnect
