require_relative 'ext/jruby-client-all.jar'
require 'com/couchbase/client/ruby/couchbase'


cluster = Couchbase::Cluster.new
cluster.authenticate('Administrator', 'password')
cluster.disconnect
