describe Couchbase::Bucket, :cluster => true do
  before(:all) do
    bucket = @cluster.open_bucket("default")
    bucket.bucket_manager.flush
    # function (doc, meta) { if (doc.type == "user") emit(doc.name, null) }
    # function (doc, meta) { if (doc.type == "user") emit(doc.age, null) }
    # _count
    1000.times do |id|
      doc = Couchbase::Document.new(
          :id => "user-#{id}",
          :content => %{{"type": "user", "name": "Mr. Foo Bar #{id}", "age": #{id % 100}, "active": #{(id % 2) == 0}}})
      bucket.insert(doc)
    end
  end

  subject(:bucket) { @cluster.open_bucket("default") }

  specify 'non reduced view' do
    result = bucket.query('users', 'by_name', :stale => false)
    expect(result).to be_a(Couchbase::ViewResult)
    expect(result).to be_success
    expect(result.debug).to be_nil
    expect(result.errors).to be_nil
    expect(result.info).to eq('{"total_rows":1000}')
    expect(result.rows).to have(1000).items
    expect(result.rows).to include('{"id":"user-0","key":"Mr. Foo Bar 0","value":null}')
  end

  specify 'reduced view' do
    result = bucket.query('users', 'by_age', :stale => false)
    expect(result).to be_a(Couchbase::ViewResult)
    expect(result).to be_success
    expect(result.debug).to be_nil
    expect(result.errors).to be_nil
    expect(result.info).to eq('')
    expect(result.rows).to have(1).item
    expect(result.rows.first).to eq('{"key":null,"value":1000}')
  end

  specify 'reduced view with manually disabled reduce' do
    result = bucket.query('users', 'by_age', :stale => false, :reduce => false)
    expect(result).to be_a(Couchbase::ViewResult)
    expect(result).to be_success
    expect(result.debug).to be_nil
    expect(result.errors).to be_nil
    expect(result.info).to eq('{"total_rows":1000}')
    expect(result.rows).to have(1000).items
    expect(result.rows).to include('{"id":"user-0","key":0,"value":null}')
  end

  specify 'result when no rows matching the query' do
    result = bucket.query('users', 'by_name', :stale => false, :key => 'hello')
    expect(result).to be_a(Couchbase::ViewResult)
    expect(result).to be_success
    expect(result.debug).to be_nil
    expect(result.errors).to be_nil
    expect(result.info).to eq('{"total_rows":1000}')
    expect(result.rows).to have(0).items
  end

  specify 'result when has rows matching the query' do
    result = bucket.query('users', 'by_name', :stale => false, :key => 'Mr. Foo Bar 0')
    expect(result).to be_a(Couchbase::ViewResult)
    expect(result).to be_success
    expect(result.debug).to be_nil
    expect(result.errors).to be_nil
    expect(result.info).to eq('{"total_rows":1000}')
    expect(result.rows).to have(1).items
    expect(result.rows).to include('{"id":"user-0","key":"Mr. Foo Bar 0","value":null}')
  end
end
