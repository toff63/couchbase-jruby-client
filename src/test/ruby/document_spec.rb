describe Couchbase::Document do
  context 'empty document' do
    subject(:document) { described_class.new }
    specify { expect(document.id).to be_nil }
    specify { expect(document.content).to be_nil }
    specify { expect(document.cas).to be_zero }
    specify { expect(document.expiry).to be_zero }
  end

  context 'full document' do
    subject(:document) { described_class.new('id', 'content', 4242, 1) }
    specify { expect(document.id).to eq('id') }
    specify { expect(document.content).to eq('content') }
    specify { expect(document.cas).to eq(4242) }
    specify { expect(document.expiry).to eq(1) }
  end

  context 'initialized by a hash' do
    subject(:document) { described_class.new(:id => 'id', :content => 'content', :cas => 4242, :expiry => 1) }
    specify { expect(document.id).to eq('id') }
    specify { expect(document.content).to eq('content') }
    specify { expect(document.cas).to eq(4242) }
    specify { expect(document.expiry).to eq(1) }
  end
end
