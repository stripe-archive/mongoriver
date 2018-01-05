require 'mongoriver'
require 'mongo'
require 'minitest/autorun'
require 'mocha/setup'

describe 'Mongoriver::Stream' do
  def create_op(op)
    ts = Time.now.to_i
    {'ts'=>BSON::Timestamp.new(ts, 0), 'h'=>1234, 'v'=>1, 'ns'=>'foo.bar'}.merge(op)
  end

  before do
    conn = stub(:use => nil)
    buildinfo_command = stub(:documents => [{}])
    conn.expects(:command).with(:buildinfo => 1).returns(buildinfo_command)
    @tailer = Mongoriver::Tailer.new([conn], :existing)
    @outlet = Mongoriver::AbstractOutlet.new
    @stream = Mongoriver::Stream.new(@tailer, @outlet)

    @outlet.expects(:update_optime).at_least_once
  end

  it 'triggers insert' do
    @outlet.expects(:insert).once.with('foo', 'bar', {'_id' => 'baz'})
    @stream.send(:handle_op, create_op({'op'=>'i', 'o'=>{'_id'=>'baz'}}))
  end

  it 'triggers update' do
    @outlet.expects(:update).once.with('foo', 'bar', {'_id' => 'baz'}, {'a' => 'b'})
    @stream.send(:handle_op, create_op({'op'=>'u', 'o2'=>{'_id'=>'baz'}, 'o'=>{'a'=>'b'}}))
  end

  it 'triggers remove' do
    @outlet.expects(:remove).once.with('foo', 'bar', {'_id' => 'baz'})
    @stream.send(:handle_op, create_op({'op'=>'d', 'b'=>true, 'o'=>{'_id'=>'baz'}}))
  end

  it 'triggers create_collection' do
    @outlet.expects(:create_collection).once.with('foo', 'bar', {:capped => true, :size => 10})
    @stream.send(:handle_op, create_op({'op'=>'c', 'ns'=>'foo.$cmd', 'o'=>{'create'=>'bar', 'capped'=>true, 'size'=>10.0}}))
  end

  it 'triggers drop_collection' do
    @outlet.expects(:drop_collection).once.with('foo', 'bar')
    @stream.send(:handle_op, create_op({'op'=>'c', 'ns'=>'foo.$cmd', 'o'=>{'drop'=>'bar'}}))
  end

  it 'triggers rename_collection' do
    @outlet.expects(:rename_collection).once.with('foo', 'bar', 'bar_2')
    @stream.send(:handle_op, create_op({'op'=>'c', 'ns'=>'admin.$cmd', 'o'=>{'renameCollection'=>'foo.bar', 'to'=>'foo.bar_2'}}))
  end

  it 'triggers create_index' do
    @outlet.expects(:create_index).once.with('foo', 'bar', [['baz', 1]], {:name => 'baz_1'})
    @stream.send(:handle_op, create_op({'op'=>'i', 'ns'=>'foo.system.indexes', 'o'=>{'_id'=>'index_id', 'ns'=>'foo.bar', 'key'=>{'baz'=>1.0}, 'name'=>'baz_1'}}))
  end

  it 'triggers drop_index' do
    @outlet.expects(:drop_index).once.with('foo', 'bar', 'baz_1')
    @stream.send(:handle_op, create_op({'op'=>'c', 'ns'=>'foo.$cmd', 'o'=>{'deleteIndexes'=>'bar', 'index'=>'baz_1'}}))
  end

  it 'triggers drop_database' do
    @outlet.expects(:drop_database).once.with('foo')
    @stream.send(:handle_op, create_op({'op'=>'c', 'ns'=>'foo.$cmd', 'o'=>{'dropDatabase'=>1.0}}))
  end
end
