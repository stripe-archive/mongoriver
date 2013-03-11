require 'mongoriver'
require 'mongo'
require 'log4r'
require 'minitest/autorun'
require 'mocha/setup'

describe 'Mongoriver::Stream' do
  def create_op(op)
    ts = Time.now.to_i
    {'ts'=>BSON::Timestamp.new(ts, 0), 'h'=>1234, 'v'=>1, 'ns'=>'foo.bar'}.merge(op)
  end

  before do
    conn = stub(:db => nil)
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

# Connected tests: run these with eg MONGO_SERVER=localhost:27017

MONGO_SERVER = ENV['MONGO_SERVER']

def connect
  host, port = MONGO_SERVER.split(':', 2)
  Mongo::Connection.new(host, port)
end

describe 'connected tests' do
  before do
    skip unless MONGO_SERVER
    @mongo = connect
  end

  describe 'Mongoriver::Stream' do
    before do
      @tailer = Mongoriver::Tailer.new([MONGO_SERVER], :direct)
      @outlet = Mongoriver::AbstractOutlet.new

      @stream = Mongoriver::Stream.new(@tailer, @outlet)

      sleep(1)
      @tail_from = Time.now.to_i
    end

    it 'triggers the correct ops in the correct order' do
      db = '_test_mongoriver_db'
      collection = '_test_mongoriver_collection'
      doc = {'_id' => 'foo', 'bar' => 'baz'}
      updated_doc = doc.dup.merge('bar' => 'qux')
      index_keys = [['bar', 1]]

      @outlet.expects(:update_optime).at_least_once

      op_sequence = sequence('op_sequence')

      @outlet.expects(:insert).once.with(db, collection, doc).in_sequence(op_sequence)
      @outlet.expects(:update).once.with(db, collection, {'_id' => 'foo'}, updated_doc).in_sequence(op_sequence)
      @outlet.expects(:remove).once.with(db, collection, {'_id' => 'foo'}).in_sequence(op_sequence)

      @outlet.expects(:create_index).once.with(db, collection, index_keys, {:name => 'bar_1'}).in_sequence(op_sequence)
      @outlet.expects(:drop_index).once.with(db, collection, 'bar_1').in_sequence(op_sequence)

      @outlet.expects(:rename_collection).once.with(db, collection, collection+'_foo').in_sequence(op_sequence)
      @outlet.expects(:drop_collection).once.with(db, collection+'_foo').in_sequence(op_sequence)
      @outlet.expects(:drop_database).once.with(db) { @stream.stop }.in_sequence(op_sequence)

      coll = @mongo[db][collection]
      coll.insert(doc)
      coll.update({'_id' => 'foo'}, doc.merge('bar' => 'qux'))
      coll.remove({'_id' => 'foo'})

      name = coll.ensure_index(index_keys)
      coll.drop_index(name)

      @mongo[db].rename_collection(collection, collection+'_foo')
      @mongo[db].drop_collection(collection+'_foo')
      @mongo.drop_database(db)

      @stream.run_forever(@tail_from)
    end

    it 'passes options to create_collection' do
      @outlet.expects(:create_collection).once.with('_test_mongoriver_db', '_test_mongoriver_collection', {:capped => true, :size => 10}) { @stream.stop }
      @outlet.expects(:update_optime).at_least_once.with(anything) { @stream.stop }

      @mongo['_test_mongoriver_db'].create_collection('_test_mongoriver_collection', :capped => true, :size => 10)
      @mongo.drop_database('_test_mongoriver_db')

      @stream.run_forever(@tail_from)
    end
  end
end