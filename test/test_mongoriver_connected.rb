require 'mongoriver'
require 'mongo'
require 'minitest/autorun'
require 'mocha/setup'

# Connected tests: run these with eg MONGO_SERVER=localhost:27017

MONGO_SERVER = ENV['MONGO_SERVER'] || 'localhost:27017'

def connect
  begin
    host, port = MONGO_SERVER.split(':', 2)
    Mongo::Connection.new(host, port)
  rescue Mongo::ConnectionFailure
    nil
  end
end

describe 'connected tests' do
  before do
    @mongo = connect
    skip unless @mongo
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
      db = 'test'
      collection = 'test'
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
      @outlet.expects(:create_collection).once.with('test', 'test', {:capped => true, :size => 10}) { @stream.stop }
      @outlet.expects(:update_optime).at_least_once.with(anything) { @stream.stop }

      @mongo['test'].create_collection('test', :capped => true, :size => 10)
      @mongo.drop_database('test')

      @stream.run_forever(@tail_from)
    end
  end
end