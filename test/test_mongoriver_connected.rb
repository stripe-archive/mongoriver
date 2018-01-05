require 'mongoriver'
require 'mongo'
require 'minitest/autorun'
require 'mocha/setup'

# Connected tests: run these with eg MONGO_SERVER=localhost:27017

MONGO_SERVER = ENV['MONGO_SERVER'] || 'localhost:27017'

def connect
  begin
    Mongo::Client.new(MONGO_SERVER)
  rescue Mongo::Error
    nil
  end
end

def run_stream(stream, start)
  Timeout::timeout(30) do
    @stream.run_forever(start)
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

      @tail_from = @tailer.most_recent_position
    end

    it 'triggers the correct ops in the correct order' do
      db = '_test_mongoriver'
      collection = '_test_mongoriver'
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

      coll = @mongo.use(db)[collection]
      coll.insert(doc)
      coll.update({'_id' => 'foo'}, doc.merge('bar' => 'qux'))
      coll.remove({'_id' => 'foo'})

      name = coll.ensure_index(index_keys)
      coll.drop_index(name)

      @mongo.rename_collection(collection, collection+'_foo')
      @mongo.drop_collection(collection+'_foo')
      @mongo.drop_database(db)

      run_stream(@stream, @tail_from)
    end

    it 'passes options to create_collection' do
      @outlet.expects(:create_collection).once.with('_test_mongoriver', '_test_mongoriver', {:capped => true, :size => 10}) { @stream.stop }
      @outlet.expects(:update_optime).at_least_once.with(anything) { @stream.stop }

      @mongo['_test_mongoriver'].create_collection('_test_mongoriver', :capped => true, :size => 10)
      @mongo.drop_database('_test_mongoriver')

      run_stream(@stream, @tail_from)
    end

    it 'ignores everything before the operation passed in' do
      name = '_test_mongoriver'

      @mongo.use(name)[name].insert(:a => 5)

      @outlet.expects(:insert).never
      @outlet.expects(:drop_database).with(anything) { @stream.stop }

      start = @tailer.most_recent_position
      @mongo.drop_database(name)
      run_stream(@stream, start)
    end

    it 'allows passing in a timestamp for the stream following as well' do
      name = '_test_mongoriver2'

      @outlet.expects(:insert).with do |db_name, col_name, value|
        db_name != name || value['record'] == 'value'
      end

      @outlet.expects(:update).with do |db_name, col_name, selector, update|
        @stream.stop if update['record'] == 'newvalue'
        db_name != name || update['record'] == 'newvalue'
      end

      @mongo.use(name)[name].insert('record' => 'value')
      @mongo.use(name)[name].update({'record' => 'value'}, {'record' => 'newvalue'})
      run_stream(@stream, Time.now-3)
    end
  end
end
