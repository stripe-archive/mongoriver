require 'mongoriver'
require 'mongo'
require 'minitest/autorun'
require 'mocha/setup'

describe 'Mongoriver::Toku' do
  def create_op(ops)
    ts = Time.now
    ops = ops.map { |op| op['ns'] ||= 'foo.bar'; op }
    {
      '_id' => BSON::Binary.new,
      'ts' => ts,
      'h' => 1234,
      'a' => true,
      'ops' => ops
    }
  end

  def convert(*ops)
    record = create_op(ops)
    result = Mongoriver::Toku.convert(record)
    assert_equal(result.length, 1)
    result.first
  end

  describe 'conversions sent to stream' do
    before do
      server_info = stub(:documents => [ {'tokumxVersion' => '2'} ])
      conn = stub(:use => nil, :command => server_info)

      @tailer = Mongoriver::Tailer.new([conn], :existing)
      @outlet = Mongoriver::AbstractOutlet.new
      @stream = Mongoriver::Stream.new(@tailer, @outlet)

      @outlet.expects(:update_optime).at_least_once
    end

    it 'triggers insert' do
      # collection.insert({a: 5})
      @outlet.expects(:insert).once.with('foo', 'bar', {'_id' => 'baz', 'a' => 5})
      @stream.send(:handle_op, convert({'op'=>'i', 'o'=>{'_id'=>'baz', 'a' => 5}}))
    end

    it 'triggers update (ur)' do
      # collection.update({a:true}, {'$set' => {c: 7}}, multi: true)
      @outlet.expects(:update).once.with('foo', 'bar', {'_id' => 'baz'}, {'$set' => {'c' => 7}})

      @stream.send(:handle_op, convert({
        'op' => 'ur',
        'pk' => {'' => 'baz'},
        'o' => {'_id' => 'baz', 'a' => true},
        'm' => {'$set' => {'c' => 7}}
      }))
    end

    it 'triggers update (u)' do
      # collection.update({a:true}, {'b' => 2})
      @outlet.expects(:update).once.with('foo', 'bar', {'_id' => 'baz'}, {'_id' => 'baz', 'b' => 2})

      @stream.send(:handle_op, convert({
        'op' => 'u',
        'pk' => {'' => 'baz'},
        'o' => {'_id' => 'baz', 'a' => true},
        'o2' => {'_id' => 'baz', 'b' => 2}
      }))
    end

    it 'triggers remove' do
      # collection.delete({a:5})
      @outlet.expects(:remove).once.with('foo', 'bar', {'_id' => 'baz'})

      @stream.send(:handle_op, convert({
        'op' => 'd',
        'o' => {'_id' => 'baz', 'a' => 5}
      }))
    end

    it 'triggers create_index' do
      # collection.create_index([["baz", 1]])
      @outlet.expects(:create_index).once.with('foo', 'bar', [['baz', 1]], {:name => 'baz_1'})

      @stream.send(:handle_op, convert({
        'op' => 'i',
        'ns' => 'foo.system.indexes',
        'o' => {'key' => {"baz" => 1}, 'ns' => 'foo.bar', 'name' => 'baz_1'}
      }))
    end

    it 'triggers commands (create_collection)' do
      # db.create_collection('bar', :capped => true, :size => 10)
      @outlet.expects(:create_collection).once.with('foo', 'bar', {:capped => true, :size => 10})

      @stream.send(:handle_op, convert({
        'op' => 'c',
        'ns' => 'foo.$cmd',
        'o' => {'create' => 'bar', 'capped' => true, 'size' => 10}
      }))
    end
  end

  describe 'large transactions are joined by convert' do
    it 'should yield the same result as separate ops' do
      operations = [
        {'op'=>'i', 'o'=>{'_id'=>'baz', 'a' => 5}},
        {'op'=>'i', 'o'=>{'_id'=>'zoo', 'b' => 6}}
      ]

      refs = [
        {'_id' => {'_oid' => 'refref'}, 'ops' => [operations[0]]},
        {'_id' => {'_oid' => 'refref'}, 'ops' => [operations[1]]}
      ]

      collection = stub()
      conn = stub(:db => stub(:collection => collection))
      collection.expects(:find).returns(refs)

      expected = Mongoriver::Toku.convert(create_op(operations))
      got = Mongoriver::Toku.convert({
        '_id' => BSON::Binary.new,
        'ts' => Time.now,
        'h' => 1234,
        'a' => true,
        'ref' => 'refref'
      }, conn)

      assert_equal(expected, got)
    end
  end
end
