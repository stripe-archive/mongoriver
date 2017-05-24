require 'minitest/autorun'
require 'mocha/setup'
require 'mongo'
require 'mongoriver'
require_relative './cursor_stub'
  
describe 'Mongoriver::Tailer' do
  before do
    cursor = CursorStub.new
    collection = stub do
      expects(:find).yields(cursor)
    end
    db = stub do
      expects(:collection).with('oplog.rs').returns(collection)
    end
    conn = stub(server_info: {}) do
      expects(:db).with('local').returns(db)
    end
    @cursor = cursor
    @tailer = Mongoriver::Tailer.new([conn], :existing)
    @tailer.tail
  end

  it 'tailer stream with limit' do
    @cursor.generate_ops(10)
    count = 0
    has_more = @tailer.stream(5) do |_|
      count += 1
    end
    assert(has_more)
    assert_equal(5, count)
  end

  it 'tailer stream without limit' do
    @cursor.generate_ops(10)
    count = 0
    has_more = @tailer.stream do |_|
      count += 1
    end
    assert(!has_more)
    assert_equal(10, count)
  end

  it 'tailer stream allow to break out' do
    @cursor.generate_ops(10)
    count = 0
    has_more = @tailer.stream do |_, state|
      count += 1
      state.break if count == 5
      assert_equal(count, state.count)
    end
    assert(has_more)
    assert_equal(5, count)
  end

  it 'tailer stream allow to break out before limit' do
    @cursor.generate_ops(10)
    count = 0
    has_more = @tailer.stream(7) do |_, state|
      count += 1
      state.break if count == 5
      assert_equal(count, state.count)
    end
    assert(has_more)
    assert_equal(5, count)
  end
end
