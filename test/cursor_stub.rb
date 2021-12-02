require 'mongo'

class CursorStub
  include Enumerable

  def initialize
    @events = []
    @index = 0
  end

  def add_option(opt) end

  def generate_ops(max)
    @index = 0
    (1..max).map do |id|
      @events << {
        'ts' => BSON::Timestamp.new(Time.now.to_i, 0),
        'ns' => 'foo.bar',
        'op' => 'i',
        'o'  => {
          '_id' => id.to_s
        }
      }
    end
  end

  def has_next?
    @index < @events.length
  end

  def next
    @index += 1
    @events[@index - 1]
  end

  def each(&block)
    @events.each do |event|
      block.call(event)
    end
  end
end
