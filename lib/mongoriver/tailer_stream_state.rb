module Mongoriver
  class TailerStreamState
    attr_reader :count

    def initialize(limit=nil)
      @break = false
      @limit = limit
      @count = 0
    end

    def break?
      return @break
    end

    def break
      @break = true
    end

    def increment()
      @count += 1
      if !@limit.nil? && @count >= @limit
        self.break
      end
    end
  end
end
