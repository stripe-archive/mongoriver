module Mongoriver
  module Assertions
    class AssertionFailure < StandardError; end

    def assert(condition, msg)
      raise AssertionFailure.new(msg) unless condition
    end
  end
end