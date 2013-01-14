module Mongoriver
  module Logging
    def log
      @@logger ||= Log4r::Logger.new("Stripe::Mongoriver")
    end
  end
end
