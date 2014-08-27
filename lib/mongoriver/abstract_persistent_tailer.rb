module Mongoriver

  # A variant of Tailer that automatically loads and persists the
  # "last placeholder processes" state. See PersistentTailer for a
  # concrete subclass that uses the same mongod you are already
  # tailing.

  class AbstractPersistentTailer < Tailer
    def initialize(upstream, type, opts={})
      raise "You can't instantiate an AbstractPersistentTailer -- did you want PersistentTailer? " if self.class == AbstractPersistentTailer
      super(upstream, type)

      @last_saved       = nil
      @batch            = opts[:batch]
      @last_read        = nil
    end

    def tail(opts={})
      opts[:from] ||= most_recent_operation
      super(opts)
    end

    def current_time
      Time.at(connection_config['localTime'])
    end

    def stream(limit=nil)
      start_time = current_time
      found_entry = false

      # Sketchy logic - yield results from Tailer.stream
      # if nothing is found and nothing in cursor, save the current placeholder
      entries_left = super(limit) do |entry|
        yield entry

        found_entry = true
        @last_read = record_for(entry)
        maybe_save_state unless @batch
      end

      if !found_entry && !entries_left
        @last_read = {
          :time => start_time,
          :placeholder => nil
        }
        maybe_save_state unless @batch
      end

      return entries_left
    end

    def batch_done
      raise "You must specify :batch => true to use the batch-processing interface." unless @batch
      maybe_save_state
    end

    # Get the current state from storage. Implement this!
    # @returns state [Hash, nil]
    # @option state [BSON::Timestamp, BSON::Binary] :placeholder
    # @option state [Time] :timestamp
    def read_state
      raise "read_state unimplemented!"
    end

    # Read the most recent timestamp of a read from storage.
    # Return nil if nothing was found.
    def read_timestamp
      state = read_state || {}
      return state[:time]
    end

    # Read the most recent placeholder from storage.
    # Return nil if nothing was found.
    def read_placeholder
      state = read_state || {}
      return state[:placeholder]
    end

    # Persist current state. Implement this!
    # @param state [Hash]
    # @option state [BSON::Timestamp, BSON::Binary] :placeholder
    # @option state [Time] :timestamp
    def write_state(state)
      raise "write_state unimplemented!"
    end

    def save_state
      write_state(@last_read)
      @last_saved = @last_read
      log.info("Saved state: #{@last_read[:placeholder]} (#{last_saved[:time]})")
    end

    def maybe_save_state
      # Write placeholder once a minute
      return unless @last_read
      if @last_saved.nil? || @last_read[:time] - @last_saved[:time] > 60.0
        save_state
      end
    end
  end
end
