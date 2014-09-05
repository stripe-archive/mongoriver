module Mongoriver

  # A variant of Tailer that automatically loads and persists the
  # "last position processes" state. See PersistentTailer for a
  # concrete subclass that uses the same mongod you are already
  # tailing.

  class AbstractPersistentTailer < Tailer
    attr_reader :last_saved, :last_read

    # How often to save position to database
    DEFAULT_SAVE_FREQUENCY = 60.0

    def initialize(upstream, type, opts={})
      raise "You can't instantiate an AbstractPersistentTailer -- did you want PersistentTailer? " if self.class == AbstractPersistentTailer
      super(upstream, type)

      @last_saved       = {}
      @batch            = opts[:batch]
      @last_read        = read_state || {}
      @save_frequency   = opts[:save_frequency] || DEFAULT_SAVE_FREQUENCY
    end

    def tail(opts={})
      opts[:from] ||= read_position
      log.debug("Persistent tail options: #{opts}")
      super(opts)
    end

    def stream(limit=nil)
      start_time = connection_config['localTime']
      found_entry = false

      # Sketchy logic - yield results from Tailer.stream
      # if nothing is found and nothing in cursor, save the current position
      entries_left = super(limit) do |entry|
        yield entry

        found_entry = true
        @last_read = state_for(entry)
        maybe_save_state unless @batch
      end

      if !found_entry && !entries_left
        @last_read['time'] = start_time
        maybe_save_state unless @batch
      end

      return entries_left
    end

    # state to save to the database for this record
    def state_for(record)
      {
        'time' => Time.at(record['ts'].seconds),
        'position' => position(record)
      }
    end

    def batch_done
      raise "You must specify :batch => true to use the batch-processing interface." unless @batch
      maybe_save_state
    end

    # Get the current state from storage. Implement this!
    # @returns state [Hash, nil]
    # @option state [BSON::Timestamp, BSON::Binary] 'position'
    # @option state [Time] 'timestamp'
    def read_state
      raise "read_state unimplemented!"
    end

    # Read the most recent timestamp of a read from storage.
    # Return nil if nothing was found.
    def read_timestamp
      state = read_state || {}
      return state['time']
    end

    # Read the most recent position from storage.
    # Return nil if nothing was found.
    def read_position
      state = read_state || {}
      return state['position']
    end

    # Persist current state. Implement this!
    # @param state [Hash]
    # @option state [BSON::Timestamp, BSON::Binary] 'position'
    # @option state [Time] 'timestamp'
    def write_state(state)
      raise "write_state unimplemented!"
    end

    def save_state(state=nil)
      if state.nil?
        state = last_read
      end
      write_state(state)
      @last_saved = state
      log.info("Saved state: #{last_saved}")
    end

    def maybe_save_state
      return unless last_read['time']
      if last_saved['time'].nil? || last_read['time'] - last_saved['time'] > @save_frequency
        save_state
      end
    end
  end
end
