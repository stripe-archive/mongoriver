module Mongoriver
  # A variant of AbstractPersistentTailer that automatically persists
  # the "last timestamp processes" state into the database we are
  # tailing.
  class PersistentTailer < AbstractPersistentTailer
    def initialize(upstream, type, service, opts={})
      if type == :slave
        raise "You can't use PersistentTailer against only a slave. How am I supposed to write state?"
      end
      super(upstream, type, opts)

      db         = opts[:db] || "_mongoriver"
      collection = opts[:collection] || 'oplog-tailers'
      @service = service
      @state_collection = @upstream_conn.db(db).collection(collection)
    end

    def read_state
      row = @state_collection.find_one(:service => @service)
      return nil unless row

      # Try to do seamless upgrades from old mongoriver versions
      case row['v']
      when nil
        log.warning("Old style timestamp found in database. Converting!")
        ts = Time.at(row['timestamp'].seconds)
        return {
          'placeholder' => most_recent_operation(ts),
          'time' => ts
        }
      when 1
        return row['state']
      end
    end

    def write_state(state)
      @state_collection.update({:service => @service},
        {:service => @service, :state => state, :v => 1}, :upsert => true)
    end
  end
end
