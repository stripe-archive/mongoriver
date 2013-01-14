module Mongoriver
  # A variant of AbstractPersistentTailer that automatically persists
  # the "last timestamp processes" state into the database we are
  # tailing.
  class PersistentTailer < AbstractPersistentTailer
    def initialize(upstream, type, service, opts={})
      raise "You can't use PersistentTailer against only a slave. How am I supposed to write state? " if type == :slave
      super(upstream, type, opts)

      db         = opts[:db] || "_mongoriver"
      collection = opts[:collection] || 'oplog-tailers'
      @service = service
      @state_collection = @upstream_conn.db(db).collection(collection)
    end

    def read_timestamp
      row = @state_collection.find_one(:service => @service)
      row ? row['timestamp'] : BSON::Timestamp.new(0, 0)
    end

    def write_timestamp(ts)
      row = @state_collection.find_one(:service => @service)
      if row
        @state_collection.update({'_id' => row['_id']}, '$set' => { 'timestamp' => ts })
      else
        @state_collection.insert('service' => @service, 'timestamp' => ts)
      end
    end
  end
end
