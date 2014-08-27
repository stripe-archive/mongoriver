module Mongoriver
  class Tailer
    include Mongoriver::Logging
    include Mongoriver::Assertions

    attr_reader :upstream_conn
    attr_reader :oplog
    attr_reader :database_type

    def initialize(upstreams, type, oplog = "oplog.rs")
      @upstreams = upstreams
      @type = type
      @oplog = oplog
      # This number seems high
      @conn_opts = {:op_timeout => 86400}

      @cursor = nil
      @stop = false

      connect_upstream
      @database_type = Mongoriver::Toku.conversion_needed?(@upstream_conn) ? :toku : :mongo
    end

    # Find the most recent entry in oplog and return a placeholder for that 
    # position. The placeholder can be passed to the tail function (or run_forever)
    # and the tailer will start tailing after that.
    #
    # @return [BSON::Timestamp] if mongo
    # @return [BSON::Binary] if tokumx
    def most_recent_operation
      record = latest_oplog_entry

      case database_type
      when :mongo
        return record['ts']
      when :toku
        return record['_id']
      end
    end

    # @return [Time] timestamp of the last oplog entry.
    def latest_timestamp
      record = latest_oplog_entry

      case database_type
      when :mongo
        return Time.at(record['ts'].seconds)
      when :toku
        return record['ts']
      end
    end

    def connect_upstream
      case @type
      when :replset
        opts = @conn_opts.merge(:read => :secondary)
        @upstream_conn = Mongo::ReplSetConnection.new(@upstreams, opts)
      when :slave, :direct
        opts = @conn_opts.merge(:slave_ok => true)
        host, port = parse_direct_upstream
        @upstream_conn = Mongo::Connection.new(host, port, opts)
        raise "Server at #{@upstream_conn.host}:#{@upstream_conn.port} is the primary -- if you're ok with that, check why your wrapper is passing :direct rather than :slave" if @type == :slave && @upstream_conn.primary?
        ensure_upstream_replset!
      when :existing
        raise "Must pass in a single existing Mongo::Connection with :existing" unless @upstreams.length == 1 && @upstreams[0].respond_to?(:db)
        @upstream_conn = @upstreams[0]
      else
        raise "Invalid connection type: #{@type.inspect}"
      end
    end

    def connection_config
      @upstream_conn['admin'].command(:ismaster => 1)
    end

    def ensure_upstream_replset!
      # Might be a better way to do this, but not seeing one.
      config = connection_config
      unless config['setName']
        raise "Server at #{@upstream_conn.host}:#{@upstream_conn.port} is not running as a replica set"
      end
    end

    def parse_direct_upstream
      raise "When connecting directly to a mongo instance, must provide a single upstream" unless @upstreams.length == 1
      upstream = @upstreams[0]
      parse_host_spec(upstream)
    end

    def parse_host_spec(host_spec)
      host, port = host_spec.split(':')
      host = '127.0.0.1' if host.to_s.length == 0
      port = '27017' if port.to_s.length == 0
      [host, port.to_i]
    end

    def oplog_collection
      @upstream_conn.db('local').collection(oplog)
    end

    # Start tailing the oplog.
    # 
    # @param [Hash]
    # @option opts [BSON::Timestamp, BSON::Binary] :from Placeholder indicating 
    #           where to start the query from. Binary value is used for tokumx.
    #           The timestamp is non-inclusive.
    # @option opts [Hash] :filter Extra filters for the query.
    # @option opts [Bool] :dont_wait(false) 
    def tail(opts = {})
      raise "Already tailing the oplog!" if @cursor

      query = build_tail_query(opts)

      mongo_opts = {:timeout => false}.merge(opts[:mongo_opts] || {})

      oplog_collection.find(query, mongo_opts) do |oplog|
        oplog.add_option(Mongo::Constants::OP_QUERY_TAILABLE)
        oplog.add_option(Mongo::Constants::OP_QUERY_OPLOG_REPLAY) if query['ts']
        oplog.add_option(Mongo::Constants::OP_QUERY_AWAIT_DATA) unless opts[:dont_wait]

        log.info("Starting oplog stream from #{opts['from'] || 'start'}")
        @cursor = oplog
      end
    end

    # Deprecated: use #tail(:from => ts, ...) instead
    def tail_from(ts, opts={})
      opts.merge(:from => ts)
      tail(opts)
    end

    def stream(limit=nil, &blk)
      count = 0
      while !@stop && @cursor.has_next?
        count += 1
        break if limit && count >= limit

        record = @cursor.next

        case database_type
        when :mongo
          blk.call(record)
        when :toku
          converted = Toku.convert(record, @upstream_conn)
          converted.each(&blk)
        end
      end

      return @cursor.has_next?
    end

    def stop
      @stop = true
    end

    def close
      @cursor.close if @cursor
      @cursor = nil
      @stop = false
    end

    private
    def build_tail_query(opts = {})
      query = opts[:filter] || {}
      return query unless opts[:from]

      case database_type
      when :mongo
        assert(opts[:from].is_a?(BSON::Timestamp),
          "For mongo databases, from must be a BSON::Timestamp")
        query['ts'] = { '$gt' => opts[:from] }
      when :toku
        assert(opts[:from].is_a?(BSON::Binary),
          "For tokumx databases, from must be a BSON::Binary")
        query['_id'] = { '$gt' => opts[:from] }
      end

      query
    end

    def latest_oplog_entry
      case database_type
      when :mongo
        record = oplog_collection.find_one({}, :sort => [['$natural', -1]])
      when :toku
        record = oplog_collection.find_one({}, :sort => [['_id', -1]])
      end
      record
    end
  end
end
