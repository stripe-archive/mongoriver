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
      @streaming = false

      connect_upstream
      @database_type = Mongoriver::Toku.conversion_needed?(@upstream_conn) ? :toku : :mongo
    end

    # Return a position for a record object
    #
    # @return [BSON::Timestamp] if mongo
    # @return [BSON::Binary] if tokumx
    def position(record)
      return nil unless record
      case database_type
      when :mongo
        return record['ts']
      when :toku
        return record['_id']
      end
    end

    # Return a time for a record object
    # @return Time
    def time_for(record)
      return nil unless record
      case database_type
      when :mongo
        return Time.at(record['ts'].seconds)
      when :toku
        return record['ts']
      end
    end

    # Find the most recent entry in oplog and return a position for that
    # position. The position can be passed to the tail function (or run_forever)
    # and the tailer will start tailing after that.
    # If before_time is given, it will return the latest position before (or at) time.
    def most_recent_position(before_time=nil)
      position(latest_oplog_entry(before_time))
    end

    def latest_oplog_entry(before_time=nil)
      query = {}
      if before_time
        case database_type
        when :mongo
          ts = BSON::Timestamp.new(before_time.to_i + 1, 0)
        when :toku
          ts = before_time + 1
        end
        query = { 'ts' => { '$lt' => ts } }
      end

      case database_type
      when :mongo
        record = oplog_collection.find(query).sort( {'$natural' => -1 } ).limit(1).first
      when :toku
        record = oplog_collection.find(query).sort( {'_id' => -1} ).limit(1).first
      end
      record
    end

    def connect_upstream
      case @type
      when :replset
        opts = @conn_opts.merge(:read => :secondary)
        @upstream_conn = Mongo::ReplSetConnection.new(@upstreams, opts)
      when :slave, :direct
        opts = @conn_opts.merge(:slave_ok => true)
        @upstream_conn = Mongo::Client.new(@upstreams[0], opts)
        raise "Server at #{@upstream_conn.host}:#{@upstream_conn.port} is the primary -- if you're ok with that, check why your wrapper is passing :direct rather than :slave" if @type == :slave && @upstream_conn.primary?
        ensure_upstream_replset!
      when :existing
        raise "Must pass in a single existing Mongo::Connection with :existing" unless @upstreams.length == 1 && @upstreams[0].respond_to?(:use)
        @upstream_conn = @upstreams[0]
      else
        raise "Invalid connection type: #{@type.inspect}"
      end
    end

    def connection_config
      @upstream_conn.use('admin').command(:ismaster => 1).documents.first
    end

    def ensure_upstream_replset!
      # Might be a better way to do this, but not seeing one.
      config = connection_config
      unless config['setName']
        raise "Server at #{@upstream_conn.host}:#{@upstream_conn.port} is not running as a replica set"
      end
    end

    def oplog_collection
      @upstream_conn.use('local')[oplog]
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

      cursor_type = opts[:dont_wait] ? :tailable : :tailable_await
      oplog_replay = query['ts'] ? true : false
      mongo_opts = {:timeout => false, :cursor_type => cursor_type, :oplog_replay => oplog_replay}.merge(opts[:mongo_opts] || {})
      @cursor = oplog_collection.find(query, mongo_opts).to_enum.lazy
    end

    # Deprecated: use #tail(:from => ts, ...) instead
    def tail_from(ts, opts={})
      opts.merge(:from => ts)
      tail(opts)
    end

    def tailing
      !@stop || @streaming
    end

    def stream(limit=nil, &blk)
      @streaming = true
      state = TailerStreamState.new(limit)
      while !@stop && !state.break? && cursor_has_more?
        state.increment

        record = @cursor.next

        case database_type
        when :mongo
          blk.call(record, state)
        when :toku
          converted = Toku.convert(record, @upstream_conn)
          converted.each do |converted_record|
            blk.call(converted_record, state)
          end
        end
      end
      @streaming = false
      return cursor_has_more?
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
          "For mongo databases, tail :from must be a BSON::Timestamp")
        query['ts'] = { '$gt' => opts[:from] }
      when :toku
        assert(opts[:from].is_a?(BSON::Binary),
          "For tokumx databases, tail :from must be a BSON::Binary")
        query['_id'] = { '$gt' => opts[:from] }
      end

      query
    end

    def cursor_has_more?
      begin
        @cursor.peek
        true
      rescue StopIteration
        false
      end
    end
  end
end
